package gr.athenarc.imsi.visualfacts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Shared backing store for point data (x, y, file-offset).
 * All tiles reference slices of the same arrays, avoiding per-tile duplication.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>Phase 1 (CSV scan): points written sequentially via {@link #set(int, double, double, long)}</li>
 *   <li>Phase 1.5: prefix-sums computed from per-tile counts</li>
 *   <li>Phase 2 ({@link #partition}): per-array sequential scatter (stable, cache-friendly)</li>
 *   <li>Phase 3: each tile wired to its slice via {@link Tile#setSlice}</li>
 * </ol>
 */
public class SharedPointStore {

    private static final Logger LOG = LogManager.getLogger(SharedPointStore.class);

    /** Buffer size for spill I/O (8 MB — large enough for throughput, small enough for heap). */
    private static final int SPILL_BUF_SIZE = 8 * 1024 * 1024;

    private double[] xs;
    private double[] ys;
    private long[] offsets;
    private int capacity;

    // Chunked storage — non-null between construction (from parallel scanner)
    // and partition (which flattens chunks into contiguous arrays).
    private boolean chunked;
    private double[][] xsChunks;
    private double[][] ysChunks;
    private long[][] offsetsChunks;
    private int[] chunkStarts; // prefix sum: chunkStarts[k] = sum of chunkSizes[0..k-1]

    /**
     * Tile IDs for partition — set via constructor (from scan) or
     * {@link #takeTileIds} before {@link #partition},
     * cleared internally after partition completes.  Holding tileIds as a field
     * (rather than a method parameter) allows the spill path to null and GC the
     * 1 GB array before the final scatter pass, staying under G1's reserve limit.
     */
    private short[] tileIds;

    /** Whether the most recent {@link #partition} call used disk-spill. */
    private boolean partitionSpilled;

    public boolean didPartitionSpill() { return partitionSpilled; }

    public SharedPointStore(int capacity) {
        this.capacity = capacity;
        this.xs = new double[capacity];
        this.ys = new double[capacity];
        this.offsets = new long[capacity];
    }

    /**
     * Creates a store by adopting pre-built arrays (used by parallel scanner).
     * The caller MUST NOT retain references to the passed arrays.
     */
    public SharedPointStore(double[] xs, double[] ys, long[] offsets, int validCount) {
        this.xs = xs;
        this.ys = ys;
        this.offsets = offsets;
        this.capacity = validCount;
    }

    /**
     * Creates a store from chunked per-thread arrays (zero-copy adoption).
     * The {@link #partition} call will flatten chunks into contiguous arrays.
     * @param chunkSizes number of valid elements in each chunk
     */
    public SharedPointStore(double[][] xsChunks, double[][] ysChunks, long[][] offsetsChunks,
                            short[][] tileIdChunks, int[] chunkSizes, int totalSize) {
        this.chunked = true;
        this.xsChunks = xsChunks;
        this.ysChunks = ysChunks;
        this.offsetsChunks = offsetsChunks;
        int numChunks = chunkSizes.length;
        this.chunkStarts = new int[numChunks + 1];
        for (int i = 0; i < numChunks; i++) {
            chunkStarts[i + 1] = chunkStarts[i] + chunkSizes[i];
        }
        this.capacity = totalSize;

        // Flatten tileIdChunks into contiguous array (only 2 bytes/point)
        this.tileIds = new short[totalSize];
        int pos = 0;
        for (int c = 0; c < numChunks; c++) {
            System.arraycopy(tileIdChunks[c], 0, this.tileIds, pos, chunkSizes[c]);
            pos += chunkSizes[c];
        }
    }

    public int getCapacity() { return capacity; }

    public double getX(int i) {
        if (chunked) { int c = chunkFor(i); return xsChunks[c][i - chunkStarts[c]]; }
        return xs[i];
    }
    public double getY(int i) {
        if (chunked) { int c = chunkFor(i); return ysChunks[c][i - chunkStarts[c]]; }
        return ys[i];
    }
    public long getOffset(int i) {
        if (chunked) { int c = chunkFor(i); return offsetsChunks[c][i - chunkStarts[c]]; }
        return offsets[i];
    }

    /** Finds the chunk containing the given global index via binary search on prefix sums. */
    private int chunkFor(int globalIndex) {
        int pos = Arrays.binarySearch(chunkStarts, globalIndex);
        return pos >= 0 ? pos : -pos - 2;
    }

    /** Writes a point at position {@code i} (used during Phase 1 sequential scan). */
    public void set(int i, double x, double y, long offset) {
        xs[i] = x;
        ys[i] = y;
        offsets[i] = offset;
    }

    /**
     * Takes ownership of the tileIds array.  The caller MUST null its own
     * reference after this call so that the array can be GC'd from inside
     * {@link #partition} when the spill path needs the memory.
     */
    public void takeTileIds(short[] ids) {
        this.tileIds = ids;
    }

    /**
     * Stable per-array sequential scatter partition.
     * After completion, tile {@code t}'s points occupy
     * {@code [starts[t], starts[t] + counts[t])}.
     * <p>
     * Requires {@link #takeTileIds} to have been called first.
     * Automatically selects between a pure in-memory path (zero overhead) and a
     * disk-spill path when the temporary array allocation would exceed available
     * heap.  The spill path also frees tileIds before the final scatter to stay
     * within G1GC's usable heap.
     *
     * @param n         number of valid points (elements [0, n) are partitioned)
     * @param starts    prefix-sum array: starts[t] = first index for tile t
     * @param numTiles  number of distinct tiles
     */
    public void partition(int n, int[] starts, int numTiles) {
        if (this.tileIds == null) {
            throw new IllegalStateException("takeTileIds() must be called before partition()");
        }
        // Peak memory during in-memory partition:
        //   3 existing arrays (24n bytes) + 1 new array (8n) + tileIds (2n) = 34n bytes
        long peakInMemory = 4L * n * 8 + (long) n * 2;
        long maxHeap = Runtime.getRuntime().maxMemory();

        if (peakInMemory > (long) (maxHeap * 0.85)) {
            LOG.info("Partition peak ~{} GB exceeds 85% of max heap {} GB; using disk-spill path",
                    String.format("%.1f", peakInMemory / (1024.0 * 1024 * 1024)),
                    String.format("%.1f", maxHeap / (1024.0 * 1024 * 1024)));
            partitionWithSpill(n, starts, numTiles);
            partitionSpilled = true;
        } else {
            partitionInMemory(n, starts, numTiles);
            partitionSpilled = false;
        }
        this.tileIds = null; // no longer needed
    }

    /** Dispatches to chunked or contiguous in-memory partition. */
    private void partitionInMemory(int n, int[] starts, int numTiles) {
        final short[] tid = this.tileIds;
        if (chunked) {
            partitionChunkedInMemory(n, starts, numTiles, tid);
        } else {
            partitionContiguousInMemory(n, starts, numTiles, tid);
        }
        this.capacity = n;
    }

    /**
     * Parallel histogram scatter: partitions chunked source arrays into
     * contiguous target arrays using all available cores.
     * <p>
     * 1. Compute per-chunk tile counts (sequential scan of tileIds).<br>
     * 2. Prefix-sum to derive per-chunk per-tile write positions (disjoint).<br>
     * 3. Parallel scatter: each chunk writes to its own disjoint ranges
     *    — no synchronization, bit-identical output to a sequential scatter.
     */
    private void partitionChunkedInMemory(int n, int[] starts, int numTiles, short[] tid) {
        final int numChunks = xsChunks.length;

        // Step 1: per-chunk tile counts (sequential — tileIds fits in cache)
        int[][] chunkTileCounts = new int[numChunks][numTiles];
        for (int c = 0; c < numChunks; c++) {
            int cStart = chunkStarts[c];
            int cSize = chunkStarts[c + 1] - cStart;
            int[] counts = chunkTileCounts[c];
            for (int j = 0; j < cSize; j++) {
                counts[tid[cStart + j]]++;
            }
        }

        // Step 2: per-chunk per-tile starting cursors (prefix sum across chunks)
        int[][] chunkTileStarts = new int[numChunks][numTiles];
        for (int t = 0; t < numTiles; t++) {
            chunkTileStarts[0][t] = starts[t];
            for (int c = 1; c < numChunks; c++) {
                chunkTileStarts[c][t] = chunkTileStarts[c - 1][t] + chunkTileCounts[c - 1][t];
            }
        }

        // Steps 3a-c: scatter each array via a separate method call so that
        // the source-chunks parameter drops off the stack when the method
        // returns, making the old chunks truly unreachable for G1 to collect
        // before the next pass allocates its destination array.
        // Without this, lambda-captured locals (xsRef, ysRef) keep 8N bytes
        // each alive across passes, inflating peak from 34N to 50N.
        this.xs = scatterDoubleChunksParallel(this.xsChunks, n, numChunks,
                chunkTileStarts, this.chunkStarts, tid);
        this.xsChunks = null;

        this.ys = scatterDoubleChunksParallel(this.ysChunks, n, numChunks,
                chunkTileStarts, this.chunkStarts, tid);
        this.ysChunks = null;

        this.offsets = scatterLongChunksParallel(this.offsetsChunks, n, numChunks,
                chunkTileStarts, this.chunkStarts, tid);
        this.offsetsChunks = null;
        this.chunkStarts = null;
        this.chunked = false;
    }

    /** Parallel scatter of chunked double arrays into a flat partitioned array. */
    private static double[] scatterDoubleChunksParallel(double[][] srcChunks, int n,
            int numChunks, int[][] chunkTileStarts, int[] chunkStarts, short[] tid) {
        double[] dest = new double[n];
        IntStream.range(0, numChunks).parallel().forEach(c -> {
            int[] cursors = chunkTileStarts[c].clone();
            double[] cArr = srcChunks[c];
            int cStart = chunkStarts[c];
            int cSize = chunkStarts[c + 1] - cStart;
            for (int j = 0; j < cSize; j++) {
                dest[cursors[tid[cStart + j]]++] = cArr[j];
            }
        });
        return dest;
    }

    /** Parallel scatter of chunked long arrays into a flat partitioned array. */
    private static long[] scatterLongChunksParallel(long[][] srcChunks, int n,
            int numChunks, int[][] chunkTileStarts, int[] chunkStarts, short[] tid) {
        long[] dest = new long[n];
        IntStream.range(0, numChunks).parallel().forEach(c -> {
            int[] cursors = chunkTileStarts[c].clone();
            long[] cArr = srcChunks[c];
            int cStart = chunkStarts[c];
            int cSize = chunkStarts[c + 1] - cStart;
            for (int j = 0; j < cSize; j++) {
                dest[cursors[tid[cStart + j]]++] = cArr[j];
            }
        });
        return dest;
    }

    /** Original contiguous scatter partition (used by legacy non-parallel path). */
    private void partitionContiguousInMemory(int n, int[] starts, int numTiles, short[] tid) {
        int[] cursors;

        // Pass 1: scatter xs
        cursors = Arrays.copyOf(starts, numTiles);
        double[] newXs = new double[n];
        for (int i = 0; i < n; i++) {
            newXs[cursors[tid[i]]++] = xs[i];
        }
        this.xs = newXs;

        // Pass 2: scatter ys
        cursors = Arrays.copyOf(starts, numTiles);
        double[] newYs = new double[n];
        for (int i = 0; i < n; i++) {
            newYs[cursors[tid[i]]++] = ys[i];
        }
        this.ys = newYs;

        // Pass 3: scatter offsets
        cursors = Arrays.copyOf(starts, numTiles);
        long[] newOffsets = new long[n];
        for (int i = 0; i < n; i++) {
            newOffsets[cursors[tid[i]]++] = offsets[i];
        }
        this.offsets = newOffsets;
    }

    /**
     * Disk-spill partition: spills each array to its own temp file, with
     * explicit GC between each so that G1 reclaims 4 GB progressively.
     * Then scatters each array from disk into a freshly allocated partitioned array.
     * <p>
     * Key: tileIds is also spilled to disk and freed before the 3rd scatter,
     * so the peak during any single scatter never exceeds xs + ys + dest = 24N,
     * staying safely under G1's 10% reserve at 14 GB max heap.
     * <p>
     * Heap trace for N points (each array = 8N bytes, tileIds = 2N):
     * <pre>
     *   Before:  xs + ys + offsets + tileIds             = 26N
     *   Spill xs,  null, gc:  ys + offsets + tileIds     = 18N
     *   Spill ys,  null, gc:  offsets + tileIds          = 10N
     *   Spill off, null, gc:  tileIds                    = 2N   ← trough
     *   Scatter xs (tileIds in memory):  xs_new + tileIds = 10N
     *   Scatter ys (tileIds in memory):  xs + ys + tileIds= 18N
     *   Spill tileIds, null, gc:         xs + ys          = 16N
     *   Scatter offsets (tileIds from disk): xs+ys+off    = 24N ← peak, fits in 14 GB
     * </pre>
     */
    private void partitionWithSpill(int n, int[] starts, int numTiles) {
        Path tmpXs = null, tmpYs = null, tmpOff = null, tmpTid = null;
        try {
            tmpXs  = Files.createTempFile("valinor_xs_",  ".bin");
            tmpYs  = Files.createTempFile("valinor_ys_",  ".bin");
            tmpOff = Files.createTempFile("valinor_off_", ".bin");
            tmpTid = Files.createTempFile("valinor_tid_", ".bin");

            long t0 = System.nanoTime();

            // Phase A: Spill each data array to its own file, GC between each.
            if (chunked) {
                spillDoubleChunksToFile(this.xsChunks, this.chunkStarts, tmpXs);
                this.xsChunks = null;
            } else {
                spillDoubleArrayToFile(this.xs, n, tmpXs);
                this.xs = null;
            }
            forceGC("xs");

            if (chunked) {
                spillDoubleChunksToFile(this.ysChunks, this.chunkStarts, tmpYs);
                this.ysChunks = null;
            } else {
                spillDoubleArrayToFile(this.ys, n, tmpYs);
                this.ys = null;
            }
            forceGC("ys");

            if (chunked) {
                spillLongChunksToFile(this.offsetsChunks, this.chunkStarts, tmpOff);
                this.offsetsChunks = null;
                this.chunkStarts = null;
                this.chunked = false;
            } else {
                spillLongArrayToFile(this.offsets, n, tmpOff);
                this.offsets = null;
            }
            forceGC("offsets");

            LOG.debug("Spilled all data arrays to disk in {} s",
                    String.format("%.3f", (System.nanoTime() - t0) / 1e9));

            // Phase B: Scatter xs and ys using in-memory tileIds.
            t0 = System.nanoTime();
            this.xs = scatterDoublesFromFile(tmpXs, n, this.tileIds, starts, numTiles);
            this.ys = scatterDoublesFromFile(tmpYs, n, this.tileIds, starts, numTiles);
            LOG.debug("Scattered xs + ys from disk in {} s",
                    String.format("%.3f", (System.nanoTime() - t0) / 1e9));

            // Phase C: Spill tileIds to disk and free it (1 GB) before 3rd scatter.
            // This brings heap from xs(4GB) + ys(4GB) + tileIds(1GB) = 9 GB
            // down to 8 GB, leaving room for offsets_new(4GB) = 12 GB total
            // which fits within G1's usable heap (14 GB × 0.9 = 12.6 GB).
            spillShortArrayToFile(this.tileIds, n, tmpTid);
            this.tileIds = null;
            forceGC("tileIds");

            // Phase D: Scatter offsets reading tileIds from disk in tandem.
            t0 = System.nanoTime();
            this.offsets = scatterLongsWithDiskTileIds(tmpOff, tmpTid, n, starts, numTiles);
            LOG.debug("Scattered offsets (tileIds from disk) in {} s",
                    String.format("%.3f", (System.nanoTime() - t0) / 1e9));

            this.capacity = n;

        } catch (IOException e) {
            throw new RuntimeException("Partition disk-spill I/O failed", e);
        } finally {
            safeDelete(tmpXs);
            safeDelete(tmpYs);
            safeDelete(tmpOff);
            safeDelete(tmpTid);
        }
    }

    /** Calls System.gc() twice and logs heap state — two passes ensure humongous reclaim. */
    private static void forceGC(String label) {
        System.gc();
        System.gc(); // second pass catches objects promoted during first
        Runtime rt = Runtime.getRuntime();
        long used = (rt.totalMemory() - rt.freeMemory()) / (1024L * 1024);
        long max  = rt.maxMemory() / (1024L * 1024);
        LOG.debug("After freeing {}: heap used={} MB / max={} MB", label, used, max);
    }

    private static void safeDelete(Path p) {
        if (p != null) {
            try { Files.deleteIfExists(p); } catch (IOException ignored) { }
        }
    }

    // ---- Spill I/O helpers ----

    /** Writes a double[] to a file (sequential, platform byte-order). */
    private static void spillDoubleArrayToFile(double[] arr, int len, Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            writeDoublesToChannel(ch, arr, len);
        }
    }

    /** Writes a long[] to a file (sequential, platform byte-order). */
    private static void spillLongArrayToFile(long[] arr, int len, Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            writeLongsToChannel(ch, arr, len);
        }
    }

    /** Writes chunked double arrays sequentially to a single file. */
    private static void spillDoubleChunksToFile(double[][] chunks, int[] chunkStarts, Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            for (int c = 0; c < chunks.length; c++) {
                int cSize = chunkStarts[c + 1] - chunkStarts[c];
                writeDoublesToChannel(ch, chunks[c], cSize);
                chunks[c] = null; // free each chunk as we go
            }
        }
    }

    /** Writes chunked long arrays sequentially to a single file. */
    private static void spillLongChunksToFile(long[][] chunks, int[] chunkStarts, Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            for (int c = 0; c < chunks.length; c++) {
                int cSize = chunkStarts[c + 1] - chunkStarts[c];
                writeLongsToChannel(ch, chunks[c], cSize);
                chunks[c] = null;
            }
        }
    }

    /** Writes a short[] to a file (sequential, platform byte-order). */
    private static void spillShortArrayToFile(short[] arr, int len, Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(SPILL_BUF_SIZE);
            int pos = 0;
            while (pos < len) {
                buf.clear();
                int chunk = Math.min(SPILL_BUF_SIZE / 2, len - pos);
                for (int i = 0; i < chunk; i++) {
                    buf.putShort(arr[pos + i]);
                }
                buf.flip();
                while (buf.hasRemaining()) {
                    ch.write(buf);
                }
                pos += chunk;
            }
        }
    }

    /** Writes a double[] sequentially to an open channel. */
    private static void writeDoublesToChannel(FileChannel ch, double[] arr, int len) throws IOException {
        ByteBuffer buf = ByteBuffer.allocateDirect(SPILL_BUF_SIZE);
        int pos = 0;
        while (pos < len) {
            buf.clear();
            int chunk = Math.min(SPILL_BUF_SIZE / 8, len - pos);
            for (int i = 0; i < chunk; i++) {
                buf.putDouble(arr[pos + i]);
            }
            buf.flip();
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            pos += chunk;
        }
    }

    /** Writes a long[] sequentially to an open channel. */
    private static void writeLongsToChannel(FileChannel ch, long[] arr, int len) throws IOException {
        ByteBuffer buf = ByteBuffer.allocateDirect(SPILL_BUF_SIZE);
        int pos = 0;
        while (pos < len) {
            buf.clear();
            int chunk = Math.min(SPILL_BUF_SIZE / 8, len - pos);
            for (int i = 0; i < chunk; i++) {
                buf.putLong(arr[pos + i]);
            }
            buf.flip();
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            pos += chunk;
        }
    }

    /**
     * Reads N doubles from a file, scattering them into a partitioned array.
     */
    private static double[] scatterDoublesFromFile(Path file, int n,
            short[] tileIds, int[] starts, int numTiles) throws IOException {
        int[] cursors = Arrays.copyOf(starts, numTiles);
        double[] dest = new double[n];
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(SPILL_BUF_SIZE);
            int pos = 0;
            while (pos < n) {
                buf.clear();
                int needed = (int) Math.min(SPILL_BUF_SIZE, (long) (n - pos) * 8);
                buf.limit(needed);
                while (buf.hasRemaining()) {
                    if (ch.read(buf) < 0) throw new IOException("Spill file truncated at element " + pos);
                }
                buf.flip();
                int chunk = buf.remaining() / 8;
                for (int i = 0; i < chunk; i++) {
                    dest[cursors[tileIds[pos + i]]++] = buf.getDouble();
                }
                pos += chunk;
            }
        }
        return dest;
    }

    /**
     * Reads N longs from a file, scattering them into a partitioned array.
     */
    private static long[] scatterLongsFromFile(Path file, int n,
            short[] tileIds, int[] starts, int numTiles) throws IOException {
        int[] cursors = Arrays.copyOf(starts, numTiles);
        long[] dest = new long[n];
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(SPILL_BUF_SIZE);
            int pos = 0;
            while (pos < n) {
                buf.clear();
                int needed = (int) Math.min(SPILL_BUF_SIZE, (long) (n - pos) * 8);
                buf.limit(needed);
                while (buf.hasRemaining()) {
                    if (ch.read(buf) < 0) throw new IOException("Spill file truncated at element " + pos);
                }
                buf.flip();
                int chunk = buf.remaining() / 8;
                for (int i = 0; i < chunk; i++) {
                    dest[cursors[tileIds[pos + i]]++] = buf.getLong();
                }
                pos += chunk;
            }
        }
        return dest;
    }

    /**
     * Scatters N longs from {@code srcFile} into a partitioned array, reading
     * tile IDs from {@code tileIdsFile} in tandem (both files are read
     * sequentially, one chunk at a time).
     * <p>
     * Used when tileIds has been freed from heap to stay within G1's usable headroom.
     */
    private static long[] scatterLongsWithDiskTileIds(Path srcFile, Path tileIdsFile,
            int n, int[] starts, int numTiles) throws IOException {
        int[] cursors = Arrays.copyOf(starts, numTiles);
        long[] dest = new long[n];
        try (FileChannel srcCh = FileChannel.open(srcFile, StandardOpenOption.READ);
             FileChannel tidCh = FileChannel.open(tileIdsFile, StandardOpenOption.READ)) {
            ByteBuffer srcBuf = ByteBuffer.allocateDirect(SPILL_BUF_SIZE);
            // For K source elements (8 bytes each), we need K tileId shorts (2 bytes each)
            ByteBuffer tidBuf = ByteBuffer.allocateDirect(SPILL_BUF_SIZE / 4);
            int pos = 0;
            while (pos < n) {
                int chunk = Math.min(SPILL_BUF_SIZE / 8, n - pos);

                // Read source longs
                srcBuf.clear();
                srcBuf.limit(chunk * 8);
                while (srcBuf.hasRemaining()) {
                    if (srcCh.read(srcBuf) < 0)
                        throw new IOException("Source file truncated at element " + pos);
                }
                srcBuf.flip();

                // Read corresponding tile IDs
                tidBuf.clear();
                tidBuf.limit(chunk * 2);
                while (tidBuf.hasRemaining()) {
                    if (tidCh.read(tidBuf) < 0)
                        throw new IOException("TileIds file truncated at element " + pos);
                }
                tidBuf.flip();

                // Scatter
                for (int i = 0; i < chunk; i++) {
                    short tid = tidBuf.getShort();
                    dest[cursors[tid]++] = srcBuf.getLong();
                }
                pos += chunk;
            }
        }
        return dest;
    }

    /**
     * Sub-partitions a slice {@code [sliceStart, sliceStart+sliceLen)} in-place.
     * Used by {@link QuadTreeTile#split()} to partition a parent's slice
     * among quadrant children. Uses cycle chasing (only ~thousands of elements,
     * so cache effects are negligible).
     *
     * @param sliceStart start index of the slice in the shared arrays
     * @param sliceLen   number of elements in the slice
     * @param subIds     per-element sub-partition ID (0..numParts-1), length = sliceLen;
     *                   overwritten during partition
     * @param subStarts  absolute start indices for each sub-partition
     *                   (pre-computed as prefix sums relative to sliceStart)
     * @param numParts   number of sub-partitions (typically 4)
     */
    public void subPartition(int sliceStart, int sliceLen, int[] subIds, int[] subStarts, int numParts) {
        // Convert subIds to absolute target positions
        int[] cursors = Arrays.copyOf(subStarts, numParts);
        for (int i = 0; i < sliceLen; i++) {
            subIds[i] = cursors[subIds[i]]++;
        }
        // Apply permutation in-place via cycle chasing
        for (int i = 0; i < sliceLen; i++) {
            int absI = sliceStart + i;
            while (subIds[i] != absI) {
                int absJ = subIds[i];
                int j = absJ - sliceStart;
                swap(absI, absJ);
                int tmp = subIds[i]; subIds[i] = subIds[j]; subIds[j] = tmp;
            }
        }
    }

    /** Swaps elements at absolute positions a and b across all three arrays. */
    private void swap(int a, int b) {
        double tx = xs[a]; xs[a] = xs[b]; xs[b] = tx;
        double ty = ys[a]; ys[a] = ys[b]; ys[b] = ty;
        long to = offsets[a]; offsets[a] = offsets[b]; offsets[b] = to;
    }
}
