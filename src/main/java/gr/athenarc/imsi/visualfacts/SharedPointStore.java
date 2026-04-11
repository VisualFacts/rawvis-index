package gr.athenarc.imsi.visualfacts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gr.athenarc.imsi.visualfacts.config.IndexConfig;

/**
 * Shared backing store for point data (x, y, file-offset).
 * All tiles reference slices of the same arrays, avoiding per-tile duplication.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>Phase 1 (CSV scan): data arrives as T heap chunks (Path A) or T×4 temp
 *       files (Path B) or B×T bucket files (Path C)</li>
 *   <li>Phase 1.5: prefix-sums computed from per-tile counts</li>
 *   <li>Phase 2 ({@link #partition}): parallel histogram scatter into contiguous
 *       arrays (Paths A/B) or bucket-by-bucket scatter into mmap (Path C)</li>
 *   <li>Phase 3: each tile wired to its slice via {@link Tile#setSlice}</li>
 * </ol>
 */
public class SharedPointStore implements AutoCloseable {

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
     * cleared internally after partition completes.
     */
    private int[] tileIds;

    // ---- Disk-chunk mode: point data in per-thread scan files (Path B) ----
    private boolean diskChunkMode;
    private Path[] diskXsFiles;
    private Path[] diskYsFiles;
    private Path[] diskOffsetsFiles;
    private Path[] diskTileIdFiles;
    private int[] diskChunkSizes;

    // ---- Mmap mode: point data backed by a single interleaved memory-mapped file ----
    // Layout per point i: [x @ 3i, y @ 3i+1, offset @ 3i+2] (24 bytes)
    private boolean mmapMode;
    private MmapArray mmapData;
    private Path mmapDir;

    /** Interleave stride: 3 eight-byte elements per point (x, y, offset). */
    private static final int MMAP_STRIDE = 3;

    // ---- Bucket mmap mode: per-thread per-bucket files ----
    private boolean bucketMode;
    private Path bucketDir;
    private int numBuckets;
    private int tilesPerBucket;
    private int numScanThreads;

    /** Which partition path was used: "in-memory", "disk-scatter", or "bucket-mmap". */
    private String partitionPath;

    public String getPartitionPath() { return partitionPath; }

    /**
     * Creates a store from chunked per-thread arrays (zero-copy adoption).
     * The {@link #partition} call will flatten chunks into contiguous arrays.
     * @param chunkSizes number of valid elements in each chunk
     */
    public SharedPointStore(double[][] xsChunks, double[][] ysChunks, long[][] offsetsChunks,
                            int[][] tileIdChunks, int[] chunkSizes, int totalSize) {
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

        // Flatten tileIdChunks into contiguous array (or adopt directly if single chunk)
        if (numChunks == 1) {
            this.tileIds = tileIdChunks[0];
        } else {
            this.tileIds = new int[totalSize];
            int pos = 0;
            for (int c = 0; c < numChunks; c++) {
                System.arraycopy(tileIdChunks[c], 0, this.tileIds, pos, chunkSizes[c]);
                pos += chunkSizes[c];
            }
        }
    }

    /**
     * Creates a store for bucket-mmap mode.  Point data lives in per-thread
     * per-bucket files; tileIds are inline in those files (no heap array).
     * The {@link #partition} call will scatter bucket-by-bucket into mmap.
     *
     * @param capacity       total number of valid points
     * @param bucketDir      directory containing scan_t{t}_b{k}.bin files
     * @param numBuckets     number of buckets (power of 2)
     * @param tilesPerBucket ceil(numTiles / numBuckets)
     * @param numScanThreads number of scan threads (per-bucket file count)
     * @param mmapDir        directory for final mmap files
     */
    public static SharedPointStore createForBucketMmap(
            int capacity, Path bucketDir, int numBuckets, int tilesPerBucket,
            int numScanThreads, Path mmapDir) {
        SharedPointStore store = new SharedPointStore();
        store.capacity = capacity;
        store.mmapMode = true;
        store.bucketMode = true;
        store.bucketDir = bucketDir;
        store.numBuckets = numBuckets;
        store.tilesPerBucket = tilesPerBucket;
        store.numScanThreads = numScanThreads;
        store.mmapDir = mmapDir;
        return store;
    }

    /**
     * Creates a store for disk-chunk mode (Path B).  Point data resides in
     * per-thread temp files produced by the disk-streaming scanner.
     * {@link #partition} scatters directly from these files — no heap merge.
     */
    public static SharedPointStore createForDiskChunks(
            int capacity, Path[] diskXsFiles, Path[] diskYsFiles,
            Path[] diskOffsetsFiles, Path[] diskTileIdFiles,
            int[] diskChunkSizes) {
        SharedPointStore store = new SharedPointStore();
        store.capacity = capacity;
        store.diskChunkMode = true;
        store.diskXsFiles = diskXsFiles;
        store.diskYsFiles = diskYsFiles;
        store.diskOffsetsFiles = diskOffsetsFiles;
        store.diskTileIdFiles = diskTileIdFiles;
        store.diskChunkSizes = diskChunkSizes;
        return store;
    }

    /** Private no-arg constructor for factory methods. */
    private SharedPointStore() {}

    public int getCapacity() { return capacity; }

    public boolean isMmapMode() { return mmapMode; }

    public double getX(int i) {
        if (mmapMode) return mmapData.getDouble(MMAP_STRIDE * i);
        if (chunked) { int c = chunkFor(i); return xsChunks[c][i - chunkStarts[c]]; }
        return xs[i];
    }
    public double getY(int i) {
        if (mmapMode) return mmapData.getDouble(MMAP_STRIDE * i + 1);
        if (chunked) { int c = chunkFor(i); return ysChunks[c][i - chunkStarts[c]]; }
        return ys[i];
    }
    public long getOffset(int i) {
        if (mmapMode) return mmapData.getLong(MMAP_STRIDE * i + 2);
        if (chunked) { int c = chunkFor(i); return offsetsChunks[c][i - chunkStarts[c]]; }
        return offsets[i];
    }

    /** Finds the chunk containing the given global index via binary search on prefix sums. */
    private int chunkFor(int globalIndex) {
        int pos = Arrays.binarySearch(chunkStarts, globalIndex);
        return pos >= 0 ? pos : -pos - 2;
    }

    /**
     * Takes ownership of the tileIds array for Path A in-memory partition.
     * The caller should null its own reference after this call.
     */
    public void takeTileIds(int[] ids) {
        this.tileIds = ids;
    }

    /**
     * Parallel histogram scatter partition.
     * After completion, tile {@code t}'s points occupy
     * {@code [starts[t], starts[t] + counts[t])}.
     * <p>
     * Dispatches to the appropriate path:
     * <ul>
     *   <li>Path C (bucket-mmap): scatter bucket files into mmap</li>
     *   <li>Path B (disk-scatter): parallel scatter from per-thread scan files</li>
     *   <li>Path A (in-memory): parallel histogram scatter from heap chunks</li>
     * </ul>
     *
     * @param n         number of valid points (elements [0, n) are partitioned)
     * @param starts    prefix-sum array: starts[t] = first index for tile t
     * @param numTiles  number of distinct tiles
     */
    public void partition(int n, int[] starts, int numTiles) {
        if (mmapMode && bucketMode) {
            LOG.info("Partition using bucket-mmap path for {} points ({} buckets)", n, numBuckets);
            partitionBucketsToMmap(n, starts, numTiles);
            partitionPath = "bucket-mmap";
            return;
        }

        if (diskChunkMode) {
            LOG.info("Partition using disk-scatter path for {} points ({} chunks)", n, diskChunkSizes.length);
            partitionFromDiskChunks(n, starts, numTiles);
            partitionPath = "disk-scatter";
            return;
        }

        if (this.tileIds == null) {
            throw new IllegalStateException("takeTileIds() must be called before partition()");
        }
        LOG.info("Partition using in-memory path for {} points ({} chunks)", n, xsChunks.length);
        partitionChunkedInMemory(n, starts, numTiles, this.tileIds);
        this.tileIds = null;
        this.capacity = n;
        partitionPath = "in-memory";
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
    private void partitionChunkedInMemory(int n, int[] starts, int numTiles, int[] tid) {
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
            int numChunks, int[][] chunkTileStarts, int[] chunkStarts, int[] tid) {
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
            int numChunks, int[][] chunkTileStarts, int[] chunkStarts, int[] tid) {
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

    // ======================================================================
    //  Path B: parallel scatter from per-thread disk files
    // ======================================================================

    /**
     * Partitions from per-thread scan files (Path B).
     * <ol>
     *   <li>Load tileIds from T per-thread files into T int[] chunks (4N bytes).</li>
     *   <li>Per-chunk tile histograms + prefix-sum → disjoint write cursors.</li>
     *   <li>Scatter xs in parallel: each thread reads its file, writes to dest.</li>
     *   <li>Scatter ys the same way.</li>
     *   <li>Scatter offsets the same way, then free tileId chunks.</li>
     * </ol>
     * Peak heap: tileIds(4N) + 2 completed arrays(16N) + 1 being built(8N) = 28N.
     */
    private void partitionFromDiskChunks(int n, int[] starts, int numTiles) {
        final int numChunks = diskChunkSizes.length;

        try {
            // Step 1: load tileIds from per-thread files (parallel)
            long t0 = System.nanoTime();
            int[][] tileIdChunks = new int[numChunks][];
            final int[][] tidChunks = tileIdChunks;
            final Path[] tidFiles = diskTileIdFiles;
            final int[] chunkSizes = diskChunkSizes;
            IOException[] loadErr = {null};
            IntStream.range(0, numChunks).parallel().forEach(c -> {
                try {
                    tidChunks[c] = readIntsFromFile(tidFiles[c], chunkSizes[c]);
                } catch (IOException e) {
                    synchronized (loadErr) { if (loadErr[0] == null) loadErr[0] = e; }
                }
            });
            if (loadErr[0] != null) throw loadErr[0];
            LOG.debug("Loaded tileIds from {} files in {} s",
                    numChunks, String.format("%.3f", (System.nanoTime() - t0) / 1e9));

            // Step 2: per-chunk tile counts
            int[][] chunkTileCounts = new int[numChunks][numTiles];
            for (int c = 0; c < numChunks; c++) {
                int[] tid = tileIdChunks[c];
                int cSize = diskChunkSizes[c];
                int[] counts = chunkTileCounts[c];
                for (int j = 0; j < cSize; j++) {
                    counts[tid[j]]++;
                }
            }

            // Step 3: per-chunk per-tile starting cursors (prefix sum across chunks)
            int[][] chunkTileStarts = new int[numChunks][numTiles];
            for (int t = 0; t < numTiles; t++) {
                chunkTileStarts[0][t] = starts[t];
                for (int c = 1; c < numChunks; c++) {
                    chunkTileStarts[c][t] = chunkTileStarts[c - 1][t] + chunkTileCounts[c - 1][t];
                }
            }

            // Step 4: scatter xs from per-thread files in parallel
            t0 = System.nanoTime();
            this.xs = scatterDoublesFromDiskParallel(diskXsFiles, diskChunkSizes, n,
                    numChunks, chunkTileStarts, tileIdChunks);
            LOG.debug("Scattered xs from disk in {} s",
                    String.format("%.3f", (System.nanoTime() - t0) / 1e9));

            // Step 5: scatter ys
            t0 = System.nanoTime();
            this.ys = scatterDoublesFromDiskParallel(diskYsFiles, diskChunkSizes, n,
                    numChunks, chunkTileStarts, tileIdChunks);
            LOG.debug("Scattered ys from disk in {} s",
                    String.format("%.3f", (System.nanoTime() - t0) / 1e9));

            // Step 6: scatter offsets, then free tileIds
            t0 = System.nanoTime();
            this.offsets = scatterLongsFromDiskParallel(diskOffsetsFiles, diskChunkSizes, n,
                    numChunks, chunkTileStarts, tileIdChunks);
            //noinspection UnusedAssignment
            tileIdChunks = null; // free 4N bytes
            LOG.debug("Scattered offsets from disk in {} s",
                    String.format("%.3f", (System.nanoTime() - t0) / 1e9));

            this.capacity = n;

        } catch (IOException e) {
            throw new RuntimeException("Disk-chunk partition I/O failed", e);
        } finally {
            // Clean up temp files
            for (Path p : diskXsFiles) safeDelete(p);
            for (Path p : diskYsFiles) safeDelete(p);
            for (Path p : diskOffsetsFiles) safeDelete(p);
            for (Path p : diskTileIdFiles) safeDelete(p);
            diskXsFiles = null;
            diskYsFiles = null;
            diskOffsetsFiles = null;
            diskTileIdFiles = null;
            diskChunkMode = false;
        }
    }

    /** Reads {@code count} ints from a binary file (native byte order). */
    private static int[] readIntsFromFile(Path file, int count) throws IOException {
        int[] arr = new int[count];
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(SPILL_BUF_SIZE).order(ByteOrder.nativeOrder());
            int pos = 0;
            while (pos < count) {
                buf.clear();
                int needed = (int) Math.min(SPILL_BUF_SIZE, (long) (count - pos) * Integer.BYTES);
                buf.limit(needed);
                while (buf.hasRemaining()) {
                    if (ch.read(buf) < 0) throw new IOException("TileId file truncated at element " + pos);
                }
                buf.flip();
                int chunk = buf.remaining() / Integer.BYTES;
                for (int i = 0; i < chunk; i++) {
                    arr[pos + i] = buf.getInt();
                }
                pos += chunk;
            }
        }
        return arr;
    }

    /**
     * Parallel scatter of doubles from T per-thread files.
     * Each thread reads its file sequentially and writes to disjoint dest regions.
     */
    private static double[] scatterDoublesFromDiskParallel(
            Path[] files, int[] chunkSizes, int n, int numChunks,
            int[][] chunkTileStarts, int[][] tileIdChunks) throws IOException {
        double[] dest = new double[n];
        IOException[] error = {null};
        IntStream.range(0, numChunks).parallel().forEach(c -> {
            int cSize = chunkSizes[c];
            if (cSize == 0) return;
            try (FileChannel ch = FileChannel.open(files[c], StandardOpenOption.READ)) {
                int[] cursors = chunkTileStarts[c].clone();
                int[] tid = tileIdChunks[c];
                ByteBuffer buf = ByteBuffer.allocateDirect(SPILL_BUF_SIZE).order(ByteOrder.nativeOrder());
                int pos = 0;
                while (pos < cSize) {
                    buf.clear();
                    int needed = (int) Math.min(SPILL_BUF_SIZE, (long) (cSize - pos) * 8);
                    buf.limit(needed);
                    while (buf.hasRemaining()) {
                        if (ch.read(buf) < 0) throw new IOException("File truncated at element " + pos);
                    }
                    buf.flip();
                    int chunk = buf.remaining() / 8;
                    for (int j = 0; j < chunk; j++) {
                        dest[cursors[tid[pos + j]]++] = buf.getDouble();
                    }
                    pos += chunk;
                }
            } catch (IOException e) {
                synchronized (error) { if (error[0] == null) error[0] = e; }
            }
        });
        if (error[0] != null) throw error[0];
        return dest;
    }

    /**
     * Parallel scatter of longs from T per-thread files.
     * Each thread reads its file sequentially and writes to disjoint dest regions.
     */
    private static long[] scatterLongsFromDiskParallel(
            Path[] files, int[] chunkSizes, int n, int numChunks,
            int[][] chunkTileStarts, int[][] tileIdChunks) throws IOException {
        long[] dest = new long[n];
        IOException[] error = {null};
        IntStream.range(0, numChunks).parallel().forEach(c -> {
            int cSize = chunkSizes[c];
            if (cSize == 0) return;
            try (FileChannel ch = FileChannel.open(files[c], StandardOpenOption.READ)) {
                int[] cursors = chunkTileStarts[c].clone();
                int[] tid = tileIdChunks[c];
                ByteBuffer buf = ByteBuffer.allocateDirect(SPILL_BUF_SIZE).order(ByteOrder.nativeOrder());
                int pos = 0;
                while (pos < cSize) {
                    buf.clear();
                    int needed = (int) Math.min(SPILL_BUF_SIZE, (long) (cSize - pos) * 8);
                    buf.limit(needed);
                    while (buf.hasRemaining()) {
                        if (ch.read(buf) < 0) throw new IOException("File truncated at element " + pos);
                    }
                    buf.flip();
                    int chunk = buf.remaining() / 8;
                    for (int j = 0; j < chunk; j++) {
                        dest[cursors[tid[pos + j]]++] = buf.getLong();
                    }
                    pos += chunk;
                }
            } catch (IOException e) {
                synchronized (error) { if (error[0] == null) error[0] = e; }
            }
        });
        if (error[0] != null) throw error[0];
        return dest;
    }

    private static void safeDelete(Path p) {
        if (p != null) {
            try { Files.deleteIfExists(p); } catch (IOException ignored) { }
        }
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
        if (mmapMode) {
            int ai = MMAP_STRIDE * a, bi = MMAP_STRIDE * b;
            double tx = mmapData.getDouble(ai);     mmapData.putDouble(ai,     mmapData.getDouble(bi));     mmapData.putDouble(bi,     tx);
            double ty = mmapData.getDouble(ai + 1); mmapData.putDouble(ai + 1, mmapData.getDouble(bi + 1)); mmapData.putDouble(bi + 1, ty);
            long  to  = mmapData.getLong(ai + 2);   mmapData.putLong(ai + 2,   mmapData.getLong(bi + 2));   mmapData.putLong(bi + 2,   to);
            return;
        }
        double tx = xs[a]; xs[a] = xs[b]; xs[b] = tx;
        double ty = ys[a]; ys[a] = ys[b]; ys[b] = ty;
        long to = offsets[a]; offsets[a] = offsets[b]; offsets[b] = to;
    }

    // ======================================================================
    //  Bucket-mmap partition: scatter per-thread bucket files into mmap
    // ======================================================================

    /** Bytes per bucket record: x(8) + y(8) + offset(8) + tileId(4) = 28. */
    private static final int BUCKET_RECORD_BYTES = 8 + 8 + 8 + IndexConfig.TILE_ID_BYTES;

    /**
     * Scatters per-thread per-bucket files into mmap files, bucket by bucket.
     * <p>
     * Each bucket covers a contiguous range of tile IDs, so the mmap write
     * target for each bucket fits in page cache (~750 MB working set for
     * B=32, N=1B with interleaved layout).  No tileIds array is needed on
     * heap — tileIds are read inline from bucket records.
     * <p>
     * Layout: single interleaved mmap file with stride 3 (x, y, offset per point).
     * <p>
     * Heap during scatter: only cursors (~1 MB) + read buffer (8 MB).
     */
    private void partitionBucketsToMmap(int n, int[] starts, int numTiles) {
        try {
            // Phase A: create single interleaved mmap file (3 elements per point)
            long t0 = System.nanoTime();
            this.mmapData = MmapArray.create(mmapDir.resolve("valinor_points.mmap"), MMAP_STRIDE * n);
            LOG.info("Mmap file created (interleaved, {} GB) in {} s",
                    String.format("%.1f", (long) MMAP_STRIDE * n * 8 / (1024.0 * 1024 * 1024)),
                    String.format("%.3f", (System.nanoTime() - t0) / 1e9));

            // Phase B: scatter bucket by bucket
            t0 = System.nanoTime();
            int[] cursors = Arrays.copyOf(starts, numTiles);

            for (int b = 0; b < numBuckets; b++) {
                // Read each thread's bucket-b file and scatter into mmap
                for (int t = 0; t < numScanThreads; t++) {
                    Path bucketFile = bucketDir.resolve(String.format("scan_t%d_b%d.bin", t, b));
                    if (!Files.exists(bucketFile)) continue;
                    long fileSize = Files.size(bucketFile);
                    if (fileSize == 0) {
                        safeDelete(bucketFile);
                        continue;
                    }

                    try (FileChannel ch = FileChannel.open(bucketFile, StandardOpenOption.READ)) {
                        // Round buffer capacity down to a multiple of BUCKET_RECORD_BYTES
                        // so every read consumes complete records (no leftover partial records).
                        int alignedBuf = (SPILL_BUF_SIZE / BUCKET_RECORD_BYTES) * BUCKET_RECORD_BYTES;
                        ByteBuffer buf = ByteBuffer.allocateDirect(alignedBuf)
                                .order(ByteOrder.nativeOrder());
                        long remaining = fileSize;
                        while (remaining > 0) {
                            buf.clear();
                            int toRead = (int) Math.min(alignedBuf, remaining);
                            buf.limit(toRead);
                            while (buf.hasRemaining()) {
                                if (ch.read(buf) < 0) break;
                            }
                            buf.flip();

                            int records = buf.remaining() / BUCKET_RECORD_BYTES;
                            for (int r = 0; r < records; r++) {
                                double x = buf.getDouble();
                                double y = buf.getDouble();
                                long offset = buf.getLong();
                                int tileId = buf.getInt();
                                int dest = MMAP_STRIDE * cursors[tileId]++;
                                mmapData.putDouble(dest,     x);
                                mmapData.putDouble(dest + 1, y);
                                mmapData.putLong(dest + 2,   offset);
                            }
                            remaining -= (long) records * BUCKET_RECORD_BYTES;
                        }
                    }
                    safeDelete(bucketFile);
                }

                if (b % 8 == 7 || b == numBuckets - 1) {
                    LOG.debug("Bucket scatter progress: {}/{} buckets", b + 1, numBuckets);
                }
            }

            LOG.info("Bucket scatter complete ({} buckets) in {} s",
                    numBuckets, String.format("%.3f", (System.nanoTime() - t0) / 1e9));

            this.capacity = n;

        } catch (IOException e) {
            throw new RuntimeException("Bucket-mmap partition failed", e);
        }
    }

    @Override
    public void close() {
        if (mmapData != null) { mmapData.close(); mmapData = null; }
    }
}
