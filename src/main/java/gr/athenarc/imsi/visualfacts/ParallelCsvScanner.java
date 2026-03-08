package gr.athenarc.imsi.visualfacts;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.IdentityHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.math.StatsAccumulator;

import gr.athenarc.imsi.visualfacts.util.csv.CsvReaderConfig;
import gr.athenarc.imsi.visualfacts.util.csv.ZsvCsvDoubleRowReader;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 * Parallel CSV scanner that divides a file into byte-range chunks and parses
 * each chunk in its own thread using a dedicated ZSV reader.
 * <p>
 * Each thread collects x, y, offset into thread-local arrays and accumulates
 * per-tile counts and measure statistics.  After all threads join, the results
 * are merged into a single {@link ScanResult} in file order.
 * <p>
 * Two storage strategies are used adaptively:
 * <ul>
 *   <li><b>In-memory</b> (default): thread-local fastutil primitive lists</li>
 *   <li><b>Disk-streaming</b>: for very large datasets where thread-local arrays
 *       would exceed available heap, values are streamed to temp files</li>
 * </ul>
 */
public final class ParallelCsvScanner {

    private static final Logger LOG = LogManager.getLogger(ParallelCsvScanner.class);

    /** Spill buffer size for the disk-streaming path (8 MB). */
    private static final int SPILL_BUF = 8 * 1024 * 1024;

    // ---- configuration ----
    private final File csvFile;
    private final char delimiter;
    private final boolean hasHeader;
    private final int[] selectedColumns;      // all columns to extract (x, y, filters, measures)
    private final int xPos;                   // position of x within selectedColumns
    private final int yPos;                   // position of y within selectedColumns
    private final int[] filterPositions;      // positions of filter columns within selectedColumns
    private final DataValidationFilter[] filters;
    private final int[] measurePositions;     // positions of measure columns within selectedColumns
    private final int measureCount;
    private final Rectangle bounds;           // spatial bounds for in-bounds check
    private final Grid grid;                  // immutable grid for tile lookup
    private final IdentityHashMap<Tile, Integer> tileIndexMap;
    private final int numTiles;
    private final int numThreads;
    private final int totalCapacity;          // schema.getObjectCount()

    public ParallelCsvScanner(File csvFile, char delimiter, boolean hasHeader,
                              int[] selectedColumns, int xPos, int yPos,
                              int[] filterPositions, DataValidationFilter[] filters,
                              int[] measurePositions,
                              Rectangle bounds, Grid grid,
                              IdentityHashMap<Tile, Integer> tileIndexMap, int numTiles,
                              int numThreads, int totalCapacity) {
        this.csvFile = csvFile;
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
        this.selectedColumns = selectedColumns;
        this.xPos = xPos;
        this.yPos = yPos;
        this.filterPositions = filterPositions;
        this.filters = filters;
        this.measurePositions = measurePositions;
        this.measureCount = measurePositions.length;
        this.bounds = bounds;
        this.grid = grid;
        this.tileIndexMap = tileIndexMap;
        this.numTiles = numTiles;
        this.numThreads = numThreads;
        this.totalCapacity = totalCapacity;
    }

    // ======================================================================
    //  Result container
    // ======================================================================

    public static final class ScanResult {
        public final double[] xs;
        public final double[] ys;
        public final long[] offsets;
        public final int validCount;
        public final long maxRowLength;

        /** Per-tile counts (index = tile index, value = number of points). */
        public final int[] tileCounts;

        /**
         * Per-tile, per-measure stats: [tileIndex][measureIndex].
         * An entry is null if no non-NaN values were seen for that measure on that tile.
         */
        public final StatsAccumulator[][] tileStats;

        /** Per-tile, per-measure processed point counts (includes NaN). */
        public final int[][] tileStatsPointCounts;

        ScanResult(double[] xs, double[] ys, long[] offsets, int validCount,
                   long maxRowLength, int[] tileCounts,
                   StatsAccumulator[][] tileStats, int[][] tileStatsPointCounts) {
            this.xs = xs;
            this.ys = ys;
            this.offsets = offsets;
            this.validCount = validCount;
            this.maxRowLength = maxRowLength;
            this.tileCounts = tileCounts;
            this.tileStats = tileStats;
            this.tileStatsPointCounts = tileStatsPointCounts;
        }
    }

    // ======================================================================
    //  Per-thread result container
    // ======================================================================

    private static final class ChunkResult {
        // These are used in the in-memory path; null in the disk path.
        DoubleArrayList localXs;
        DoubleArrayList localYs;
        LongArrayList localOffsets;

        // These are used in the disk path; null in the in-memory path.
        Path tmpXs;
        Path tmpYs;
        Path tmpOffsets;

        int validCount;
        long maxRowLength;

        // Per-tile counts (always computed)
        int[] tileCounts;

        // Per-tile, per-measure stats (always computed)
        StatsAccumulator[][] tileStats;
        int[][] tileStatsPointCounts;

        Throwable error;
    }

    // ======================================================================
    //  Public entry point
    // ======================================================================

    public ScanResult scan() throws IOException {
        long t0 = System.nanoTime();

        // 1. Determine chunk boundaries
        long[] chunkStarts = computeChunkBoundaries();

        // 2. Scan-time storage strategy (uses capacity estimate, needed before launch)
        // Workers must know up-front whether to collect into fastutil lists (in-memory)
        // or stream to temp files (disk).  This uses the capacity estimate; the merge
        // decision is deferred to after the join where we know the actual count.
        long maxHeap = Runtime.getRuntime().maxMemory();
        long estimatedPeak = 32L * totalCapacity;  // 3×8×N + 8×N = 32N bytes
        boolean useDiskScan = estimatedPeak > (long) (maxHeap * 0.85);
        LOG.info("Parallel scan: {} threads, {} scan path, estimated peak={} MB, maxHeap={} MB",
                numThreads, useDiskScan ? "disk-streaming" : "in-memory",
                estimatedPeak / (1024 * 1024), maxHeap / (1024 * 1024));

        // 3. Launch worker threads
        ChunkResult[] results = new ChunkResult[numThreads];
        Thread[] threads = new Thread[numThreads];

        for (int t = 0; t < numThreads; t++) {
            final int threadIdx = t;
            final long start = chunkStarts[t];
            // end_offset = first byte of the NEXT chunk.  The JNI reader uses
            // row-start comparison (cum - row_len >= end - start) which is
            // line-ending agnostic (works for both \n and \r\n).
            final long end = (t < numThreads - 1) ? chunkStarts[t + 1] : -1; // -1 = EOF
            final boolean disk = useDiskScan;

            threads[t] = new Thread(() -> {
                results[threadIdx] = scanChunk(threadIdx, start, end, disk);
            }, "csv-scan-" + t);
            threads[t].start();
        }

        // 4. Join all threads
        for (int t = 0; t < numThreads; t++) {
            try {
                threads[t].join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted waiting for scan thread " + t, e);
            }
            if (results[t].error != null) {
                throw new IOException("Scan thread " + t + " failed", results[t].error);
            }
        }

        double scanSec = (System.nanoTime() - t0) / 1e9;
        int totalValid = 0;
        long globalMaxRowLen = 0;
        for (ChunkResult cr : results) {
            totalValid += cr.validCount;
            if (cr.maxRowLength > globalMaxRowLen) globalMaxRowLen = cr.maxRowLength;
        }
        LOG.info("Parallel scan complete: {} valid points in {} s",
                totalValid, String.format("%.3f", scanSec));

        // 5. Merge results
        // If scan was in-memory, the trimmed local arrays are already live;
        // mergeInMemory copies them one-at-a-time into a global array while
        // nulling each local, so peak usage during merge is strictly less
        // than what was already held during scan.  No need to re-check.
        long t1 = System.nanoTime();
        ScanResult result;
        if (useDiskScan) {
            result = mergeDisk(results, totalValid, globalMaxRowLen);
        } else {
            result = mergeInMemory(results, totalValid, globalMaxRowLen);
        }
        double mergeSec = (System.nanoTime() - t1) / 1e9;
        LOG.info("Array merge complete in {} s", String.format("%.3f", mergeSec));

        return result;
    }

    // ======================================================================
    //  Chunk boundary computation
    // ======================================================================

    /**
     * Returns an array of numThreads byte offsets, one per chunk.
     * Each offset is aligned to the byte <em>after</em> a newline character
     * so that no CSV row is split across chunks.
     */
    private long[] computeChunkBoundaries() throws IOException {
        long fileSize;
        long headerEnd = 0;

        try (RandomAccessFile raf = new RandomAccessFile(csvFile, "r")) {
            fileSize = raf.length();

            if (hasHeader) {
                // Skip the header line; readLine handles \n and \r\n
                raf.readLine();
                headerEnd = raf.getFilePointer();
            }
        }

        long dataSize = fileSize - headerEnd;
        long chunkSize = dataSize / numThreads;

        long[] starts = new long[numThreads];
        starts[0] = headerEnd;

        try (RandomAccessFile raf = new RandomAccessFile(csvFile, "r")) {
            for (int t = 1; t < numThreads; t++) {
                long nominalBoundary = headerEnd + t * chunkSize;
                if (nominalBoundary >= fileSize) {
                    // File too small for this many chunks; collapse remaining
                    starts[t] = fileSize;
                    continue;
                }
                raf.seek(nominalBoundary);
                // Scan forward to past the next newline (aligns to row boundary)
                raf.readLine(); // consumes remainder of partial row
                starts[t] = raf.getFilePointer();
            }
        }

        return starts;
    }

    // ======================================================================
    //  Worker: scan one chunk
    // ======================================================================

    private ChunkResult scanChunk(int threadIdx, long startOffset, long endOffset, boolean useDisk) {
        ChunkResult cr = new ChunkResult();
        cr.tileCounts = new int[numTiles];
        cr.tileStats = new StatsAccumulator[numTiles][measureCount];
        cr.tileStatsPointCounts = new int[numTiles][measureCount];

        // Estimate rows per thread for initial capacity (in-memory path)
        int estimatedRows = Math.max(1024, totalCapacity / numThreads);

        DoubleArrayList localXs = null;
        DoubleArrayList localYs = null;
        LongArrayList localOffsets = null;

        FileChannel chXs = null, chYs = null, chOff = null;
        ByteBuffer spillBuf = null;

        try {
            if (useDisk) {
                cr.tmpXs = Files.createTempFile("par_xs_" + threadIdx + "_", ".bin");
                cr.tmpYs = Files.createTempFile("par_ys_" + threadIdx + "_", ".bin");
                cr.tmpOffsets = Files.createTempFile("par_off_" + threadIdx + "_", ".bin");
                chXs = FileChannel.open(cr.tmpXs, StandardOpenOption.WRITE);
                chYs = FileChannel.open(cr.tmpYs, StandardOpenOption.WRITE);
                chOff = FileChannel.open(cr.tmpOffsets, StandardOpenOption.WRITE);
                spillBuf = ByteBuffer.allocate(SPILL_BUF).order(ByteOrder.nativeOrder());
            } else {
                localXs = new DoubleArrayList(estimatedRows);
                localYs = new DoubleArrayList(estimatedRows);
                localOffsets = new LongArrayList(estimatedRows);
            }

            // Open the chunk reader
            CsvReaderConfig chunkConfig = new CsvReaderConfig(
                    csvFile,
                    Charset.forName("UTF-8"),
                    selectedColumns,
                    false,    // header already skipped via chunk offset
                    delimiter,
                    startOffset,
                    endOffset);

            try (ZsvCsvDoubleRowReader reader = new ZsvCsvDoubleRowReader()) {
                reader.open(chunkConfig);
                double[] row;
                int validCount = 0;

                final int fCount = filters.length;
                final int mc = measureCount;

                while ((row = reader.nextRow()) != null) {
                    long offset = reader.currentOffset();

                    // Apply validation filters
                    boolean skip = false;
                    for (int i = 0; i < fCount; i++) {
                        if (filters[i].test(row[filterPositions[i]])) {
                            skip = true;
                            break;
                        }
                    }
                    if (skip) continue;

                    double x = row[xPos];
                    double y = row[yPos];
                    if (!bounds.contains(x, y)) continue;

                    // Grid lookup — pure arithmetic, thread-safe on immutable grid
                    Tile leafTile = (Tile) grid.getLeafTile(x, y);
                    int tileIdx = tileIndexMap.get(leafTile).intValue();

                    // Accumulate per-tile count
                    cr.tileCounts[tileIdx]++;

                    // Accumulate per-tile measure stats
                    for (int m = 0; m < mc; m++) {
                        if (measurePositions[m] < 0) continue;
                        cr.tileStatsPointCounts[tileIdx][m]++;
                        double val = row[measurePositions[m]];
                        if (!Double.isNaN(val)) {
                            StatsAccumulator sa = cr.tileStats[tileIdx][m];
                            if (sa == null) {
                                sa = new StatsAccumulator();
                                cr.tileStats[tileIdx][m] = sa;
                            }
                            sa.add(val);
                        }
                    }

                    // Store x, y, offset
                    if (useDisk) {
                        // Write to spill buffer, flush when full
                        if (spillBuf.remaining() < 24) { // 8+8+8 bytes
                            flushSpillBuffers(spillBuf, chXs, chYs, chOff, validCount);
                            spillBuf.clear();
                        }
                        spillBuf.putDouble(x);
                        spillBuf.putDouble(y);
                        spillBuf.putLong(offset);
                    } else {
                        localXs.add(x);
                        localYs.add(y);
                        localOffsets.add(offset);
                    }

                    validCount++;
                }

                // Flush remaining spill data
                if (useDisk && spillBuf.position() > 0) {
                    flushSpillBuffers(spillBuf, chXs, chYs, chOff, validCount);
                }

                cr.validCount = validCount;
                cr.maxRowLength = reader.maxRowLength();
            }

            if (!useDisk) {
                // Trim backing arrays to exact size so merge peak is
                // predictable: 3×8×N (local) + 8×N (one global) = 32N.
                localXs.trim();
                localYs.trim();
                localOffsets.trim();
                cr.localXs = localXs;
                cr.localYs = localYs;
                cr.localOffsets = localOffsets;
            }

        } catch (Throwable t) {
            cr.error = t;
        } finally {
            closeQuietly(chXs);
            closeQuietly(chYs);
            closeQuietly(chOff);
        }

        return cr;
    }

    /**
     * The spill buffer interleaves x, y, offset (24 bytes per row).
     * This helper writes the accumulated data into separate per-attribute channels.
     */
    private static void flushSpillBuffers(ByteBuffer interleaved,
                                          FileChannel chXs, FileChannel chYs,
                                          FileChannel chOff, int rowsSoFar) throws IOException {
        interleaved.flip();
        int rows = interleaved.remaining() / 24;
        // De-interleave into per-attribute temp buffers
        ByteBuffer bXs = ByteBuffer.allocate(rows * 8).order(ByteOrder.nativeOrder());
        ByteBuffer bYs = ByteBuffer.allocate(rows * 8).order(ByteOrder.nativeOrder());
        ByteBuffer bOff = ByteBuffer.allocate(rows * 8).order(ByteOrder.nativeOrder());
        for (int i = 0; i < rows; i++) {
            bXs.putDouble(interleaved.getDouble());
            bYs.putDouble(interleaved.getDouble());
            bOff.putLong(interleaved.getLong());
        }
        bXs.flip();
        bYs.flip();
        bOff.flip();
        while (bXs.hasRemaining()) chXs.write(bXs);
        while (bYs.hasRemaining()) chYs.write(bYs);
        while (bOff.hasRemaining()) chOff.write(bOff);
    }

    // ======================================================================
    //  Merge: in-memory path
    // ======================================================================

    private ScanResult mergeInMemory(ChunkResult[] results, int totalValid, long maxRowLen) {
        // Merge xs: allocate global, copy each thread's data, free local
        double[] globalXs = new double[totalValid];
        int offset = 0;
        for (ChunkResult cr : results) {
            if (cr.validCount > 0) {
                System.arraycopy(cr.localXs.elements(), 0, globalXs, offset, cr.validCount);
            }
            offset += cr.validCount;
            cr.localXs = null; // release for GC
        }

        // Merge ys
        double[] globalYs = new double[totalValid];
        offset = 0;
        for (ChunkResult cr : results) {
            if (cr.validCount > 0) {
                System.arraycopy(cr.localYs.elements(), 0, globalYs, offset, cr.validCount);
            }
            offset += cr.validCount;
            cr.localYs = null;
        }

        // Merge offsets
        long[] globalOffsets = new long[totalValid];
        offset = 0;
        for (ChunkResult cr : results) {
            if (cr.validCount > 0) {
                System.arraycopy(cr.localOffsets.elements(), 0, globalOffsets, offset, cr.validCount);
            }
            offset += cr.validCount;
            cr.localOffsets = null;
        }

        return buildResult(globalXs, globalYs, globalOffsets, totalValid, maxRowLen, results);
    }

    // ======================================================================
    //  Merge: disk-streaming path
    // ======================================================================

    private ScanResult mergeDisk(ChunkResult[] results, int totalValid, long maxRowLen) throws IOException {
        try {
            // Merge xs from disk
            double[] globalXs = new double[totalValid];
            int offset = 0;
            for (ChunkResult cr : results) {
                if (cr.validCount > 0) {
                    readDoublesFromFile(cr.tmpXs, globalXs, offset, cr.validCount);
                }
                offset += cr.validCount;
                safeDelete(cr.tmpXs);
                cr.tmpXs = null;
            }

            // Merge ys from disk
            double[] globalYs = new double[totalValid];
            offset = 0;
            for (ChunkResult cr : results) {
                if (cr.validCount > 0) {
                    readDoublesFromFile(cr.tmpYs, globalYs, offset, cr.validCount);
                }
                offset += cr.validCount;
                safeDelete(cr.tmpYs);
                cr.tmpYs = null;
            }

            // Merge offsets from disk
            long[] globalOffsets = new long[totalValid];
            offset = 0;
            for (ChunkResult cr : results) {
                if (cr.validCount > 0) {
                    readLongsFromFile(cr.tmpOffsets, globalOffsets, offset, cr.validCount);
                }
                offset += cr.validCount;
                safeDelete(cr.tmpOffsets);
                cr.tmpOffsets = null;
            }

            return buildResult(globalXs, globalYs, globalOffsets, totalValid, maxRowLen, results);
        } finally {
            // Cleanup any remaining temp files on error
            for (ChunkResult cr : results) {
                safeDelete(cr.tmpXs);
                safeDelete(cr.tmpYs);
                safeDelete(cr.tmpOffsets);
            }
        }
    }

    // ======================================================================
    //  Merge: combine per-tile counts and stats
    // ======================================================================

    private ScanResult buildResult(double[] xs, double[] ys, long[] offsets,
                                   int totalValid, long maxRowLen,
                                   ChunkResult[] results) {
        // Merge per-tile counts
        int[] globalCounts = new int[numTiles];
        for (ChunkResult cr : results) {
            for (int t = 0; t < numTiles; t++) {
                globalCounts[t] += cr.tileCounts[t];
            }
        }

        // Merge per-tile stats
        StatsAccumulator[][] globalStats = new StatsAccumulator[numTiles][measureCount];
        int[][] globalPointCounts = new int[numTiles][measureCount];
        for (ChunkResult cr : results) {
            for (int t = 0; t < numTiles; t++) {
                for (int m = 0; m < measureCount; m++) {
                    globalPointCounts[t][m] += cr.tileStatsPointCounts[t][m];
                    StatsAccumulator local = cr.tileStats[t][m];
                    if (local == null || local.count() == 0) continue;
                    StatsAccumulator global = globalStats[t][m];
                    if (global == null) {
                        global = new StatsAccumulator();
                        globalStats[t][m] = global;
                    }
                    global.addAll(local.snapshot());
                }
            }
        }

        return new ScanResult(xs, ys, offsets, totalValid, maxRowLen,
                globalCounts, globalStats, globalPointCounts);
    }

    // ======================================================================
    //  Disk I/O helpers
    // ======================================================================

    private static void readDoublesFromFile(Path path, double[] dest, int destOffset, int count) throws IOException {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(Math.min(SPILL_BUF, count * 8)).order(ByteOrder.nativeOrder());
            int remaining = count;
            int pos = destOffset;
            while (remaining > 0) {
                buf.clear();
                int toRead = Math.min(remaining, buf.capacity() / 8);
                buf.limit(toRead * 8);
                int bytesRead = 0;
                while (buf.hasRemaining()) {
                    int n = ch.read(buf);
                    if (n < 0) break;
                    bytesRead += n;
                }
                buf.flip();
                while (buf.remaining() >= 8) {
                    dest[pos++] = buf.getDouble();
                }
                remaining -= toRead;
            }
        }
    }

    private static void readLongsFromFile(Path path, long[] dest, int destOffset, int count) throws IOException {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(Math.min(SPILL_BUF, count * 8)).order(ByteOrder.nativeOrder());
            int remaining = count;
            int pos = destOffset;
            while (remaining > 0) {
                buf.clear();
                int toRead = Math.min(remaining, buf.capacity() / 8);
                buf.limit(toRead * 8);
                while (buf.hasRemaining()) {
                    int n = ch.read(buf);
                    if (n < 0) break;
                }
                buf.flip();
                while (buf.remaining() >= 8) {
                    dest[pos++] = buf.getLong();
                }
                remaining -= toRead;
            }
        }
    }

    private static void safeDelete(Path p) {
        if (p != null) {
            try { Files.deleteIfExists(p); } catch (IOException ignored) { }
        }
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try { c.close(); } catch (Exception ignored) { }
        }
    }
}
