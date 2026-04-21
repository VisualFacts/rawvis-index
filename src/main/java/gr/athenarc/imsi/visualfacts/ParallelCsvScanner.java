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
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.math.StatsAccumulator;

import gr.athenarc.imsi.visualfacts.config.IndexConfig;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.util.csv.CsvReaderConfig;
import gr.athenarc.imsi.visualfacts.util.csv.ZsvCsvDoubleRowReader;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 * Parallel CSV scanner that divides a file into byte-range chunks and parses
 * each chunk in its own thread using a dedicated ZSV reader.
 * <p>
 * Each thread collects x, y, offset into thread-local arrays and accumulates
 * per-tile counts and measure statistics.  After all threads join, the results
 * are merged into a single {@link ScanResult} in file order.
 * <p>
 * Three storage strategies are used adaptively based on memory pressure:
 * <ul>
 *   <li><b>Path A — In-memory</b> (default): thread-local fastutil lists on heap.
 *       Used when both scan peak (32N + metadata) and partition peak (36N) fit
 *       in 85% of max heap.  Partition uses parallel histogram scatter from
 *       T heap chunks.</li>
 *   <li><b>Path B — Disk-streaming</b>: thread-local data streamed to T×4
 *       per-attribute temp files, metadata stays on heap.  Used when scan or
 *       partition peak exceeds 85% heap but steady-state (24N) fits.
 *       Partition scatters directly from the T per-thread files in parallel
 *       — no intermediate merge to heap.</li>
 *   <li><b>Path C — Bucket-streaming</b>: data streamed to B×T per-bucket files
 *       for cache-friendly mmap scatter.  Used when even 24N exceeds 85% heap.
 *       Final arrays live in mmap files, not on the Java heap.</li>
 * </ul>
 */
public final class ParallelCsvScanner {

    static {
        // All spill I/O uses ByteBuffer.putInt/getInt for tile IDs.
        // If TILE_ID_BYTES changes from 4, every putInt/getInt call in
        // flushSpillBuffers and scanChunk must be updated.
        if (IndexConfig.TILE_ID_BYTES != Integer.BYTES) {
            throw new AssertionError(
                    "ParallelCsvScanner spill I/O assumes TILE_ID_BYTES == 4, got " + IndexConfig.TILE_ID_BYTES);
        }
    }

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
    private final String nullstr;             // null-string for CSV parsing (e.g. "\\N"), or null
    private final Path tmpDir;                // directory for bucket/spill temp files (null → system default)
    private final Rectangle q0Rect;           // q0 query rectangle (null if no q0)
    private final List<Integer> q0MeasureCols; // q0 measure columns (null if no q0)

    public ParallelCsvScanner(File csvFile, char delimiter, boolean hasHeader,
                              int[] selectedColumns, int xPos, int yPos,
                              int[] filterPositions, DataValidationFilter[] filters,
                              int[] measurePositions,
                              Rectangle bounds, Grid grid,
                              IdentityHashMap<Tile, Integer> tileIndexMap, int numTiles,
                              int numThreads, int totalCapacity, String nullstr,
                              Path tmpDir, Query q0) {
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
        this.nullstr = nullstr;
        this.tmpDir = tmpDir;
        this.q0Rect = (q0 != null) ? q0.getRect() : null;
        this.q0MeasureCols = (q0 != null) ? q0.getMeasureCols() : null;
    }

    // ======================================================================
    //  Result container
    // ======================================================================

    public static final class ScanResult {

        public final int validCount;
        public final long maxRowLength;

        /** Per-tile counts (index = tile index, value = number of points). */
        public final int[] tileCounts;
        /** Per-tile, per-measure stats: [tileIndex][measureIndex]. Null if no non-NaN values for that cell. */
        public final StatsAccumulator[][] tileStats;
        /** Per-tile, per-measure processed point counts (includes NaN). */
        public final int[][] tileStatsPointCounts;
        /** Scan path used: "in-memory", "disk-streaming", or "bucket-streaming". */
        public final String scanPath;
        /** Q0 query statistics per measure (null if no q0). Key = measure column index. */
        public final Map<Integer, StatsAccumulator> q0Stats;

        // ---- Path A: in-memory heap chunks (null for paths B and C) ----

        /** Per-thread chunk arrays on heap. */
        public final double[][] xsChunks;
        public final double[][] ysChunks;
        public final long[][] offsetsChunks;
        public final int[][] tileIdChunks;
        public final int[] chunkSizes;

        // ---- Path B: disk-streaming per-thread files (null for paths A and C) ----

        /** Per-thread xs temp file paths. */
        public final Path[] diskXsFiles;
        public final Path[] diskYsFiles;
        public final Path[] diskOffsetsFiles;
        public final Path[] diskTileIdFiles;
        /** Number of valid elements per thread (parallel to disk*Files). */
        public final int[] diskChunkSizes;

        // ---- Path C: bucket-streaming (null for paths A and B) ----

        /** When true, point data is in per-thread per-bucket files for cache-friendly mmap scatter. */
        public final boolean bucketMode;
        /** Directory containing bucket files: scan_t{t}_b{k}.bin */
        public final Path bucketDir;
        public final int numBuckets;
        public final int tilesPerBucket;
        public final int numScanThreads;
        /** Per-thread tile counts for parallel partition: [thread][tileIndex]. Null for paths A and B. */
        public final int[][] perThreadTileCounts;

        /** Path A constructor: heap chunks. */
        ScanResult(double[][] xsChunks, double[][] ysChunks, long[][] offsetsChunks,
                   int[][] tileIdChunks, int[] chunkSizes,
                   int validCount, long maxRowLength,
                   int[] tileCounts, StatsAccumulator[][] tileStats, int[][] tileStatsPointCounts,
                   String scanPath, Map<Integer, StatsAccumulator> q0Stats) {
            this.validCount = validCount;
            this.maxRowLength = maxRowLength;
            this.tileCounts = tileCounts;
            this.tileStats = tileStats;
            this.tileStatsPointCounts = tileStatsPointCounts;
            this.scanPath = scanPath;
            this.q0Stats = q0Stats;
            // Path A fields
            this.xsChunks = xsChunks;
            this.ysChunks = ysChunks;
            this.offsetsChunks = offsetsChunks;
            this.tileIdChunks = tileIdChunks;
            this.chunkSizes = chunkSizes;
            // Other paths null
            this.diskXsFiles = null; this.diskYsFiles = null;
            this.diskOffsetsFiles = null; this.diskTileIdFiles = null;
            this.diskChunkSizes = null;
            this.bucketMode = false; this.bucketDir = null;
            this.numBuckets = 0; this.tilesPerBucket = 0; this.numScanThreads = 0;
            this.perThreadTileCounts = null;
        }

        /** Path B constructor: per-thread disk files. */
        ScanResult(Path[] diskXsFiles, Path[] diskYsFiles, Path[] diskOffsetsFiles,
                   Path[] diskTileIdFiles, int[] diskChunkSizes,
                   int validCount, long maxRowLength,
                   int[] tileCounts, StatsAccumulator[][] tileStats, int[][] tileStatsPointCounts,
                   String scanPath, Map<Integer, StatsAccumulator> q0Stats) {
            this.validCount = validCount;
            this.maxRowLength = maxRowLength;
            this.tileCounts = tileCounts;
            this.q0Stats = q0Stats;
            this.tileStats = tileStats;
            this.tileStatsPointCounts = tileStatsPointCounts;
            this.scanPath = scanPath;
            // Path A fields null
            this.xsChunks = null; this.ysChunks = null;
            this.offsetsChunks = null; this.tileIdChunks = null;
            this.chunkSizes = null;
            // Path B fields
            this.diskXsFiles = diskXsFiles;
            this.diskYsFiles = diskYsFiles;
            this.diskOffsetsFiles = diskOffsetsFiles;
            this.diskTileIdFiles = diskTileIdFiles;
            this.diskChunkSizes = diskChunkSizes;
            // Path C null
            this.bucketMode = false; this.bucketDir = null;
            this.numBuckets = 0; this.tilesPerBucket = 0; this.numScanThreads = 0;
            this.perThreadTileCounts = null;
        }

        /** Path C constructor: bucket mode. */
        ScanResult(int validCount, long maxRowLength,
                   int[] tileCounts, StatsAccumulator[][] tileStats, int[][] tileStatsPointCounts,
                   String scanPath,
                   Path bucketDir, int numBuckets, int tilesPerBucket, int numScanThreads,
                   int[][] perThreadTileCounts, Map<Integer, StatsAccumulator> q0Stats) {
            this.validCount = validCount;
            this.maxRowLength = maxRowLength;
            this.tileCounts = tileCounts;
            this.tileStats = tileStats;
            this.tileStatsPointCounts = tileStatsPointCounts;
            this.scanPath = scanPath;
            this.q0Stats = q0Stats;
            // Path A null
            this.xsChunks = null; this.ysChunks = null;
            this.offsetsChunks = null; this.tileIdChunks = null;
            this.chunkSizes = null;
            // Path B null
            this.diskXsFiles = null; this.diskYsFiles = null;
            this.diskOffsetsFiles = null; this.diskTileIdFiles = null;
            this.diskChunkSizes = null;
            // Path C fields
            this.bucketMode = true;
            this.bucketDir = bucketDir;
            this.numBuckets = numBuckets;
            this.tilesPerBucket = tilesPerBucket;
            this.numScanThreads = numScanThreads;
            this.perThreadTileCounts = perThreadTileCounts;
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
        IntArrayList localTileIds;

        // These are used in the disk (non-bucket) path; null in other paths.
        Path tmpXs;
        Path tmpYs;
        Path tmpOffsets;
        Path tmpTileIds;

        int validCount;
        long maxRowLength;

        // Per-tile counts (always computed)
        int[] tileCounts;

        // Per-tile, per-measure stats (always computed)
        StatsAccumulator[][] tileStats;
        int[][] tileStatsPointCounts;

        // Q0 query statistics per measure (null if no q0)
        Map<Integer, StatsAccumulator> q0Stats;

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
        long maxHeap = Runtime.getRuntime().maxMemory();
        long estimatedPointData = (24L + 2L * IndexConfig.TILE_ID_BYTES) * totalCapacity;
        long estimatedMetadata = (long) numThreads * (long) numTiles * (24L + 76L * measureCount);
        long estimatedPeak = estimatedPointData + estimatedMetadata;
        long estimatedPartitionPeak = 36L * totalCapacity; // 3 arrays + 1 new + tileIds during scatter

        // Use disk if EITHER the scan peak or the partition peak won't fit.
        // This ensures the disk-streaming path is always used when partition
        // would need to spill, avoids the heap→disk→heap→disk round-trip.
        boolean useDiskScan = estimatedPeak > (long) (maxHeap * 0.85)
                || estimatedPartitionPeak > (long) (maxHeap * 0.85);

        // Determine if mmap will be needed (steady-state point data > 85% heap)
        long estimatedSteadyState = 24L * totalCapacity;
        boolean useBucketScan = useDiskScan
                && estimatedSteadyState > (long) (maxHeap * 0.85);

        if (useBucketScan && tmpDir == null) {
            throw new IOException("Dataset requires bucket-mmap path (steady-state " +
                    (estimatedSteadyState / (1024 * 1024)) + " MB > 85% of max heap " +
                    (maxHeap / (1024 * 1024)) + " MB) but tmpDir is unavailable");
        }

        // Compute bucket count for the bucket path
        int numBuckets = 0;
        int tilesPerBucket = 0;
        if (useBucketScan) {
            // Target: each bucket's mmap output region fits comfortably in page cache.
            // Interleaved layout: region_per_bucket = N * 24 / B (all 3 attrs together)
            // Target 768 MB per region (fits in typical page cache budget)
            long targetRegion = 768L * 1024 * 1024;
            int bRaw = Math.max(1, (int) Math.ceil(24.0 * totalCapacity / targetRegion));
            numBuckets = Integer.highestOneBit(bRaw);
            if (numBuckets < bRaw) numBuckets <<= 1;
            numBuckets = Math.max(4, Math.min(numBuckets, 64));
            tilesPerBucket = (numTiles + numBuckets - 1) / numBuckets;
        }

        final boolean bucket = useBucketScan;
        final int fNumBuckets = numBuckets;
        final int fTilesPerBucket = tilesPerBucket;

        LOG.info("Parallel scan: {} threads, {} scan path, estimated peak={} MB (point data={} MB, metadata={} MB, partition={}), maxHeap={} MB{}",
                numThreads,
                bucket ? "bucket-streaming" : (useDiskScan ? "disk-streaming" : "in-memory"),
                estimatedPeak / (1024 * 1024),
                estimatedPointData / (1024 * 1024),
                estimatedMetadata / (1024 * 1024),
                estimatedPartitionPeak / (1024 * 1024),
                maxHeap / (1024 * 1024),
                bucket ? String.format(", buckets=%d, tilesPerBucket=%d", numBuckets, tilesPerBucket) : "");

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
            final boolean disk = useDiskScan && !bucket;

            threads[t] = new Thread(() -> {
                if (bucket) {
                    results[threadIdx] = scanChunkBucket(threadIdx, start, end, fNumBuckets, fTilesPerBucket);
                } else {
                    results[threadIdx] = scanChunk(threadIdx, start, end, disk);
                }
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

        // 5. Merge results (stats only for disk/bucket paths; zero-copy for in-memory)
        long t1 = System.nanoTime();
        ScanResult result;
        String scanPath;
        if (bucket) {
            scanPath = "bucket-streaming";
            result = buildBucketResult(results, totalValid, globalMaxRowLen,
                    numBuckets, tilesPerBucket, scanPath);
        } else if (useDiskScan) {
            // Path B: keep per-thread files for direct scatter during partition.
            // Only merge per-tile stats here — no point data is loaded to heap.
            scanPath = "disk-streaming";
            result = buildDiskChunkResult(results, totalValid, globalMaxRowLen, scanPath);
        } else {
            scanPath = "in-memory";
            result = adoptChunks(results, totalValid, globalMaxRowLen, scanPath);
        }
        double adoptSec = (System.nanoTime() - t1) / 1e9;
        LOG.info("Chunk {} complete in {} s",
                bucket ? "bucket (zero-copy)" : (useDiskScan ? "merge (disk)" : "adopt (zero-copy)"),
                String.format("%.3f", adoptSec));

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
        // Initialize q0Stats if q0 is provided
        if (q0Rect != null && q0MeasureCols != null) {
            cr.q0Stats = new HashMap<>();
        }

        // Estimate rows per thread for initial capacity (in-memory path)
        int estimatedRows = Math.max(1024, totalCapacity / numThreads);

        DoubleArrayList localXs = null;
        DoubleArrayList localYs = null;
        LongArrayList localOffsets = null;

        FileChannel chXs = null, chYs = null, chOff = null, chTid = null;
        ByteBuffer spillBuf = null;

        try {
            IntArrayList localTileIds = null;

            if (useDisk) {
                cr.tmpXs = Files.createTempFile("par_xs_" + threadIdx + "_", ".bin");
                cr.tmpYs = Files.createTempFile("par_ys_" + threadIdx + "_", ".bin");
                cr.tmpOffsets = Files.createTempFile("par_off_" + threadIdx + "_", ".bin");
                cr.tmpTileIds = Files.createTempFile("par_tid_" + threadIdx + "_", ".bin");
                chXs = FileChannel.open(cr.tmpXs, StandardOpenOption.WRITE);
                chYs = FileChannel.open(cr.tmpYs, StandardOpenOption.WRITE);
                chOff = FileChannel.open(cr.tmpOffsets, StandardOpenOption.WRITE);
                chTid = FileChannel.open(cr.tmpTileIds, StandardOpenOption.WRITE);
                spillBuf = ByteBuffer.allocate(SPILL_BUF).order(ByteOrder.nativeOrder());
            } else {
                localXs = new DoubleArrayList(estimatedRows);
                localYs = new DoubleArrayList(estimatedRows);
                localOffsets = new LongArrayList(estimatedRows);
                localTileIds = new IntArrayList(estimatedRows);
            }

            // Open the chunk reader
            CsvReaderConfig chunkConfig = new CsvReaderConfig(
                    csvFile,
                    Charset.forName("UTF-8"),
                    selectedColumns,
                    false,    // header already skipped via chunk offset
                    delimiter,
                    startOffset,
                    endOffset,
                    nullstr);

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

                    // Accumulate q0 stats if point is within q0 rectangle
                    if (q0Rect != null && q0MeasureCols != null && q0Rect.contains(x, y)) {
                        for (Integer measureCol : q0MeasureCols) {
                            // Find position of this measure in the row
                            int measurePos = -1;
                            for (int m = 0; m < mc; m++) {
                                if (measurePositions[m] >= 0 && selectedColumns[measurePositions[m]] == measureCol) {
                                    measurePos = measurePositions[m];
                                    break;
                                }
                            }
                            if (measurePos >= 0) {
                                double val = row[measurePos];
                                if (!Double.isNaN(val)) {
                                    StatsAccumulator q0sa = cr.q0Stats.get(measureCol);
                                    if (q0sa == null) {
                                        q0sa = new StatsAccumulator();
                                        cr.q0Stats.put(measureCol, q0sa);
                                    }
                                    q0sa.add(val);
                                }
                            }
                        }
                    }

                    // Store x, y, offset, tileId
                    if (useDisk) {
                        // Write to spill buffer, flush when full
                        if (spillBuf.remaining() < 24 + IndexConfig.TILE_ID_BYTES) {
                            flushSpillBuffers(spillBuf, chXs, chYs, chOff, chTid, validCount);
                            spillBuf.clear();
                        }
                        spillBuf.putDouble(x);
                        spillBuf.putDouble(y);
                        spillBuf.putLong(offset);
                        spillBuf.putInt(tileIdx);
                    } else {
                        localXs.add(x);
                        localYs.add(y);
                        localOffsets.add(offset);
                        localTileIds.add(tileIdx);
                    }

                    validCount++;
                }

                // Flush remaining spill data
                if (useDisk && spillBuf.position() > 0) {
                    flushSpillBuffers(spillBuf, chXs, chYs, chOff, chTid, validCount);
                }

                cr.validCount = validCount;
                cr.maxRowLength = reader.maxRowLength();
            }

            if (!useDisk) {
                // Trim backing arrays to exact size to minimize memory footprint.
                // After adoption, these become chunks in SharedPointStore.
                localXs.trim();
                localYs.trim();
                localOffsets.trim();
                localTileIds.trim();
                cr.localXs = localXs;
                cr.localYs = localYs;
                cr.localOffsets = localOffsets;
                cr.localTileIds = localTileIds;
            }

        } catch (Throwable t) {
            cr.error = t;
        } finally {
            closeQuietly(chTid);
            closeQuietly(chXs);
            closeQuietly(chYs);
            closeQuietly(chOff);
        }

        return cr;
    }

    // ======================================================================
    //  Worker: scan one chunk — bucket-streaming path
    // ======================================================================

    /** Bytes per bucket record: x(8) + y(8) + offset(8) + tileId(4) = 28. */
    private static final int BUCKET_RECORD_BYTES = 8 + 8 + 8 + IndexConfig.TILE_ID_BYTES;

    /** Per-bucket flush buffer (64 KB — small enough for B × threads buffers to fit in heap). */
    private static final int BUCKET_BUF_SIZE = 64 * 1024;

    /**
     * Scans a CSV chunk and writes interleaved records to per-bucket files.
     * Each bucket covers {@code tilesPerBucket} contiguous tile IDs, so
     * the downstream scatter writes to contiguous mmap regions.
     */
    private ChunkResult scanChunkBucket(int threadIdx, long startOffset, long endOffset,
                                        int numBuckets, int tilesPerBucket) {
        ChunkResult cr = new ChunkResult();
        cr.tileCounts = new int[numTiles];
        cr.tileStats = new StatsAccumulator[numTiles][measureCount];
        cr.tileStatsPointCounts = new int[numTiles][measureCount];
        // Initialize q0Stats if q0 is provided
        if (q0Rect != null && q0MeasureCols != null) {
            cr.q0Stats = new HashMap<>();
        }

        FileChannel[] bucketChannels = new FileChannel[numBuckets];
        ByteBuffer[] bucketBufs = new ByteBuffer[numBuckets];

        try {
            // Open one file per bucket
            for (int b = 0; b < numBuckets; b++) {
                Path bucketFile = tmpDir.resolve(String.format("scan_t%d_b%d.bin", threadIdx, b));
                bucketChannels[b] = FileChannel.open(bucketFile,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
                bucketBufs[b] = ByteBuffer.allocate(BUCKET_BUF_SIZE).order(ByteOrder.nativeOrder());
            }

            // Open the chunk reader
            CsvReaderConfig chunkConfig = new CsvReaderConfig(
                    csvFile,
                    Charset.forName("UTF-8"),
                    selectedColumns,
                    false,
                    delimiter,
                    startOffset,
                    endOffset,
                    nullstr);

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

                    // Accumulate q0 stats if point is within q0 rectangle
                    if (q0Rect != null && q0MeasureCols != null && q0Rect.contains(x, y)) {
                        for (Integer measureCol : q0MeasureCols) {
                            // Find position of this measure in the row
                            int measurePos = -1;
                            for (int m = 0; m < mc; m++) {
                                if (measurePositions[m] >= 0 && selectedColumns[measurePositions[m]] == measureCol) {
                                    measurePos = measurePositions[m];
                                    break;
                                }
                            }
                            if (measurePos >= 0) {
                                double val = row[measurePos];
                                if (!Double.isNaN(val)) {
                                    StatsAccumulator q0sa = cr.q0Stats.get(measureCol);
                                    if (q0sa == null) {
                                        q0sa = new StatsAccumulator();
                                        cr.q0Stats.put(measureCol, q0sa);
                                    }
                                    q0sa.add(val);
                                }
                            }
                        }
                    }

                    // Write to the appropriate bucket
                    int bucket = tileIdx / tilesPerBucket;
                    ByteBuffer buf = bucketBufs[bucket];
                    if (buf.remaining() < BUCKET_RECORD_BYTES) {
                        buf.flip();
                        while (buf.hasRemaining()) bucketChannels[bucket].write(buf);
                        buf.clear();
                    }
                    buf.putDouble(x);
                    buf.putDouble(y);
                    buf.putLong(offset);
                    buf.putInt(tileIdx);

                    validCount++;
                }

                // Flush all bucket buffers
                for (int b = 0; b < numBuckets; b++) {
                    ByteBuffer buf = bucketBufs[b];
                    if (buf.position() > 0) {
                        buf.flip();
                        while (buf.hasRemaining()) bucketChannels[b].write(buf);
                    }
                }

                cr.validCount = validCount;
                cr.maxRowLength = reader.maxRowLength();
            }

        } catch (Throwable t) {
            cr.error = t;
        } finally {
            for (FileChannel ch : bucketChannels) {
                closeQuietly(ch);
            }
        }

        return cr;
    }

    // ======================================================================
    //  Merge: bucket path — zero-copy, files stay on disk
    // ======================================================================

    /**
     * Builds a ScanResult for the bucket path.  No point data is loaded to heap;
     * the bucket files remain on disk and their location is communicated via
     * {@link ScanResult#bucketDir}.
     */
    private ScanResult buildBucketResult(ChunkResult[] results, int totalValid,
                                         long maxRowLen, int numBuckets, int tilesPerBucket,
                                         String scanPath) {
        // Preserve per-thread tile counts for parallel partition
        int[][] perThreadTileCounts = new int[results.length][];
        for (int t = 0; t < results.length; t++) {
            perThreadTileCounts[t] = results[t].tileCounts.clone();
        }

        int[] globalCounts = mergePerTileCounts(results);
        StatsAccumulator[][] globalStats = new StatsAccumulator[numTiles][measureCount];
        int[][] globalPointCounts = new int[numTiles][measureCount];
        mergePerTileStats(results, globalStats, globalPointCounts);
        
        Map<Integer, StatsAccumulator> globalQ0Stats = mergeQ0Stats(results);

        return new ScanResult(totalValid, maxRowLen,
                globalCounts, globalStats, globalPointCounts,
                scanPath,
                tmpDir, numBuckets, tilesPerBucket, results.length,
                perThreadTileCounts, globalQ0Stats);
    }

    /**
     * The spill buffer interleaves x, y, offset, tileId per row.
     * This helper writes the accumulated data into separate per-attribute channels.
     */
    private static void flushSpillBuffers(ByteBuffer interleaved,
                                          FileChannel chXs, FileChannel chYs,
                                          FileChannel chOff, FileChannel chTid,
                                          int rowsSoFar) throws IOException {
        interleaved.flip();
        int bytesPerRow = 24 + IndexConfig.TILE_ID_BYTES; // 8+8+8+TILE_ID_BYTES
        int rows = interleaved.remaining() / bytesPerRow;
        // De-interleave into per-attribute temp buffers
        ByteBuffer bXs = ByteBuffer.allocate(rows * 8).order(ByteOrder.nativeOrder());
        ByteBuffer bYs = ByteBuffer.allocate(rows * 8).order(ByteOrder.nativeOrder());
        ByteBuffer bOff = ByteBuffer.allocate(rows * 8).order(ByteOrder.nativeOrder());
        ByteBuffer bTid = ByteBuffer.allocate(rows * IndexConfig.TILE_ID_BYTES).order(ByteOrder.nativeOrder());
        for (int i = 0; i < rows; i++) {
            bXs.putDouble(interleaved.getDouble());
            bYs.putDouble(interleaved.getDouble());
            bOff.putLong(interleaved.getLong());
            bTid.putInt(interleaved.getInt());
        }
        bXs.flip();
        bYs.flip();
        bOff.flip();
        bTid.flip();
        while (bXs.hasRemaining()) chXs.write(bXs);
        while (bYs.hasRemaining()) chYs.write(bYs);
        while (bOff.hasRemaining()) chOff.write(bOff);
        while (bTid.hasRemaining()) chTid.write(bTid);
    }

    // ======================================================================
    //  Merge: in-memory path
    // ======================================================================

    /**
     * Adopts thread-local arrays as chunks (zero-copy).
     * Each thread's trimmed backing array becomes one chunk in the result,
     * avoiding the merge allocation that caused OOM on tight heaps.
     * Peak memory stays at 24N (the arrays already held during scan).
     */
    private ScanResult adoptChunks(ChunkResult[] results, int totalValid, long maxRowLen,
                                   String scanPath) {
        int numChunks = results.length;
        double[][] xsChunks = new double[numChunks][];
        double[][] ysChunks = new double[numChunks][];
        long[][] offsetsChunks = new long[numChunks][];
        int[][] tileIdChunks = new int[numChunks][];
        int[] chunkSizes = new int[numChunks];

        for (int i = 0; i < numChunks; i++) {
            ChunkResult cr = results[i];
            if (cr.validCount > 0) {
                xsChunks[i] = cr.localXs.elements();
                ysChunks[i] = cr.localYs.elements();
                offsetsChunks[i] = cr.localOffsets.elements();
                tileIdChunks[i] = cr.localTileIds.elements();
            } else {
                xsChunks[i] = new double[0];
                ysChunks[i] = new double[0];
                offsetsChunks[i] = new long[0];
                tileIdChunks[i] = new int[0];
            }
            chunkSizes[i] = cr.validCount;
            cr.localXs = null;
            cr.localYs = null;
            cr.localOffsets = null;
            cr.localTileIds = null;
        }

        return buildResult(xsChunks, ysChunks, offsetsChunks, tileIdChunks, chunkSizes,
                totalValid, maxRowLen, results, scanPath);
    }

    // ======================================================================
    //  Merge: disk-streaming path — keep file references, merge stats only
    // ======================================================================

    /**
     * Builds a ScanResult for the disk-streaming path (Path B).
     * Point data stays in per-thread temp files; only per-tile stats are merged.
     * The file paths are passed through to SharedPointStore for direct scatter
     * during partition — avoiding the old mergeDisk heap round-trip.
     */
    private ScanResult buildDiskChunkResult(ChunkResult[] results, int totalValid,
                                            long maxRowLen, String scanPath) {
        int numChunks = results.length;
        Path[] xsFiles = new Path[numChunks];
        Path[] ysFiles = new Path[numChunks];
        Path[] offsetsFiles = new Path[numChunks];
        Path[] tileIdFiles = new Path[numChunks];
        int[] chunkSizes = new int[numChunks];

        for (int i = 0; i < numChunks; i++) {
            ChunkResult cr = results[i];
            xsFiles[i] = cr.tmpXs;
            ysFiles[i] = cr.tmpYs;
            offsetsFiles[i] = cr.tmpOffsets;
            tileIdFiles[i] = cr.tmpTileIds;
            chunkSizes[i] = cr.validCount;
            // Null out so cleanup in error paths doesn't double-delete
            cr.tmpXs = null;
            cr.tmpYs = null;
            cr.tmpOffsets = null;
            cr.tmpTileIds = null;
        }

        int[] globalCounts = mergePerTileCounts(results);
        StatsAccumulator[][] globalStats = new StatsAccumulator[numTiles][measureCount];
        int[][] globalPointCounts = new int[numTiles][measureCount];
        mergePerTileStats(results, globalStats, globalPointCounts);
        
        Map<Integer, StatsAccumulator> globalQ0Stats = mergeQ0Stats(results);

        return new ScanResult(xsFiles, ysFiles, offsetsFiles, tileIdFiles, chunkSizes,
                totalValid, maxRowLen,
                globalCounts, globalStats, globalPointCounts, scanPath, globalQ0Stats);
    }

    // ======================================================================
    //  Merge: combine per-tile counts and stats
    // ======================================================================

    private int[] mergePerTileCounts(ChunkResult[] results) {
        int[] globalCounts = new int[numTiles];
        for (ChunkResult cr : results) {
            for (int t = 0; t < numTiles; t++) {
                globalCounts[t] += cr.tileCounts[t];
            }
        }
        return globalCounts;
    }

    private void mergePerTileStats(ChunkResult[] results,
                                   StatsAccumulator[][] globalStats, int[][] globalPointCounts) {
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
    }

    private Map<Integer, StatsAccumulator> mergeQ0Stats(ChunkResult[] results) {
        Map<Integer, StatsAccumulator> globalQ0Stats = null;
        for (ChunkResult cr : results) {
            if (cr.q0Stats != null && !cr.q0Stats.isEmpty()) {
                if (globalQ0Stats == null) {
                    globalQ0Stats = new HashMap<>();
                }
                for (Map.Entry<Integer, StatsAccumulator> entry : cr.q0Stats.entrySet()) {
                    Integer measureCol = entry.getKey();
                    StatsAccumulator local = entry.getValue();
                    if (local == null || local.count() == 0) continue;
                    StatsAccumulator global = globalQ0Stats.get(measureCol);
                    if (global == null) {
                        global = new StatsAccumulator();
                        globalQ0Stats.put(measureCol, global);
                    }
                    global.addAll(local.snapshot());
                }
            }
        }
        return globalQ0Stats;
    }

    private ScanResult buildResult(double[][] xsChunks, double[][] ysChunks, long[][] offsetsChunks,
                                   int[][] tileIdChunks,
                                   int[] chunkSizes, int totalValid, long maxRowLen,
                                   ChunkResult[] results, String scanPath) {
        int[] globalCounts = mergePerTileCounts(results);

        StatsAccumulator[][] globalStats = new StatsAccumulator[numTiles][measureCount];
        int[][] globalPointCounts = new int[numTiles][measureCount];
        mergePerTileStats(results, globalStats, globalPointCounts);
        
        Map<Integer, StatsAccumulator> globalQ0Stats = mergeQ0Stats(results);

        return new ScanResult(xsChunks, ysChunks, offsetsChunks, tileIdChunks, chunkSizes,
                totalValid, maxRowLen, globalCounts, globalStats, globalPointCounts, scanPath, globalQ0Stats);
    }

    // ======================================================================
    //  Utilities
    // ======================================================================

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
