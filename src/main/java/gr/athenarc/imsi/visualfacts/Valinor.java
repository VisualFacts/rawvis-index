package gr.athenarc.imsi.visualfacts;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openjdk.jol.info.GraphLayout;

import com.google.common.collect.Range;
import com.google.common.math.Stats;
import com.google.common.math.StatsAccumulator;

import gr.athenarc.imsi.visualfacts.init.InitializationPolicy;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults.SamplingStatus;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.XContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.XYContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.YContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.io.RandomAccessRowReader;

/**
 * Unified Valinor index supporting both exact and approximate query modes.
 * <p>
 * When {@code errorThreshold <= 0}, runs in exact mode: reads all points from
 * multi-round sampling, confidence intervals, and error bounds.
 */
public class Valinor implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(Valinor.class);

    private boolean isInitialized = false;

    // Pipelined batch reader — opened lazily on first query, reused for the lifetime of the index
    private RandomAccessRowReader batchReader;

    // Maximum row length (bytes) observed during init scan — used to size io_uring read buffers
    private int maxRowLength;

    private Grid grid;

    private Schema schema;

    private SharedPointStore pointStore;

    private InitializationPolicy initializationPolicy;

    private int objectsIndexed = 0;

    private double errorThreshold = 0;

    private long samplingSeed = 0L;

    private long approximateQueryOrdinal = 0L;

    /**
     * When true, disables all aggregate metadata reuse:
     * - No frozen stats short-circuit
     * - No FC+Stats (complete leaf stats) reuse
     * - No sampledTracker persistence across queries
     * - No tile stats updates during sampling
     * This mode implements the VALINOR-S baseline: plain incremental sampling
     * over the VALINOR spatial index without precomputed aggregate metadata.
     */
    private boolean samplingOnly = false;

    /**
     * Grid initialization mode.
     * <ul>
     *   <li>{@code null} — uniform grid: a regular RESOLUTION×RESOLUTION grid
     *       with no sub-tiling bias.</li>
     *   <li>{@code "queryBiased"} — query-biased grid: uses
     *       {@link InitializationPolicy} to place denser sub-tiles near the
     *       initial query (q0) region based on a 2D normal distribution.</li>
     * </ul>
     */
    private String initMode;

    // Global statistics per measure column, computed during initialization (approximate mode only)
    private StatsAccumulator[] globalMeasureStats;

    /**
     * Optional outlier-aware AQP support (set only when {@code IndexConfig.OUTLIER_K > 0}
     * and we are in approximate mode).  When non-null, query-time CI methods
     * subtract per-tile outliers from the sampling population and add their
     * exact (closed-form) sums to the deterministic bucket.  When null, all
     * outlier-related code paths are bypassed entirely \u2014 behavior is then
     * byte-identical to the pre-outlier system.
     */
    private OutlierIndex outlierIndex;

    // Init timing breakdown (phase name → value)
    private java.util.LinkedHashMap<String, Object> initTimingBreakdown;

    /** Valid values for {@link #initMode}. */
    public static final String INIT_MODE_QUERY_BIASED = "queryBiased";

    /**
     * Creates a Valinor index in exact mode with a uniform grid.
     */
    public Valinor(Schema schema) {
        this(schema, 0, false, null);
    }

    /**
     * Creates a Valinor index. If errorThreshold &gt; 0, runs in approximate mode
     * with adaptive sampling. If errorThreshold &lt;= 0, runs in exact mode.
     * Uses a uniform grid.
     */
    public Valinor(Schema schema, double errorThreshold) {
        this(schema, errorThreshold, false, null);
    }

    /**
     * Creates a Valinor index.
     *
     * @param schema         dataset schema
     * @param errorThreshold if &gt; 0, approximate mode; otherwise exact mode
     * @param samplingOnly   if true, disables aggregate metadata reuse (VALINOR-S)
     * @param initMode       grid initialization mode: {@code null} for uniform,
     *                       {@code "queryBiased"} for query-biased sub-tiling.
     *                       Any other value throws {@link IllegalArgumentException}.
     */
    public Valinor(Schema schema, double errorThreshold, boolean samplingOnly, String initMode) {
        this(schema, errorThreshold, samplingOnly, initMode, 0L);
    }

    public Valinor(Schema schema, double errorThreshold, boolean samplingOnly, String initMode,
            long samplingSeed) {
        this.schema = schema;
        this.errorThreshold = errorThreshold;
        this.samplingOnly = samplingOnly;
        this.samplingSeed = samplingSeed;
        if (initMode != null && !INIT_MODE_QUERY_BIASED.equalsIgnoreCase(initMode)) {
            throw new IllegalArgumentException(
                    "Unknown initMode '" + initMode + "'. Valid values: null (uniform), '" + INIT_MODE_QUERY_BIASED + "'");
        }
        this.initMode = initMode;
    }

    public boolean isExactMode() {
        return errorThreshold <= 0;
    }

    public long getSamplingSeed() {
        return samplingSeed;
    }

    public String getInitMode() {
        return initMode;
    }

    public void generateGrid(Query q0) {
        if (isInitialized) {
            throw new IllegalStateException("The index is already initialized");
        }

        if (q0 != null && INIT_MODE_QUERY_BIASED.equalsIgnoreCase(initMode)) {
            initializationPolicy = new InitializationPolicy(q0,
                    (int) (RESOLUTION * RESOLUTION * SUBTILE_RATIO), schema);
        }
        LOG.debug("Generating initial grid with resolution " + RESOLUTION + "x" + RESOLUTION);
        grid = new Grid(initializationPolicy, schema.getBounds(), RESOLUTION);
        grid.split();
    }

    /**
     * Initializes the index from the CSV file.
     * <p>
     * <b>Init flow overview (3 adaptive paths):</b>
     * <pre>
     *  Phase 1 — Parallel CSV scan (ParallelCsvScanner):
     *    Path A (in-memory):       heap chunks, used when 32N+meta AND 36N fit in 85% heap
     *    Path B (disk-streaming):  per-thread temp files, used when scan or partition peak
     *                              exceeds 85% heap but steady-state 24N fits
     *    Path C (bucket-streaming): per-bucket files for mmap, used when 24N exceeds 85% heap
     *
     *  Phase 1.5 — Prefix sums from per-tile counts
     *
     *  Phase 2 — Partition (SharedPointStore.partition):
     *    Path A: parallel histogram scatter from T heap chunks      (peak 36N)
     *    Path B: parallel histogram scatter from T disk files       (peak 28N)
     *    Path C: counting-sort + O_DIRECT write into mmap file    (peak ~T×maxBucketSize×24)
     *
     *  Phase 3 — Wire tiles to shared store slices
     * </pre>
     */
    public QueryResults initialize(Query q0) {
        long initOverallStart = System.nanoTime();
        generateGrid(q0);

        List<DataValidationFilter> validationFilters = schema.getValidationFilters();

        HashSet<Integer> colIndexes = new HashSet<>();

        colIndexes.add(schema.getxColumn());
        colIndexes.add(schema.getyColumn());
        validationFilters.forEach(filter -> colIndexes.add(filter.getFilterColumn()));

        colIndexes.addAll(schema.getMeasureCols());

        LOG.debug("Columns to be read: " + colIndexes);

        int[] selectedColumns = colIndexes.stream().mapToInt(Integer::intValue).toArray();
        Map<Integer, Integer> colIndexToRowPos = new HashMap<>();
        // Build mapping from original column index to position in selectedColumns.
        for (int i = 0; i < selectedColumns.length; i++) {
            colIndexToRowPos.put(selectedColumns[i], i);
        }

        objectsIndexed = 0;

        final int xPos = colIndexToRowPos.get(schema.getxColumn());
        final int yPos = colIndexToRowPos.get(schema.getyColumn());

        List<Integer> measureCols = schema.getMeasureCols();
        final int measureCount = measureCols.size();
        final int[] measurePositions = new int[measureCount];
        for (int i = 0; i < measureCount; i++) {
            Integer mc = measureCols.get(i);
            Integer pos = colIndexToRowPos.get(mc);
            measurePositions[i] = (pos != null) ? pos : -1;
        }

        final int filterCount = validationFilters.size();
        final int[] filterPositions = new int[filterCount];
        final DataValidationFilter[] filterArray = new DataValidationFilter[filterCount];
        for (int i = 0; i < filterCount; i++) {
            DataValidationFilter f = validationFilters.get(i);
            filterPositions[i] = colIndexToRowPos.get(f.getFilterColumn());
            filterArray[i] = f;
        }

        // Build tile index mapping for partitioning.
        List<?> leafTileList = grid.getLeafTiles();
        int numTiles = leafTileList.size();
        if (numTiles > Integer.MAX_VALUE) {
            throw new IllegalStateException("Tile count " + numTiles + " exceeds int range; cannot use int[] tileIds");
        }
        IdentityHashMap<Tile, Integer> tileIndexMap = new IdentityHashMap<>(numTiles);
        for (int t = 0; t < numTiles; t++) {
            tileIndexMap.put((Tile) leafTileList.get(t), t);
        }

        final int capacity = schema.getObjectCount();
        final int availableThreads = Runtime.getRuntime().availableProcessors();
        final int scanThreads = Math.max(1, availableThreads);

        // Resolve mmap/tmp dirs early so tmpDir can be passed to the scanner.
        // Clean up tmpDir before timing starts.
        String mmapDirProp = System.getProperty("valinor.mmap.dir", "/tmp");
        java.nio.file.Path mmapDir = java.nio.file.Paths.get(mmapDirProp);
        java.nio.file.Path tmpDir = mmapDir.resolve("valinor_tmp");
        try {
            if (java.nio.file.Files.exists(tmpDir)) {
                try (var walk = java.nio.file.Files.walk(tmpDir)) {
                    walk.sorted(java.util.Comparator.reverseOrder())
                        .map(java.nio.file.Path::toFile)
                        .forEach(java.io.File::delete);
                }
            }
            java.nio.file.Files.createDirectories(tmpDir);
        } catch (IOException e) {
            LOG.warn("Failed to prepare tmpDir {}: {}", tmpDir, e.getMessage());
            tmpDir = null;
        }

        Map<Integer, StatsAccumulator> q0StatsFromScan = null;

        // Outlier-aware AQP is built only for approximate queries with K > 0.
        // Exact mode stays on the same path when the feature is disabled.
        OutlierIndex outlierIndexLocal = null;
        if (!isExactMode() && OUTLIER_K > 0 && measureCount > 0) {
            outlierIndexLocal = new OutlierIndex(OUTLIER_K, measureCount, scanThreads);
            LOG.info("Outlier-aware AQP enabled: K={}, measures={}, threads={}",
                    OUTLIER_K, measureCount, scanThreads);
        }
        final OutlierIndex outlierIndex = outlierIndexLocal;

        try {
            long phase1Start = System.nanoTime();

            // Phase 1: parallel CSV scan into merged arrays plus per-tile counts/stats.
            ParallelCsvScanner scanner = new ParallelCsvScanner(
                    new File(schema.getCsv()), schema.getDelimiter(), schema.getHasHeader(),
                    selectedColumns, xPos, yPos,
                    filterPositions, filterArray, measurePositions,
                    grid.getBounds(), grid, tileIndexMap, numTiles,
                    scanThreads, capacity, schema.getNullstr(), tmpDir, q0,
                    outlierIndex);

            ParallelCsvScanner.ScanResult scanResult = scanner.scan();
            long scanEndNanos = System.nanoTime();

            int validCount = scanResult.validCount;
            objectsIndexed = validCount;
            this.maxRowLength = (int) scanResult.maxRowLength;
            LOG.info("Max row length observed during init: {} bytes", maxRowLength);

            SharedPointStore store;
            if (scanResult.bucketMode) {
                // Path C: bucket-mmap scatter per-thread bucket files into mmap arrays.
                LOG.info("Using bucket-mmap partition path ({} buckets), mmap dir: {}",
                        scanResult.numBuckets, mmapDir);
                store = SharedPointStore.createForBucketMmap(
                        validCount, scanResult.bucketDir,
                        scanResult.numBuckets, scanResult.tilesPerBucket,
                        scanResult.numScanThreads, mmapDir,
                        scanResult.perThreadTileCounts);
            } else if (scanResult.diskXsFiles != null) {
                // Path B: disk-chunk scatter directly from per-thread scan files.
                LOG.info("Using disk-scatter partition path ({} chunks)", scanResult.diskChunkSizes.length);
                store = SharedPointStore.createForDiskChunks(
                        validCount, scanResult.diskXsFiles, scanResult.diskYsFiles,
                        scanResult.diskOffsetsFiles, scanResult.diskTileIdFiles,
                        scanResult.diskChunkSizes);
            } else {
                // Path A: in-memory chunked arrays on heap.
                store = new SharedPointStore(
                        scanResult.xsChunks, scanResult.ysChunks, scanResult.offsetsChunks,
                        scanResult.tileIdChunks, scanResult.chunkSizes, validCount);
            }

            // Wire per-tile counts and stats from the parallel scan into tiles.
            for (int t = 0; t < numTiles; t++) {
                int count = scanResult.tileCounts[t];
                if (count > 0) {
                    Tile tile = (Tile) leafTileList.get(t);
                    tile.setSize(count);

                    if (measureCount > 0) {
                        tile.setPrebuiltStats(scanResult.tileStats[t],
                                scanResult.tileStatsPointCounts[t]);
                    }
                }
            }

            LOG.info("Phase 1 total (scan + merge): {} s",
                    String.format("%.3f", (System.nanoTime() - phase1Start) / 1e9));

                // Compute prefix sums from per-tile counts.
            int[] counts = scanResult.tileCounts;
            int[] starts = new int[numTiles];
            if (numTiles > 0) {
                starts[0] = 0;
                for (int t = 1; t < numTiles; t++) {
                    starts[t] = starts[t - 1] + counts[t - 1];
                }
            }
            long prefixSumEnd = System.nanoTime();

            String scanPath = scanResult.scanPath;
            q0StatsFromScan = scanResult.q0Stats;
            // Release scanner and large scan-result fields before partition allocates.
            scanResult = null;
            scanner = null;

            // Encourage G1 to uncommit empty regions before the partition phase.
            long gcStart = System.nanoTime();
            {
                Runtime rt = Runtime.getRuntime();
                long committedBefore = rt.totalMemory();
                long usedBefore = committedBefore - rt.freeMemory();
                System.gc();
                long committedAfter = rt.totalMemory();
                long usedAfter = committedAfter - rt.freeMemory();
                LOG.info("Pre-partition GC: committed {} MB → {} MB, used {} MB → {} MB",
                        committedBefore / (1024L * 1024), committedAfter / (1024L * 1024),
                        usedBefore / (1024L * 1024), usedAfter / (1024L * 1024));
            }
            long gcEndNanos = System.nanoTime();

            LOG.info("Partitioning {} points across {} tiles", validCount, numTiles);
            long partStart = System.nanoTime();
            store.partition(validCount, starts, numTiles);
            long partEndNanos = System.nanoTime();
            LOG.info("Partition done in {} s",
                    String.format("%.3f", (partEndNanos - partStart) / 1e9));

                // Phase 3: wire tiles to shared store slices.
            long wireStart = System.nanoTime();
            for (int t = 0; t < numTiles; t++) {
                Tile tile = (Tile) leafTileList.get(t);
                if (tile.getSize() > 0) {
                    tile.setSlice(store, starts[t], counts[t]);
                }
            }
            long wireEndNanos = System.nanoTime();

            this.pointStore = store;

            // Record init timing breakdown.
            initTimingBreakdown = new java.util.LinkedHashMap<>();
            initTimingBreakdown.put("scanPath", scanPath);
            initTimingBreakdown.put("partitionPath", store.getPartitionPath());
            initTimingBreakdown.put("mmapMode", store.isMmapMode() ? 1.0 : 0.0);
            initTimingBreakdown.put("scan", (scanEndNanos - phase1Start) / 1e9);
            initTimingBreakdown.put("prefixSum", (prefixSumEnd - scanEndNanos) / 1e9);
            initTimingBreakdown.put("gc", (gcEndNanos - gcStart) / 1e9);
            initTimingBreakdown.put("partition", (partEndNanos - partStart) / 1e9);
            initTimingBreakdown.put("wire", (wireEndNanos - wireStart) / 1e9);

        } catch (IOException e) {
            throw new RuntimeException("Unable to read CSV", e);
        }
        isInitialized = true;
        LOG.debug("Indexing Complete. Total Indexed Objects: " + objectsIndexed);

        {
            // Release transient partition memory back to the OS/page cache.
            Runtime rt = Runtime.getRuntime();
            long committedBefore = rt.totalMemory();
            System.gc();
            long committedAfter = rt.totalMemory();
            long usedAfter = committedAfter - rt.freeMemory();
            LOG.info("Post-init GC: committed {} MB → {} MB (freed {} MB to OS), live {} MB",
                    committedBefore / (1024L * 1024), committedAfter / (1024L * 1024),
                    (committedBefore - committedAfter) / (1024L * 1024),
                    usedAfter / (1024L * 1024));
            initTimingBreakdown.put("heapCommittedMB", (double) (committedAfter / (1024L * 1024)));
            initTimingBreakdown.put("heapLiveMB", (double) (usedAfter / (1024L * 1024)));
        }

        if (!isExactMode()) {
            long globalStatsStart = System.nanoTime();
            computeGlobalMeasureStats();
            if (initTimingBreakdown != null) {
                initTimingBreakdown.put("globalStats", (System.nanoTime() - globalStatsStart) / 1e9);
            }

            if (outlierIndex != null) {
                // Outlier-aware AQP phases: merge candidates, select top-K, then partition by tile.
                long outlierStart = System.nanoTime();
                long t0 = System.nanoTime();
                List<OutlierIndex.Candidate> pool = outlierIndex.mergeCandidates();
                long mergeNs = System.nanoTime() - t0;
                int poolSize = pool.size();
                t0 = System.nanoTime();
                outlierIndex.selectByScore(globalMeasureStats, pool);
                long greedyNs = System.nanoTime() - t0;
                t0 = System.nanoTime();
                outlierIndex.partitionByTile(grid.getLeafTiles());
                long partitionNs = System.nanoTime() - t0;
                this.outlierIndex = outlierIndex;
                if (initTimingBreakdown != null) {
                    initTimingBreakdown.put("outlierBuild", (System.nanoTime() - outlierStart) / 1e9);
                    initTimingBreakdown.put("outlierMerge", mergeNs / 1e9);
                    initTimingBreakdown.put("outlierSelect", greedyNs / 1e9);
                    initTimingBreakdown.put("outlierPartition", partitionNs / 1e9);
                    initTimingBreakdown.put("outlierPoolSize", (double) poolSize);
                    initTimingBreakdown.put("outlierSelected", (double) outlierIndex.getSelectedCount());
                    initTimingBreakdown.put("outlierReheapifies", (double) outlierIndex.getTotalReheapifies());
                    initTimingBreakdown.put("outlierReheapifyTime", outlierIndex.getTotalReheapifyNanos() / 1e9);
                    initTimingBreakdown.put("outlierMaxThreadReheapifies", (double) outlierIndex.getMaxThreadReheapifies());
                    initTimingBreakdown.put("outlierMaxSigmaChangePct", outlierIndex.getMaxReheapifySigmaChangePct());
                }
            }
        }

        // Record total init time.
        if (initTimingBreakdown != null) {
            initTimingBreakdown.put("total", (System.nanoTime() - initOverallStart) / 1e9);
        }

        // Create QueryResults and populate q0 stats if available.
        QueryResults queryResults;
        if (isExactMode()) {
            queryResults = new QueryResults(q0);
        } else {
            queryResults = new ApproximateQueryResults(q0);
        }

        if (q0StatsFromScan != null && q0 != null) {
            for (Map.Entry<Integer, StatsAccumulator> entry : q0StatsFromScan.entrySet()) {
                queryResults.adjustStats(entry.getKey(), entry.getValue().snapshot());
            }
            LOG.info("Q0 evaluation complete during initialization: {}", queryResults.getStats());
        }

        return queryResults;
    }

    public int getObjectsIndexed() {
        return objectsIndexed;
    }

    /**
     * Returns the init timing breakdown as a map of phase name to seconds.
     * Keys: "scan", "setup", "partition", "wire", "globalStats" (AQP only), "total".
     * Returns null if the index has not been initialized.
     */
    public java.util.LinkedHashMap<String, Object> getInitTimingBreakdown() {
        return initTimingBreakdown;
    }

    public synchronized QueryResults executeQuery(Query query) throws IOException {
        if (!isInitialized) {
            return initialize(query);
        }
        if (isExactMode()) {
            return executeExactQuery(query);
        } else {
            return executeApproximateQuery(query);
        }
    }

    // ==================== Exact Mode ====================

    private QueryResults executeExactQuery(Query query) throws IOException {
        Rectangle rect = query.getRect();
        QueryResults queryResults = new QueryResults(query);

        if (batchReader == null) {
            String ns = schema.getNullstr();
            byte[] nsBytes = (ns != null && !ns.isEmpty()) ? ns.getBytes(java.nio.charset.StandardCharsets.UTF_8) : null;
            batchReader = new RandomAccessRowReader(schema.getCsv(), maxRowLength, nsBytes);
        }

        List<AbstractNodePointIterator> rawIterators = new ArrayList<>();
        int fullyContainedWithStatsCount = 0;
        int fullyContainedWithoutStatsCount = 0;

        List<Tile> leafTiles = this.grid.getOverlappedLeafTiles(query);

        for (Tile leafTile : leafTiles) {
            // Short-circuited non-leaf tile with frozen exact stats
            if (leafTile.hasFrozenStats()) {
                fullyContainedWithStatsCount++;
                queryResults.addTotalCount(leafTile.getFrozenPointCount());
                query.getMeasureCols().forEach(measureCol -> {
                    queryResults.adjustStats(measureCol,
                            leafTile.getFrozenStats(schema.getMeasureIndex(measureCol)));
                });
                continue;
            }

            ContainmentExaminer containmentExaminer = getContainmentExaminer(leafTile, rect);
            boolean isFullyContained = containmentExaminer == null;

            List<QueryNode> queryNodes = leafTile.getQueryNodes(query, containmentExaminer, schema);
            int count = 0;
            for (QueryNode queryNode : queryNodes) {
                Tile qnTile = queryNode.getTile();
                if ((!isFullyContained
                        || query.getMeasureCols().stream().anyMatch(mc -> !qnTile.hasStats(schema.getMeasureIndex(mc))))
                        && qnTile.hasPoints()) {
                    count += qnTile.getSize();
                }
            }

            if (count > THRESHOLD) {
                leafTile.split();
                // After splitting, recompute the containment examiner per child:
                // a child of a partially-overlapping parent may itself be fully
                // contained in the query, in which case its QueryNode must report
                // isFullyContained()==true so the scan loop populates its stats.
                queryNodes = leafTile.getOverlappedActualLeafTiles(query).stream()
                        .flatMap(tile -> tile.getQueryNodes(query, getContainmentExaminer(tile, rect), schema).stream())
                        .collect(Collectors.toList());
            }

            for (QueryNode queryNode : queryNodes) {
                Tile qnTile = queryNode.getTile();
                boolean qnFullyContained = queryNode.isFullyContained();
                if (qnFullyContained && query.getMeasureCols().stream().allMatch(mc -> qnTile.hasStats(schema.getMeasureIndex(mc)))) {
                    fullyContainedWithStatsCount++;
                    queryResults.addTotalCount(queryNode.getIntersectionCount());
                    query.getMeasureCols().forEach(measureCol -> {
                        StatsAccumulator acc = queryNode.getTile().getStats(schema.getMeasureIndex(measureCol));
                        if (acc != null) {
                            queryResults.adjustStats(measureCol, acc.snapshot());
                        }
                    });
                } else {
                    if (qnFullyContained) {
                        fullyContainedWithoutStatsCount++;
                    }
                    rawIterators.add(new NodePointsIterator(queryNode));
                }
            }
        }

        // Prepare sorted measure column indices for fast extraction
        List<Integer> measureColsList = schema.getMeasureCols();
        int[] sortedMeasureCols = measureColsList.stream().mapToInt(Integer::intValue).sorted().toArray();

        // Build mapping from original column index to position in sorted array
        Map<Integer, Integer> measureColToExtractedPos = new HashMap<>();
        for (int i = 0; i < sortedMeasureCols.length; i++) {
            measureColToExtractedPos.put(sortedMeasureCols[i], i);
        }

        byte delimiterByte = (byte) schema.getDelimiter().charValue();

        // Read and process rows in fixed-size chunks to bound memory usage
        KWayMergePointIterator pointIterator = new KWayMergePointIterator(rawIterators);
        int chunkSize = RandomAccessRowReader.BATCH_SIZE;
        long[]       offsets = new long[chunkSize];
        QueryNode[]  nodes   = new QueryNode[chunkSize];
        int ioCount = 0;
        int measureCount = schema.getMeasureCount();

        while (pointIterator.hasNext()) {
            // Fill chunk from the sorted merge iterator
            int n = 0;
            while (n < chunkSize && pointIterator.hasNext()) {
                offsets[n] = pointIterator.nextOffset();
                nodes[n]   = pointIterator.getCurrentQueryNode();
                n++;
            }
            int batchRows = batchReader.readBatch(offsets, n, sortedMeasureCols, delimiterByte);
            ioCount += batchRows;

            // Distribute parsed values to query results and tile stats
            for (int rowIdx = 0; rowIdx < batchRows; rowIdx++) {
                QueryNode queryNode = nodes[rowIdx];
                Tile      qnTile    = queryNode.getTile();
                int       idx       = 0;
                for (Integer measureCol : measureColsList) {
                    Integer ep = measureColToExtractedPos.get(measureCol);
                    double  value = Double.NaN;
                    if (ep != null && batchReader.isPresent(rowIdx, ep)) {
                        value = batchReader.getValue(rowIdx, ep);
                    }
                    if (!Double.isNaN(value)) {
                        queryResults.adjustStats(measureCol, value);
                    }
                    if (queryNode.isFullyContained()) {
                        qnTile.adjustStats(idx, measureCount, value);
                    }
                    idx++;
                }
            }
        }

        queryResults.addTotalCount(ioCount);
        queryResults.setTileCount(leafTiles.size());
        queryResults.setFullyContainedTileCount(fullyContainedWithStatsCount);
        queryResults.setFullyContainedTileWithoutStatsCount(fullyContainedWithoutStatsCount);
        queryResults.setIoCount(ioCount);

        return queryResults;
    }

    // ==================== Approximate Mode ====================

    private ApproximateQueryResults executeApproximateQuery(Query query) throws IOException {
        Rectangle rect = query.getRect();

        ApproximateQueryResults queryResults = new ApproximateQueryResults(query);

        if (batchReader == null) {
            String ns = schema.getNullstr();
            byte[] nsBytes = (ns != null && !ns.isEmpty()) ? ns.getBytes(java.nio.charset.StandardCharsets.UTF_8) : null;
            batchReader = new RandomAccessRowReader(schema.getCsv(), maxRowLength, nsBytes);
        }
        List<QueryNode> nonRawNodes = new ArrayList<>();

        List<Tile> leafTiles = samplingOnly
                ? this.grid.getOverlappedActualLeafTiles(query)
                : this.grid.getOverlappedLeafTiles(query);

        List<QueryNode> fullyContainedNodesWithStats = new ArrayList<>();
        List<QueryNode> fullyContainedNodesWithoutStats = new ArrayList<>();
        List<QueryNode> partialNodes = new ArrayList<>();

        int frozenStatsTileCount = 0;

        for (Tile leafTile : leafTiles) {
            // Short-circuited non-leaf tile with frozen exact stats.
            if (!samplingOnly && leafTile.hasFrozenStats()) {
                frozenStatsTileCount++;
                queryResults.addTotalCount(leafTile.getFrozenPointCount());
                query.getMeasureCols().forEach(measureCol -> {
                    Stats frozen = leafTile.getFrozenStats(schema.getMeasureIndex(measureCol));
                    if (frozen != null) {
                        queryResults.adjustStats(measureCol, frozen);
                    }
                });
                continue;
            }

            ContainmentExaminer containmentExaminer = getContainmentExaminer(leafTile, rect);
            boolean isFullyContained = containmentExaminer == null;

            List<QueryNode> queryNodes = leafTile.getQueryNodes(query, containmentExaminer, schema);
            for (QueryNode queryNode : queryNodes) {
                Tile qnTile = queryNode.getTile();
                if (qnTile.getSize() == 0) {
                    continue;
                }

                if (isFullyContained && !samplingOnly
                        && query.getMeasureCols().stream().allMatch(mc -> qnTile.hasStats(schema.getMeasureIndex(mc)))) {
                    fullyContainedNodesWithStats.add(queryNode);
                } else if (!isFullyContained && qnTile.getSize() > THRESHOLD) {
                    leafTile.split();
                    // Recompute the containment examiner per child after the split.
                    // A child of a partially overlapping parent may itself be fully contained.
                    leafTile.getOverlappedActualLeafTiles(query).stream()
                            .flatMap(tile -> tile.getQueryNodes(query, getContainmentExaminer(tile, rect), schema).stream())
                            .forEach(qn -> {
                                if (qn.isFullyContained()) {
                                    fullyContainedNodesWithoutStats.add(qn);
                                } else {
                                    partialNodes.add(qn);
                                }
                            });
                } else {
                    if (isFullyContained) {
                        fullyContainedNodesWithoutStats.add(queryNode);
                    } else {
                        partialNodes.add(queryNode);
                    }
                }
            }
        }
        for (QueryNode queryNode : fullyContainedNodesWithStats) {
            queryResults.addTotalCount(queryNode.getIntersectionCount());
            query.getMeasureCols().forEach(measureCol -> {
                StatsAccumulator acc = queryNode.getTile().getStats(schema.getMeasureIndex(measureCol));
                if (acc != null) {
                    queryResults.adjustStats(measureCol, acc.snapshot());
                }
            });
            nonRawNodes.add(queryNode);
        }

        // Prepare sorted measure column indices for fast extraction.
        List<Integer> measureColsList = schema.getMeasureCols();
        int[] sortedMeasureCols = measureColsList.stream().mapToInt(Integer::intValue).sorted().toArray();

        Map<Integer, Integer> measureColToExtractedPos = new HashMap<>();
        for (int i = 0; i < sortedMeasureCols.length; i++) {
            measureColToExtractedPos.put(sortedMeasureCols[i], i);
        }

        byte delimiterByte = (byte) schema.getDelimiter().charValue();

        int ioCount = 0;

        List<QueryNode> samplingNodes = new ArrayList<>();
        samplingNodes.addAll(partialNodes);
        samplingNodes.addAll(fullyContainedNodesWithoutStats);

        // Total count is exact from the query-node geometry plus any in-query outliers.
        for (QueryNode qn : samplingNodes) {
            queryResults.addTotalCount(qn.getIntersectionCount());
            BitSet inQuery = qn.getInQueryOutliers();
            if (inQuery != null) {
                queryResults.addTotalCount(inQuery.cardinality());
            }
        }

        // ---- Stratified Neyman+FPC sample allocation ----
        // Compute per-measure exact contributions (frozen-stats tiles +
        // FC-with-stats tiles + in-query outliers) so the allocator can size
        // V* = (eps * |T_anticipated| / z)^2 correctly.  Larger exact part
        // ⇒ larger V* ⇒ smaller per-stratum sample sizes.
        Map<Integer, Double> exactSumPerMeasure = new HashMap<>();
        Map<Integer, Long>   exactCountPerMeasure = new HashMap<>();
        for (Integer measureCol : query.getMeasureCols()) {
            double exactSum = 0.0;
            long   exactCount = 0L;
            if (queryResults.getStats().containsKey(measureCol)) {
                Stats st = queryResults.getStats().get(measureCol);
                exactSum   = st.sum();
                exactCount = st.count();
            }
            if (outlierIndex != null) {
                int midx = schema.getMeasureIndex(measureCol);
                exactSum   += sumInQueryOutliers(samplingNodes, midx);
                exactCount += countInQueryOutliers(samplingNodes, midx);
            }
            exactSumPerMeasure.put(measureCol, exactSum);
            exactCountPerMeasure.put(measureCol, exactCount);
        }

        SampleAllocator allocator = new SampleAllocator(schema, outlierIndex,
                globalMeasureStats, errorThreshold, getZScoreForConfidence(0.95));
        Map<QueryNode, Integer> targetSamples = allocator.planInitial(
                samplingNodes, query, exactSumPerMeasure, exactCountPerMeasure);

        Map<Integer, double[]> sumConfidenceIntervals = new HashMap<>();
        Map<Integer, double[]> countConfidenceIntervals = new HashMap<>();
        Map<Integer, double[]> meanConfidenceIntervals = new HashMap<>();
        Map<Integer, Double> errorBounds = new HashMap<>();
        Map<Integer, Double> sumErrorBounds = new HashMap<>();
        Map<Integer, Double> countErrorBounds = new HashMap<>();
        Map<Integer, Double> meanErrorBounds = new HashMap<>();
        int samplingRounds = 0;
        int adaptiveRounds = 0;
        int consecutiveLowImprovementRounds = 0;
        double previousAdaptiveError = Double.NaN;
        double preExactificationErrorBound = Double.NaN;
        String samplingStopReason = "converged";
        final int minAdaptiveRounds = 2;
        final double minRelativeImprovement = 0.05;
        final int stallPatience = 3;
        final double largeDeltaFraction = 0.50;
        final double meanInflationSafety = 1.10;
        // Includes the final exactification pass. We reserve the last round for
        // exactifying all residual sampling nodes in one K-way pass, so adaptive
        // sampling cannot keep issuing random reads indefinitely.
        final int maxSamplingRounds = 10;
        final int maxAdaptiveRoundsBeforeExactification = maxSamplingRounds - 1;
        final long querySeedOrdinal = approximateQueryOrdinal++;
        boolean converged = false;
        boolean exactificationFallback = false;
        SamplingStatus samplingStatus = SamplingStatus.EXHAUSTED_UNCONVERGED;
        do {
            samplingRounds++;
            if (!exactificationFallback) {
                adaptiveRounds++;
            }
            // Create per-node sampling iterators with absolute target counts.
            // Each iterator self-deduplicates against the node's sampledTracker,
            // so passing the same cumulative target on every round is safe.
            final Map<QueryNode, Integer> targetsRef = targetSamples;
            List<SamplingNodePointsIterator> samplingIterators = new ArrayList<>(samplingNodes.size());
            for (int nodeOrdinal = 0; nodeOrdinal < samplingNodes.size(); nodeOrdinal++) {
                QueryNode queryNode = samplingNodes.get(nodeOrdinal);
                int target = targetsRef.getOrDefault(queryNode, 0);
                long iteratorSeed = samplingIteratorSeed(querySeedOrdinal, samplingRounds, nodeOrdinal, queryNode, target);
                samplingIterators.add(new SamplingNodePointsIterator(queryNode, target, iteratorSeed));
            }
            KWayMergePointIterator pointIterator = new KWayMergePointIterator(samplingIterators);

            // Read and process sampled rows in fixed-size chunks
            int chunkSize = RandomAccessRowReader.BATCH_SIZE;
            long[]       offsets = new long[chunkSize];
            QueryNode[]  nodes   = new QueryNode[chunkSize];
            int measureCount = schema.getMeasureCount();

            while (pointIterator.hasNext()) {
                int n = 0;
                while (n < chunkSize && pointIterator.hasNext()) {
                    offsets[n] = pointIterator.nextOffset();
                    nodes[n]   = pointIterator.getCurrentQueryNode();
                    n++;
                }
                int batchRows = batchReader.readBatch(offsets, n, sortedMeasureCols, delimiterByte);
                ioCount += batchRows;

                // Distribute parsed values to per-tile sample accumulators
                for (int rowIdx = 0; rowIdx < batchRows; rowIdx++) {
                    QueryNode queryNode = nodes[rowIdx];
                    Tile      qnTile    = queryNode.getTile();
                    int       idx       = 0;
                    for (Integer measureCol : measureColsList) {
                        Integer ep = measureColToExtractedPos.get(measureCol);
                        double  value = Double.NaN;
                        if (ep != null && batchReader.isPresent(rowIdx, ep)) {
                            value = batchReader.getValue(rowIdx, ep);
                        }
                        if (!Double.isNaN(value)) {
                            queryNode.addSampleValue(measureCol, value);
                        }
                        if (!samplingOnly && queryNode.isFullyContained()) {
                            qnTile.adjustStats(idx, measureCount, value);
                        }
                        idx++;
                    }
                }
            }

            // Compute confidence intervals for all aggregate types and measures.
            // SUM CI uses the null-as-zero variance on continuous values.
            // COUNT CI uses smoothed Bernoulli variance so boundary samples
            // (all null / all non-null) still carry uncertainty.
            // MEAN CI uses the delta method on the ratio SUM/COUNT.
            double effRateForCI = SampleAllocator.effectiveSamplingRate(samplingNodes, targetSamples);
            for (Integer measureCol : query.getMeasureCols()) {
                sumConfidenceIntervals.put(measureCol,
                    getQuerySumConfidenceInterval(samplingNodes, queryResults, measureCol));
                countConfidenceIntervals.put(measureCol,
                    getQueryCountConfidenceInterval(samplingNodes, queryResults, measureCol));
                meanConfidenceIntervals.put(measureCol,
                    getQueryMeanConfidenceInterval(samplingNodes, queryResults, measureCol));
            }

            // Error bound per measure: max relative error across all aggregate types
            for (Integer measureCol : query.getMeasureCols()) {
                double sumError = calculateRelativeError(sumConfidenceIntervals.get(measureCol),
                    sumScaleFloor(measureCol, queryResults.getTotalCount()));
                double countError = calculateRelativeError(countConfidenceIntervals.get(measureCol),
                    countScaleFloor(queryResults.getTotalCount()));
                double meanError = calculateRelativeError(meanConfidenceIntervals.get(measureCol),
                    meanScaleFloor(measureCol));
                sumErrorBounds.put(measureCol, sumError);
                countErrorBounds.put(measureCol, countError);
                meanErrorBounds.put(measureCol, meanError);
                errorBounds.put(measureCol, Math.max(sumError, Math.max(countError, meanError)));
            }

            // Find the maximum error bound across all measures
            double maxErrorBound = errorBounds.values().stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
            converged = maxErrorBound <= errorThreshold;
            if (converged) {
                samplingStatus = exactificationFallback
                        ? SamplingStatus.EXACTIFIED_CONVERGED
                        : SamplingStatus.CONVERGED;
                samplingStopReason = exactificationFallback ? samplingStopReason : "converged";
                break;
            }

            // Adaptive escalation: re-plan with observed per-stratum sample
            // variances and observed (CI-midpoint) totals. Targets are
            // absolute cumulative node targets, not per-round deltas. A
            // "query-wide sampling round" therefore means rebuilding the
            // merged iterator for every sampling node, letting nodes whose
            // targets grew emit only their newly requested rows, and then
            // recomputing the global CIs from the expanded cumulative sample.
            if (allSamplingNodesFullySampled(samplingNodes)) {
                samplingStatus = SamplingStatus.EXHAUSTED_UNCONVERGED;
                samplingStopReason = "exhausted_unconverged";
                // Defensive branch: the current CI code short-circuits fully
                // sampled nodes into exact contributions with zero sampling
                // variance, so this path should normally have converged above.
                LOG.warn("Sampling exhausted all residual nodes after {} round(s) but did not converge " +
                    "(error={}, threshold={}). Returning best estimate.",
                    samplingRounds, maxErrorBound, errorThreshold);
                break;
            }

            if (exactificationFallback) {
                samplingStatus = SamplingStatus.EXHAUSTED_UNCONVERGED;
                samplingStopReason = "exactification_unconverged";
                LOG.warn("Single residual exactification pass did not converge after {} round(s) " +
                    "(error={}, threshold={}). Returning best estimate.",
                    samplingRounds, maxErrorBound, errorThreshold);
                break;
            }

            if (!Double.isNaN(previousAdaptiveError) && previousAdaptiveError > 0.0) {
                double relativeImprovement = (previousAdaptiveError - maxErrorBound) / previousAdaptiveError;
                if (adaptiveRounds >= minAdaptiveRounds && relativeImprovement < minRelativeImprovement) {
                    consecutiveLowImprovementRounds++;
                } else {
                    consecutiveLowImprovementRounds = 0;
                }
            }
            previousAdaptiveError = maxErrorBound;
            boolean lowImprovementStalled = consecutiveLowImprovementRounds >= stallPatience;

            if (adaptiveRounds >= maxAdaptiveRoundsBeforeExactification) {
                exactificationFallback = true;
                preExactificationErrorBound = maxErrorBound;
                samplingStopReason = "exactified_round_cap";
                targetSamples = exactifyResidualNodes(samplingNodes, targetSamples);
                LOG.warn("Adaptive sampling reached {} adaptive round(s) (max sampling rounds={}, " +
                    "error={}, threshold={}). Reserving the final round for residual exactification.",
                    adaptiveRounds, maxSamplingRounds, maxErrorBound, errorThreshold);
                continue;
            }

            Map<Integer, Double> obsSum = new HashMap<>();
            Map<Integer, Double> obsCount = new HashMap<>();
            for (Integer mc : query.getMeasureCols()) {
                double[] sCI = sumConfidenceIntervals.get(mc);
                double[] cCI = countConfidenceIntervals.get(mc);
                obsSum.put(mc, sCI != null ? (sCI[0] + sCI[1]) / 2.0
                                            : exactSumPerMeasure.getOrDefault(mc, 0.0));
                obsCount.put(mc, cCI != null ? (cCI[0] + cCI[1]) / 2.0
                                              : exactCountPerMeasure.getOrDefault(mc, 0L).doubleValue());
            }
            Map<QueryNode, Integer> nextTargets = allocator.planAdaptive(samplingNodes, query,
                    exactSumPerMeasure, exactCountPerMeasure, obsSum, obsCount);
            double maxMeanErrorBound = maxValue(meanErrorBounds);
            double maxSumCountErrorBound = Math.max(maxValue(sumErrorBounds), maxValue(countErrorBounds));
            boolean meanInflated = inflateTargetsForMeanDominatedError(samplingNodes, targetSamples, nextTargets,
                    maxMeanErrorBound, maxSumCountErrorBound, errorThreshold, meanInflationSafety);
            boolean targetIncreased = makeMonotoneAndDetectIncrease(samplingNodes, targetSamples, nextTargets);
            if (!targetIncreased) {
                targetIncreased = forceBoundedAdaptiveIncrease(samplingNodes, targetSamples, nextTargets);
                if (targetIncreased) {
                    LOG.trace("Adaptive allocation did not increase any cumulative target after {} round(s); " +
                        "issuing a bounded sampling nudge (error={}, threshold={}).",
                        samplingRounds, maxErrorBound, errorThreshold);
                }
            } else if (lowImprovementStalled) {
                if (forceBoundedAdaptiveIncrease(samplingNodes, targetSamples, nextTargets)) {
                    LOG.trace("Adaptive sampling had {} consecutive low-improvement round(s); " +
                        "raising cumulative targets with a bounded safety nudge (error={}, threshold={}).",
                        consecutiveLowImprovementRounds, maxErrorBound, errorThreshold);
                }
            }
            if (!targetIncreased) {
                exactificationFallback = true;
                preExactificationErrorBound = maxErrorBound;
                samplingStopReason = "exactified_no_target_progress";
                targetSamples = exactifyResidualNodes(samplingNodes, targetSamples);
                LOG.warn("Adaptive allocation stalled after {} round(s) (error={}, threshold={}). " +
                    "No bounded sampling nudge was possible; exactifying residual nodes in one pass.",
                    samplingRounds, maxErrorBound, errorThreshold);
                continue;
            }
            double plannedDeltaFraction = plannedDeltaFraction(samplingNodes, nextTargets);
            if (plannedDeltaFraction > largeDeltaFraction) {
                exactificationFallback = true;
                preExactificationErrorBound = maxErrorBound;
                samplingStopReason = "exactified_large_delta";
                targetSamples = exactifyResidualNodes(samplingNodes, targetSamples);
                LOG.warn("Adaptive allocation requested {} of the remaining residual rows after {} round(s) " +
                    "(error={}, threshold={}). Exactifying residual nodes in one pass.",
                    plannedDeltaFraction, samplingRounds, maxErrorBound, errorThreshold);
                continue;
            }
            LOG.trace("Round {}: error={} > threshold={}, re-planning (effRate {} → {}, meanInflated={})",
                samplingRounds, maxErrorBound, errorThreshold,
                effRateForCI,
                SampleAllocator.effectiveSamplingRate(samplingNodes, nextTargets), meanInflated);
            targetSamples = nextTargets;

        } while (!converged);

        double finalEffectiveRate = SampleAllocator.actualSamplingRate(samplingNodes);
        LOG.trace("Sampling completed in {} round(s), effective rate={}, I/Os={}",
            samplingRounds, finalEffectiveRate, ioCount);

        // Persist sampledTracker for future queries
        if (!samplingOnly) {
            fullyContainedNodesWithoutStats.forEach(queryNode -> {
                queryNode.getTile().setSampledTracker(queryNode.getSampledTracker());
            });
        }

        queryResults.setTileCount(leafTiles.size());
        queryResults.setFullyContainedTileCount(fullyContainedNodesWithStats.size() + frozenStatsTileCount);
        queryResults.setFullyContainedTileWithoutStatsCount(fullyContainedNodesWithoutStats.size());
        queryResults.setSamplingTileCount(samplingNodes.size());
        queryResults.setSamplingRounds(samplingRounds);
        queryResults.setSamplingRate(finalEffectiveRate);
        queryResults.setIoCount(ioCount);
        queryResults.setConverged(converged);
        queryResults.setSamplingStatus(samplingStatus);
        queryResults.setSamplingStopReason(samplingStopReason);
        queryResults.setPreExactificationErrorBound(preExactificationErrorBound);
        queryResults.setSamplingSeed(samplingSeed);

        queryResults.setSumConfidenceIntervals(sumConfidenceIntervals);
        queryResults.setCountConfidenceIntervals(countConfidenceIntervals);
        queryResults.setMeanConfidenceIntervals(meanConfidenceIntervals);
        queryResults.setErrorBounds(errorBounds);
        queryResults.setSumErrorBounds(sumErrorBounds);
        queryResults.setCountErrorBounds(countErrorBounds);
        queryResults.setMeanErrorBounds(meanErrorBounds);

        return queryResults;
    }

    /**
     * Clamp adaptive targets so they remain cumulative and monotone.
     * <p>
     * {@link SampleAllocator#planAdaptive(List, gr.athenarc.imsi.visualfacts.query.Query, Map, Map, Map, Map)}
     * returns desired absolute sample counts per node. Before using them we
     * enforce three invariants:
     * <ul>
     *   <li>never go below the previous target,</li>
     *   <li>never go below the number of rows already sampled, and</li>
     *   <li>never exceed the node population {@code N_h}.</li>
     * </ul>
      * The returned boolean answers the only question the outer loop cares
      * about: does the next plan ask for any new rows at all?
     */
    private boolean makeMonotoneAndDetectIncrease(List<QueryNode> samplingNodes,
            Map<QueryNode, Integer> currentTargets,
            Map<QueryNode, Integer> nextTargets) {
        boolean increased = false;
        for (QueryNode qn : samplingNodes) {
            int prev = currentTargets.getOrDefault(qn, 0);
            int alreadySampled = qn.getSampledPointCount();
            int floor = Math.max(prev, alreadySampled);
            int next = nextTargets.getOrDefault(qn, 0);
            int Nh = qn.getIntersectionCount();
            if (next < floor) next = floor;
            if (next > Nh) next = Nh;
            if (next > floor) increased = true;
            nextTargets.put(qn, next);
        }
        return increased;
    }

    private boolean inflateTargetsForMeanDominatedError(List<QueryNode> samplingNodes,
            Map<QueryNode, Integer> currentTargets,
            Map<QueryNode, Integer> nextTargets,
            double maxMeanErrorBound,
            double maxSumCountErrorBound,
            double threshold,
            double safety) {
        if (!(threshold > 0.0) || !(maxMeanErrorBound > threshold) || maxMeanErrorBound < maxSumCountErrorBound) {
            return false;
        }
        double ratio = maxMeanErrorBound / threshold;
        double targetFactor = Math.max(1.0, ratio * ratio * safety);
        boolean inflated = false;
        for (QueryNode qn : samplingNodes) {
            int Nh = qn.getIntersectionCount();
            int floor = Math.max(currentTargets.getOrDefault(qn, 0), qn.getSampledPointCount());
            int planned = nextTargets.getOrDefault(qn, floor);
            int base = Math.max(floor, planned);
            int inflatedTarget = (int) Math.ceil(base * targetFactor);
            if (inflatedTarget > Nh) inflatedTarget = Nh;
            if (inflatedTarget > planned) {
                nextTargets.put(qn, inflatedTarget);
                inflated = true;
            }
        }
        return inflated;
    }

    private boolean forceBoundedAdaptiveIncrease(List<QueryNode> samplingNodes,
            Map<QueryNode, Integer> currentTargets,
            Map<QueryNode, Integer> nextTargets) {
        boolean increased = false;
        for (QueryNode qn : samplingNodes) {
            int Nh = qn.getIntersectionCount();
            int floor = Math.max(currentTargets.getOrDefault(qn, 0), qn.getSampledPointCount());
            if (floor >= Nh) {
                nextTargets.put(qn, Nh);
                continue;
            }
            int nudge = Math.max(2, floor / 2);
            int boundedTarget = (int) Math.min((long) Nh, (long) floor + nudge);
            int planned = nextTargets.getOrDefault(qn, floor);
            int next = Math.max(planned, boundedTarget);
            if (next > Nh) next = Nh;
            if (next > floor) increased = true;
            nextTargets.put(qn, next);
        }
        return increased;
    }

    private double maxValue(Map<Integer, Double> values) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }
        return values.values().stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
    }

    private long samplingIteratorSeed(long queryOrdinal, int samplingRound, int nodeOrdinal, QueryNode qn,
            int targetSampleCount) {
        long seed = samplingSeed;
        seed ^= 0x9E3779B97F4A7C15L * (queryOrdinal + 1L);
        seed ^= 0xBF58476D1CE4E5B9L * (long) samplingRound;
        seed ^= 0x94D049BB133111EBL * (long) (nodeOrdinal + 1);
        seed ^= ((long) qn.getTile().getStart() << 32) ^ (long) qn.getIntersectionCount();
        seed ^= (long) targetSampleCount * 0xD6E8FEB86659FD93L;
        return mix64(seed);
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    private double plannedDeltaFraction(List<QueryNode> samplingNodes, Map<QueryNode, Integer> nextTargets) {
        long plannedDelta = 0L;
        long remaining = 0L;
        for (QueryNode qn : samplingNodes) {
            int sampled = qn.getSampledPointCount();
            int Nh = qn.getIntersectionCount();
            plannedDelta += Math.max(0, nextTargets.getOrDefault(qn, sampled) - sampled);
            remaining += Math.max(0, Nh - sampled);
        }
        return remaining > 0L ? (double) plannedDelta / (double) remaining : 0.0;
    }

    /** Exactifies every residual sampling node in one additional K-way pass. */
    private Map<QueryNode, Integer> exactifyResidualNodes(List<QueryNode> samplingNodes,
            Map<QueryNode, Integer> currentTargets) {
        Map<QueryNode, Integer> nextTargets = new IdentityHashMap<>(currentTargets);
        for (QueryNode qn : samplingNodes) {
            nextTargets.put(qn, qn.getIntersectionCount());
        }
        return nextTargets;
    }

    private boolean allSamplingNodesFullySampled(List<QueryNode> samplingNodes) {
        for (QueryNode qn : samplingNodes) {
            if (qn.getSampledPointCount() < qn.getIntersectionCount()) {
                return false;
            }
        }
        return true;
    }

    // ==================== Sampling Helpers ====================

    /**
     * Computes global statistics for each measure column by aggregating
     * stats from all leaf tile nodes. Called once after initialization.
     */
    private void computeGlobalMeasureStats() {
        int measureCount = schema.getMeasureCount();
        globalMeasureStats = new StatsAccumulator[measureCount];
        for (int i = 0; i < measureCount; i++) {
            globalMeasureStats[i] = new StatsAccumulator();
        }
        
        for (Object tileObj : grid.getLeafTiles()) {
            Tile tile = (Tile) tileObj;
            if (tile.hasPoints()) {
                aggregateTileStats(tile, measureCount);
            }
        }
        
        LOG.debug("Computed global measure stats for {} measures", measureCount);
        for (int i = 0; i < measureCount; i++) {
            StatsAccumulator s = globalMeasureStats[i];
            LOG.debug("Measure {}: count={}, mean={}",
                schema.getMeasureCols().get(i),
                s != null ? s.count() : 0,
                s != null ? s.mean() : 0);
        }
    }
    
    /**
     * Aggregates stats from a Tile into global stats.
     */
    private void aggregateTileStats(Tile tile, int measureCount) {
        if (tile.hasPoints()) {
            for (int i = 0; i < measureCount; i++) {
                StatsAccumulator tileStats = tile.getStats(i);
                if (tileStats != null && tileStats.count() > 0) {
                    globalMeasureStats[i].addAll(Objects.requireNonNull(tileStats.snapshot()));
                }
            }
        }
    }

    // ==================== Confidence Interval Computation ====================

    /**
     * Outlier-aware AQP helper: returns the closed-form sum of values for the
     * given measure (by index) over every in-query outlier of every sampling
     * node.  Returns 0 if the outlier index is disabled or no node has any
     * in-query outliers.
     */
    private double sumInQueryOutliers(List<QueryNode> samplingNodes, int measureIdx) {
        if (outlierIndex == null || samplingNodes == null) return 0.0;
        double s = 0.0;
        for (QueryNode qn : samplingNodes) {
            BitSet bits = qn.getInQueryOutliers();
            if (bits == null || bits.isEmpty()) continue;
            int[] idxs = qn.getTile().getOutlierIdxs();
            if (idxs == null) continue;
            for (int p = bits.nextSetBit(0); p >= 0; p = bits.nextSetBit(p + 1)) {
                int outIdx = idxs[p];
                if (outIdx < 0) continue;
                double v = outlierIndex.getOutlierValue(outIdx, measureIdx);
                if (!Double.isNaN(v)) s += v;
            }
        }
        return s;
    }

    /**
     * Outlier-aware AQP helper: returns the count of non-NaN values for the
     * given measure over every in-query outlier of every sampling node.
     */
    private long countInQueryOutliers(List<QueryNode> samplingNodes, int measureIdx) {
        if (outlierIndex == null || samplingNodes == null) return 0L;
        long c = 0L;
        for (QueryNode qn : samplingNodes) {
            BitSet bits = qn.getInQueryOutliers();
            if (bits == null || bits.isEmpty()) continue;
            int[] idxs = qn.getTile().getOutlierIdxs();
            if (idxs == null) continue;
            for (int p = bits.nextSetBit(0); p >= 0; p = bits.nextSetBit(p + 1)) {
                int outIdx = idxs[p];
                if (outIdx < 0) continue;
                if (!Double.isNaN(outlierIndex.getOutlierValue(outIdx, measureIdx))) c++;
            }
        }
        return c;
    }

    /**
     * Computes the confidence interval for the SUM of {@code measureCol} across
     * sampled nodes, combined with any already-known exact sums.
     * <p>
     * Uses the <b>null-as-zero</b> variance formulation: since
     * {@code SUM(col)} = {@code SUM(COALESCE(col, 0))}, NULL values contribute
     * 0 to the sum. By modelling the population as fully observed (with NULLs
     * replaced by 0), we avoid the need to estimate a separate null-ratio and
     * its variance. The CI correctly widens to account for null-rate uncertainty,
     * which is especially important for columns with high null rates.
     * <p>
     * Formally, for a node with population size N, m sampled points (including
     * NULLs), n non-null values with sum S and sum-of-squares Q:
     * <ul>
     *   <li>With-zeros sample mean: z̄ = S / m</li>
     *   <li>With-zeros sample variance: s²_z = (Q − S²/m) / (m − 1)</li>
     *   <li>SUM estimator: Ŝ = N · z̄</li>
     *   <li>Variance with FPC: Var(Ŝ) = N² · (s²_z / m) · (1 − m/N)</li>
     * </ul>
     * The point estimate is identical to the previous formulation
     * (N · nonNaNRatio · mean = N · S/m), but the variance now correctly
     * accounts for the null-proportion uncertainty.
     */
    private double[] getQuerySumConfidenceInterval(List<QueryNode> samplingNodes, QueryResults queryResults,
            int measureCol) {
        double exactSum = 0;
        if (queryResults.getStats().containsKey(measureCol)) {
            exactSum = queryResults.getStats().get(measureCol).sum();
        }

        // Outlier-aware AQP: add deterministic (closed-form) outlier sums for
        // every in-query outlier of every sampling node.  Outliers were
        // pre-removed from the sampling pool by QueryNode.applyOutlierRemoval,
        // so the sample-based estimator below produces an UN-biased estimate
        // of the trimmed sum; we add the exact outlier sum back here.
        // No-op when outlierIndex is null (feature disabled) or the tile has
        // no in-query outliers.
        if (outlierIndex != null) {
            int measureIdx = schema.getMeasureIndex(measureCol);
            exactSum += sumInQueryOutliers(samplingNodes, measureIdx);
        }

        if (samplingNodes == null || samplingNodes.isEmpty()) {
            return new double[] { exactSum, exactSum };
        }

        double totalEstimate = 0.0;
        double totalVariance = 0.0;
        double z = getZScoreForConfidence(0.95);

        for (QueryNode qnode : samplingNodes) {
            int n = (int) qnode.getSampleStatsAcc(measureCol).count();  // non-null sample count
            double N = qnode.getIntersectionCount();                    // total population (null + non-null)
            int m = qnode.getSampledPointCount();                       // total sampled from trimmed population

            if (m <= 0) {
                continue;
            }

            // SHORT-CIRCUIT: if every point in the node has been read,
            // the non-null sum in sampleStatsAcc is exact — no estimation needed.
            if (m >= (int) N) {
                double nodeExactSum = n > 0 ? qnode.getSampleStatsAcc(measureCol).sum() : 0.0;
                exactSum += nodeExactSum;
                continue;
            }

            // With fewer than 2 total samples we cannot estimate variance.
            // Add best-effort point estimate but no variance contribution.
            if (m < 2) {
                if (n > 0) {
                    totalEstimate += N * qnode.getSampleStatsAcc(measureCol).sum() / m;
                }
                LOG.trace("Node with {} valid samples out of {} sampled (intersectionCount={})",
                    n, m, (int) N);
                continue;
            }

            // --- Null-as-zero SUM CI ---
            // S = sum of non-null sampled values (nulls contribute 0)
            double sampleSum = n > 0 ? qnode.getSampleStatsAcc(measureCol).sum() : 0.0;

            // Q = sum of squares of non-null values; derived from sample variance:
            //   sampleVar = (Q - n·mean²) / (n-1)  =>  Q = (n-1)·sampleVar + n·mean²
            double sumOfSquaresNonNull;
            if (n >= 2) {
                double stdev = qnode.getSampleStatsAcc(measureCol).sampleStandardDeviation();
                double mean = qnode.getSampleStatsAcc(measureCol).mean();
                sumOfSquaresNonNull = (n - 1) * stdev * stdev + n * mean * mean;
            } else if (n == 1) {
                double val = qnode.getSampleStatsAcc(measureCol).mean();
                sumOfSquaresNonNull = val * val;
            } else {
                // n == 0: all sampled values were null → sum = 0, no variance
                sumOfSquaresNonNull = 0.0;
            }

            // With-zeros sample variance: s²_z = (Q - S²/m) / (m - 1)
            // This treats the (m - n) null samples as zeros, correctly inflating
            // variance to reflect null-rate uncertainty.
            double varWithZeros = (sumOfSquaresNonNull - sampleSum * sampleSum / m) / (m - 1);
            if (varWithZeros < 0) varWithZeros = 0.0;  // guard against fp rounding

            // Heavy-tail safeguard: floor the plug-in sample variance at the
            // exact tile-prior null-as-zero variance.  When the tile has its
            // own complete stats this is mathematically tight (the prior IS
            // the true stratum variance); when it comes from a frozen
            // ancestor it is a conservative regularizer that protects
            // against samples that miss rare large values.
            double priorVar = priorNullAsZeroVariance(qnode, schema.getMeasureIndex(measureCol));
            if (priorVar > varWithZeros) varWithZeros = priorVar;

            // SUM estimator: Ŝ = N · (S / m)
            double nodeEstimate = N * sampleSum / m;

            // Variance with finite population correction: Var(Ŝ) = N² · (s²_z / m) · (1 - m/N)
            double fpc = 1.0 - m / N;
            double nodeVariance = N * N * (varWithZeros / m) * fpc;

            totalEstimate += nodeEstimate;
            totalVariance += nodeVariance;
        }

        double finalEstimate = exactSum + totalEstimate;
        double stdError = Math.sqrt(totalVariance);
        double margin = z * stdError;

        double lower = finalEstimate - margin;
        double upper = finalEstimate + margin;

        return new double[] { lower, upper };
    }

    /**
     * Computes a confidence interval for the non-null COUNT of a measure column,
     * combining exact counts from fully-processed nodes with Horvitz-Thompson
     * estimation from sampling nodes.
     *
     * <p>For each sampling node, the indicator variable z_i ∈ {0,1} (1 = non-null)
     * gives a Bernoulli population. With m samples, n non-null:
     * <ul>
     *   <li>p̂ = n/m (estimated non-null proportion)</li>
     *   <li>COUNT estimator: Ĉ = N · p̂ = N · n/m</li>
     *   <li>Sample variance: s² = p̂(1−p̂)·m/(m−1)</li>
     *   <li>Variance with FPC: Var(Ĉ) = N² · s²/m · (1 − m/N)
     *       = N² · p̂(1−p̂)/(m−1) · (1 − m/N)</li>
     * </ul>
     */
    private double[] getQueryCountConfidenceInterval(List<QueryNode> samplingNodes, QueryResults queryResults,
            int measureCol) {
        // Exact count from frozen-stats and fully-contained-with-stats tiles
        double exactCount = 0;
        if (queryResults.getStats().containsKey(measureCol)) {
            exactCount = queryResults.getStats().get(measureCol).count();
        }

        // Outlier-aware AQP: add deterministic non-NaN counts for in-query
        // outliers of every sampling node (see getQuerySumConfidenceInterval
        // for the rationale).  No-op when the feature is disabled.
        if (outlierIndex != null) {
            int measureIdx = schema.getMeasureIndex(measureCol);
            exactCount += countInQueryOutliers(samplingNodes, measureIdx);
        }

        if (samplingNodes == null || samplingNodes.isEmpty()) {
            return new double[] { exactCount, exactCount };
        }

        double totalEstimate = 0.0;
        double totalVariance = 0.0;
        double z = getZScoreForConfidence(0.95);

        for (QueryNode qnode : samplingNodes) {
            int n = (int) qnode.getSampleStatsAcc(measureCol).count();  // non-null sample count
            double N = qnode.getIntersectionCount();                    // total population (null + non-null)
            int m = qnode.getSampledPointCount();                       // total sampled from trimmed population

            if (m <= 0) {
                continue;
            }

            // SHORT-CIRCUIT: all points sampled → exact count
            if (m >= (int) N) {
                exactCount += n;
                continue;
            }

            // With one sample, keep the HT point estimate and use the most
            // conservative Bernoulli variance because sample variance is undefined.
            if (m < 2) {
                double pHat = (double) n / m;
                totalEstimate += N * pHat;
                double fpc = 1.0 - m / N;
                totalVariance += N * N * 0.25 / m * fpc;
                continue;
            }

            // --- Bernoulli COUNT CI ---
            double pHat = (double) n / m;    // estimated non-null proportion

            // COUNT estimator: Ĉ = N · p̂
            double nodeEstimate = N * pHat;

            // Bernoulli sample variance: s² = p̂(1-p̂) · m/(m-1)
            // Var(Ĉ) = N² · s²/m · (1 - m/N) = N² · p̂(1-p̂)/(m-1) · (1 - m/N)
            double fpc = 1.0 - m / N;
            double pForVariance = adjustedBernoulliProportion(n, m, z);
            double nodeVariance = N * N * (pForVariance * (1.0 - pForVariance)) / (m - 1) * fpc;

            totalEstimate += nodeEstimate;
            totalVariance += nodeVariance;
        }

        double finalEstimate = exactCount + totalEstimate;
        double stdError = Math.sqrt(totalVariance);
        double margin = z * stdError;

        double lower = Math.max(exactCount, finalEstimate - margin);
        double upper = finalEstimate + margin;

        return new double[] { lower, upper };
    }

    /**
     * Computes a confidence interval for the MEAN of a measure column using the
     * delta-method (ratio estimator) applied to MEAN = SUM / COUNT.
     *
     * <p>Because MEAN is a ratio of two estimated quantities (both affected by
     * sampling), its variance requires the covariance between SUM and COUNT
     * estimators. Per sampling node, with m total samples, n non-null, S = sum
     * of non-null values:
     * <ul>
     *   <li>Ŝ = N · S/m (SUM estimator, null-as-zero)</li>
     *   <li>Ĉ = N · n/m (COUNT estimator)</li>
     *   <li>Cov(Ŝ,Ĉ) = N² · S(m−n) / [m²(m−1)] · (1 − m/N)</li>
     * </ul>
     * Global variance via the delta method:
     * <pre>
     *   Var(μ̂) ≈ (1/Ĉ²) · [Var(Ŝ) − 2μ̂·Cov(Ŝ,Ĉ) + μ̂²·Var(Ĉ)]
     * </pre>
     * where all sums/variances/covariances are aggregated across sampling nodes,
     * and exact nodes contribute to the point estimate with zero variance.
     */
    private double[] getQueryMeanConfidenceInterval(List<QueryNode> samplingNodes, QueryResults queryResults,
            int measureCol) {
        // Exact contributions from frozen-stats and fully-contained-with-stats tiles
        double exactSum = 0;
        double exactCount = 0;
        if (queryResults.getStats().containsKey(measureCol)) {
            exactSum = queryResults.getStats().get(measureCol).sum();
            exactCount = queryResults.getStats().get(measureCol).count();
        }

        // Outlier-aware AQP: add deterministic outlier contributions to both
        // numerator (sum) and denominator (non-NaN count) of MEAN = SUM/COUNT.
        // No-op when the feature is disabled.
        if (outlierIndex != null) {
            int measureIdx = schema.getMeasureIndex(measureCol);
            exactSum += sumInQueryOutliers(samplingNodes, measureIdx);
            exactCount += countInQueryOutliers(samplingNodes, measureIdx);
        }

        if (samplingNodes == null || samplingNodes.isEmpty()) {
            if (exactCount == 0) {
                return new double[] { Double.NaN, Double.NaN };
            }
            double mean = exactSum / exactCount;
            return new double[] { mean, mean };
        }

        double totalSumEstimate = 0.0;
        double totalCountEstimate = 0.0;
        double totalSumVariance = 0.0;
        double totalCountVariance = 0.0;
        double totalCovariance = 0.0;

        for (QueryNode qnode : samplingNodes) {
            int n = (int) qnode.getSampleStatsAcc(measureCol).count();
            double N = qnode.getIntersectionCount();
            int m = qnode.getSampledPointCount();

            if (m <= 0) {
                continue;
            }

            // Fully sampled node → exact, zero variance/covariance
            if (m >= (int) N) {
                double nodeSum = n > 0 ? qnode.getSampleStatsAcc(measureCol).sum() : 0.0;
                exactSum += nodeSum;
                exactCount += n;
                continue;
            }

            if (m < 2) {
                // Best-effort point estimate, no variance/covariance contribution
                double sampleSum = n > 0 ? qnode.getSampleStatsAcc(measureCol).sum() : 0.0;
                totalSumEstimate += N * sampleSum / m;
                totalCountEstimate += N * n / (double) m;
                continue;
            }

            double sampleSum = n > 0 ? qnode.getSampleStatsAcc(measureCol).sum() : 0.0;

            // --- SUM variance (null-as-zero, same as getQuerySumConfidenceInterval) ---
            double sumOfSquaresNonNull;
            if (n >= 2) {
                double stdev = qnode.getSampleStatsAcc(measureCol).sampleStandardDeviation();
                double mean = qnode.getSampleStatsAcc(measureCol).mean();
                sumOfSquaresNonNull = (n - 1) * stdev * stdev + n * mean * mean;
            } else if (n == 1) {
                double val = qnode.getSampleStatsAcc(measureCol).mean();
                sumOfSquaresNonNull = val * val;
            } else {
                sumOfSquaresNonNull = 0.0;
            }
            double varWithZeros = (sumOfSquaresNonNull - sampleSum * sampleSum / m) / (m - 1);
            if (varWithZeros < 0) varWithZeros = 0.0;

            // Heavy-tail safeguard (see getQuerySumConfidenceInterval): floor
            // SUM variance at the exact tile-prior null-as-zero variance.
            // Covariance is left unfloored: it is a function of the observed
            // sample sum, which is unbiased even when the tail is missed.
            double priorVar = priorNullAsZeroVariance(qnode, schema.getMeasureIndex(measureCol));
            if (priorVar > varWithZeros) varWithZeros = priorVar;

            double fpc = 1.0 - m / N;

            // SUM estimator and variance
            double nodeSumEst = N * sampleSum / m;
            double nodeSumVar = N * N * (varWithZeros / m) * fpc;

            // COUNT estimator and variance (Bernoulli)
            double pHat = (double) n / m;
            double nodeCountEst = N * pHat;
            double pForVariance = adjustedBernoulliProportion(n, m, getZScoreForConfidence(0.95));
            double nodeCountVar = N * N * (pForVariance * (1.0 - pForVariance)) / (m - 1) * fpc;

            // Covariance between SUM and COUNT estimators.
            // The null-as-zero value z_j and the indicator I_j = 1{non-null} satisfy:
            //   sum(z_j · I_j) = sum(z_j) = S  (since z_j = 0 when null)
            //   Cov_sample(z, I) = [S - S·n/m] / (m-1) = S·(m-n) / [m·(m-1)]
            // Scaled to population: Cov(Ŝ,Ĉ) = N² · Cov_sample(z,I)/m · fpc
            double sampleCov = sampleSum * (m - n) / ((double) m * (m - 1));
            double nodeCov = N * N * (sampleCov / m) * fpc;

            totalSumEstimate += nodeSumEst;
            totalCountEstimate += nodeCountEst;
            totalSumVariance += nodeSumVar;
            totalCountVariance += nodeCountVar;
            totalCovariance += nodeCov;
        }

        double globalSum = exactSum + totalSumEstimate;
        double globalCount = exactCount + totalCountEstimate;

        if (globalCount <= 0) {
            return new double[] { Double.NaN, Double.NaN };
        }

        double meanEst = globalSum / globalCount;

        // Delta method: Var(μ̂) ≈ (1/Ĉ²)[Var(Ŝ) − 2μ̂·Cov(Ŝ,Ĉ) + μ̂²·Var(Ĉ)]
        double meanVar = (totalSumVariance - 2.0 * meanEst * totalCovariance
                + meanEst * meanEst * totalCountVariance) / (globalCount * globalCount);
        if (meanVar < 0) meanVar = 0.0;

        double stdError = Math.sqrt(meanVar);
        double z = getZScoreForConfidence(0.95);
        double margin = z * stdError;

        return new double[] { meanEst - margin, meanEst + margin };
    }

    private double getZScoreForConfidence(double confidenceLevel) {
        if (confidenceLevel == 0.90) {
            return 1.645;
        } else if (confidenceLevel == 0.95) {
            return 1.96;
        } else if (confidenceLevel == 0.99) {
            return 2.575;
        }
        throw new IllegalArgumentException("Unsupported confidence level: " + confidenceLevel);
    }

    private double calculateRelativeError(double[] confidenceInterval, double scaleFloor) {
        if (confidenceInterval == null || confidenceInterval.length < 2) {
            return Double.POSITIVE_INFINITY;
        }
        double lo = confidenceInterval[0];
        double hi = confidenceInterval[1];
        if (Double.isNaN(lo) || Double.isNaN(hi)) {
            return 0.0; // undefined (e.g. zero-count MEAN) — not a convergence blocker
        }
        double halfWidth = Math.abs(hi - lo) / 2.0;
        return halfWidth / relativeErrorDenominator(confidenceInterval, scaleFloor);
    }

    private double relativeErrorDenominator(double[] confidenceInterval, double scaleFloor) {
        if (confidenceInterval == null || confidenceInterval.length < 2) {
            return Math.max(scaleFloor, 1e-12);
        }
        double lo = confidenceInterval[0];
        double hi = confidenceInterval[1];
        if (Double.isNaN(lo) || Double.isNaN(hi)) {
            return Math.max(scaleFloor, 1e-12);
        }
        double midpoint = (hi + lo) / 2.0;
        return Math.max(Math.abs(midpoint), Math.max(scaleFloor, 1e-12));
    }

    private double adjustedBernoulliProportion(int successes, int samples, double z) {
        double z2 = z * z;
        return (successes + z2 / 2.0) / (samples + z2);
    }

    /**
     * Heavy-tail safeguard: returns the null-as-zero variance implied by the
     * exact tile-population (or nearest-ancestor frozen) prior for stratum
     * {@code qnode} on measure index {@code measureIdx}.  Used to floor the
     * plug-in sample variance in SUM/MEAN CI computation, preventing the
     * reported interval from being narrower than the indexed full-population
     * variance implies.  Returns {@code 0.0} when no exact prior is
     * available (e.g. very deep tile with no frozen ancestor on this
     * measure), in which case the floor is inactive and the sample variance
     * is used as-is.
     *
     * <p>The expression matches the SUM variance term used by
     * {@link SampleAllocator}:
     * <pre>
     *   s²_prior = p_NN · σ_NN² + p_NN · (1 - p_NN) · μ_NN²
     * </pre>
     * which is the exact null-as-zero variance of a population with
     * non-null ratio {@code p_NN}, non-null mean {@code μ_NN} and non-null
     * standard deviation {@code σ_NN}.
     *
     * <p><b>Statistical justification:</b> when the prior comes from this
     * tile's own complete stats, {@code s²_prior} is the true variance of
     * the residual stratum and the floor is mathematically tight.  When it
     * comes from a frozen ancestor or the global fallback, the floor is a
     * conservative regularizer that protects against under-sampled tails;
     * it may slightly overstate uncertainty when a query intersects a
     * low-variance sub-region of a high-variance ancestor.
     */
    private double priorNullAsZeroVariance(QueryNode qnode, int measureIdx) {
        double[] prior = qnode.getTile().getPrior(measureIdx, outlierIndex);
        if (prior == null) return 0.0;
        double muNN = prior[0];
        double sigNN = prior[1];
        double pNN = prior[2];
        double s2 = pNN * sigNN * sigNN + pNN * (1.0 - pNN) * muNN * muNN;
        return Math.max(0.0, s2);
    }

    private double sumScaleFloor(int measureCol, long totalCount) {
        int midx = schema.getMeasureIndex(measureCol);
        double magnitude = 1.0;
        if (globalMeasureStats != null && midx >= 0 && midx < globalMeasureStats.length) {
            StatsAccumulator stats = globalMeasureStats[midx];
            if (stats != null && stats.count() > 0) {
                magnitude = Math.max(1.0, Math.abs(stats.mean()));
            }
        }
        return Math.max(1e-12, 1e-6 * magnitude * Math.max(1L, totalCount));
    }

    private double countScaleFloor(long totalCount) {
        return Math.max(1.0, 1e-6 * Math.max(1L, totalCount));
    }

    private double meanScaleFloor(int measureCol) {
        int midx = schema.getMeasureIndex(measureCol);
        if (globalMeasureStats != null && midx >= 0 && midx < globalMeasureStats.length) {
            StatsAccumulator stats = globalMeasureStats[midx];
            if (stats != null && stats.count() > 0) {
                return Math.max(1e-12, 1e-6 * Math.max(1.0, Math.abs(stats.mean())));
            }
        }
        return 1e-6;
    }



    // ==================== Shared Utilities ====================

    private ContainmentExaminer getContainmentExaminer(Tile tile, Rectangle query) {
        Range<Double> queryXRange = query.getXRange();
        Range<Double> queryYRange = query.getYRange();
        Range<Double> tileXRange = Objects.requireNonNull(tile.getBounds().getXRange());
        Range<Double> tileYRange = Objects.requireNonNull(tile.getBounds().getYRange());
        boolean checkX = !queryXRange.encloses(tileXRange);
        boolean checkY = !queryYRange.encloses(tileYRange);

        ContainmentExaminer containmentExaminer = null;
        if (checkX && checkY) {
            containmentExaminer = new XYContainmentExaminer(queryXRange, queryYRange);
        } else if (checkX) {
            containmentExaminer = new XContainmentExaminer(queryXRange);
        } else if (checkY) {
            containmentExaminer = new YContainmentExaminer(queryYRange);
        }
        return containmentExaminer;
    }

    public int getLeafTileCount() {
        return this.grid.getLeafTileCount();
    }

    public int getMaxDepth() {
        return grid.getMaxDepth();
    }

    // ==================== Memory Measurement ====================

    /**
     * Measures the deep (retained) heap size of the index using JOL
     * (Java Object Layout). Traverses the full object graph reachable from
     * the grid, point store, and global measure stats, deduplicating shared
     * references automatically.
     *
     * @return deep size in bytes, or -1 if the index has not been initialized
     */
    public long measureDeepSizeBytes() {
        if (!isInitialized || grid == null) return -1;

        // Measure all core index components in a single graph traversal.
        // GraphLayout deduplicates shared references (e.g., SharedPointStore
        // referenced by both pointStore field and every tile's store field).
        if (globalMeasureStats != null) {
            return GraphLayout.parseInstance(grid, pointStore, globalMeasureStats).totalSize();
        }
        return GraphLayout.parseInstance(grid, pointStore).totalSize();
    }

    @Override
    public String toString() {
        return grid.printTiles();
    }

    public Schema getSchema() {
        return schema;
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    @Override
    public synchronized void close() {
        if (batchReader != null) {
            batchReader.close();
            batchReader = null;
        }
        if (pointStore != null) {
            pointStore.close();
        }
    }
}
