package gr.athenarc.imsi.visualfacts;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openjdk.jol.info.GraphLayout;

import com.google.common.collect.Range;
import com.google.common.math.Stats;
import com.google.common.math.StatsAccumulator;
import com.google.common.util.concurrent.AtomicDouble;

import gr.athenarc.imsi.visualfacts.init.InitializationPolicy;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults;
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
 * disk and returns precise aggregate statistics.
 * <p>
 * When {@code errorThreshold > 0}, runs in approximate mode with adaptive
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
        this.schema = schema;
        this.errorThreshold = errorThreshold;
        this.samplingOnly = samplingOnly;
        if (initMode != null && !INIT_MODE_QUERY_BIASED.equalsIgnoreCase(initMode)) {
            throw new IllegalArgumentException(
                    "Unknown initMode '" + initMode + "'. Valid values: null (uniform), '" + INIT_MODE_QUERY_BIASED + "'");
        }
        this.initMode = initMode;
    }

    public boolean isExactMode() {
        return errorThreshold <= 0;
    }

    public String getInitMode() {
        return initMode;
    }

    public void generateGrid(Query q0) {
        if (isInitialized)
            throw new IllegalStateException("The index is already initialized");

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
        // Build mapping from original col index to position in selectedColumns
        Map<Integer, Integer> colIndexToRowPos = new HashMap<>();
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

        // Build tile index mapping for partition
        List leafTileList = grid.getLeafTiles();
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
        // Clean up tmpDir BEFORE timing starts.
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

        try {
            // --- Phase 1: parallel CSV scan → merged arrays + per-tile counts/stats ---
            long phase1Start = System.nanoTime();

            ParallelCsvScanner scanner = new ParallelCsvScanner(
                    new File(schema.getCsv()), schema.getDelimiter(), schema.getHasHeader(),
                    selectedColumns, xPos, yPos,
                    filterPositions, filterArray, measurePositions,
                    grid.getBounds(), grid, tileIndexMap, numTiles,
                    scanThreads, capacity, schema.getNullstr(), tmpDir, q0);

            ParallelCsvScanner.ScanResult scanResult = scanner.scan();
            long scanEndNanos = System.nanoTime();

            int validCount = scanResult.validCount;
            objectsIndexed = validCount;
            this.maxRowLength = (int) scanResult.maxRowLength;
            LOG.info("Max row length observed during init: {} bytes", maxRowLength);

            // Adopt scan results into a SharedPointStore
            SharedPointStore store;
            if (scanResult.bucketMode) {
                // Path C: bucket-mmap — scatter per-thread bucket files into mmap arrays
                LOG.info("Using bucket-mmap partition path ({} buckets), mmap dir: {}",
                        scanResult.numBuckets, mmapDir);
                store = SharedPointStore.createForBucketMmap(
                        validCount, scanResult.bucketDir,
                        scanResult.numBuckets, scanResult.tilesPerBucket,
                        scanResult.numScanThreads, mmapDir,
                        scanResult.perThreadTileCounts);
            } else if (scanResult.diskXsFiles != null) {
                // Path B: disk-chunk — scatter directly from per-thread scan files
                LOG.info("Using disk-scatter partition path ({} chunks)", scanResult.diskChunkSizes.length);
                store = SharedPointStore.createForDiskChunks(
                        validCount, scanResult.diskXsFiles, scanResult.diskYsFiles,
                        scanResult.diskOffsetsFiles, scanResult.diskTileIdFiles,
                        scanResult.diskChunkSizes);
            } else {
                // Path A: in-memory — chunked arrays on heap (zero-copy adoption)
                store = new SharedPointStore(
                        scanResult.xsChunks, scanResult.ysChunks, scanResult.offsetsChunks,
                        scanResult.tileIdChunks, scanResult.chunkSizes, validCount);
            }

            // Wire per-tile counts and stats from the parallel scan into tiles
            for (int t = 0; t < numTiles; t++) {
                int count = scanResult.tileCounts[t];
                if (count > 0) {
                    Tile tile = (Tile) leafTileList.get(t);
                    tile.setSize(count);

                    // Set pre-built stats
                    if (measureCount > 0) {
                        tile.setPrebuiltStats(scanResult.tileStats[t],
                                scanResult.tileStatsPointCounts[t]);
                    }
                }
            }

            // --- Phase 1b removed: tileIds are now computed during scan ---

            LOG.info("Phase 1 total (scan + merge): {} s",
                    String.format("%.3f", (System.nanoTime() - phase1Start) / 1e9));

            // --- Phase 1.5: compute prefix sums from per-tile counts ---
            int[] counts = scanResult.tileCounts;
            int[] starts = new int[numTiles];
            if (numTiles > 0) {
                starts[0] = 0;
                for (int t = 1; t < numTiles; t++) {
                    starts[t] = starts[t - 1] + counts[t - 1];
                }
            }
            long prefixSumEnd = System.nanoTime();

            // Release scanner and large scan-result fields so GC can reclaim
            // per-thread arrays and StatsAccumulators before partition allocates.
            String scanPath = scanResult.scanPath;
            q0StatsFromScan = scanResult.q0Stats;
            scanResult = null;
            scanner = null;

            // --- Phase 1.5b: explicit GC before partition ---
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

            // --- Phase 3: wire tiles to shared store slices ---
            long wireStart = System.nanoTime();
            for (int t = 0; t < numTiles; t++) {
                Tile tile = (Tile) leafTileList.get(t);
                if (tile.getSize() > 0) {
                    tile.setSlice(store, starts[t], counts[t]);
                }
            }
            long wireEndNanos = System.nanoTime();

            this.pointStore = store;

            // Record init timing breakdown
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

        // Release transient partition memory back to the OS.
        // G1GC uncommits empty regions below -Xmx when -Xms is unset,
        // making those pages available for page cache during queries.
        {
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
        }
        
        // Record total init time
        if (initTimingBreakdown != null) {
            initTimingBreakdown.put("total", (System.nanoTime() - initOverallStart) / 1e9);
        }
        
        // Create QueryResults and populate with q0 stats if available
        QueryResults queryResults;
        if (isExactMode()) {
            queryResults = new QueryResults(q0);
        } else {
            queryResults = new ApproximateQueryResults(q0);
        }
        
        // Populate q0 statistics from the scan
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
        int fullyContainedTilesCount = 0;

        List<Tile> leafTiles = this.grid.getOverlappedLeafTiles(query);

        for (Tile leafTile : leafTiles) {
            // Short-circuited non-leaf tile with frozen exact stats
            if (leafTile.hasFrozenStats()) {
                fullyContainedTilesCount++;
                queryResults.addTotalCount(leafTile.getFrozenPointCount());
                query.getMeasureCols().forEach(measureCol -> {
                    queryResults.adjustStats(measureCol,
                            leafTile.getFrozenStats(schema.getMeasureIndex(measureCol)));
                });
                continue;
            }

            ContainmentExaminer containmentExaminer = getContainmentExaminer(leafTile, rect);
            boolean isFullyContained = containmentExaminer == null;
            if (isFullyContained) {
                fullyContainedTilesCount++;
            }

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
                queryNodes = leafTile.getOverlappedActualLeafTiles(query).stream()
                        .flatMap(tile -> tile.getQueryNodes(query, containmentExaminer, schema).stream())
                        .collect(Collectors.toList());
            }

            for (QueryNode queryNode : queryNodes) {
                Tile qnTile = queryNode.getTile();
                if (isFullyContained && query.getMeasureCols().stream().allMatch(mc -> qnTile.hasStats(schema.getMeasureIndex(mc)))) {
                    queryResults.addTotalCount(queryNode.getIntersectionCount());
                    query.getMeasureCols().forEach(measureCol -> {
                        StatsAccumulator acc = queryNode.getTile().getStats(schema.getMeasureIndex(measureCol));
                        if (acc != null) {
                            queryResults.adjustStats(measureCol, acc.snapshot());
                        }
                    });
                } else {
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
        queryResults.setFullyContainedTileCount(fullyContainedTilesCount);
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
            // Short-circuited non-leaf tile with frozen exact stats
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

                if (isFullyContained && !samplingOnly && query.getMeasureCols().stream().allMatch(mc -> qnTile.hasStats(schema.getMeasureIndex(mc)))) {
                    fullyContainedNodesWithStats.add(queryNode);
                } else if (!isFullyContained && qnTile.getSize() > THRESHOLD) {
                    leafTile.split();
                    leafTile.getOverlappedActualLeafTiles(query).stream()
                            .flatMap(tile -> tile.getQueryNodes(query, containmentExaminer, schema).stream())
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

        // Prepare sorted measure column indices for fast extraction
        List<Integer> measureColsList = schema.getMeasureCols();
        int[] sortedMeasureCols = measureColsList.stream().mapToInt(Integer::intValue).sorted().toArray();
        
        // Build mapping from original column index to position in sorted array
        Map<Integer, Integer> measureColToExtractedPos = new HashMap<>();
        for (int i = 0; i < sortedMeasureCols.length; i++) {
            measureColToExtractedPos.put(sortedMeasureCols[i], i);
        }
        
        byte delimiterByte = (byte) schema.getDelimiter().charValue();

        int ioCount = 0;

        List<QueryNode> samplingNodes = new ArrayList<>();
        samplingNodes.addAll(partialNodes);
        samplingNodes.addAll(fullyContainedNodesWithoutStats);

        // Total count (COUNT*) — exact from x,y coordinates, independent of sampling
        for (QueryNode qn : samplingNodes) {
            queryResults.addTotalCount(qn.getIntersectionCount());
        }

        AtomicDouble samplingRate = new AtomicDouble(computeInitialSamplingRate(samplingNodes));
        Map<Integer, double[]> sumConfidenceIntervals = new HashMap<>();
        Map<Integer, double[]> countConfidenceIntervals = new HashMap<>();
        Map<Integer, double[]> meanConfidenceIntervals = new HashMap<>();
        Map<Integer, Double> errorBounds = new HashMap<>();
        int samplingRounds = 0;
        int maxSamplingRounds = 50;
        do {
            samplingRounds++;
            // Create Sampling Iterators for all tiles needing sampling
            KWayMergePointIterator pointIterator = new KWayMergePointIterator(samplingNodes.stream()
                    .map(queryNode -> new SamplingNodePointsIterator(queryNode, samplingRate.get()))
                    .collect(Collectors.toList()));

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
            // COUNT CI uses Bernoulli variance p̂(1-p̂) ≤ 0.25, which is bounded,
            // so COUNT is guaranteed to converge whenever SUM converges — SUM is
            // always the bottleneck.
            // MEAN CI uses the delta method on the ratio SUM/COUNT.
            for (Integer measureCol : query.getMeasureCols()) {
                sumConfidenceIntervals.put(measureCol,
                        getQuerySumConfidenceInterval(samplingNodes, queryResults, samplingRate.get(), measureCol));
                countConfidenceIntervals.put(measureCol,
                        getQueryCountConfidenceInterval(samplingNodes, queryResults, samplingRate.get(), measureCol));
                meanConfidenceIntervals.put(measureCol,
                        getQueryMeanConfidenceInterval(samplingNodes, queryResults, samplingRate.get(), measureCol));
            }

            // Error bound per measure: max relative error across all aggregate types
            for (Integer measureCol : query.getMeasureCols()) {
                double sumError = calculateRelativeError(sumConfidenceIntervals.get(measureCol));
                double countError = calculateRelativeError(countConfidenceIntervals.get(measureCol));
                double meanError = calculateRelativeError(meanConfidenceIntervals.get(measureCol));
                errorBounds.put(measureCol, Math.max(sumError, Math.max(countError, meanError)));
            }

            // Find the maximum error bound across all measures
            double maxErrorBound = errorBounds.values().stream().max(Double::compare).orElse(0.0);

            // If error bound is still too high, increase sampling rate
            if (maxErrorBound > errorThreshold) {
                if (samplingRounds >= maxSamplingRounds) {
                    LOG.warn("Sampling did not converge after {} rounds (error={}, threshold={}). " +
                        "Likely caused by NaN-heavy nodes. Returning best estimate.",
                        samplingRounds, maxErrorBound, errorThreshold);
                    break;
                }
                LOG.debug("Round {}: error={} > threshold={}, increasing rate from {} to {}", 
                    samplingRounds, maxErrorBound, errorThreshold, samplingRate.get(),
                    adjustSamplingRate(samplingRate.get(), maxErrorBound, errorThreshold));
                samplingRate.set(adjustSamplingRate(samplingRate.get(), maxErrorBound, errorThreshold));
            }

        } while (errorBounds.values().stream().anyMatch(error -> error > errorThreshold));

        LOG.trace("Sampling completed in {} round(s), final rate={}, I/Os={}", 
            samplingRounds, samplingRate.get(), ioCount);

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
        queryResults.setSamplingRate(samplingRate.get());
        queryResults.setIoCount(ioCount);

        queryResults.setSumConfidenceIntervals(sumConfidenceIntervals);
        queryResults.setCountConfidenceIntervals(countConfidenceIntervals);
        queryResults.setMeanConfidenceIntervals(meanConfidenceIntervals);
        queryResults.setErrorBounds(errorBounds);

        return queryResults;
    }

    // ==================== Sampling Helpers ====================

    /**
     * Adjusts the sampling rate based on the current relative error and the target error threshold.
     */
    private double adjustSamplingRate(double currentRate, double currentError, double errorThreshold) {
        if (currentError <= errorThreshold) {
            return currentRate;
        }
        double factor = Math.pow(currentError / errorThreshold, 2);
        double maxFactor = 2.0;
        if (factor > maxFactor) {
            factor = maxFactor;
        }
        double newRate = currentRate * factor;
        double delta = newRate - currentRate;
        double minDelta = 0.01;
        if (delta < minDelta) {
            newRate = currentRate + minDelta;
        }
        if (newRate > 1.0) {
            newRate = 1.0;
        }
        return newRate;
    }

    /**
     * Computes the initial sampling rate using CV-based estimation (Cochran's formula).
     */
    private double computeInitialSamplingRate(List<QueryNode> samplingNodes) {
        if (samplingNodes == null || samplingNodes.isEmpty()) {
            return 0.01d;
        }
        
        double maxCV = 0.0;
        for (int i = 0; i < schema.getMeasureCount(); i++) {
            double cv = getMeasureCV(i);
            if (cv > maxCV) {
                maxCV = cv;
            }
        }
        if (maxCV <= 0) {
            maxCV = 1.0;
        }
        
        double z = 1.96;  // 95% confidence
        double requiredN = Math.pow(z * maxCV / errorThreshold, 2);
        
        final double SAFETY_MARGIN = 1.0;
        requiredN *= SAFETY_MARGIN;
        
        long totalPopulation = samplingNodes.stream()
            .mapToLong(QueryNode::getIntersectionCount)
            .sum();
        
        if (totalPopulation == 0) {
            return 0.01d;
        }
        
        double rate = requiredN / totalPopulation;
        
        final int MIN_SAMPLES = 50;
        double minRateForCLT = (double) MIN_SAMPLES / totalPopulation;
        rate = Math.max(rate, minRateForCLT);
        rate = Math.min(1.0, rate);
        
        LOG.trace("Initial sampling rate: {} (CV={}, requiredN={}, population={})", 
            rate, maxCV, requiredN, totalPopulation);
        
        return rate;
    }

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
        
        LOG.debug("Global CV computed for {} measures", measureCount);
        for (int i = 0; i < measureCount; i++) {
            LOG.debug("Measure {}: count={}, mean={}, CV={}", 
                schema.getMeasureCols().get(i),
                globalMeasureStats[i].count(),
                globalMeasureStats[i].mean(),
                getMeasureCV(i));
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
                    globalMeasureStats[i].addAll(tileStats.snapshot());
                }
            }
        }
    }

    /**
     * Maximum CV cap to prevent pathological cases from requiring 100% sampling.
     */
    private static final double MAX_CV_CAP = 2.0;

    /**
     * Returns the coefficient of variation (CV = std/mean) for a given measure column.
     */
    public double getMeasureCV(int measureIndex) {
        if (globalMeasureStats == null || measureIndex < 0 || measureIndex >= globalMeasureStats.length) {
            return 1.0;
        }
        StatsAccumulator stats = globalMeasureStats[measureIndex];
        if (stats == null || stats.count() < 2) {
            return 1.0;
        }
        double mean = stats.mean();
        if (mean == 0) {
            return 1.0;
        }
        double cv = stats.sampleStandardDeviation() / Math.abs(mean);
        return Math.min(cv, MAX_CV_CAP);
    }

    /**
     * Returns the global statistics for a given measure column.
     */
    public Stats getGlobalMeasureStats(int measureIndex) {
        if (globalMeasureStats == null || measureIndex < 0 || measureIndex >= globalMeasureStats.length) {
            return null;
        }
        StatsAccumulator stats = globalMeasureStats[measureIndex];
        return stats != null ? stats.snapshot() : null;
    }

    // ==================== Confidence Interval Computation ====================

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
            double samplingRate, int measureCol) {
        double exactSum = 0;
        if (queryResults.getStats().containsKey(measureCol)) {
            exactSum = queryResults.getStats().get(measureCol).sum();
        }

        if (samplingNodes == null || samplingNodes.isEmpty()) {
            return new double[] { exactSum, exactSum };
        }

        double totalEstimate = 0.0;
        double totalVariance = 0.0;

        for (QueryNode qnode : samplingNodes) {
            int n = (int) qnode.getSampleStatsAcc(measureCol).count();  // non-null sample count
            double N = qnode.getIntersectionCount();                    // total population (null + non-null)
            int m = qnode.getSampledTracker().cardinality();            // total sampled  (null + non-null)

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
        double z = getZScoreForConfidence(0.95);
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
            double samplingRate, int measureCol) {
        // Exact count from frozen-stats and fully-contained-with-stats tiles
        double exactCount = 0;
        if (queryResults.getStats().containsKey(measureCol)) {
            exactCount = queryResults.getStats().get(measureCol).count();
        }

        if (samplingNodes == null || samplingNodes.isEmpty()) {
            return new double[] { exactCount, exactCount };
        }

        double totalEstimate = 0.0;
        double totalVariance = 0.0;

        for (QueryNode qnode : samplingNodes) {
            int n = (int) qnode.getSampleStatsAcc(measureCol).count();  // non-null sample count
            double N = qnode.getIntersectionCount();                    // total population (null + non-null)
            int m = qnode.getSampledTracker().cardinality();            // total sampled  (null + non-null)

            // SHORT-CIRCUIT: all points sampled → exact count
            if (m >= (int) N) {
                exactCount += n;
                continue;
            }

            // With fewer than 2 samples, best-effort point estimate, no variance
            if (m < 2) {
                totalEstimate += N * n / (double) m;
                continue;
            }

            // --- Bernoulli COUNT CI ---
            double pHat = (double) n / m;    // estimated non-null proportion

            // COUNT estimator: Ĉ = N · p̂
            double nodeEstimate = N * pHat;

            // Bernoulli sample variance: s² = p̂(1-p̂) · m/(m-1)
            // Var(Ĉ) = N² · s²/m · (1 - m/N) = N² · p̂(1-p̂)/(m-1) · (1 - m/N)
            double fpc = 1.0 - m / N;
            double nodeVariance = N * N * (pHat * (1.0 - pHat)) / (m - 1) * fpc;

            totalEstimate += nodeEstimate;
            totalVariance += nodeVariance;
        }

        double finalEstimate = exactCount + totalEstimate;
        double stdError = Math.sqrt(totalVariance);
        double z = getZScoreForConfidence(0.95);
        double margin = z * stdError;

        double lower = finalEstimate - margin;
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
            double samplingRate, int measureCol) {
        // Exact contributions from frozen-stats and fully-contained-with-stats tiles
        double exactSum = 0;
        double exactCount = 0;
        if (queryResults.getStats().containsKey(measureCol)) {
            exactSum = queryResults.getStats().get(measureCol).sum();
            exactCount = queryResults.getStats().get(measureCol).count();
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
            int m = qnode.getSampledTracker().cardinality();

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

            double fpc = 1.0 - m / N;

            // SUM estimator and variance
            double nodeSumEst = N * sampleSum / m;
            double nodeSumVar = N * N * (varWithZeros / m) * fpc;

            // COUNT estimator and variance (Bernoulli)
            double pHat = (double) n / m;
            double nodeCountEst = N * pHat;
            double nodeCountVar = N * N * (pHat * (1.0 - pHat)) / (m - 1) * fpc;

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

    private double calculateRelativeError(double[] confidenceInterval) {
        double lo = confidenceInterval[0];
        double hi = confidenceInterval[1];
        if (Double.isNaN(lo) || Double.isNaN(hi)) {
            return 0.0; // undefined (e.g. zero-count MEAN) — not a convergence blocker
        }
        double denom = hi + lo;
        if (denom == 0.0) {
            return 0.0;
        }
        return (hi - lo) / denom;
    }



    // ==================== Shared Utilities ====================

    private ContainmentExaminer getContainmentExaminer(Tile tile, Rectangle query) {
        Range<Double> queryXRange = query.getXRange();
        Range<Double> queryYRange = query.getYRange();
        boolean checkX = !queryXRange.encloses(tile.getBounds().getXRange());
        boolean checkY = !queryYRange.encloses(tile.getBounds().getYRange());

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
