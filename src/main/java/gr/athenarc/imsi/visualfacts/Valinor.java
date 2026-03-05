package gr.athenarc.imsi.visualfacts;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import gr.athenarc.imsi.visualfacts.util.csv.CsvDoubleRowReader;
import gr.athenarc.imsi.visualfacts.util.csv.CsvReaderConfig;
import gr.athenarc.imsi.visualfacts.util.csv.ZsvCsvDoubleRowReader;
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

    private String sort = "asc";

    private InitializationPolicy initializationPolicy;

    private int objectsIndexed = 0;

    private double errorThreshold = 0;

    /**
     * When true, disables all aggregate metadata reuse:
     * - No frozen stats short-circuit
     * - No FC+Stats (complete leaf stats) reuse
     * - No sampledTracker persistence across queries
     * - No TreeNode stats updates during sampling
     * This mode implements the VALINOR-S baseline: plain incremental sampling
     * over the VALINOR spatial index without precomputed aggregate metadata.
     */
    private boolean samplingOnly = false;

    // Global statistics per measure column, computed during initialization (approximate mode only)
    private StatsAccumulator[] globalMeasureStats;

    /**
     * Creates a Valinor index in exact mode.
     */
    public Valinor(Schema schema) {
        this.schema = schema;
        this.errorThreshold = 0;
    }

    /**
     * Creates a Valinor index. If errorThreshold &gt; 0, runs in approximate mode
     * with adaptive sampling. If errorThreshold &lt;= 0, runs in exact mode.
     */
    public Valinor(Schema schema, double errorThreshold) {
        this.schema = schema;
        this.errorThreshold = errorThreshold;
    }

    /**
     * Creates a Valinor index in approximate mode with optional sampling-only baseline.
     */
    public Valinor(Schema schema, double errorThreshold, boolean samplingOnly) {
        this.schema = schema;
        this.errorThreshold = errorThreshold;
        this.samplingOnly = samplingOnly;
    }

    public boolean isExactMode() {
        return errorThreshold <= 0;
    }

    public void generateGrid(Query q0) {
        if (isInitialized)
            throw new IllegalStateException("The index is already initialized");

        if (q0 != null) {
            initializationPolicy = InitializationPolicy.getInitializationPolicy("valinor", q0,
                    (int) (GRID_SIZE * GRID_SIZE * SUBTILE_RATIO), schema, null, null);
            initializationPolicy.setSort(sort);
        }
        LOG.debug("Generating initial grid with size " + GRID_SIZE + "x" + GRID_SIZE);
        grid = new Grid(initializationPolicy, schema.getBounds(), schema.getCategoricalColumns(), GRID_SIZE);
        grid.split();
        if (initializationPolicy != null) {
            initializationPolicy.initTileTreeCategoricalAttrs(grid.getLeafTiles());
        }
    }

    public QueryResults initialize(Query q0) {
        generateGrid(q0);

        List<CategoricalColumn> categoricalColumns = schema.getCategoricalColumns();

        List<Integer> catColIndexes = categoricalColumns.stream().mapToInt(CategoricalColumn::getIndex).boxed()
                .collect(Collectors.toList());

        List<DataValidationFilter> validationFilters = schema.getValidationFilters();

        HashSet<Integer> colIndexes = new HashSet<>();

        colIndexes.add(schema.getxColumn());
        colIndexes.add(schema.getyColumn());
        colIndexes.addAll(catColIndexes);
        validationFilters.forEach(filter -> colIndexes.add(filter.getFilterColumn()));

        colIndexes.addAll(schema.getMeasureCols());

        LOG.debug("Columns to be read: " + colIndexes);

        int[] selectedColumns = colIndexes.stream().mapToInt(Integer::intValue).toArray();
        // Build mapping from original col index to position in selectedColumns
        Map<Integer, Integer> colIndexToRowPos = new HashMap<>();
        for (int i = 0; i < selectedColumns.length; i++) {
            colIndexToRowPos.put(selectedColumns[i], i);
        }
        CsvReaderConfig readerConfig = new CsvReaderConfig(
                new File(schema.getCsv()),
                Charset.forName("UTF-8"),
                selectedColumns,
                schema.getHasHeader(),
                schema.getDelimiter());
        CsvDoubleRowReader rowReader = new ZsvCsvDoubleRowReader();

        objectsIndexed = 0;
        int objectsSkipped = 0;

        final int xPos = colIndexToRowPos.get(schema.getxColumn());
        final int yPos = colIndexToRowPos.get(schema.getyColumn());
        final int logInterval = Math.max(1, schema.getObjectCount() / 10);

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

        try {
            rowReader.open(readerConfig);
            double[] row;

            // Build tile index mapping for partition
            List leafTileList = grid.getLeafTiles();
            int numTiles = leafTileList.size();
            IdentityHashMap<Tile, Integer> tileIndexMap = new IdentityHashMap<>(numTiles);
            for (int t = 0; t < numTiles; t++) {
                tileIndexMap.put((Tile) leafTileList.get(t), t);
            }

            // --- Phase 1: CSV scan → shared store + tileIds + per-tile counts + stats ---
            final int capacity = schema.getObjectCount();
            SharedPointStore store = new SharedPointStore(capacity);
            if (numTiles > Short.MAX_VALUE) {
                throw new IllegalStateException("Tile count " + numTiles + " exceeds short range; cannot use short[] tileIds");
            }
            short[] tileIds = new short[capacity];
            int validCount = 0;

            while ((row = rowReader.nextRow()) != null) {
                long rowOffset = rowReader.currentOffset();

                boolean shouldSkip = false;
                for (int i = 0; i < filterCount; i++) {
                    if (filterArray[i].test(row[filterPositions[i]])) {
                        shouldSkip = true;
                        break;
                    }
                }

                if (shouldSkip) {
                    objectsSkipped++;
                    continue;
                }

                double x = row[xPos];
                double y = row[yPos];

                if (!grid.getBounds().contains(x, y)) {
                    continue;
                }
                Tile leafTile = (Tile) grid.getLeafTile(x, y);
                TreeNode node = leafTile.getOrCreateRoot();

                store.set(validCount, x, y, rowOffset);
                tileIds[validCount] = tileIndexMap.get(leafTile).shortValue();
                validCount++;

                node.incrementCount();

                for (int i = 0; i < measureCount; i++) {
                    if (measurePositions[i] < 0) continue;
                    node.adjustStats(i, measureCount, row[measurePositions[i]]);
                }

                if (++objectsIndexed % logInterval == 0) {
                    LOG.debug("Indexing object " + objectsIndexed);
                }

            }

            // --- Phase 1.5: compute prefix sums from per-tile counts ---
            int[] counts = new int[numTiles];
            int[] starts = new int[numTiles];
            for (int t = 0; t < numTiles; t++) {
                TreeNode root = ((Tile) leafTileList.get(t)).getRoot();
                counts[t] = root != null ? root.getSize() : 0;
            }
            if (numTiles > 0) {
                starts[0] = 0;
                for (int t = 1; t < numTiles; t++) {
                    starts[t] = starts[t - 1] + counts[t - 1];
                }
            }

            // --- Phase 2: partition by tile ---
            // Transfer tileIds ownership to the store so the spill path can free
            // it before the final scatter (avoids G1 OOM on 500M+ row datasets).
            store.takeTileIds(tileIds);
            tileIds = null; // release caller's reference — store is sole owner now

            LOG.info("Partitioning {} points across {} tiles", validCount, numTiles);
            long partStart = System.nanoTime();
            store.partition(validCount, starts, numTiles);
            LOG.info("Partition done in {:.3f} s".replace("{:.3f}", 
                    String.format("%.3f", (System.nanoTime() - partStart) / 1e9)));

            // --- Phase 3: wire tiles to shared store slices ---
            for (int t = 0; t < numTiles; t++) {
                TreeNode root = ((Tile) leafTileList.get(t)).getRoot();
                if (root != null && counts[t] > 0) {
                    root.setSlice(store, starts[t], counts[t]);
                }
            }

            this.pointStore = store;

            // Capture max row length before closing the reader (needs live JNI handle)
            this.maxRowLength = (int) rowReader.maxRowLength();
            LOG.info("Max row length observed during init: {} bytes", maxRowLength);

        } catch (IOException e) {
            throw new RuntimeException("Unable to read CSV", e);
        } finally {
            try {
                rowReader.close();
            } catch (IOException ignore) {
            }
        }
        isInitialized = true;
        LOG.debug("Indexing Complete. Total Indexed Objects: " + objectsIndexed);
        LOG.debug("Total Skipped Objects: " + objectsSkipped);
        
        if (!isExactMode()) {
            // Compute global stats by aggregating from all leaf tile nodes (needed for initial sampling rate)
            computeGlobalMeasureStats();
        }
        
        if (isExactMode()) {
            return new QueryResults(q0);
        } else {
            return new ApproximateQueryResults(q0);
        }
    }

    public int getObjectsIndexed() {
        return objectsIndexed;
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
            batchReader = new RandomAccessRowReader(schema.getCsv(), maxRowLength);
        }

        List<AbstractNodePointIterator> rawIterators = new ArrayList<>();
        int fullyContainedTilesCount = 0;

        List<Tile> leafTiles = this.grid.getOverlappedLeafTiles(query);

        for (Tile leafTile : leafTiles) {
            // Short-circuited non-leaf tile with frozen exact stats
            if (leafTile.hasFrozenStats()) {
                fullyContainedTilesCount++;
                query.getMeasureCols().forEach(measureCol -> {
                    queryResults.adjustStats(null, measureCol,
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
                TreeNode node = queryNode.getNode();
                if ((!isFullyContained
                        || query.getMeasureCols().stream().anyMatch(mc -> !node.hasStats(schema.getMeasureIndex(mc))))
                        && node.hasPoints()) {
                    count += node.getSize();
                }
            }

            if (count > THRESHOLD) {
                leafTile.split();
                queryNodes = leafTile.getOverlappedActualLeafTiles(query).stream()
                        .flatMap(tile -> tile.getQueryNodes(query, containmentExaminer, schema).stream())
                        .collect(Collectors.toList());
            }

            for (QueryNode queryNode : queryNodes) {
                TreeNode node = queryNode.getNode();
                if (isFullyContained && query.getMeasureCols().stream().allMatch(mc -> node.hasStats(schema.getMeasureIndex(mc)))) {
                    query.getMeasureCols().forEach(measureCol -> {
                        queryResults.adjustStats(null, measureCol,
                                queryNode.getNode().getStats(schema.getMeasureIndex(measureCol)).snapshot());
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

            // Distribute parsed values to query results and node stats
            for (int rowIdx = 0; rowIdx < batchRows; rowIdx++) {
                QueryNode queryNode = nodes[rowIdx];
                TreeNode  node      = queryNode.getNode();
                int       idx       = 0;
                for (Integer measureCol : measureColsList) {
                    Integer ep = measureColToExtractedPos.get(measureCol);
                    double  value = Double.NaN;
                    if (ep != null && batchReader.isPresent(rowIdx, ep)) {
                        value = batchReader.getValue(rowIdx, ep);
                    }
                    if (!Double.isNaN(value)) {
                        queryResults.adjustStats(null, measureCol, value);
                    }
                    if (queryNode.isFullyContained()) {
                        node.adjustStats(idx, measureCount, value);
                    }
                    idx++;
                }
            }
        }

        queryResults.setTileCount(leafTiles.size());
        queryResults.setFullyContainedTileCount(fullyContainedTilesCount);
        queryResults.setIoCount(ioCount);

        // Compute rectStats by aggregating stats across all groups
        Map<Integer, StatsAccumulator> rectStatsAccumulators = new HashMap<>();
        queryResults.getStats().forEach((groupByValues, measureStats) -> {
            measureStats.forEach((measureCol, stats) -> {
                rectStatsAccumulators
                        .computeIfAbsent(measureCol, m -> new StatsAccumulator())
                        .addAll(stats);
            });
        });
        Map<Integer, Stats> rectStats = rectStatsAccumulators.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().snapshot()));
        queryResults.setRectStats(rectStats);

        return queryResults;
    }

    // ==================== Approximate Mode ====================

    private ApproximateQueryResults executeApproximateQuery(Query query) throws IOException {
        Rectangle rect = query.getRect();

        ApproximateQueryResults queryResults = new ApproximateQueryResults(query);

        if (batchReader == null) {
            batchReader = new RandomAccessRowReader(schema.getCsv(), maxRowLength);
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
                query.getMeasureCols().forEach(measureCol -> {
                    queryResults.adjustStats(null, measureCol,
                            leafTile.getFrozenStats(schema.getMeasureIndex(measureCol)));
                });
                continue;
            }

            ContainmentExaminer containmentExaminer = getContainmentExaminer(leafTile, rect);
            boolean isFullyContained = containmentExaminer == null;

            List<QueryNode> queryNodes = leafTile.getQueryNodes(query, containmentExaminer, schema);
            for (QueryNode queryNode : queryNodes) {
                TreeNode node = queryNode.getNode();
                if (node.getSize() == 0) {
                    continue;
                }

                if (isFullyContained && !samplingOnly && query.getMeasureCols().stream().allMatch(mc -> node.hasStats(schema.getMeasureIndex(mc)))) {
                    fullyContainedNodesWithStats.add(queryNode);
                } else if (node.getSize() > THRESHOLD) {
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
            query.getMeasureCols().forEach(measureCol -> {
                queryResults.adjustStats(null, measureCol,
                        queryNode.getNode().getStats(schema.getMeasureIndex(measureCol)).snapshot());
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

        AtomicDouble samplingRate = new AtomicDouble(computeInitialSamplingRate(samplingNodes));
        Map<Integer, double[]> confidenceIntervals = new HashMap<>();
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

                // Distribute parsed values to per-node sample accumulators
                for (int rowIdx = 0; rowIdx < batchRows; rowIdx++) {
                    QueryNode queryNode = nodes[rowIdx];
                    TreeNode  node      = queryNode.getNode();
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
                            node.adjustStats(idx, measureCount, value);
                        }
                        idx++;
                    }
                }
            }

            // Calculate the confidence intervals for all measures
            for (Integer measureCol : query.getMeasureCols()) {
                confidenceIntervals.put(measureCol,
                        getQueryConfidenceInterval(samplingNodes, queryResults, samplingRate.get(), measureCol));
            }
            // Calculate the error bounds for all measures
            for (Map.Entry<Integer, double[]> entry : confidenceIntervals.entrySet()) {
                Integer measureCol = entry.getKey();
                double[] confidenceInterval = entry.getValue();
                errorBounds.put(measureCol, calculateMaxErrorBound(confidenceInterval));
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

        LOG.debug("Sampling completed in {} round(s), final rate={}, I/Os={}", 
            samplingRounds, samplingRate.get(), ioCount);

        // Persist sampledTracker for future queries
        if (!samplingOnly) {
            fullyContainedNodesWithoutStats.forEach(queryNode -> {
                queryNode.getNode().setSampledTracker(queryNode.getSampledTracker());
            });
        }

        queryResults.setTileCount(leafTiles.size());
        queryResults.setFullyContainedTileCount(fullyContainedNodesWithStats.size() + frozenStatsTileCount);
        queryResults.setFullyContainedTileWithoutStatsCount(fullyContainedNodesWithoutStats.size());
        queryResults.setSamplingTileCount(samplingNodes.size());
        queryResults.setSamplingRounds(samplingRounds);
        queryResults.setSamplingRate(samplingRate.get());
        queryResults.setIoCount(ioCount);

        queryResults.setConfidenceIntervals(confidenceIntervals);
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
        
        LOG.debug("Initial sampling rate: {} (CV={}, requiredN={}, population={})", 
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
            TreeNode root = tile.getRoot();
            if (root != null) {
                aggregateNodeStats(root, measureCount);
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
     * Recursively aggregates stats from a TreeNode and all its children.
     */
    private void aggregateNodeStats(TreeNode node, int measureCount) {
        if (node.hasPoints()) {
            for (int i = 0; i < measureCount; i++) {
                StatsAccumulator nodeStats = node.getStats(i);
                if (nodeStats != null && nodeStats.count() > 0) {
                    globalMeasureStats[i].addAll(nodeStats.snapshot());
                }
            }
        }
        if (node.getChildren() != null) {
            for (TreeNode child : node.getChildren()) {
                aggregateNodeStats(child, measureCount);
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
    private double[] getQueryConfidenceInterval(List<QueryNode> samplingNodes, QueryResults queryResults,
            double samplingRate, int measureCol) {
        double exactSum = 0;
        if (queryResults.getStats().containsKey(null)) {
            exactSum = queryResults.getStats().get(null).get(measureCol).sum();
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

    private double calculateMaxErrorBound(double[] confidenceInterval) {
        double minSum = confidenceInterval[0];
        double maxSum = confidenceInterval[1];
        return (maxSum - minSum) / (maxSum + minSum);
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

    public double getTotalUtil() {
        return initializationPolicy.computeTotalUtil(grid.getLeafTiles());
    }

    public void setSort(String sort) {
        this.sort = sort;
    }

    @Override
    public synchronized void close() {
        if (batchReader != null) {
            batchReader.close();
            batchReader = null;
        }
    }
}
