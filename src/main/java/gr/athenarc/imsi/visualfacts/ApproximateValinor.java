package gr.athenarc.imsi.visualfacts;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import gr.athenarc.imsi.visualfacts.util.csv.CsvFloatRowReader;
import gr.athenarc.imsi.visualfacts.util.csv.CsvReaderConfig;
import gr.athenarc.imsi.visualfacts.util.csv.ZsvCsvFloatRowReader;
import gr.athenarc.imsi.visualfacts.util.io.MappedFileReader;

public class ApproximateValinor implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(ApproximateValinor.class);

    private boolean isInitialized = false;

    private MappedFileReader mappedFileReader;

    private Grid grid;

    private Schema schema;

    private String sort = "asc";

    private InitializationPolicy initializationPolicy;

    private int objectsIndexed = 0;

    private double errorThreshold = 0.05;

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

    // Global statistics per measure column, computed during initialization
    private StatsAccumulator[] globalMeasureStats;

    public ApproximateValinor(Schema schema, Double errorThreshold) {
        this.schema = schema;
        this.errorThreshold = errorThreshold;
    }

    public ApproximateValinor(Schema schema, Double errorThreshold, boolean samplingOnly) {
        this.schema = schema;
        this.errorThreshold = errorThreshold;
        this.samplingOnly = samplingOnly;
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

    public ApproximateQueryResults initialize(Query q0) {
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
        CsvFloatRowReader rowReader = new ZsvCsvFloatRowReader();

        objectsIndexed = 0;
        int objectsSkipped = 0; // Counter for skipped rows

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
            float[] row;

            // --- Phase 1: CSV scan → global staging arrays + per-tile counts + stats ---
            final int capacity = schema.getObjectCount();
            float[] gXs = new float[capacity];
            float[] gYs = new float[capacity];
            long[] gOffsets = new long[capacity];
            int validCount = 0;

            while ((row = rowReader.nextRow()) != null) {
                long rowOffset = rowReader.currentOffset();

                boolean shouldSkip = false;
                for (int i = 0; i < filterCount; i++) {
                    if (filterArray[i].test((double) row[filterPositions[i]])) {
                        shouldSkip = true;
                        break;
                    }
                }

                if (shouldSkip) {
                    objectsSkipped++;
                    continue;
                }

                float x = row[xPos];
                float y = row[yPos];

                TreeNode node = this.grid.getOrCreateLeafRoot(x, y);
                if (node == null) {
                    continue;
                }

                gXs[validCount] = x;
                gYs[validCount] = y;
                gOffsets[validCount] = rowOffset;
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

            // --- Phase 2: allocate exact per-tile arrays, distribute from global ---
            for (Object obj : grid.getLeafTiles()) {
                Tile leafTile = (Tile) obj;
                TreeNode root = leafTile.getRoot();
                if (root != null && root.getSize() > 0) {
                    root.allocateExact();
                }
            }
            for (int i = 0; i < validCount; i++) {
                TreeNode node = this.grid.getOrCreateLeafRoot(gXs[i], gYs[i]);
                node.insertAtCursor(gXs[i], gYs[i], gOffsets[i]);
            }

            // Free staging arrays
            gXs = null;
            gYs = null;
            gOffsets = null;

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
        
        // Compute global stats by aggregating from all leaf tile nodes
        computeGlobalMeasureStats();
        
        // todo evaluate q0
        ApproximateQueryResults queryResults = new ApproximateQueryResults(q0);
        return queryResults;
    }

    public int getObjectsIndexed() {
        return objectsIndexed;
    }

    public synchronized ApproximateQueryResults executeQuery(Query query) throws IOException {
        if (!isInitialized) {
            return initialize(query);
        }
        Rectangle rect = query.getRect();

        ApproximateQueryResults queryResults = new ApproximateQueryResults(query);

        if (mappedFileReader == null) {
            mappedFileReader = MappedFileReader.open(new File(schema.getCsv()));
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
            // This tile was previously split but its pre-split stats were preserved.
            // Since it's guaranteed fully contained (the only way it's returned from
            // getOverlappedLeafTiles), use the frozen stats directly.
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
                                // Check for full containment on each returned subtile node
                                if (qn.isFullyContained()) {
                                    fullyContainedNodesWithoutStats.add(qn);
                                } else {
                                    partialNodes.add(qn);
                                }
                            });
                } else {
                    // For nodes that do not require splitting, check full containment:
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

        // ////////////
        // /// baseline method that includes reading all points from fully contained
        // tiles
        // KWayMergePointIterator fullyContainedPointIterator = new
        // KWayMergePointIterator(
        // fullyContainedNodesWithoutStats.stream()
        // .map(queryNode -> new NodePointsIterator(queryNode))
        // .collect(Collectors.toList()));

        // // Read all points from fully contained tiles and adjust their stats
        // ioCount += readFullyContainedFromFile(query, queryResults, measureCol0,
        // parser, fullyContainedPointIterator);

        // for (QueryNode queryNode : fullyContainedNodesWithoutStats) {
        // queryResults.adjustStats(null, queryNode.getNode().getStats().snapshot());
        // }
        // // end of baseline method
        // ////////////

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

            // Read the sampled points from the file in sorted order
            ioCount += readFromFile(query, queryResults, sortedMeasureCols, measureColToExtractedPos, delimiterByte, pointIterator);

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

        // Iterate over fully contained query nodes without stats and set their
        // TreeNode's sampled tracker for using in future queries. Their stats have been
        // updated in the readFromFile method
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

    /**
     * Adjusts the sampling rate based on the current relative error and the target
     * error threshold.
     * 
     * @param currentRate    the current sampling rate (e.g., 0.1 for 10% sampling)
     * @param currentError   the current relative error from the sample estimates
     * @param errorThreshold the desired error threshold
     * @return the new sampling rate, capped at 1.0 (i.e., 100% sampling)
     */
    private double adjustSamplingRate(double currentRate, double currentError, double errorThreshold) {
        // If the current error is already below or equal to the threshold, no
        // adjustment is needed.
        if (currentError <= errorThreshold) {
            return currentRate;
        }

        // Compute the multiplicative factor based on the error ratio squared.
        // The intuition: variance (and thus error) decreases approximately as
        // 1/sqrt(n),
        // so to reduce error by a factor of (currentError/errorThreshold),
        // you need roughly (currentError/errorThreshold)^2 times more samples.
        double factor = Math.pow(currentError / errorThreshold, 2);

        // To avoid an overly large jump in sampling rate, cap the maximum increase.
        // For example, limit the increase to a maximum factor of 2x.
        double maxFactor = 2.0;
        if (factor > maxFactor) {
            factor = maxFactor;
        }

        // Calculate the new sampling rate.
        double newRate = currentRate * factor;

        // Compute the delta increase
        double delta = newRate - currentRate;

        // Set a minimum delta (for example, 0.01) to ensure noticeable progress
        double minDelta = 0.01;
        if (delta < minDelta) {
            newRate = currentRate + minDelta;
        }

        // Ensure the new sampling rate does not exceed 100%.
        if (newRate > 1.0) {
            newRate = 1.0;
        }

        return newRate;
    }

    /**
     * Computes the initial sampling rate for the given sampling nodes.
     * CV-based estimation for more efficient sampling.
     *
     * @param samplingNodes the list of nodes that require sampling
     * @return the initial sampling rate (between 0 and 1)
     */
    private double computeInitialSamplingRate(List<QueryNode> samplingNodes) {
        if (samplingNodes == null || samplingNodes.isEmpty()) {
            return 0.01d;
        }
        
        // Get the maximum CV across all measure columns
        double maxCV = 0.0;
        for (int i = 0; i < schema.getMeasureCount(); i++) {
            double cv = getMeasureCV(i);
            if (cv > maxCV) {
                maxCV = cv;
            }
        }
        // If no valid CV found, fall back to conservative default
        if (maxCV <= 0) {
            maxCV = 1.0;
        }
        
        // Cochran's formula: n = (z * CV / error)^2
        double z = 1.96;  // 95% confidence
        double requiredN = Math.pow(z * maxCV / errorThreshold, 2);
        
        // Safety margin multiplier (1.0 = no margin, 1.5 = 50% extra samples)
        final double SAFETY_MARGIN = 1.0;
        requiredN *= SAFETY_MARGIN;
        
        // Total population in sampling nodes
        long totalPopulation = samplingNodes.stream()
            .mapToLong(QueryNode::getIntersectionCount)
            .sum();
        
        if (totalPopulation == 0) {
            return 0.01d;
        }
        
        double rate = requiredN / totalPopulation;
        
        // Ensure at least MIN_SAMPLES for CLT validity, then clamp to max 100%
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
        
        // Traverse all leaf tiles and aggregate their root node stats
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
        // If this is a leaf node (has points), aggregate its stats
        if (node.hasPoints()) {
            for (int i = 0; i < measureCount; i++) {
                StatsAccumulator nodeStats = node.getStats(i);
                if (nodeStats != null && nodeStats.count() > 0) {
                    globalMeasureStats[i].addAll(nodeStats.snapshot());
                }
            }
        }
        
        // Recurse into children if any
        if (node.getChildren() != null) {
            for (TreeNode child : node.getChildren()) {
                aggregateNodeStats(child, measureCount);
            }
        }
    }

    /**
     * Maximum CV cap to prevent pathological cases from requiring 100% sampling.
     * CV > 2.0 is statistically "very high variance"; beyond this, approximation
     * quality degrades but capping ensures practical sample sizes.
     */
    private static final double MAX_CV_CAP = 2.0;

    /**
     * Returns the coefficient of variation (CV = std/mean) for a given measure column.
     * CV is used to estimate the required sample size for a given error bound.
     * The CV is capped at MAX_CV_CAP to ensure practical sample sizes for high-variance data.
     *
     * @param measureIndex the index of the measure (0-based index into measureCols)
     * @return the coefficient of variation, capped at MAX_CV_CAP, or 1.0 if not available
     */
    public double getMeasureCV(int measureIndex) {
        if (globalMeasureStats == null || measureIndex < 0 || measureIndex >= globalMeasureStats.length) {
            return 1.0;  // Conservative default
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
     *
     * @param measureIndex the index of the measure (0-based index into measureCols)
     * @return the Stats snapshot, or null if not available
     */
    public Stats getGlobalMeasureStats(int measureIndex) {
        if (globalMeasureStats == null || measureIndex < 0 || measureIndex >= globalMeasureStats.length) {
            return null;
        }
        StatsAccumulator stats = globalMeasureStats[measureIndex];
        return stats != null ? stats.snapshot() : null;
    }

    // private double adjustSamplingRate(double currentRate, double currentError,
    // double errorThreshold) {
    // LOG.debug("Adjusting sampling rate: currentRate={}, currentError={},
    // errorThreshold={}", currentRate,
    // currentError, errorThreshold);
    // double adjustmentFactor = (currentError - errorThreshold) / errorThreshold;
    // // How far above the limit we are
    // return Math.min(1.0, currentRate * (1.0 + adjustmentFactor)); // Increase
    // sampling but cap at 100%
    // }

    private double[] getQueryConfidenceInterval(List<QueryNode> samplingNodes, QueryResults queryResults,
            double samplingRate, int measureCol) {
        double exactSum = 0;
        if (queryResults.getStats().containsKey(null)) {
            exactSum = queryResults.getStats().get(null).get(measureCol).sum();
            // sum from "fully contained" nodes with known stats
        }

        if (samplingNodes == null || samplingNodes.isEmpty()) {
            return new double[] { exactSum, exactSum };
        }

        // We'll accumulate total estimated sum & total variance from sampled nodes:
        double totalEstimate = 0.0;
        double totalVariance = 0.0;

        for (QueryNode qnode : samplingNodes) {
            int n = (int) qnode.getSampleStatsAcc(measureCol).count();
            double N = qnode.getIntersectionCount();
            int totalSampled = qnode.getSampledTracker().cardinality();

            // SHORT-CIRCUIT if we sampled 100% of that node's points (all read from disk)
            if (totalSampled >= (int) N) {
                // We have read every point in this node — n is the exact non-NaN count.
                double nodeExactSum = qnode.getSampleStatsAcc(measureCol).sum();
                exactSum += nodeExactSum; // Add to exact part,
                // variance contribution is 0
                continue;
            }
            if (n < 2) {
                // Too few non-NaN samples (common with NaN-heavy columns like Gaia's ~19% null rate).
                // Use init-time stats if available, otherwise use the single sample or skip.
                if (n == 1) {
                    // Single valid sample — use it as the mean estimate with high uncertainty
                    double singleValue = qnode.getSampleStatsAcc(measureCol).mean();
                    int sampledCount = qnode.getSampledTracker().cardinality();
                    // Effective non-NaN population: scale down N by observed non-NaN ratio
                    double effectiveN = N * ((double) n / Math.max(sampledCount, 1));
                    totalEstimate += effectiveN * singleValue;
                    // No variance contribution (can't compute stdev from 1 sample)
                    // This is conservative — the loop guard will prevent infinite retries
                } 
                // n == 0: all sampled points were NaN for this measure — node contributes nothing
                LOG.trace("Node with {} valid samples out of {} sampled (intersectionCount={})",
                    n, qnode.getSampledTracker().cardinality(), (int) N);
                continue;
            }

            double mean = qnode.getSampleStatsAcc(measureCol).mean();
            double stdev = qnode.getSampleStatsAcc(measureCol).sampleStandardDeviation();

            // Estimate effective non-NaN population from observed non-NaN ratio
            double nonNaNRatio = (double) n / Math.max(totalSampled, 1);
            double effectiveN = N * nonNaNRatio;

            // node-level estimate (over estimated non-NaN population)
            double nodeEstimate = effectiveN * mean;
            // node-level variance considering the finite population correction for without
            // replacement sampling
            double nodeVariance = effectiveN * effectiveN * (stdev * stdev / n) * (1.0 - ((double) n / effectiveN));
            ;

            totalEstimate += nodeEstimate;
            totalVariance += nodeVariance;
        }

        double finalEstimate = exactSum + totalEstimate;

        // standard error from partial region
        double stdError = Math.sqrt(totalVariance);
        double z = getZScoreForConfidence(0.95);
        double margin = z * stdError;

        double lower = finalEstimate - margin;
        double upper = finalEstimate + margin;

        /*
         * LOG.
         * debug("samplingRate={}, exactSum={}, totalEstimate={}, totalVariance={}, lb={}, up={}"
         * , samplingRate,
         * exactSum, totalEstimate, totalVariance, lower, upper);
         */

        return new double[] { lower, upper };
    }

    // Helper to retrieve z-score for a confidence level
    private double getZScoreForConfidence(double confidenceLevel) {
        // For a two-tailed confidence interval, the "confidenceLevel"
        // is usually something like 0.90, 0.95, or 0.99.
        // We map these to z-scores from the standard Normal distribution.

        if (confidenceLevel == 0.90) {
            return 1.645; // ~90% CI
        } else if (confidenceLevel == 0.95) {
            return 1.96; // ~95% CI
        } else if (confidenceLevel == 0.99) {
            return 2.575; // ~99% CI
        }

        // Fallback: either throw or pick a default
        throw new IllegalArgumentException(
                "Unsupported confidence level: " + confidenceLevel);

    }

    private double calculateMaxErrorBound(double[] confidenceInterval) {
        double minSum = confidenceInterval[0];
        double maxSum = confidenceInterval[1];
        return (maxSum - minSum) / (maxSum + minSum);
    }

    private int readFromFile(Query query, QueryResults queryResults, int[] sortedMeasureCols,
            Map<Integer, Integer> measureColToExtractedPos, byte delimiterByte,
            KWayMergePointIterator pointIterator) {
        int ioCount = 0;
        List<Integer> measureColsList = schema.getMeasureCols();
        while (pointIterator.hasNext()) {
            ioCount++;
            long fileOffset = pointIterator.nextOffset();
            try {
                mappedFileReader.seek(fileOffset);
                
                // Fast path: extract only measure columns as floats directly from mmap
                float[] extractedValues = mappedFileReader.extractFloats(sortedMeasureCols, delimiterByte);
                
                QueryNode queryNode = pointIterator.getCurrentQueryNode();
                TreeNode node = queryNode.getNode();
                
                // Process all measures in the schema
                int idx = 0;
                for (Integer measureCol : measureColsList) {
                    Integer extractedPos = measureColToExtractedPos.get(measureCol);
                    float value = (extractedPos != null && extractedPos < extractedValues.length)
                            ? extractedValues[extractedPos] : Float.NaN;
                    if (!Float.isNaN(value)) {
                        queryNode.addSampleValue(measureCol, value);
                    }
                    // Progressive stats building for post-split children (safe via statsPointCount).
                    if (!samplingOnly && queryNode.isFullyContained()) {
                        node.adjustStats(idx, schema.getMeasureCount(), value);
                    }
                    idx++;
                }
            } catch (Exception e) {
                LOG.error("Error reading from file at offset " + fileOffset + ": " + e.getMessage(), e);
            }
        }
        return ioCount;
    }

    private int readFullyContainedFromFile(Query query, QueryResults queryResults,
            int[] sortedMeasureCols, Map<Integer, Integer> measureColToExtractedPos, 
            byte delimiterByte, KWayMergePointIterator pointIterator) {
        int ioCount = 0;
        List<Integer> measureColsList = schema.getMeasureCols();
        while (pointIterator.hasNext()) {
            ioCount++;
            long fileOffset = pointIterator.nextOffset();
            try {
                mappedFileReader.seek(fileOffset);
                
                // Fast path: extract only measure columns as floats directly from mmap
                float[] extractedValues = mappedFileReader.extractFloats(sortedMeasureCols, delimiterByte);
                
                QueryNode queryNode = pointIterator.getCurrentQueryNode();
                TreeNode node = queryNode.getNode();
                
                // Process all measures — progressive stats building for post-split children
                int idx = 0;
                for (Integer measureCol : measureColsList) {
                    Integer extractedPos = measureColToExtractedPos.get(measureCol);
                    float value = (extractedPos != null && extractedPos < extractedValues.length)
                            ? extractedValues[extractedPos] : Float.NaN;
                    node.adjustStats(idx, schema.getMeasureCount(), value);
                    idx++;
                }
            } catch (Exception e) {
                LOG.error("Error reading from file at offset " + fileOffset + ": " + e.getMessage(), e);
            }
        }
        return ioCount;
    }

    private ContainmentExaminer getContainmentExaminer(Tile tile, Rectangle query) {

        Range<Float> queryXRange = query.getXRange();
        Range<Float> queryYRange = query.getYRange();
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
    public void close() {
        if (mappedFileReader != null) {
            try {
                mappedFileReader.close();
            } catch (IOException e) {
                LOG.warn("Failed to close mappedFileReader", e);
            } finally {
                mappedFileReader = null;
            }
        }
    }
}
