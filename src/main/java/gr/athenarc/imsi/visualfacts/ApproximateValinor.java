package gr.athenarc.imsi.visualfacts;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.AtomicDouble;
import gr.athenarc.imsi.visualfacts.init.InitializationPolicy;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.XContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.XYContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.YContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.io.RandomAccessReader;
import gr.athenarc.imsi.visualfacts.util.csv.CsvReaderConfig;
import gr.athenarc.imsi.visualfacts.util.csv.CsvRowReader;
import gr.athenarc.imsi.visualfacts.util.csv.UnivocityCsvRowReader;

public class ApproximateValinor {

    private static final Logger LOG = LogManager.getLogger(ApproximateValinor.class);

    private boolean isInitialized = false;

    private RandomAccessReader randomAccessReader;

    private Grid grid;

    private Schema schema;

    private String sort = "asc";

    private InitializationPolicy initializationPolicy;

    private int objectsIndexed = 0;

    private double errorThreshold = 0.05;

    public ApproximateValinor(Schema schema, Double errorThreshold) {
        this.schema = schema;
        this.errorThreshold = errorThreshold;
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
        LOG.debug("Validation filters: " + validationFilters);
        validationFilters.forEach(filter -> colIndexes.add(filter.getFilterColumn()));

        colIndexes.addAll(schema.getMeasureCols());

        LOG.debug("Columns to be read: " + colIndexes);

        int[] selectedColumns = colIndexes.stream().mapToInt(Integer::intValue).toArray();
        CsvReaderConfig readerConfig = new CsvReaderConfig(
                new File(schema.getCsv()),
                Charset.forName("US-ASCII"),
                selectedColumns,
                schema.getHasHeader(),
                DELIMITER);
        CsvRowReader rowReader = new UnivocityCsvRowReader();

        objectsIndexed = 0;
        int objectsSkipped = 0; // Counter for skipped rows

        try {
            rowReader.open(readerConfig);
            String[] row;
            while ((row = rowReader.nextRow()) != null) {
                long rowOffset = rowReader.currentOffset();
                try {
                    final String[] finalRow = row;
                    boolean shouldSkip = validationFilters.stream().anyMatch(filter -> {
                        int colIndex = filter.getFilterColumn();
                        try {
                            Double value = Double.parseDouble(finalRow[colIndex]);
                            return filter.test(value);
                        } catch (NumberFormatException e) {
                            LOG.debug("Skipping row due to invalid numeric value: " + Arrays.toString(finalRow));
                            return true; // Skip invalid numeric entries
                        }
                    });

                    if (shouldSkip) {
                        LOG.debug("Skipping row: " + Arrays.toString(row));
                        objectsSkipped++;
                        continue;
                    }
                    Point point = new Point(Float.parseFloat(row[schema.getxColumn()]),
                            Float.parseFloat(row[schema.getyColumn()]), rowOffset);

                    TreeNode node = this.grid.addPoint(point, row);
                    if (node == null) {
                        continue;
                    }

                    for (Integer measureCol : schema.getMeasureCols()) {
                        Float value = Float.parseFloat(row[measureCol]);
                        node.adjustStats((short) (int) measureCol, value);
                    }

                    if (++objectsIndexed % 1000000 == 0) {
                        LOG.debug("Indexing object " + objectsIndexed);
                        LOG.debug("Row: " + Arrays.toString(row));
                        LOG.debug(point);
                    }
                } catch (Exception e) {
                    LOG.error("Problem parsing row number " + objectsIndexed + ": " + Arrays.toString(row), e);
                }
            }
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

        if (randomAccessReader == null) {
            randomAccessReader = RandomAccessReader.open(new File(schema.getCsv()));
        }
        List<QueryNode> nonRawNodes = new ArrayList<>();

        List<Tile> leafTiles = this.grid.getOverlappedLeafTiles(query);

        List<QueryNode> fullyContainedNodesWithStats = new ArrayList<>();
        List<QueryNode> fullyContainedNodesWithoutStats = new ArrayList<>();
        List<QueryNode> partialNodes = new ArrayList<>();

        for (Tile leafTile : leafTiles) {
            ContainmentExaminer containmentExaminer = getContainmentExaminer(leafTile, rect);
            boolean isFullyContained = containmentExaminer == null;

            List<QueryNode> queryNodes = leafTile.getQueryNodes(query, containmentExaminer, schema);
            for (QueryNode queryNode : queryNodes) {
                TreeNode node = queryNode.getNode();
                if (node.getPoints() == null) {
                    continue;
                }

                if (isFullyContained && query.getMeasureCols().stream().allMatch(node::hasStats)) {
                    fullyContainedNodesWithStats.add(queryNode);
                } else if (node.points.size() > THRESHOLD) {
                    leafTile.split();
                    leafTile.getOverlappedLeafTiles(query).stream()
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
                        queryNode.getNode().getStats(measureCol).snapshot());
            });
            nonRawNodes.add(queryNode);
        }

        List<Integer> cols = new ArrayList<>();

        cols.addAll(schema.getMeasureCols());

        int[] parseColumns = cols.stream().mapToInt(Integer::intValue).toArray();
        CsvRowReader lineParser = new UnivocityCsvRowReader();
        CsvReaderConfig lineConfig = new CsvReaderConfig(null, Charset.forName("US-ASCII"),
                parseColumns, false, DELIMITER);
        try {
            lineParser.open(lineConfig);
        } catch (IOException e) {
            throw new RuntimeException("Unable to configure CSV line parser", e);
        }

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

        AtomicDouble samplingRate = new AtomicDouble(0.01d); // Start with small sampling rate (1%)
        Map<Integer, double[]> confidenceIntervals = new HashMap<>();
        Map<Integer, Double> errorBounds = new HashMap<>();
        do {
            // Create Sampling Iterators for all tiles needing sampling
            KWayMergePointIterator pointIterator = new KWayMergePointIterator(samplingNodes.stream()
                    .map(queryNode -> new SamplingNodePointsIterator(queryNode, samplingRate.get()))
                    .collect(Collectors.toList()));

            // Read the sampled points from the file in sorted order
            ioCount += readFromFile(query, queryResults, lineParser, pointIterator);

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
                samplingRate.set(adjustSamplingRate(samplingRate.get(), maxErrorBound, errorThreshold));
                // LOG.info("Increasing sampling rate to: {}", samplingRate.get());
            }

        } while (errorBounds.values().stream().anyMatch(error -> error > errorThreshold));

        // Iterate over fully contained query nodes without stats and set their
        // TreeNode's sampled tracker for using in future queries. Their stats have been
        // updated in the readFromFile method
        fullyContainedNodesWithoutStats.forEach(queryNode -> {
            queryNode.getNode().setSampledTracker(queryNode.getSampledTracker());
        });

        queryResults.setTileCount(leafTiles.size());
        queryResults.setFullyContainedTileCount(fullyContainedNodesWithStats.size());
        queryResults.setFullyContainedTileWithoutStatsCount(fullyContainedNodesWithoutStats.size());
        queryResults.setSamplingTileCount(samplingNodes.size());
        queryResults.setSamplingRate(samplingRate.get());
        queryResults.setIoCount(ioCount);

        queryResults.setConfidenceIntervals(confidenceIntervals);
        queryResults.setErrorBounds(errorBounds);

        try {
            lineParser.close();
        } catch (IOException ignore) {
        }

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

            // SHORT-CIRCUIT if we sampled 100% of that node
            if (n == N) {
                // We have the entire sub-population in this node, so no sampling uncertainty.
                // sampleStatsAcc.sum() == sum of all values in that node
                double nodeExactSum = qnode.getSampleStatsAcc(measureCol).sum();
                exactSum += nodeExactSum; // Add to exact part,
                // variance contribution is 0
                continue;
            }
            if (n < 2) {
                // fallback path: use minSum / maxSum or skip
                // Or you can add a big variance chunk if you want to keep it approximate
                LOG.error("Sampling Node with less than 2 samples: {}", qnode);
                continue;
            }

            double mean = qnode.getSampleStatsAcc(measureCol).mean();
            double stdev = qnode.getSampleStatsAcc(measureCol).sampleStandardDeviation();

            // // node-level estimate
            double nodeEstimate = N * mean;
            // node-level variance considering the finite population correction for without
            // replacement sampling
            double nodeVariance = N * N * (stdev * stdev / n) * (1.0 - ((double) n / N));
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

    private int readFromFile(Query query, QueryResults queryResults, CsvRowReader parser,
            KWayMergePointIterator pointIterator) {
        String line;
        String[] row;
        int ioCount = 0;
        while (pointIterator.hasNext()) {
            ioCount++;
            Point point = pointIterator.next();
            try {
                randomAccessReader.seek(point.getFileOffset());
                line = randomAccessReader.readLine();
                if (line != null) {
                    row = parser.parseLine(line);
                    if (row != null) {
                        QueryNode queryNode = pointIterator.getCurrentQueryNode();
                        // Process all measures in the schema
                        for (Integer measureCol : schema.getMeasureCols()) {
                            if (row[measureCol] != null) {
                                double measureValue = Double.parseDouble(row[measureCol]);
                                queryNode.addSampleValue(measureCol, measureValue);
                                if (queryNode.isFullyContained()) {
                                    queryNode.getNode().adjustStats(measureCol.shortValue(), measureValue);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Error reading from file at offset " + point.getFileOffset() + ": " + e.getMessage(), e);
            }
        }
        return ioCount;
    }

    private int readFullyContainedFromFile(Query query, QueryResults queryResults,
            CsvRowReader parser, KWayMergePointIterator pointIterator) {
        String line;
        String[] row;
        int ioCount = 0;
        while (pointIterator.hasNext()) {
            ioCount++;
            Point point = pointIterator.next();
            try {
                randomAccessReader.seek(point.getFileOffset());
                line = randomAccessReader.readLine();
                if (line != null) {
                    row = parser.parseLine(line);
                    if (row != null) {
                        QueryNode queryNode = pointIterator.getCurrentQueryNode();
                        // Process all measures in the schema
                        for (Integer measureCol : schema.getMeasureCols()) {
                            if (row[measureCol] != null) {
                                double measureValue = Double.parseDouble(row[measureCol]);
                                queryNode.getNode().adjustStats(measureCol.shortValue(), measureValue);

                            }
                        }

                    }
                }
            } catch (Exception e) {
                LOG.error("Error reading from file at offset " + point.getFileOffset() + ": " + e.getMessage(), e);
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
    public void finalize() {
        try {
            randomAccessReader.close();
        } catch (IOException e) {
            LOG.error("Error closing RandomAccessReader: " + e.getMessage(), e);
        }
    }
}
