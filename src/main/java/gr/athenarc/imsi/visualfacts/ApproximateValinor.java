package gr.athenarc.imsi.visualfacts;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Range;
import com.google.common.math.PairedStatsAccumulator;
import com.google.common.math.StatsAccumulator;
import com.google.common.util.concurrent.AtomicDouble;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import gr.athenarc.imsi.visualfacts.init.InitializationPolicy;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.XContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.XYContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.YContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.io.RandomAccessReader;

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
        List<Integer> colIndexes = new ArrayList<>();

        colIndexes.add(schema.getxColumn());
        colIndexes.add(schema.getyColumn());
        colIndexes.addAll(catColIndexes);

        Integer measureCol0 = schema.getMeasureCol0();
        if (measureCol0 != null) {
            colIndexes.add(measureCol0);
        }

        CsvParserSettings parserSettings = schema.createCsvParserSettings();
        parserSettings.selectIndexes(colIndexes.toArray(new Integer[colIndexes.size()]));
        parserSettings.setColumnReorderingEnabled(false);
        parserSettings.setHeaderExtractionEnabled(schema.getHasHeader());
        CsvParser parser = new CsvParser(parserSettings);

        objectsIndexed = 0;

        parser.beginParsing(new File(schema.getCsv()), Charset.forName("US-ASCII"));
        String[] row;
        long rowOffset = parser.getContext().currentChar() - 1;
        while ((row = parser.parseNext()) != null) {
            try {
                Point point = new Point(Float.parseFloat(row[schema.getxColumn()]),
                        Float.parseFloat(row[schema.getyColumn()]), rowOffset);

                TreeNode node = this.grid.addPoint(point, row);
                if (node == null) {
                    continue;
                }

                if (measureCol0 != null) {
                    Float value0 = Float.parseFloat(row[measureCol0]);
                    Float value1 = 0f;
                    node.adjustStats(value0, value1);
                }
                if (++objectsIndexed % 1000000 == 0) {
                    LOG.debug("Indexing object " + objectsIndexed);
                    LOG.debug(point);
                }
            } catch (Exception e) {
                LOG.error("Problem parsing row number " + objectsIndexed + ": " + Arrays.toString(row), e);
                continue;
            } finally {
                rowOffset = parser.getContext().currentChar() - 1;
            }
        }

        parser.stopParsing();
        isInitialized = true;
        LOG.debug("Indexing Complete. Total Indexed Objects: " + objectsIndexed);
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

        int fullyContainedTilesCount = 0;

        List<Tile> leafTiles = this.grid.getOverlappedLeafTiles(query);

        List<QueryNode> fullyContainedNodesWithStats = new ArrayList<>();
        List<QueryNode> samplingNodes = new ArrayList<>();

        for (Tile leafTile : leafTiles) {
            ContainmentExaminer containmentExaminer = getContainmentExaminer(leafTile, rect);
            boolean isFullyContained = containmentExaminer == null;
            if (isFullyContained) {
                fullyContainedTilesCount++;
            }

            List<QueryNode> queryNodes = leafTile.getQueryNodes(query, containmentExaminer, schema);
            for (QueryNode queryNode : queryNodes) {
                TreeNode node = queryNode.getNode();
                if (node.getPoints() == null) {
                    continue;
                }

                if (isFullyContained && node.hasStats()) {
                    fullyContainedNodesWithStats.add(queryNode);
                } else if (node.points.size() > THRESHOLD) {
                    leafTile.split();
                    samplingNodes.addAll(leafTile.getOverlappedLeafTiles(query).stream()
                            .flatMap(tile -> tile.getQueryNodes(query, containmentExaminer,
                                    schema).stream())
                            .collect(Collectors.toList()));
                } else {
                    samplingNodes.add(queryNode);
                }

            }
        }
        for (QueryNode queryNode : fullyContainedNodesWithStats) {
            queryResults.adjustStats(null, queryNode.getNode().getStats().snapshot());
            nonRawNodes.add(queryNode);
        }

        List<Integer> cols = new ArrayList<>();

        Integer measureCol0 = schema.getMeasureCol0();
        if (measureCol0 != null) {
            cols.add(measureCol0);
        }

        CsvParserSettings parserSettings = schema
                .createCsvParserSettings();
        parserSettings.selectIndexes(cols.toArray(new Integer[cols.size()]));
        parserSettings.setColumnReorderingEnabled(false);
        CsvParser parser = new CsvParser(parserSettings);

        int ioCount = 0;
        AtomicDouble samplingRate = new AtomicDouble(0.01d); // Start with small sampling rate (1%)
        double[] confidenceInterval;
        double maxErrorBound;
        do {
            // Create Sampling Iterators for all tiles needing sampling
            KWayMergePointIterator pointIterator = new KWayMergePointIterator(samplingNodes.stream()
                    .map(queryNode -> new SamplingNodePointsIterator(queryNode, samplingRate.get()))
                    .collect(Collectors.toList()));

            // Read the sampled points from the file in sorted order
            ioCount += readFromFile(query, queryResults, measureCol0, parser, pointIterator);

            // Calculate the confidence interval for the query
            confidenceInterval = getQueryConfidenceInterval(samplingNodes, queryResults);
            // Recalculate the error bound
            maxErrorBound = calculateMaxErrorBound(confidenceInterval);

            // If error bound is still too high, increase sampling rate
            if (maxErrorBound > errorThreshold) {
                samplingRate.set(adjustSamplingRate(samplingRate.get(), maxErrorBound, errorThreshold));
                LOG.info("Increasing sampling rate to: {}", samplingRate.get());
            }

        } while (maxErrorBound > errorThreshold);

        queryResults.setTileCount(leafTiles.size());
        queryResults.setFullyContainedTileCount(fullyContainedTilesCount);
        queryResults.setIoCount(ioCount);
        queryResults.setConfidenceInterval(confidenceInterval);
        queryResults.setErrorBound(maxErrorBound);
        return queryResults;
    }

    private double adjustSamplingRate(double currentRate, double currentError, double errorThreshold) {
        LOG.debug("Adjusting sampling rate: currentRate={}, currentError={}, errorThreshold={}", currentRate,
                currentError, errorThreshold);
        double adjustmentFactor = (currentError - errorThreshold) / errorThreshold; // How far above the limit we are
        return Math.min(1.0, currentRate * (1.0 + adjustmentFactor)); // Increase sampling but cap at 100%
    }

    private double[] getQueryConfidenceInterval(List<QueryNode> samplingNodes, QueryResults queryResults) {
        double exactSum = 0;
        if (queryResults.getStats().containsKey(null)) {
            exactSum = queryResults.getStats().get(null).xStats().sum(); 
            // sum from "fully contained" nodes with known stats
        }
    
        // We'll accumulate total estimated sum & total variance from partial/sampled nodes:
        double totalEstimate = 0.0;
        double totalVariance = 0.0;
    
        for (QueryNode qnode : samplingNodes) {
            int n = (int) qnode.getSampleStatsAcc().count();
            // if no samples, fallback to deterministic bounds or skip
            if (n < 2) {
                // fallback path: use minSum / maxSum or skip
                // Or you can add a big variance chunk if you want to keep it approximate
                continue;
            }
    
            double mean = qnode.getSampleStatsAcc().mean();
            double stdev = qnode.getSampleStatsAcc().sampleStandardDeviation();
            double N = qnode.getIntersectionCount();
    
            // node-level estimate
            double nodeEstimate = N * mean;
            // node-level variance: N^2 * stdev^2 / n
            double nodeVariance = N*N * (stdev*stdev / n);
    
            totalEstimate += nodeEstimate;
            totalVariance += nodeVariance;
        }
    
        // final estimate = exactSum + partialEstimate
        double finalEstimate = exactSum + totalEstimate;
        
        // standard error from partial region
        double stdError = Math.sqrt(totalVariance);
        // no variance from "exactSum" portion (fully contained is known exactly)
        
        // pick a z-score
        double z = getZScoreForConfidence(0.95);
    
        double margin = z * stdError;
        double lower = finalEstimate - margin;
        double upper = finalEstimate + margin;
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

    // private double[] getQueryConfidenceInterval(List<QueryNode> samplingNodes,
    // QueryResults queryResults) {
    // double exactSum = 0; // Default to zero if no fully contained stats exist

    // if (queryResults.getStats().containsKey(null)) {
    // exactSum = queryResults.getStats().get(null).xStats().sum(); // Use fully
    // contained nodes' sum if available
    // } else {
    // LOG.warn("No fully contained tiles with stats found. Using only approximate
    // information.");
    // }

    // double minSum = exactSum; // Start with fully contained sum (or 0 if none
    // exist)
    // double maxSum = exactSum;

    // for (QueryNode queryNode : samplingNodes) {
    // if (queryNode.getIntersectionCount() == 0) {
    // continue;
    // }
    // double[] confidenceInterval = queryNode.getConfidenceInterval(0.95);
    // if (!Double.isNaN(confidenceInterval[0]) &&
    // !Double.isNaN(confidenceInterval[1])) {
    // // Use confidence interval from sampling
    // minSum += confidenceInterval[0];
    // maxSum += confidenceInterval[1];
    // } else {
    // LOG.debug("No samples available to compute confidence interval. QueryNode
    // context: {}", queryNode.toString());
    // // Use deterministic bounds for unsampled nodes
    // minSum += queryNode.getMinSum();
    // maxSum += queryNode.getMaxSum();
    // }
    // }

    // return new double[] { minSum, maxSum };
    // }

    private double calculateMaxErrorBound(double[] confidenceInterval) {
        double minSum = confidenceInterval[0];
        double maxSum = confidenceInterval[1];
        return (maxSum - minSum) / (maxSum + minSum);
    }

    private int readFromFile(Query query, QueryResults queryResults, Integer measureCol0,
            CsvParser parser, KWayMergePointIterator pointIterator) {
        String line;
        String[] row;
        int ioCount = 0;
        while (pointIterator.hasNext()) {
            ioCount++;
            Point point = pointIterator.next();
            try {
                randomAccessReader.seek(point.getFileOffset());
                line = randomAccessReader.readLine();
                Float measureValue0 = null;
                Float measureValue1 = 0f;
                if (line != null) {
                    row = parser.parseLine(line);
                    if (row != null) {
                        if (measureCol0 != null && row[measureCol0] != null) {
                            measureValue0 = Float.parseFloat(row[measureCol0]);
                            measureValue1 = 0f;
                        }

                        QueryNode queryNode = pointIterator.getCurrentQueryNode();
                        queryNode.addSampleValue(measureValue0);
                        // if (queryNode.isFullyContained()) {
                        // if (measureValue0 != null) {
                        // queryNode.getNode().adjustStats(measureValue0, measureValue1);
                        // }
                        // }
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
