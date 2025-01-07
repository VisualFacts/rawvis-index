package gr.athenarc.imsi.visualfacts;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Range;
import com.google.common.math.PairedStatsAccumulator;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import gr.athenarc.imsi.visualfacts.init.InitializationPolicy;
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

    private String initMode;

    private String sort = "asc";

    private InitializationPolicy initializationPolicy;

    private int objectsIndexed = 0;

    private double errorBound = 0.05;

    public ApproximateValinor(Schema schema, Double errorBound) {
        this.schema = schema;
        this.errorBound = errorBound;
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

    public QueryResults initialize(Query q0) {
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
        QueryResults queryResults = new QueryResults(q0);
        return queryResults;
    }

    public int getObjectsIndexed() {
        return objectsIndexed;
    }

    public synchronized QueryResults executeQuery(Query query) throws IOException {
        if (!isInitialized) {
            return initialize(query);
        }
        Rectangle rect = query.getRect();

        QueryResults queryResults = new QueryResults(query);

        if (randomAccessReader == null) {
            randomAccessReader = RandomAccessReader.open(new File(schema.getCsv()));
        }
        List<QueryNode> nonRawNodes = new ArrayList<>();

        List<float[]> points = new ArrayList<>();

        int fullyContainedTilesCount = 0;

        List<Tile> leafTiles = this.grid.getOverlappedLeafTiles(query);

        List<QueryNode> fullyContainedNodesWithStats = new ArrayList<>();
        List<QueryNode> partiallyContainedNodesWithStats = new ArrayList<>();
        List<QueryNode> nodesWithoutStats = new ArrayList<>();

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

                if (!node.hasStats()) {
                    if (node.points.size() > THRESHOLD) {
                        leafTile.split();
                        nodesWithoutStats.addAll(leafTile.getOverlappedLeafTiles(query).stream()
                                .flatMap(tile -> tile.getQueryNodes(query, containmentExaminer,
                                        schema).stream())
                                .collect(Collectors.toList()));
                    } else {
                        nodesWithoutStats.add(queryNode);
                    }
                } else if (isFullyContained) {
                    fullyContainedNodesWithStats.add(queryNode);
                } else {
                    partiallyContainedNodesWithStats.add(queryNode);
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

        KWayMergePointIterator pointIterator;

        pointIterator = new KWayMergePointIterator(nodesWithoutStats.stream()
                .map(queryNode -> new NodePointsIterator(queryNode)).collect(Collectors.toList()));
        ioCount += readFromFile(query, queryResults, measureCol0, parser, pointIterator);

        for (QueryNode queryNode : partiallyContainedNodesWithStats) {
            NodePointsIterator nodePointsIterator = new NodePointsIterator(queryNode);
            int count = 0;
            while (nodePointsIterator.hasNext()) {
                nodePointsIterator.next();
                count++;
            }
            queryNode.intersectionCount = count;
            queryNode.minSum = queryNode.intersectionCount * queryNode.getNode().getStats().xStats().min();
            queryNode.maxSum = queryNode.intersectionCount * queryNode.getNode().getStats().xStats().max();
            queryNode.maxErrorBound = (queryNode.maxSum - queryNode.minSum) / (queryNode.minSum + queryNode.maxSum);
        }


        // Sort partially contained nodes by their respective max error bound
        partiallyContainedNodesWithStats.sort(Comparator.comparingDouble(QueryNode::getMaxErrorBound));

        int currentIndex = 0;
        double maxErrorBound = calculateMaxErrorBound(partiallyContainedNodesWithStats, currentIndex, queryResults);

        // Process partially contained tiles and read from the file until the error bound is acceptable.
        while (currentIndex < partiallyContainedNodesWithStats.size() && maxErrorBound > errorBound) {
            QueryNode queryNode = partiallyContainedNodesWithStats.get(currentIndex);
            pointIterator = new KWayMergePointIterator(Arrays.asList(new NodePointsIterator(queryNode)));
            ioCount += readFromFile(query, queryResults, measureCol0, parser, pointIterator);
            currentIndex++;
            maxErrorBound = calculateMaxErrorBound(partiallyContainedNodesWithStats, currentIndex, queryResults);
        }

        // For the remaining partially contained tiles, we approximate their values using the mean value for all the objects in each tile
        while (currentIndex < partiallyContainedNodesWithStats.size()) {
            QueryNode queryNode = partiallyContainedNodesWithStats.get(currentIndex);
            NodePointsIterator nodePointsIterator = new NodePointsIterator(queryNode);
            
            while (nodePointsIterator.hasNext()) {
                nodePointsIterator.next();
                queryResults.adjustStats(null, (float) queryNode.getNode().getStats().xStats().mean(), 0f);
            }
            currentIndex++;
        }

        queryResults.setTileCount(leafTiles.size());
        queryResults.setFullyContainedTileCount(fullyContainedTilesCount);
        queryResults.setIoCount(ioCount);

        PairedStatsAccumulator pairedStatsAccumulator = new PairedStatsAccumulator();
        queryResults.getStats().entrySet().stream().forEach(e -> {
            pairedStatsAccumulator.addAll(e.getValue());
        });
        queryResults.setRectStats(pairedStatsAccumulator);
        return queryResults;
    }

    private double calculateMaxErrorBound(List<QueryNode> queryNodes, int current, QueryResults queryResults) {
        double minSum = 0, maxSum = 0;
        try {
            minSum = queryResults.getStats().get(null).xStats().sum();
            maxSum = queryResults.getStats().get(null).xStats().sum();
        } catch (Exception e) {
            LOG.debug(e);
        }
        for (int i = current; i < queryNodes.size(); i++) {
            QueryNode queryNode = queryNodes.get(i);
            minSum += queryNode.minSum;
            maxSum += queryNode.maxSum;
        }
        LOG.debug("Min Sum: " + minSum + " Max Sum: " + maxSum);
        return (maxSum - minSum) / (minSum + maxSum);
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
                Float measureValue1 = null;
                if (line != null) {
                    row = parser.parseLine(line);
                    if (row != null) {
                        if (measureCol0 != null && row[measureCol0] != null) {
                            measureValue0 = Float.parseFloat(row[measureCol0]);
                            measureValue1 = 0f;
                        }

                        QueryNode queryNode = pointIterator.getCurrentQueryNode();
                        TreeNode node = queryNode.getNode();

                        if (queryNode.isFullyContained()) {
                            // we expand the node with unknown attrs
                            if (!initMode.equals("valinor") && queryNode.getUnknownCatAttrs() != null
                                    && !queryNode.getUnknownCatAttrs().isEmpty()) {
                                for (CategoricalColumn unknownAttr : queryNode.getUnknownCatAttrs()) {
                                    node = node.getOrAddChild(unknownAttr.getValueKey(row[unknownAttr.getIndex()]));
                                }
                                node.addPoint(point);
                                if (measureValue0 != null && measureValue1 != null) {
                                    node.adjustStats(measureValue0, measureValue1);
                                }
                            } else if (queryNode.getUnknownCatAttrs() == null
                                    || queryNode.getUnknownCatAttrs().isEmpty()) {
                                if (measureValue0 != null && measureValue1 != null) {
                                    queryNode.getNode().adjustStats(measureValue0, measureValue1);
                                }
                            }
                        }
                        if (checkUnknownAttrs(query, row, queryNode.getUnknownCatAttrs()) && measureValue0 != null
                                && measureValue1 != null) {
                            queryResults.adjustStats(null, measureValue0, measureValue1);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.debug(e);
            }
        }
        return ioCount;
    }

    private boolean checkUnknownAttrs(Query query, String[] row, List<CategoricalColumn> unknownCatAttrs) {
        boolean check = true;
        for (CategoricalColumn categoricalColumn : unknownCatAttrs) {
            String filterValue = query.getCategoricalFilters().get(categoricalColumn.getIndex());
            if (filterValue != null) {
                String rowValue = row[categoricalColumn.getIndex()];
                check = check && rowValue != null && rowValue.equals(filterValue);
            }
        }
        return check;
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
            LOG.error(e);
        }
    }
}
