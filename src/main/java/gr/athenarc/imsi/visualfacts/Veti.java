package gr.athenarc.imsi.visualfacts;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.math.PairedStatsAccumulator;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import gr.athenarc.imsi.visualfacts.init.InitializationPolicy;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.queryER.TokenMap;
import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.XContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.XYContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.YContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.io.RandomAccessReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

public class Veti {

    private static final Logger LOG = LogManager.getLogger(Veti.class);

    private boolean isInitialized = false;

    private RandomAccessReader randomAccessReader;

    private Grid grid;

    private Schema schema;

    private String initMode;

    private Integer catNodeBudget;

    private Integer binCount;

    private String sort = "asc";

    private InitializationPolicy initializationPolicy;

    private int objectsIndexed = 0;

    private TokenMap tokenMap;

    public Veti(Schema schema, Integer catNodeBudget, String initMode, Integer binCount) {
        this.schema = schema;
        this.initMode = initMode;
        this.catNodeBudget = catNodeBudget;
        this.binCount = binCount;
    }

    public void generateGrid(Query q0) {
        if (isInitialized)
            throw new IllegalStateException("The index is already initialized");

        if (q0 != null) {
            initializationPolicy = InitializationPolicy.getInitializationPolicy(initMode, q0, (int) (GRID_SIZE * GRID_SIZE * SUBTILE_RATIO), schema, catNodeBudget, binCount);
            initializationPolicy.setSort(sort);
        }


        grid = new Grid(initializationPolicy, schema.getBounds(), schema.getCategoricalColumns(), GRID_SIZE);
        grid.split();
        if (initializationPolicy != null) {
            initializationPolicy.initTileTreeCategoricalAttrs(grid.getLeafTiles());
        }
    }

    public QueryResults initialize(Query q0) throws IOException, ClassNotFoundException {
        generateGrid(q0);
        tokenMap = new TokenMap(schema);

        List<CategoricalColumn> categoricalColumns = schema.getCategoricalColumns();


        List<Integer> catColIndexes = categoricalColumns.stream().mapToInt(CategoricalColumn::getIndex).boxed().collect(Collectors.toList());
        List<Integer> colIndexes = new ArrayList<>();

        colIndexes.add(schema.getxColumn());
        colIndexes.add(schema.getyColumn());
        colIndexes.addAll(catColIndexes);

        Integer measureCol0 = schema.getMeasureCol0();
        Integer measureCol1 = schema.getMeasureCol1();
        if (measureCol0 != null) {
            colIndexes.add(measureCol0);
            if (measureCol1 != null) {
                colIndexes.add(measureCol1);
            }
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
                Point point = new Point(Float.parseFloat(row[schema.getxColumn()]), Float.parseFloat(row[schema.getyColumn()]), rowOffset);

                TreeNode node = this.grid.addPoint(point, row);
                if (node == null) {
                    continue;
                }

                if (measureCol0 != null) {
                    Float value0 = Float.parseFloat(row[measureCol0]);
                    Float value1 = 0f;
                    if (measureCol1 != null) {
                        value1 = Float.parseFloat(row[measureCol1]);
                    }
                    node.adjustStats(value0, value1);
                }

                tokenMap.processRow(row);

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

    public synchronized QueryResults executeQuery(Query query) throws IOException, ClassNotFoundException {
        if (!isInitialized) {
            return initialize(query);
        }
        Rectangle rect = query.getRect();

        List<CategoricalColumn> groupByColumns = null;
        if (query.getGroupByCols() != null) {
            groupByColumns = query.getGroupByCols().stream().map(index -> schema.getCategoricalColumn(index)).collect(Collectors.toList());
        }

        QueryResults queryResults = new QueryResults(query);

        if (randomAccessReader == null) {
            randomAccessReader = RandomAccessReader.open(new File(schema.getCsv()));
        }
        List<NodePointsIterator> rawIterators = new ArrayList<>();
        List<QueryNode> nonRawNodes = new ArrayList<>();

        List<float[]> points = new ArrayList<>();

        int fullyContainedTilesCount = 0;

        List<QueryNode> nodesToExpand = new ArrayList<>();

        List<Tile> leafTiles = this.grid.getOverlappedLeafTiles(query);


        Set<CategoricalColumn> catAttrsToRead = new HashSet<>();
        for (Tile leafTile : leafTiles) {
            ContainmentExaminer containmentExaminer = getContainmentExaminer(leafTile, rect);
            boolean isFullyContained = containmentExaminer == null;
            if (isFullyContained) {
                fullyContainedTilesCount++;
            }

            List<QueryNode> queryNodes = leafTile.getQueryNodes(query, containmentExaminer, schema);
            int count = 0;
            for (QueryNode queryNode : queryNodes) {
                TreeNode node = queryNode.getNode();
                if ((!isFullyContained || !node.hasStats()) && node.getPoints() != null) {
                    count += node.getPoints().size();
                }
            }

            if (count > THRESHOLD) {
                leafTile.split();
                queryNodes = leafTile.getOverlappedLeafTiles(query).stream()
                        .flatMap(tile -> tile.getQueryNodes(query, containmentExaminer, schema).stream()).collect(Collectors.toList());
            }

            for (QueryNode queryNode : queryNodes) {
                TreeNode node = queryNode.getNode();

                //add unknown attrs for that node to cat attrs to read. These do not include only query attrs but also missing attrs in incomplete leaves
                catAttrsToRead.addAll(queryNode.getUnknownCatAttrs());

                PairedStatsAccumulator nodeStats = node.getStats();
                Map<Integer, Short> groupByValues = queryNode.getGroupByValues();

                boolean hasUnknownAttrs = queryNode.getUnknownCatAttrs() != null && !queryNode.getUnknownCatAttrs().isEmpty();

                if (isFullyContained && hasUnknownAttrs && !initMode.equals("valinor")) {
                    nodesToExpand.add(queryNode);
                }

                //todo unknownCatAttrs may not be empty but including only attrs missing from the node but not present in the query
                if (isFullyContained && nodeStats != null && !hasUnknownAttrs) {
                    queryResults.adjustStats(groupByColumns == null || groupByColumns.isEmpty() ? null :
                            groupByColumns.stream().map(categoricalColumn -> {
                                return categoricalColumn.getValue(groupByValues.get(categoricalColumn.getIndex()));
                            }).collect(ImmutableList.toImmutableList()), nodeStats.snapshot());
                    nonRawNodes.add(queryNode);
                } else {
                    rawIterators.add(new NodePointsIterator(queryNode));
                }
            }
        }

        List<Integer> cols = new ArrayList<>();

        Integer measureCol0 = schema.getMeasureCol0();
        Integer measureCol1 = schema.getMeasureCol1();
        if (measureCol0 != null) {
            cols.add(measureCol0);
            if (measureCol1 != null) {
                cols.add(measureCol1);
            }
        }
        cols.addAll(catAttrsToRead.stream().map(CategoricalColumn::getIndex).collect(Collectors.toList()));

        CsvParserSettings parserSettings = schema.createCsvParserSettings();
        parserSettings.selectIndexes(cols.toArray(new Integer[cols.size()]));
        parserSettings.setColumnReorderingEnabled(false);
        CsvParser parser = new CsvParser(parserSettings);

        KWayMergePointIterator pointIterator = new KWayMergePointIterator(rawIterators);
        int ioCount = 0;
        String line = null;
        String[] row = null;
        while (pointIterator.hasNext()) {
            ioCount++;
            Point point = pointIterator.next();
            points.add(new float[]{point.getY(), point.getX()});
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
                            if (measureCol1 == null) {
                                measureValue1 = 0f;
                            } else if (row[measureCol1] != null) {
                                measureValue1 = Float.parseFloat(row[measureCol1]);
                            }
                        }

                        QueryNode queryNode = pointIterator.getCurrentQueryNode();
                        TreeNode node = queryNode.getNode();

                        if (queryNode.isFullyContained()) {
                            //we expand the node with unknown attrs
                            if (!initMode.equals("valinor") && queryNode.getUnknownCatAttrs() != null && !queryNode.getUnknownCatAttrs().isEmpty()) {
                                for (CategoricalColumn unknownAttr : queryNode.getUnknownCatAttrs()) {
                                    node = node.getOrAddChild(unknownAttr.getValueKey(row[unknownAttr.getIndex()]));
                                }
                                node.addPoint(point);
                                if (measureValue0 != null && measureValue1 != null) {
                                    node.adjustStats(measureValue0, measureValue1);
                                }
                            } else if (queryNode.getUnknownCatAttrs() == null || queryNode.getUnknownCatAttrs().isEmpty()) {
                                if (measureValue0 != null && measureValue1 != null) {
                                    queryNode.getNode().adjustStats(measureValue0, measureValue1);
                                }
                            }
                        }
                        ImmutableList<String> groupByValuesList = null;
                        if (query.getGroupByCols() != null) {
                            String[] finalRow = row;
                            groupByValuesList = groupByColumns.stream().map(categoricalColumn ->
                                    queryNode.getGroupByValues().containsKey(categoricalColumn.getIndex()) ?
                                            categoricalColumn.getValue(queryNode.getGroupByValues().get(categoricalColumn.getIndex())) :
                                            finalRow[categoricalColumn.getIndex()]).collect(ImmutableList.toImmutableList());
                        }

                        if (checkUnknownAttrs(query, row, queryNode.getUnknownCatAttrs()) && measureValue0 != null && measureValue1 != null) {
                            queryResults.adjustStats(groupByColumns == null || groupByColumns.isEmpty() ? null : groupByValuesList, measureValue0, measureValue1);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.debug(e);
            }
        }
        for (QueryNode node : nonRawNodes) {
            for (Point point : node) {
                points.add(new float[]{point.getY(), point.getX()});
            }
        }

        for (QueryNode queryNode : nodesToExpand) {
            queryNode.getNode().convertToNonleaf();
        }

        queryResults.setTileCount(leafTiles.size());
        queryResults.setFullyContainedTileCount(fullyContainedTilesCount);
        queryResults.setIoCount(ioCount);
        queryResults.setExpandedNodeCount(nodesToExpand.size());
        queryResults.setPoints(points);

        PairedStatsAccumulator pairedStatsAccumulator = new PairedStatsAccumulator();
        queryResults.getStats().entrySet().stream().forEach(e -> {
            pairedStatsAccumulator.addAll(e.getValue());
        });
        queryResults.setRectStats(pairedStatsAccumulator);
        return queryResults;
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
