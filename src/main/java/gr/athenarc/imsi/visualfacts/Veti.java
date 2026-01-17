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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.math.Stats;
import com.google.common.math.StatsAccumulator;

import gr.athenarc.imsi.visualfacts.init.InitializationPolicy;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.XContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.XYContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.YContainmentExaminer;
import gr.athenarc.imsi.visualfacts.util.csv.CsvFloatRowReader;
import gr.athenarc.imsi.visualfacts.util.csv.CsvReaderConfig;
import gr.athenarc.imsi.visualfacts.util.csv.CsvRowReader;
import gr.athenarc.imsi.visualfacts.util.csv.UnivocityCsvRowReader;
import gr.athenarc.imsi.visualfacts.util.csv.ZsvCsvFloatRowReader;
import gr.athenarc.imsi.visualfacts.util.io.RandomAccessReader;

public class Veti implements AutoCloseable {

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
            initializationPolicy = InitializationPolicy.getInitializationPolicy(initMode, q0,
                    (int) (GRID_SIZE * GRID_SIZE * SUBTILE_RATIO), schema, catNodeBudget, binCount);
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
        HashSet<Integer> colIndexes = new HashSet<>();

        colIndexes.add(schema.getxColumn());
        colIndexes.add(schema.getyColumn());
        colIndexes.addAll(catColIndexes);

        List<DataValidationFilter> validationFilters = schema.getValidationFilters();

        validationFilters.forEach(filter -> colIndexes.add(filter.getFilterColumn()));

        colIndexes.addAll(schema.getMeasureCols());

        CsvFloatRowReader rowReader = new ZsvCsvFloatRowReader();
        int[] selectedColumns = colIndexes.stream().mapToInt(Integer::intValue).toArray();

        // Build mapping from original col index to position in selectedColumns
        Map<Integer, Integer> colIndexToRowPos = new HashMap<>();
        for (int i = 0; i < selectedColumns.length; i++) {
            colIndexToRowPos.put(selectedColumns[i], i);
        }

        CsvReaderConfig readerConfig = new CsvReaderConfig(
                new File(schema.getCsv()),
                Charset.forName("US-ASCII"),
                selectedColumns,
                schema.getHasHeader(),
                DELIMITER);

        objectsIndexed = 0;
        int objectsSkipped = 0; // Counter for skipped rows

        try {
            rowReader.open(readerConfig);
            float[] row;
            while ((row = rowReader.nextRow()) != null) {
                long rowOffset = rowReader.currentOffset();
                try {
                    // Check if row should be skipped based on validation filters
                    final float[] finalRow = row;
                    boolean shouldSkip = validationFilters.stream().anyMatch(filter -> {
                        int origColIndex = filter.getFilterColumn();
                        Integer rowPos = colIndexToRowPos.get(origColIndex);
                        return filter.test((double) finalRow[rowPos]);
                    });

                    if (shouldSkip) {
                        LOG.debug("Skipping row: " + Arrays.toString(row));
                        objectsSkipped++;
                        continue;
                    }

                    Integer xPos = colIndexToRowPos.get(schema.getxColumn());
                    Integer yPos = colIndexToRowPos.get(schema.getyColumn());
                    Point point = new Point(row[xPos], row[yPos], rowOffset);



                    TreeNode node = this.grid.addPoint(point, (String[]) null);
                    if (node == null) {
                        continue;
                    }

                    int idx = 0;
                    for (Integer measureCol : schema.getMeasureCols()) {
                        Integer mPos = colIndexToRowPos.get(measureCol);
                        if (mPos == null) {
                            idx++;
                            continue;
                        }
                        Float value = row[mPos];
                        node.adjustStats(idx, schema.getMeasureCount(), value);
                        idx++;
                    }

                    int logInterval = Math.max(1, schema.getObjectCount() / 10);
                    if (++objectsIndexed % logInterval == 0) {
                        LOG.debug("Indexing object " + objectsIndexed);
                        LOG.debug(point);
                    }
                } catch (Exception e) {
                    LOG.error("Problem parsing row number " + objectsIndexed + ": " + Arrays.toString(row), e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to read CSV file", e);
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

        List<CategoricalColumn> groupByColumns = null;
        if (query.getGroupByCols() != null) {
            groupByColumns = query.getGroupByCols().stream().map(index -> schema.getCategoricalColumn(index))
                    .collect(Collectors.toList());
        }

        QueryResults queryResults = new QueryResults(query);

        if (randomAccessReader == null) {
            randomAccessReader = RandomAccessReader.open(new File(schema.getCsv()));
        }
        List<AbstractNodePointIterator> rawIterators = new ArrayList<>();
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
                if ((!isFullyContained
                        || query.getMeasureCols().stream().anyMatch(measureCol -> !node.hasStats(schema.getMeasureIndex(measureCol))))
                        && node.getPoints() != null) {
                    count += node.getPoints().size();
                }
            }

            if (count > THRESHOLD) {
                leafTile.split();
                queryNodes = leafTile.getOverlappedLeafTiles(query).stream()
                        .flatMap(tile -> tile.getQueryNodes(query, containmentExaminer, schema).stream())
                        .collect(Collectors.toList());
            }

            for (QueryNode queryNode : queryNodes) {
                TreeNode node = queryNode.getNode();

                // add unknown attrs for that node to cat attrs to read. These do not include
                // only query attrs but also missing attrs in incomplete leaves
                catAttrsToRead.addAll(queryNode.getUnknownCatAttrs());

                Map<Integer, Short> groupByValues = queryNode.getGroupByValues();

                boolean hasUnknownAttrs = queryNode.getUnknownCatAttrs() != null
                        && !queryNode.getUnknownCatAttrs().isEmpty();

                if (isFullyContained && hasUnknownAttrs && !initMode.equals("valinor")) {
                    nodesToExpand.add(queryNode);
                }

                // todo unknownCatAttrs may not be empty but including only attrs missing from
                // the node but not present in the query
                if (isFullyContained && query.getMeasureCols().stream().allMatch(mc -> node.hasStats(schema.getMeasureIndex(mc))) && !hasUnknownAttrs) {
                    ImmutableList<String> groupByValuesList = groupByColumns == null || groupByColumns.isEmpty() ? null
                            : groupByColumns.stream().map(categoricalColumn -> {
                                return categoricalColumn.getValue(groupByValues.get(categoricalColumn.getIndex()));
                            }).collect(ImmutableList.toImmutableList());
                    query.getMeasureCols().forEach(measureCol -> {
                        queryResults.adjustStats(groupByValuesList, measureCol,
                                queryNode.getNode().getStats(schema.getMeasureIndex(measureCol)).snapshot());
                    });
                    nonRawNodes.add(queryNode);
                } else {
                    rawIterators.add(new NodePointsIterator(queryNode));
                }
            }
        }

        List<Integer> cols = new ArrayList<>();

        cols.addAll(schema.getMeasureCols());
        cols.addAll(catAttrsToRead.stream().map(CategoricalColumn::getIndex).collect(Collectors.toList()));

        int[] parseColumns = cols.stream().mapToInt(Integer::intValue).toArray();
        CsvRowReader lineParser = new UnivocityCsvRowReader();
        CsvReaderConfig lineConfig = new CsvReaderConfig(null, Charset.forName("US-ASCII"),
                parseColumns, false, DELIMITER);
        try {
            lineParser.open(lineConfig);
        } catch (IOException e) {
            throw new RuntimeException("Unable to configure CSV line parser", e);
        }

        KWayMergePointIterator pointIterator = new KWayMergePointIterator(rawIterators);
        int ioCount = 0;
        String line = null;
        String[] row = null;
        while (pointIterator.hasNext()) {
            ioCount++;
            Point point = pointIterator.next();
            points.add(new float[] { point.getY(), point.getX() });
            try {
                randomAccessReader.seek(point.getFileOffset());
                line = randomAccessReader.readLine();
                if (line != null) {
                    row = lineParser.parseLine(line);
                    if (row != null) {
                        QueryNode queryNode = pointIterator.getCurrentQueryNode();
                        TreeNode node = queryNode.getNode();

                        // Parse measure values once and store them in a temporary map
                        Map<Integer, Float> measureValues = new HashMap<>();
                        for (Integer measureCol : schema.getMeasureCols()) {
                            measureValues.put(measureCol, Float.parseFloat(row[measureCol]));
                        }
                        if (queryNode.isFullyContained()) {
                            // we expand the node with unknown attrs
                            if (!initMode.equals("valinor") && queryNode.getUnknownCatAttrs() != null
                                    && !queryNode.getUnknownCatAttrs().isEmpty()) {
                                for (CategoricalColumn unknownAttr : queryNode.getUnknownCatAttrs()) {
                                    node = node.getOrAddChild(unknownAttr.getValueKey(row[unknownAttr.getIndex()]));
                                }
                                node.addPoint(point);
                                int idx1 = 0;
                                for (Map.Entry<Integer, Float> entry : measureValues.entrySet()) {
                                    node.adjustStats(idx1, schema.getMeasureCount(), entry.getValue());
                                    idx1++;
                                }
                            } else if (queryNode.getUnknownCatAttrs() == null
                                    || queryNode.getUnknownCatAttrs().isEmpty()) {
                                int idx2 = 0;
                                for (Map.Entry<Integer, Float> entry : measureValues.entrySet()) {
                                    node.adjustStats(idx2, schema.getMeasureCount(), entry.getValue());
                                    idx2++;
                                }
                            }
                        }
                        ImmutableList<String> groupByValuesList = null;
                        if (query.getGroupByCols() != null && !query.getGroupByCols().isEmpty()) {
                            String[] finalRow = row;
                            groupByValuesList = groupByColumns.stream().map(categoricalColumn -> queryNode
                                    .getGroupByValues().containsKey(categoricalColumn.getIndex())
                                            ? categoricalColumn.getValue(
                                                    queryNode.getGroupByValues().get(categoricalColumn.getIndex()))
                                            : finalRow[categoricalColumn.getIndex()])
                                    .collect(ImmutableList.toImmutableList());
                        }

                        if (checkUnknownAttrs(query, row, queryNode.getUnknownCatAttrs())) {
                            for (Map.Entry<Integer, Float> entry : measureValues.entrySet()) {
                                queryResults.adjustStats(groupByValuesList, entry.getKey(), entry.getValue());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.debug("An unexpected exception occurred: ", e);
            }
        }

        for (QueryNode node : nonRawNodes) {
            for (Point point : node) {
                points.add(new float[] { point.getY(), point.getX() });
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

        Map<Integer, StatsAccumulator> rectStatsAccumulators = new HashMap<>();
        // Aggregate stats for each measure across all groups
        queryResults.getStats().forEach((groupByValues, measureStats) -> {
            measureStats.forEach((measureCol, stats) -> {
                rectStatsAccumulators
                        .computeIfAbsent(measureCol, m -> new StatsAccumulator())
                        .addAll(stats);
            });
        });
        // Store the aggregated stats in QueryResults
        Map<Integer, Stats> rectStats = rectStatsAccumulators.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().snapshot()));
        queryResults.setRectStats(rectStats);
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
    public void close() {
        if (randomAccessReader != null) {
            try {
                randomAccessReader.close();
            } catch (IOException e) {
                LOG.warn("Failed to close randomAccessReader", e);
            } finally {
                randomAccessReader = null;
            }
        }
    }
}
