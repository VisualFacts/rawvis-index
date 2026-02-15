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
import gr.athenarc.imsi.visualfacts.util.csv.ZsvCsvFloatRowReader;
import gr.athenarc.imsi.visualfacts.util.io.MappedFileReader;

public class Veti implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(Veti.class);

    private boolean isInitialized = false;

    private MappedFileReader randomAccessReader;

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
        LOG.debug("Initializing VETI index with initial query: " + q0);
        LOG.debug("Schema: " + schema);
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
                schema.getDelimiter());

        objectsIndexed = 0;
        int objectsSkipped = 0; // Counter for skipped rows

        try {
            rowReader.open(readerConfig);
            float[] row;

            // --- Phase 1: CSV scan → global staging arrays + per-tile counts + stats ---
            final int capacity = schema.getObjectCount();
            float[] gXs = new float[capacity];
            float[] gYs = new float[capacity];
            long[] gOffsets = new long[capacity];
            int validCount = 0;

            final Integer xPos = colIndexToRowPos.get(schema.getxColumn());
            final Integer yPos = colIndexToRowPos.get(schema.getyColumn());
            final int logInterval = Math.max(1, schema.getObjectCount() / 10);

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

                    if (++objectsIndexed % logInterval == 0) {
                        LOG.debug("Indexing object " + objectsIndexed);
                    }
                } catch (Exception e) {
                    LOG.error("Problem parsing row number " + objectsIndexed + ": " + Arrays.toString(row), e);
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
            randomAccessReader = MappedFileReader.open(new File(schema.getCsv()));
        }
        List<AbstractNodePointIterator> rawIterators = new ArrayList<>();
        List<QueryNode> nonRawNodes = new ArrayList<>();

        int fullyContainedTilesCount = 0;

        List<QueryNode> nodesToExpand = new ArrayList<>();

        List<Tile> leafTiles = this.grid.getOverlappedLeafTiles(query);

        Set<CategoricalColumn> catAttrsToRead = new HashSet<>();
        for (Tile leafTile : leafTiles) {
            // Short-circuited non-leaf tile with frozen exact stats.
            // This tile was previously split but its pre-split stats were preserved.
            // Since it's guaranteed fully contained (the only way it's returned from
            // getOverlappedLeafTiles), use the frozen stats directly.
            // Note: categorical group-by is unsupported for frozen tiles.
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
                        || query.getMeasureCols().stream().anyMatch(measureCol -> !node.hasStats(schema.getMeasureIndex(measureCol))))
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

        KWayMergePointIterator pointIterator = new KWayMergePointIterator(rawIterators);
        int ioCount = 0;
        
        // Prepare sorted measure column indices for fast extraction
        // extractFloats returns a compact array indexed by position, not by column index
        List<Integer> measureColsList = schema.getMeasureCols();
        int[] sortedMeasureCols = measureColsList.stream().mapToInt(Integer::intValue).sorted().toArray();
        
        // Build mapping from original column index to position in sorted array
        Map<Integer, Integer> measureColToExtractedPos = new HashMap<>();
        for (int i = 0; i < sortedMeasureCols.length; i++) {
            measureColToExtractedPos.put(sortedMeasureCols[i], i);
        }
        
        byte delimiterByte = (byte) schema.getDelimiter().charValue();
        
        while (pointIterator.hasNext()) {
            ioCount++;
            long fileOffset = pointIterator.nextOffset();
            try {
                randomAccessReader.seek(fileOffset);
                
                // Fast path: extract only measure columns as floats directly from mmap
                float[] extractedValues = randomAccessReader.extractFloats(sortedMeasureCols, delimiterByte);
                
                QueryNode queryNode = pointIterator.getCurrentQueryNode();
                TreeNode node = queryNode.getNode();

                // Build measure values map from extracted floats
                Map<Integer, Float> measureValues = new HashMap<>();
                for (Integer measureCol : measureColsList) {
                    Integer extractedPos = measureColToExtractedPos.get(measureCol);
                    if (extractedPos != null && extractedPos < extractedValues.length) {
                        float value = extractedValues[extractedPos];
                        if (!Float.isNaN(value)) {
                            measureValues.put(measureCol, value);
                        }
                    }
                }
                
                if (queryNode.isFullyContained()) {
                    // Skip categorical attribute expansion for now (floats-only mode)
                    if (queryNode.getUnknownCatAttrs() == null || queryNode.getUnknownCatAttrs().isEmpty()) {
                        // Use schema measure column order (not HashMap iteration order)
                        // to match the index ordering established during initialization
                        int idx = 0;
                        for (Integer measureCol : measureColsList) {
                            Float value = measureValues.get(measureCol);
                            if (value != null) {
                                node.adjustStats(idx, schema.getMeasureCount(), value);
                            }
                            idx++;
                        }
                    }
                }
                
                ImmutableList<String> groupByValuesList = null;
                // Skip group-by with categorical columns for now (floats-only mode)
                
                // Always include measure values in results
                for (Map.Entry<Integer, Float> entry : measureValues.entrySet()) {
                    queryResults.adjustStats(groupByValuesList, entry.getKey(), entry.getValue());
                }
            } catch (Exception e) {
                LOG.debug("An unexpected exception occurred: ", e);
            }
        }

        for (QueryNode queryNode : nodesToExpand) {
            queryNode.getNode().convertToNonleaf();
        }

        queryResults.setTileCount(leafTiles.size());
        queryResults.setFullyContainedTileCount(fullyContainedTilesCount);
        queryResults.setIoCount(ioCount);
        queryResults.setExpandedNodeCount(nodesToExpand.size());

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
