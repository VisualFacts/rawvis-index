package gr.athenarc.imsi.visualfacts;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.math.PairedStatsAccumulator;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import gr.athenarc.imsi.visualfacts.CalciteConnectionPool.CalciteConnectionPool;
import gr.athenarc.imsi.visualfacts.init.InitializationPolicy;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.queryER.BlockIndex;
import gr.athenarc.imsi.visualfacts.queryER.DedupQueryResults;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.EntityResolvedTuple;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.IdDuplicates;
import gr.athenarc.imsi.visualfacts.queryER.DeduplicationExecution;
import gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.ComparisonRefinement.AbstractDuplicatePropagation;
import gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.ComparisonRefinement.UnilateralDuplicatePropagation;
import gr.athenarc.imsi.visualfacts.queryER.QueryBlockIndex;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.BlockStatistics;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.ExecuteBlockComparisons;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.OffsetIdsMap;
import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.VizCluster;
import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.VizOutput;
import gr.athenarc.imsi.visualfacts.util.*;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

public class Veti {

    private static final Logger LOG = LogManager.getLogger(Veti.class);
    private static FileWriter csvWriter;
    private boolean isInitialized = false;
    private Grid grid;
    private Schema schema;
    private RawFileService rawFileService;
    private String initMode;
    private Integer catNodeBudget;
    private Integer binCount;
    private String sort = "asc";
    private String modelPath = "";
    private InitializationPolicy initializationPolicy;
    private int objectsIndexed = 0;
    private DeduplicationExecution deduplicationExecution = new DeduplicationExecution();


    public Veti(Schema schema, Integer catNodeBudget, String initMode, Integer binCount) {
        this.schema = schema;
        this.initMode = initMode;
        this.catNodeBudget = catNodeBudget;
        this.binCount = binCount;
    }
    
    public Veti(Schema schema, Integer catNodeBudget, String initMode, Integer binCount, String modelPath) {
        this.schema = schema;
        this.initMode = initMode;
        this.catNodeBudget = catNodeBudget;
        this.binCount = binCount;
        this.modelPath = modelPath;
    }

    private static OffsetIdsMap offsetToIds(Schema schema) {
        List<CategoricalColumn> categoricalColumns = schema.getCategoricalColumns();


        List<Integer> colIndexes = new ArrayList<>();

        colIndexes.add(schema.getidColumn());

        CsvParserSettings parserSettings = schema.createCsvParserSettings();
        parserSettings.selectIndexes(colIndexes.toArray(new Integer[colIndexes.size()]));
        parserSettings.setColumnReorderingEnabled(false);
        parserSettings.setHeaderExtractionEnabled(schema.getHasHeader());
        CsvParser parser = new CsvParser(parserSettings);
        parser.beginParsing(new File(schema.getCsv()), Charset.forName("US-ASCII"));
        parser.parseNext();  //skip header row
        String[] row;
        long rowOffset = parser.getContext().currentChar() - 1;
        HashMap<Long, Integer> offsetToId = new HashMap<>();
        HashMap<Integer, Long> idToOffset = new HashMap<>();
        while ((row = parser.parseNext()) != null) {
            int idCol = schema.getidColumn();
//        	System.out.println(rowOffset + ": " + row[idCol]);
            try {
                Integer id = Integer.parseInt(row[idCol]);
                offsetToId.put(rowOffset, id);
                idToOffset.put(id, rowOffset);
            } catch (Exception e) {
                continue;
            } finally {
                rowOffset = parser.getContext().currentChar() - 1;

            }

        }
        return new OffsetIdsMap(offsetToId, idToOffset);
    }

    @SuppressWarnings("unchecked")
    private void calculateGroundTruth(Schema schema) throws SQLException, IOException {
        // Trick to get table name from a single sp query
        OffsetIdsMap offsetIdsMap = offsetToIds(schema);

        HashMap<Long, Integer> offsetToId = offsetIdsMap.offsetToId;
        HashMap<Integer, Long> idsToOffset = offsetIdsMap.idToOffset;

        String tableName = "";
        final String csv = schema.getCsv();
        if (csv.contains("\\")) tableName = csv.substring(csv.lastIndexOf("\\") + 1).replace(".csv", "");
        else tableName = csv.substring(csv.lastIndexOf("/") + 1).replace(".csv", "");

        // Construct ground truth query
        String calciteConnectionString = getCalciteConnectionString();
        CalciteConnectionPool calciteConnectionPool = new CalciteConnectionPool();
        CalciteConnection calciteConnection = null;
        try {
            calciteConnection = (CalciteConnection) calciteConnectionPool.setUp(calciteConnectionString);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Set<IdDuplicates> groundDups = new HashSet<IdDuplicates>();
        Set<String> groundMatches = new HashSet<>();

        System.out.println("Calculating ground truth..");
        Set<Long> qIds = DeduplicationExecution.qIds;
        List<Set<Integer>> inIdsSets = new ArrayList<>();
        Set<Integer> currSet = null;
        for (Long value : qIds) {
            if (currSet == null || currSet.size() == 500)
                inIdsSets.add(currSet = new HashSet<>());
            currSet.add(offsetToId.get(value));
        }

        List<String> inIds = new ArrayList<>();
        inIdsSets.forEach(inIdSet -> {
            String inId = "(";
            for (Integer qId : inIdSet) {
                inId += qId + ",";
            }
            inId = inId.substring(0, inId.length() - 1) + ")";
            inIds.add(inId);
        });
        System.out.println("Will execute " + inIds.size() + " queries");

        for (String inIdd : inIds) {
            String groundTruthQuery = "SELECT id_d, id_s FROM ground_truth.ground_truth_" + tableName +
                    " WHERE id_s IN " + inIdd + " OR id_d IN " + inIdd;
            ResultSet gtQueryResults = runQuery(calciteConnection, groundTruthQuery);
            while (gtQueryResults.next()) {
                Integer id_d = Integer.parseInt(gtQueryResults.getString("id_d"));
                Integer id_s = Integer.parseInt(gtQueryResults.getString("id_s"));
                Long offset_d = idsToOffset.get(id_d);
                Long offset_s = idsToOffset.get(id_s);
                //System.out.println(offset_s + " = " + offset_d);
                IdDuplicates idd = new IdDuplicates(offset_d, offset_s);
                groundDups.add(idd);

                String uniqueComp = "";
                if (offset_d > offset_s)
                    uniqueComp = offset_d + "u" + offset_s;
                else
                    uniqueComp = offset_s + "u" + offset_d;
                if (groundMatches.contains(uniqueComp))
                    continue;
                groundMatches.add(uniqueComp);
            }
        }

        final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(groundDups);
        System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

        duplicatePropagation.resetDuplicates();
        List<AbstractBlock> blocks = DeduplicationExecution.blocks;
//		for(AbstractBlock block : blocks) {
//			DecomposedBlock uBlock = (DecomposedBlock) block;
//			for (long entity : uBlock.getEntities1())
//				System.out.println(entity);
//			//System.out.print(String.valueOf(offsetToId.get(entity)) + " ");
//			System.out.println();
//			for (long entity : uBlock.getEntities2())
//				System.out.println(entity);
//			//System.out.print(String.valueOf(offsetToId.get(entity)) + " ");
//			System.out.println();
//		}
        BlockStatistics bStats = new BlockStatistics(blocks, duplicatePropagation, csvWriter);
        bStats.applyProcessing();
        csvWriter.append("\n");
        csvWriter.flush();

        Set<String> matches = ExecuteBlockComparisons.matches;
        double sz_before = matches.size();
        matches.removeAll(groundMatches);
        double sz_after = matches.size();
        System.out.println("ACC\t:\t " + sz_after / sz_before);
        csvWriter.flush();
    }

    private static ResultSet runQuery(CalciteConnection calciteConnection, String query) throws SQLException {
        System.out.println("Running query...");
        return calciteConnection.createStatement().executeQuery(query);

    }

    private  String getCalciteConnectionString() {
//        URL res = Veti.class.getClassLoader().getResource("model.json");
//        File file = null;
//		file = Paths.get(res.toExternalForm()).toFile();
    	LOG.debug("jdbc:calcite:model=" + modelPath);
        return "jdbc:calcite:model=" + modelPath;
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

        List<CategoricalColumn> categoricalColumns = schema.getCategoricalColumns();


        List<Integer> catColIndexes = categoricalColumns.stream().mapToInt(CategoricalColumn::getIndex).boxed().collect(Collectors.toList());
        Set<Integer> colIndexes = new HashSet<>();

        colIndexes.add(schema.getxColumn());
        colIndexes.add(schema.getyColumn());
        colIndexes.addAll(catColIndexes);
        colIndexes.addAll(schema.getDedupCols());

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
        File queryFile = new File("groundTruthResults.csv");
        csvWriter = new FileWriter(queryFile);
        //csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,total_entities,entities_in_blocks,singleton_entities,average_block,BC,detected_duplicates,PC,PQ\n");
        csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,entities_in_blocks,detected_duplicates,PC,PQ\n");
        //LOG.debug("Global Token Map: " + tokenMap.map);
        // todo evaluate q0
        QueryResults queryResults = new QueryResults(q0);
        return queryResults;
    }

    public int getObjectsIndexed() {
        return objectsIndexed;
    }

    public synchronized QueryResults executeQuery(Query query) throws IOException, ClassNotFoundException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        if (!isInitialized) {
            return initialize(query);
        }
        Rectangle rect = query.getRect();

        List<CategoricalColumn> groupByColumns = null;
        if (query.getGroupByCols() != null) {
            groupByColumns = query.getGroupByCols().stream().map(index -> schema.getCategoricalColumn(index)).collect(Collectors.toList());
        }

        QueryResults queryResults = new QueryResults(query);

        if (rawFileService == null) {
            rawFileService = new RawFileService(schema);
        }
        List<NodePointsIterator> rawIterators = new ArrayList<>();
        List<QueryNode> nonRawNodes = new ArrayList<>();

        List<Point> points = new ArrayList<>();

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

        KWayMergePointIterator pointIterator = new KWayMergePointIterator(rawIterators);
        int ioCount = 0;
        String[] row;
        while (pointIterator.hasNext()) {
            ioCount++;
            Point point = pointIterator.next();
            points.add(point);
            try {
                row = rawFileService.getObject(point.getFileOffset());
                Float measureValue0 = null;
                Float measureValue1 = null;
                if (row != null) {
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
                points.add(point);
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


        LOG.debug("Actual query execution complete. Time required: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        LOG.debug("Number of query objects: " + queryResults.getPoints().size());

        if (query.isDedupEnabled()) {
            deduplicateQueryResults(queryResults);
        }

        return queryResults;
    }

    private void deduplicateQueryResults(QueryResults queryResults) throws IOException {
        LOG.debug("Starting query deduplication...");
        Query query = queryResults.getQuery();
       
        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<String, Set<Point>> invertedIndex = new HashMap<>();

        this.grid.getOverlappedLeafTiles(query).stream().map(Tile::getBlockIndex).filter(Objects::nonNull).map(BlockIndex::getInvertedIndex)
                .forEach(tileInvIndex -> tileInvIndex.entrySet().stream().
                        forEach(e -> invertedIndex.computeIfAbsent(e.getKey(), s -> new HashSet<>()).addAll(e.getValue())));


        QueryBlockIndex queryBlockIndex = new QueryBlockIndex(schema, rawFileService);
        queryBlockIndex.processQueryResults(queryResults, invertedIndex);

        LOG.debug("QueryBlockIndex Created. Time required: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        // invertedIndex.entrySet().stream().forEach(stringSetEntry -> LOG.debug(stringSetEntry.getKey() + ": " + stringSetEntry.getValue().size()));
        Set<Long> qIds = queryResults.getPoints().stream().mapToLong(Point::getFileOffset).boxed().collect(Collectors.toSet());

        LOG.debug("Qids Retrieved. Time required: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        HashMap<Long, Object[]> queryData = getQueryData(qIds);

        LOG.debug("QueryData Retrieved. Time required: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        List<AbstractBlock> abstractBlocks = QueryBlockIndex.parseIndex(queryBlockIndex.invertedIndex);

        LOG.debug("Blocks created. Time required: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        EntityResolvedTuple entityResolvedTuple = deduplicationExecution.deduplicate(abstractBlocks,
        		queryData, qIds, schema.getCsv().replace(".csv", ""), schema.getCategoricalColumns().size(), rawFileService, schema.getidColumn());
        
        DedupQueryResults dedupQueryResults = new DedupQueryResults(entityResolvedTuple);
        VizOutput vizOutput = dedupQueryResults.groupSimilar();

        for(VizCluster cluster : vizOutput.VizDataset) {
        	System.out.println(cluster.clusterColumns);
        	System.out.println(cluster.clusterColumnSimilarity);
        	System.out.println(cluster.clusterSimilarities);
        }
        
        LOG.debug("Actual Deduplication Completed. Time required: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
       
        LOG.debug("Deduplication complete. Time required: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        LOG.debug("# of Comparisons: " + dedupQueryResults.getComparisons());
        
        try {
            calculateGroundTruth(schema);
        } catch (SQLException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

    private HashMap<Long, Object[]> getQueryData(Set<Long> qIds) {
        return qIds.stream().collect(Collectors.toMap(offset -> offset, offset -> {
            try {
                return rawFileService.getObject(offset);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return null;
        }, (left, right) -> right, HashMap::new));

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

}
