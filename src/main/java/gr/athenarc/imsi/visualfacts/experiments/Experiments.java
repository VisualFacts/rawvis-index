package gr.athenarc.imsi.visualfacts.experiments;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ehcache.sizeof.SizeOf;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import gr.athenarc.imsi.visualfacts.ApproximateValinor;
import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.DataValidationFilter;
import gr.athenarc.imsi.visualfacts.DummyCategoricalColumn;
import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.TreeNode;
import gr.athenarc.imsi.visualfacts.Veti;
import gr.athenarc.imsi.visualfacts.config.IndexConfig;
import gr.athenarc.imsi.visualfacts.experiments.util.DataValidationFilterConverter;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor.QueryResult;
import gr.athenarc.imsi.visualfacts.experiments.util.FilterConverter;
import gr.athenarc.imsi.visualfacts.experiments.util.QuerySequenceGenerator;
import gr.athenarc.imsi.visualfacts.experiments.util.RangeConverter;
import gr.athenarc.imsi.visualfacts.experiments.util.RectangleConverter;
import gr.athenarc.imsi.visualfacts.experiments.util.SyntheticDatasetGenerator;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;

public class Experiments {

    private static final Logger LOG = LogManager.getLogger(Experiments.class);

    
    @Parameter(names = "-errorBound", description = "")
    public Double errorBound;

    @Parameter(names = "-catBudget", description = "Categorical Node budget in GB")
    public Double catBudget;
    @Parameter(names = "-csv", description = "The csv file")
    public String csv;

    @Parameter(names = "-zoomFactor", description = "Zoom factor for zoom in operation. The inverse applies to zoom out operation.")
    public Float zoomFactor = 0f;
    @Parameter(names = "-catCols", variableArity = true, description = "Categorical columns")
    List<Integer> categoricalCols = new ArrayList<>();
    @Parameter(names = "-c", required = true)
    private String command;
    @Parameter(names = "-xCol", description = "The x column")
    private String xCol;
    @Parameter(names = "-yCol", description = "The y column")
    private String yCol;
    @Parameter(names = "-cols", description = "Number of columns")
    private Integer cols = 10;
    @Parameter(names = "-out", description = "The output file")
    private String outFile;
    @Parameter(names = "-initMode")
    private String initMode;
    @Parameter(names= "-duckDbMode")
    private String duckDbMode;
    @Parameter(names = "-bounds", converter = RectangleConverter.class, description = "Grid boundaries")
    private Rectangle bounds;
    @Parameter(names = "-seqCount", description = "Number of queries in the sequence")
    private Integer seqCount;
    @Parameter(names = "-objCount", description = "Number of objects")
    private Integer objCount;
    @Parameter(names = "-minShift", description = "Min shift in the query sequence")
    private Integer minShift;
    @Parameter(names = "-maxShift", description = "Max shift in the query sequence")
    private Integer maxShift;
    @Parameter(names = "-minFilters", description = "Min filters in the query sequence")
    private Integer minFilters = 0;
    @Parameter(names = "-maxFilters", description = "Max filters in the query sequence")
    private Integer maxFilters = 0;
    @Parameter(names = "--measureMem", description = "Measure index memory after every query in the sequence")
    private boolean measureMem = false;
    @Parameter(names = "--measureMaxDepth", description = "Measure index max depth after every query in the sequence")
    private boolean measureMaxDepth = false;
    @Parameter(names = "-rect", converter = RectangleConverter.class, description = "Rectangle")
    private Rectangle rect = null;
    @Parameter(names = "-measureCols", description = "The measure columns")
    private List<Integer> measureCols;

    @Parameter(names = "-groupBy", description = "Group by col")
    private Integer groupBy;
    @Parameter(names = "-filters", converter = FilterConverter.class, description = "Q0 Filters")
    private Map<Integer, String> categoricalFilters;

    @Parameter(names = "-valid", description = "Filters for skipping invalid rows before indexing", converter = DataValidationFilterConverter.class)
    private List<DataValidationFilter> validationFilters = new ArrayList<>();

    @Parameter(names = "-sort")
    private String sort;

    @Parameter(names = "-valueRange", converter = RangeConverter.class, description = "Value range")
    private Range<Float> valueRange;

    @Parameter(names = "-binCount", description = "Number of bins for BINN method")
    private Integer binCount;

    @Parameter(names = "-gridSize")
    private Integer gridSize;

    @Parameter(names = "-cardinality")
    private Integer cardinality;

    @Parameter(names = "-run")
    private Integer run;

    @Parameter(names = "--help", help = true, description = "Displays help")
    private boolean help;

    public static void main(String... args) throws IOException, ClassNotFoundException {
        Experiments experiments = new Experiments();
        JCommander jCommander = new JCommander(experiments, args);
        if (experiments.help) {
            jCommander.usage();
        } else {
            experiments.run();
        }
    }

    private void run() throws IOException {
        SyntheticDatasetGenerator generator;
        switch (command) {
            case "timeInitialization":
                timeInitialization();
                break;
            case "timeAssignmentTime":
                timeAssignmentTime();
                break;
            case "timeQueries":
                timeQueries();
                break;
            case "timeApproximateQueries":
                timeApproximateQueries();
                break;
            case "timeDuckDBQueries":
                Preconditions.checkNotNull(duckDbMode, "You must specify the duckDbMode parameter. Mode can be: directCSV, table, spatialIndex");
                timeDuckDBQueries();
                break;
            case "findBounds":
                findBounds();
                break;
            case "computeUtils":
                computeUtils();
                break;
            case "synth10":
                generator = new SyntheticDatasetGenerator(100000000, 10, Arrays.asList(2, 3, 4, 5, 6, 7), 10, outFile);
                generator.generate();
                break;
            case "synth50":
                List<Integer> catCols = new ArrayList<>();
                for (int i = 10; i < 30; i++) {
                    catCols.add(i);
                }
                generator = new SyntheticDatasetGenerator(100000000, 50, catCols, 10, outFile);
                generator.generate();
                break;
            default:
        }
    }

    private int getCategoricalNodeBudget(double sizeInGb) {
        SizeOf sizeOf = SizeOf.newInstance();
        TreeNode root = new TreeNode((short) 0);
        int nodeCount = 1;
        for (int i = 0; i < 10; i++) {
            nodeCount++;
            TreeNode child = root.getOrAddChild((short) i);
            if (measureCols != null) {
                for (Integer measureCol : measureCols) {
                    child.adjustStats(measureCol.shortValue(), 0f); // Initialize each measure with 0f
                }
            } else {
                child.adjustStats((short) 0, 0f); // Fallback for single measure
            }
        }
        int nodeSize = (int) sizeOf.deepSizeOf(root) / nodeCount;
        LOG.debug("average categorical node size: " + nodeSize);
        return (int) Math.floor(sizeInGb * (int) Math.pow(10, 9) / nodeSize);
    }

    public void findBounds() {
        Integer x = Integer.parseInt(xCol), y = Integer.parseInt(yCol);

        CsvParserSettings readerSettings = new CsvParserSettings();
        CsvParser parser = new CsvParser(readerSettings);

        float minX = Float.POSITIVE_INFINITY;
        float maxX = Float.NEGATIVE_INFINITY;
        float minY = Float.POSITIVE_INFINITY;
        float maxY = Float.NEGATIVE_INFINITY;

        parser.beginParsing(new File(csv));
        String[] row;
        int i = 0;
        while ((row = parser.parseNext()) != null) {
            i++;
            minX = Math.min(minX, Float.parseFloat(row[x]));
            maxX = Math.max(maxX, Float.parseFloat(row[x]));
            minY = Math.min(minY, Float.parseFloat(row[y]));
            maxY = Math.max(maxY, Float.parseFloat(row[y]));
            if (i % 1000000 == 0) {
                LOG.debug("Parsing row " + i);
            }
        }
        LOG.debug(new Rectangle(Range.open(minX, maxX), Range.open(minY, maxY)));
    }

    private void timeAssignmentTime() throws IOException {
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;

        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, true), csvWriterSettings);

        int leafTiles = 0;
        int categoricalNodeBudget = getCategoricalNodeBudget(catBudget);

        csv = "NO CSV";
        Schema schema = new Schema(csv, DELIMITER, Integer.parseInt(xCol), Integer.parseInt(yCol), measureCols,
                bounds, objCount, validationFilters);
        List<CategoricalColumn> categoricalColumns = new ArrayList<>();
        for (int i = 0; i < categoricalCols.size(); i++) {
            categoricalColumns.add(new DummyCategoricalColumn(categoricalCols.get(i), cardinality));
        }
        schema.setCategoricalColumns(categoricalColumns);

        IndexConfig.GRID_SIZE = gridSize;
        IndexConfig.SUBTILE_RATIO = 0;

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        Veti veti = new Veti(schema, categoricalNodeBudget, initMode, binCount);
        Query q0 = new Query(rect, categoricalFilters, groupBy != null ? Arrays.asList(groupBy) : new ArrayList<>(), measureCols);
        veti.generateGrid(q0);
        stopwatch.stop();

        if (addHeader) {
            csvWriter.writeHeaders("csv", "initMode", "grid size", "Leaf tiles", "initCatBudget (Gb)",
                    "initCatBudget (nodes)", "q0", "# of categorical columns", "cardinality", "Total Util",
                    "Time (sec)");
        }

        csvWriter.addValue(csv);
        csvWriter.addValue(initMode);
        csvWriter.addValue(gridSize);
        csvWriter.addValue(leafTiles);
        csvWriter.addValue(catBudget);
        csvWriter.addValue(categoricalNodeBudget);
        csvWriter.addValue(q0);
        csvWriter.addValue(schema.getCategoricalColumns().size());
        csvWriter.addValue(cardinality);
        csvWriter.addValue(veti.getTotalUtil());
        csvWriter.addValue(stopwatch.elapsed(TimeUnit.NANOSECONDS));
        csvWriter.writeValuesToRow();
        csvWriter.close();
    }

    private void computeUtils() throws IOException {
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;

        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, true), csvWriterSettings);
        int categoricalNodeBudget = getCategoricalNodeBudget(catBudget);

        Schema schema;
        if (csv != null)
            schema = getSchemaWithSampling();
        else {
            schema = new Schema(csv, DELIMITER, Integer.parseInt(xCol), Integer.parseInt(yCol), measureCols,
                    bounds, objCount, validationFilters);
            List<CategoricalColumn> categoricalColumns = new ArrayList<>();
            for (int i = 0; i < categoricalCols.size(); i++) {
                categoricalColumns.add(new DummyCategoricalColumn(categoricalCols.get(i), cardinality));
            }
            schema.setCategoricalColumns(categoricalColumns);
        }

        Veti veti = new Veti(schema, categoricalNodeBudget, initMode, binCount);

        Query q0 = new Query(rect, categoricalFilters, groupBy != null ? Arrays.asList(groupBy) : new ArrayList<>(), measureCols);
        veti.generateGrid(q0);

        if (addHeader) {
            csvWriter.writeHeaders("csv", "initMode", "initCatBudget (Gb)", "initCatBudget (nodes)", "Tree Node Count",
                    "q0", "categoricalColumns", "Total Util");
        }

        csvWriter.addValue(csv);
        csvWriter.addValue(initMode);
        csvWriter.addValue(catBudget);
        csvWriter.addValue(categoricalNodeBudget);
        csvWriter.addValue(TreeNode.getInstanceCount());
        csvWriter.addValue(q0);
        csvWriter.addValue(schema.getCategoricalColumns());
        csvWriter.addValue(veti.getTotalUtil());
        csvWriter.writeValuesToRow();
        csvWriter.close();
    }

    private void timeInitialization() throws IOException {
        Preconditions.checkNotNull(csv, "You must define the csv file.");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;

        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, true), csvWriterSettings);

        long memorySize = 0;
        SizeOf sizeOf = SizeOf.newInstance();
        int leafTiles = 0;
        int categoricalNodeBudget = getCategoricalNodeBudget(catBudget);
        Schema schema = getSchemaWithSampling();

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();

        Veti veti = new Veti(schema, categoricalNodeBudget, initMode, binCount);
        veti.setSort(sort);

        Query q0 = new Query(rect, categoricalFilters, Arrays.asList(groupBy), measureCols);
        veti.initialize(q0);
        stopwatch.stop();

        leafTiles = veti.getLeafTileCount();
        try {
            memorySize = sizeOf.deepSizeOf(veti);
        } catch (Exception e) {
        }
        if (addHeader) {
            csvWriter.writeHeaders("csv", "initMode", "initCatBudget (Gb)", "initCatBudget (nodes)", "Tree Node Count",
                    "q0", "categoricalColumns", "Time (sec)", "Total Util", "Leaf tiles", "Memory (Gb)");
        }

        csvWriter.addValue(csv);
        csvWriter.addValue(initMode);
        csvWriter.addValue(catBudget);
        csvWriter.addValue(categoricalNodeBudget);
        csvWriter.addValue(TreeNode.getInstanceCount());
        csvWriter.addValue(q0);
        csvWriter.addValue(schema.getCategoricalColumns());
        csvWriter.addValue(stopwatch.elapsed(TimeUnit.SECONDS));
        csvWriter.addValue(veti.getTotalUtil());
        csvWriter.addValue(leafTiles);
        csvWriter.addValue((double) memorySize / 1000000000d);
        csvWriter.writeValuesToRow();
        csvWriter.close();
    }

    private void timeQueries() throws IOException {
        Preconditions.checkNotNull(csv, "You must define the csv file.");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, true), csvWriterSettings);
        if (addHeader) {
            csvWriter.writeHeaders("csv", "errorBound", "initMode", "initCatBudget (Gb)",
                    "initCatBudget (nodes)", "binCount", "i", "query", "indexUtil", "Tree Node Count", "Leaf tiles",
                    "Overlapped tiles",
                    "Fully Contained Tiles", "Expanded nodes", "I/Os", "Time (sec)", "Query Result", "Query Result Sum");
        }

        Stopwatch stopwatch;
        int categoricalNodeBudget = 0;
        if (categoricalCols != null && categoricalCols.size() > 0) {
            categoricalNodeBudget = getCategoricalNodeBudget(catBudget);
        }

        Schema schema = getSchemaWithSampling();


        Veti veti = new Veti(schema, categoricalNodeBudget, initMode, binCount);

        Query q0 = new Query(rect, categoricalFilters, groupBy != null ? Arrays.asList(groupBy) : null, measureCols);
        List<Query> sequence = generateQuerySequence(q0, schema);

        for (int i = 0; i < sequence.size(); i++) {
            Query query = sequence.get(i);
            LOG.debug("Executing query " + i);

            stopwatch = Stopwatch.createStarted();
            QueryResults queryResults = veti.executeQuery(query);
            stopwatch.stop();

            csvWriter.addValue(csv);
            csvWriter.addValue(0);
            csvWriter.addValue(initMode);
            csvWriter.addValue(catBudget);
            csvWriter.addValue(categoricalNodeBudget);
            csvWriter.addValue(binCount);
            csvWriter.addValue(i);
            csvWriter.addValue(queryResults.getQuery());
            csvWriter.addValue(veti.getTotalUtil());
            csvWriter.addValue(TreeNode.getInstanceCount());
            csvWriter.addValue(veti.getLeafTileCount());
            csvWriter.addValue(queryResults.getTileCount());
            csvWriter.addValue(queryResults.getFullyContainedTileCount());
            csvWriter.addValue(queryResults.getExpandedNodeCount());
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9));
            csvWriter.addValue(queryResults.getStats());
            csvWriter.addValue(queryResults.getStats() != null && queryResults.getStats().get(null) != null
                    ? queryResults.getStats().get(null).entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().sum())).toString()
                    : null);
            csvWriter.writeValuesToRow();
        }
        csvWriter.close();
    }

    private void timeApproximateQueries() throws IOException {
        Preconditions.checkNotNull(csv, "You must define the csv file.");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        // If errorBound is 0, we are using the exact index
        if (errorBound == 0){
            timeQueries();
            return;
        }

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        csvWriter.writeHeaders("csv", "errorBound", "initMode", "i", "query", "indexUtil", "Tree Node Count", "Leaf tiles",
                "Overlapped tiles", "Fully Contained Tiles With Stats", "Fully Contained Tiles Without Stats", "Sampling Tiles", "Sampling Rate", "Expanded nodes", "I/Os", "Time (sec)", "Confidence Interval", "Error Bound", "run");
        

        Stopwatch stopwatch;

        Schema schema = getSchemaWithSampling();

        ApproximateValinor index = new ApproximateValinor(schema, errorBound);

        Query q0 = new Query(rect, categoricalFilters, groupBy != null ? Arrays.asList(groupBy) : new ArrayList<>(), measureCols);
        List<Query> sequence = generateQuerySequence(q0, schema);

        for (int i = 0; i < sequence.size(); i++) {
            Query query = sequence.get(i);
            LOG.debug("Executing query {}: {}", i, query);

            stopwatch = Stopwatch.createStarted();
            ApproximateQueryResults queryResults = index.executeQuery(query);
            stopwatch.stop();

            csvWriter.addValue(csv);
            csvWriter.addValue(errorBound);
            csvWriter.addValue(initMode);
            csvWriter.addValue(i);
            csvWriter.addValue(queryResults.getQuery());
            csvWriter.addValue(index.getTotalUtil());
            csvWriter.addValue(TreeNode.getInstanceCount());
            csvWriter.addValue(index.getLeafTileCount());
            csvWriter.addValue(queryResults.getTileCount());
            csvWriter.addValue(queryResults.getFullyContainedTileCount());
            csvWriter.addValue(queryResults.getFullyContainedTileWithoutStatsCount());
            csvWriter.addValue(queryResults.getSamplingTileCount());
            csvWriter.addValue(queryResults.getSamplingRate());
            csvWriter.addValue(queryResults.getExpandedNodeCount());
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9));
            csvWriter.addValue(queryResults.getConfidenceIntervals() != null
                            ? queryResults.getConfidenceIntervals().entrySet().stream()
                                    .collect(Collectors.toMap(
                                            Map.Entry::getKey,
                                            e -> Arrays.asList(e.getValue()[0], e.getValue()[1])))
                                    .toString()
                            : "null");
            csvWriter.addValue(queryResults.getErrorBounds());
            csvWriter.addValue(run);
            csvWriter.writeValuesToRow();
            csvWriter.flush();
        }
        csvWriter.close();
    }

    private Schema getSchemaWithSampling() {
        Schema schema = new Schema(csv, DELIMITER, Integer.parseInt(xCol), Integer.parseInt(yCol), measureCols,
                bounds, objCount, validationFilters);

        List<CategoricalColumn> categoricalColumns = new ArrayList<>();
        for (int i = 0; i < categoricalCols.size(); i++) {
            categoricalColumns.add(new CategoricalColumn(categoricalCols.get(i)));
        }
        schema.setCategoricalColumns(categoricalColumns);

        CsvParserSettings parserSettings = schema.createCsvParserSettings();
        CsvParser parser = new CsvParser(parserSettings);

        int i = 0;
        parser.beginParsing(new File(schema.getCsv()));
        String[] row;
        while ((row = parser.parseNext()) != null && i < 1000000) {
            for (CategoricalColumn column : categoricalColumns) {
                column.getValueKey(row[column.getIndex()]);
            }
            i++;
        }
        parser.stopParsing();
        return schema;
    }

    private List<Query> generateQuerySequence(Query q0, Schema schema) {
        Preconditions.checkNotNull(seqCount, "No sequence count specified.");
        Preconditions.checkNotNull(minShift, "Min query shift must be specified.");
        Preconditions.checkNotNull(maxShift, "Max query shift must be specified.");


        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(minShift, maxShift, minFilters,
                maxFilters, zoomFactor);
        return sequenceGenerator.generateQuerySequence(q0, seqCount, schema);
    }

    private void timeDuckDBQueries() throws IOException {
        Preconditions.checkNotNull(csv, "You must define the csv file.");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, true), csvWriterSettings);
        
        if (addHeader) {
            csvWriter.writeHeaders("csv", "version", "i", "rowCount", "Time (sec)", "Query");
        }

        try {
            // Parse x and y column indices
            int xColIdx = Integer.parseInt(xCol);
            int yColIdx = Integer.parseInt(yCol);

            // Generate query sequence
            Schema dummySchema = new Schema(csv, DELIMITER, xColIdx, yColIdx, measureCols,
                    bounds, objCount, validationFilters);
            Query q0 = new Query(rect, categoricalFilters, groupBy != null ? Arrays.asList(groupBy) : new ArrayList<>(), measureCols);
            List<Query> sequence = generateQuerySequence(q0, dummySchema);
            
            // Determine execution mode
            DuckDBQueryExecutor.ExecutionMode mode = parseExecutionMode(duckDbMode);
            
            // Format column indices as zero-padded strings (e.g., "05" instead of "5")
            String xColStr = String.format("%02d", xColIdx);
            String yColStr = String.format("%02d", yColIdx);
            // Create DuckDB executor with the appropriate mode
            DuckDBQueryExecutor executor = new DuckDBQueryExecutor(csv, mode, "column" + xColStr, "column" + yColStr);
            
            // Log initialization timing metrics
            long tableCreationTimeMs = executor.getTableCreationTimeNanos() / 1_000_000;
            long indexCreationTimeMs = executor.getIndexCreationTimeNanos() / 1_000_000;
            LOG.info("DuckDB initialization timings - Mode: {}, Table creation: {}ms, Index creation: {}ms", 
                     duckDbMode, tableCreationTimeMs, indexCreationTimeMs);
            
            LOG.info("Executing DuckDB queries in {} mode", duckDbMode);
            
            for (int i = 0; i < sequence.size(); i++) {
                Query query = sequence.get(i);
                LOG.debug("Executing query {}", i);
                try {
                    QueryResult result = executor.executeQuery(query);
                    
                    csvWriter.addValue(csv);
                    csvWriter.addValue(duckDbMode); 
                    csvWriter.addValue(i);
                    csvWriter.addValue(result.getRowCount());
                    csvWriter.addValue(result.getExecutionTimeSeconds());
                    csvWriter.addValue(result.getQuery());
                    csvWriter.writeValuesToRow();
                } catch (Exception e) {
                    LOG.warn("Error executing query {}: {}", i, e.getMessage());
                    csvWriter.addValue(csv);
                    csvWriter.addValue(duckDbMode);
                    csvWriter.addValue(i);
                    csvWriter.addValue(-1);
                    csvWriter.addValue(-1);
                    csvWriter.addValue("ERROR: " + e.getMessage());
                    csvWriter.writeValuesToRow();
                }
            }
            executor.close();
            csvWriter.close();
            LOG.info("DuckDB query execution completed");
            
        } catch (Exception e) {
            LOG.error("Fatal error in timeDuckDBQueries", e);
            csvWriter.close();
            throw new IOException(e);
        }
    }

    private DuckDBQueryExecutor.ExecutionMode parseExecutionMode(String mode) {
        switch(mode) {
            case "directCSV":
                return DuckDBQueryExecutor.ExecutionMode.DIRECT_CSV;
            case "table":
                return DuckDBQueryExecutor.ExecutionMode.TABLE;
            case "spatialIndex":
                return DuckDBQueryExecutor.ExecutionMode.SPATIAL_INDEX;
            default:
                throw new IllegalArgumentException("Invalid duckDbMode: " + mode + ". Valid modes are: directCSV, table, spatialIndex");
        }
    }

}
