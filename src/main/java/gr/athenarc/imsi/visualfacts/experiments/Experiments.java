package gr.athenarc.imsi.visualfacts.experiments;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import gr.athenarc.imsi.visualfacts.*;
import gr.athenarc.imsi.visualfacts.config.IndexConfig;
import gr.athenarc.imsi.visualfacts.experiments.util.*;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ehcache.sizeof.SizeOf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.DELIMITER;


public class Experiments {

    private static final Logger LOG = LogManager.getLogger(Experiments.class);


    @Parameter(names = "-catBudget", description = "Categorical Node budget in GB")
    public Double catBudget;
    @Parameter(names = "-csv", description = "The csv file")
    public String csv;

    @Parameter(names = "-zoomFactor", description = "Zoom factor for zoom in operation. The inverse applies to zoom out operation.")
    public Float zoomFactor = 0f;
    @Parameter(names = "-catCols", variableArity = true, description = "Categorical columns")
    List<Integer> categoricalCols = new ArrayList<>();

    @Parameter(names = "-dedupCols", variableArity = true, description = "Deduplication columns")
    List<Integer> dedupCols = new ArrayList<>();

    @Parameter(names = "-c", required = true)
    private String command;
    @Parameter(names = "-xCol", description = "The x column")
    private String xCol;
    @Parameter(names = "-yCol", description = "The y column")
    private String yCol;
    @Parameter(names = "-idCol", description = "The id column")
    private String idCol;
    @Parameter(names = "-cols", description = "Number of columns")
    private Integer cols = 10;
    @Parameter(names = "-out", description = "The output file")
    private String outFile;
    @Parameter(names = "-initMode")
    private String initMode;

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
    private Integer minFilters;
    @Parameter(names = "-maxFilters", description = "Max filters in the query sequence")
    private Integer maxFilters;
    @Parameter(names = "--measureMem", description = "Measure index memory after every query in the sequence")
    private boolean measureMem = false;
    @Parameter(names = "--measureMaxDepth", description = "Measure index max depth after every query in the sequence")
    private boolean measureMaxDepth = false;
    @Parameter(names = "-rect", converter = RectangleConverter.class, description = "Rectangle")
    private Rectangle rect = null;
    @Parameter(names = "-measureCol", description = "The measure column")
    private Integer measureCol;
    @Parameter(names = "-groupBy", description = "Group by col")
    private Integer groupBy;
    @Parameter(names = "-filters", converter = FilterConverter.class, description = "Q0 Filters")
    private Map<Integer, String> categoricalFilters;

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

    @Parameter(names = "--help", help = true, description = "Displays help")
    private boolean help;

    @Parameter(names = "-model", description = "The calcite model file")
    public String model;

    public static void main(String... args) throws IOException, ClassNotFoundException {
        Experiments experiments = new Experiments();
        JCommander jCommander = new JCommander(experiments, args);
        if (experiments.help) {
            jCommander.usage();
        } else {
            experiments.run();
        }
    }

    private void run() throws IOException, ClassNotFoundException {
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
            child.adjustStats(1f, 0f);
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

        Schema schema = new Schema(csv, DELIMITER, Integer.parseInt(xCol), Integer.parseInt(yCol), measureCol, null, bounds, objCount, Integer.parseInt(idCol));
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
        Query q0 = new Query(rect, categoricalFilters, Arrays.asList(groupBy), measureCol, true);
        veti.generateGrid(q0);
        stopwatch.stop();


        if (addHeader) {
            csvWriter.writeHeaders("csv", "initMode", "grid size", "Leaf tiles", "initCatBudget (Gb)", "initCatBudget (nodes)", "q0", "# of categorical columns", "cardinality", "Total Util", "Time (sec)");
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
            schema = new Schema(csv, DELIMITER, Integer.parseInt(xCol), Integer.parseInt(yCol), measureCol, null, bounds, objCount, 0);
            List<CategoricalColumn> categoricalColumns = new ArrayList<>();
            for (int i = 0; i < categoricalCols.size(); i++) {
                categoricalColumns.add(new DummyCategoricalColumn(categoricalCols.get(i), cardinality));
            }
            schema.setCategoricalColumns(categoricalColumns);
        }

        Veti veti = new Veti(schema, categoricalNodeBudget, initMode, binCount);

        Query q0 = new Query(rect, categoricalFilters, Arrays.asList(groupBy), measureCol, true);
        veti.generateGrid(q0);


        if (addHeader) {
            csvWriter.writeHeaders("csv", "initMode", "initCatBudget (Gb)", "initCatBudget (nodes)", "Tree Node Count", "q0", "categoricalColumns", "Total Util");
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

    private void timeInitialization() throws IOException, ClassNotFoundException {
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

        Query q0 = new Query(rect, categoricalFilters, Arrays.asList(groupBy), measureCol, true);
        veti.initialize(q0);
        stopwatch.stop();

        leafTiles = veti.getLeafTileCount();
        try {
            memorySize = sizeOf.deepSizeOf(veti);
        } catch (Exception e) {
        }
        if (addHeader) {
            csvWriter.writeHeaders("csv", "initMode", "initCatBudget (Gb)", "initCatBudget (nodes)", "Tree Node Count", "q0", "categoricalColumns", "Time (sec)", "Total Util", "Leaf tiles", "Memory (Gb)");
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

    private void timeQueries() throws IOException, ClassNotFoundException {
        Preconditions.checkNotNull(csv, "You must define the csv file.");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, true), csvWriterSettings);
        if (addHeader) {
            csvWriter.writeHeaders("csv", "categoricalCols", "initMode", "initCatBudget (Gb)",
                    "initCatBudget (nodes)", "binCount", "i", "query", "indexUtil", "Tree Node Count", "Leaf tiles", "Overlapped tiles",
                    "Fully Contained Tiles", "Expanded nodes", "I/Os", "Time (sec)", "Query Result");
        }


        Stopwatch stopwatch;

        int categoricalNodeBudget = getCategoricalNodeBudget(catBudget);
        Schema schema = getSchemaWithSampling();

        LOG.debug(schema.getCategoricalColumns());

        Veti veti = new Veti(schema, categoricalNodeBudget, initMode, binCount, model);

        Query q0 = new Query(rect, categoricalFilters, Arrays.asList(groupBy), measureCol,schema.getDedupCols() != null && schema.getDedupCols().size() > 0);
        List<Query> sequence = generateQuerySequence(q0, schema);

        for (int i = 0; i < sequence.size(); i++) {
            Query query = sequence.get(i);
            LOG.debug("Executing query " + i);

            stopwatch = Stopwatch.createStarted();
            QueryResults queryResults = veti.executeQuery(query);
            stopwatch.stop();

            csvWriter.addValue(csv);
            csvWriter.addValue(schema.getCategoricalColumns());
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
            csvWriter.addValue(queryResults.getStats().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().xStats(), (oldValue, newValue) -> oldValue)));
            csvWriter.writeValuesToRow();
        }
        csvWriter.close();
    }


    private Schema getSchemaWithSampling() {
        Schema schema = new Schema(csv, DELIMITER, Integer.parseInt(xCol), Integer.parseInt(yCol), measureCol, null, bounds, objCount, Integer.parseInt(idCol));
        schema.setDedupCols(new HashSet<>(dedupCols));

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
        Preconditions.checkNotNull(minFilters, "Min filters must be specified.");
        Preconditions.checkNotNull(maxFilters, "Max filters must be specified.");

        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(minShift, maxShift, minFilters, maxFilters, zoomFactor);
        return sequenceGenerator.generateQuerySequence(q0, seqCount, schema);
    }

}
