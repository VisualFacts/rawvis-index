package gr.athenarc.imsi.visualfacts.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import gr.athenarc.imsi.visualfacts.ApproximateValinor;
import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.TreeNode;
import gr.athenarc.imsi.visualfacts.Veti;
import gr.athenarc.imsi.visualfacts.experiments.config.ExperimentConfig;
import gr.athenarc.imsi.visualfacts.experiments.config.ExperimentConfigLoader;
import gr.athenarc.imsi.visualfacts.experiments.config.ExplorationScenarioConfig;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor.QueryResult;
import gr.athenarc.imsi.visualfacts.experiments.util.PhasedQuerySequenceGenerator;
import gr.athenarc.imsi.visualfacts.experiments.util.QuerySequenceGenerator;
import gr.athenarc.imsi.visualfacts.experiments.util.SyntheticDatasetGenerator;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;

public class Experiments {

    private static final Logger LOG = LogManager.getLogger(Experiments.class);

    // ========== Scenario-based configuration ==========
    @Parameter(names = "-scenario", description = "Name of the scenario to run (defined in YAML config)")
    private String scenario;

    @Parameter(names = "-configFile", description = "Path to YAML config file (optional, defaults to classpath resource)")
    private String configFile;

    // ========== Runtime/execution parameters ==========
    @Parameter(names = "-c", required = true, description = "Command to execute")
    private String command;

    @Parameter(names = "-errorBound", description = "Error bound for approximate queries")
    public Double errorBound;

    @Parameter(names = "-out", description = "The output file")
    private String outFile;

    @Parameter(names = "-initMode", description = "Initialization mode")
    private String initMode;

    @Parameter(names = "-duckDbMode", description = "DuckDB execution mode: directCSV, table, spatialIndex")
    private String duckDbMode;

    @Parameter(names = "-gridSize", description = "Grid size for index")
    private Integer gridSize;

    @Parameter(names = "-run", description = "Run number for experiments")
    private Integer run;

    @Parameter(names = "-queries", description = "Path to file containing saved query sequence (one query per line)")
    private String queriesFile;

    @Parameter(names = "-groupBy", description = "Group by column")
    private Integer groupBy;

    @Parameter(names = "-sort", description = "Sort mode")
    private String sort;

    @Parameter(names = "-cardinality", description = "Cardinality for dummy categorical columns")
    private Integer cardinality;

    @Parameter(names = "-minFilters", description = "Min filters in the query sequence")
    private Integer minFilters = 0;

    @Parameter(names = "-maxFilters", description = "Max filters in the query sequence")
    private Integer maxFilters = 0;

    @Parameter(names = "-numMeasures", description = "Number of measure columns to use (uses first N from config). If not specified, uses all.")
    private Integer numMeasures;

    @Parameter(names = "--measureMem", description = "Measure index memory after every query in the sequence")
    private boolean measureMem = false;

    @Parameter(names = "--measureMaxDepth", description = "Measure index max depth after every query in the sequence")
    private boolean measureMaxDepth = false;

    @Parameter(names = "--help", help = true, description = "Displays help")
    private boolean help;

    // ========== Loaded configuration ==========
    private ExperimentConfig experimentConfig;
    private ExplorationScenarioConfig scenarioConfig;
    private Schema schema;

    public static void main(String... args) throws IOException, ClassNotFoundException {
        Experiments experiments = new Experiments();
        JCommander jCommander = new JCommander(experiments, args);
        if (experiments.help) {
            jCommander.usage();
        } else {
            experiments.run();
        }
    }

    /**
     * Loads the experiment configuration.
     * Populates schema and scenario config from YAML.
     */
    private void loadConfiguration() throws IOException {
        if (scenario != null && !scenario.isEmpty()) {
            LOG.info("Loading configuration for scenario: {}", scenario);
            experimentConfig = ExperimentConfigLoader.load(configFile);

            scenarioConfig = experimentConfig.getScenario(scenario);
            if (scenarioConfig == null) {
                throw new IllegalArgumentException("Scenario not found in config: " + scenario);
            }

            schema = experimentConfig.getSchemaForScenario(scenario);
            
            // Apply numMeasures limit if specified
            if (numMeasures != null && numMeasures > 0) {
                List<Integer> allMeasures = schema.getMeasureCols();
                if (numMeasures > allMeasures.size()) {
                    throw new IllegalArgumentException(String.format(
                            "Requested %d measures but only %d available. Aborting experiment.",
                            numMeasures, allMeasures.size()));
                } else {
                    List<Integer> limitedMeasures = allMeasures.subList(0, numMeasures);
                    schema.setMeasureCols(limitedMeasures);
                    LOG.info("Limited measures to first {}: {}", numMeasures, limitedMeasures);
                }
            }
            
            LOG.info("Loaded scenario '{}' with dataset '{}'", scenario, scenarioConfig.getDataset());

        }
    }

    /**
     * Validates that a scenario has been loaded.
     * Call this at the start of any command that requires schema/scenario configuration.
     */
    private void requireScenario(String commandName) {
        if (scenario == null || scenario.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Command '%s' requires -scenario parameter. " +
                            "Available scenarios are defined in the YAML config file.", commandName));
        }
        if (scenarioConfig == null || schema == null) {
            throw new IllegalStateException(
                    String.format("Scenario '%s' was specified but configuration failed to load.", scenario));
        }
    }

    private void run() throws IOException {
        // Load configuration first if scenario is specified
        loadConfiguration();

        SyntheticDatasetGenerator generator;
        switch (command) {
            case "timeQueries":
                timeQueries();
                break;
            case "timeApproximateQueries":
                timeApproximateQueries();
                break;
            case "timeDuckDBQueries":
                Preconditions.checkNotNull(duckDbMode,
                        "You must specify the duckDbMode parameter. Mode can be: directCSV, table, spatialIndex");
                timeDuckDBQueries();
                break;
            case "generateAndSaveQuerySequence":
                generateAndSaveQuerySequence();
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

    private void timeQueries() throws IOException {
        requireScenario("timeQueries");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;

        CsvWriter csvWriter = null;
        Veti veti = null;

        try {

            csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
            if (addHeader) {
                csvWriter.writeHeaders("csv", "errorBound", "initMode", "i", "query", "indexUtil", "Tree Node Count",
                        "Leaf tiles",
                        "Overlapped tiles",
                        "Fully Contained Tiles", "Expanded nodes", "I/Os", "Time (sec)", "Query Result",
                        "Query Result Sum");
            }

            Stopwatch stopwatch;

            veti = new Veti(schema, 0, initMode, 0);

            // Build initial query from scenario config
            Rectangle rect = scenarioConfig.getQ0().toRectangle();
            Map<Integer, String> categoricalFilters = scenarioConfig.getQ0().getFilters();
            Query q0 = new Query(rect, categoricalFilters, groupBy != null ? Arrays.asList(groupBy) : null,
                    schema.getMeasureCols());
            List<Query> sequence = generateQuerySequence(q0, schema);

            for (int i = 0; i < sequence.size(); i++) {
                Query query = sequence.get(i);
                LOG.debug("Executing query {}: {}", i, query);

                stopwatch = Stopwatch.createStarted();
                QueryResults queryResults = veti.executeQuery(query);
                stopwatch.stop();

                csvWriter.addValue(schema.getCsv());
                csvWriter.addValue(0);
                csvWriter.addValue(initMode);
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
                LOG.debug("Finished query {} in {} sec", i, stopwatch.elapsed(TimeUnit.NANOSECONDS) / 1_000_000_000.0);
            }
        } finally {
            // Close Veti (if you added a close() method)
            if (veti != null) {
                try {
                    veti.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close Veti", e);
                }
            }

            // Close CsvWriter (flushes + closes underlying FileWriter)
            if (csvWriter != null) {
                try {
                    csvWriter.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close CsvWriter", e);
                }
            }
        }
    }

    private void timeApproximateQueries() throws IOException {
        requireScenario("timeApproximateQueries");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        // If errorBound is 0, we are using the exact index
        if (errorBound == 0) {
            timeQueries();
            return;
        }

        CsvWriter csvWriter = null;
        ApproximateValinor index = null;
        try {
            CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
            csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
            csvWriter.writeHeaders("csv", "errorBound", "initMode", "i", "query", "indexUtil", "Tree Node Count",
                    "Leaf tiles",
                    "Overlapped tiles", "Fully Contained Tiles With Stats", "Fully Contained Tiles Without Stats",
                    "Sampling Tiles", "Sampling Rate", "Expanded nodes", "I/Os", "Time (sec)", "Confidence Interval",
                    "Error Bound", "run");

            Stopwatch stopwatch;

            index = new ApproximateValinor(schema, errorBound);

            // Build initial query from scenario config
            Rectangle rect = scenarioConfig.getQ0().toRectangle();
            Map<Integer, String> categoricalFilters = scenarioConfig.getQ0().getFilters();
            Query q0 = new Query(rect, categoricalFilters, groupBy != null ? Arrays.asList(groupBy) : new ArrayList<>(),
                    schema.getMeasureCols());
            List<Query> sequence = generateQuerySequence(q0, schema);

            for (int i = 0; i < sequence.size(); i++) {
                Query query = sequence.get(i);
                LOG.debug("Executing query {}: {}", i, query);

                stopwatch = Stopwatch.createStarted();
                ApproximateQueryResults queryResults = index.executeQuery(query);
                stopwatch.stop();

                csvWriter.addValue(schema.getCsv());
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
                LOG.debug("Finished query {} in {} sec", i, stopwatch.elapsed(TimeUnit.NANOSECONDS) / 1_000_000_000.0);
            }
        } finally {
            // Close Veti (if you added a close() method)
            if (index != null) {
                try {
                    index.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close ApproximateValinor", e);
                }
            }

            // Close CsvWriter (flushes + closes underlying FileWriter)
            if (csvWriter != null) {
                try {
                    csvWriter.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close CsvWriter", e);
                }
            }
        }
    }

    private List<Query> generateQuerySequence(Query q0, Schema schema) throws IOException {
        // If queries file is provided, read from it instead of generating new queries
        if (queriesFile != null && !queriesFile.isEmpty()) {
            return loadQueriesFromFile(queriesFile);
        }

        // Use phased generator if phases are configured
        if (scenarioConfig.isPhased()) {
            LOG.info("Using phased query sequence generator with {} phases", scenarioConfig.getPhases().size());
            PhasedQuerySequenceGenerator generator = new PhasedQuerySequenceGenerator(scenarioConfig.getPhases());
            return generator.generateQuerySequence(q0, schema);
        }

        // Otherwise use simple pan-based generator
        int seqCount = scenarioConfig.getSeqCount();
        int minShift = scenarioConfig.getMinShift();
        int maxShift = scenarioConfig.getMaxShift();
        float zoomFactor = scenarioConfig.getZoomFactor();

        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(minShift, maxShift, minFilters,
                maxFilters, zoomFactor, scenarioConfig.getDirectionWeights());
        return sequenceGenerator.generateQuerySequence(q0, seqCount, schema);
    }

    /**
     * Loads query sequence from a file.
     * Each line in the file should contain a serialized query.
     */
    private List<Query> loadQueriesFromFile(String filePath) throws IOException {
        List<Query> queries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    queries.add(Query.fromSerializedString(line));
                }
            }
        }
        LOG.info("Loaded {} queries from file: {}", queries.size(), filePath);
        return queries;
    }

    private void timeDuckDBQueries() throws IOException {
        requireScenario("timeDuckDBQueries");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);

        if (addHeader) {
            csvWriter.writeHeaders("csv", "version", "i", "rowCount", "Time (sec)", "Query", "errorBound",
                    "Query Result", "Query Result Sum");
        }

        try {
            // Build initial query from scenario config
            Rectangle rect = scenarioConfig.getQ0().toRectangle();
            Map<Integer, String> categoricalFilters = scenarioConfig.getQ0().getFilters();
            Query q0 = new Query(rect, categoricalFilters, groupBy != null ? Arrays.asList(groupBy) : new ArrayList<>(),
                    schema.getMeasureCols());
            List<Query> sequence = generateQuerySequence(q0, schema);

            // Determine execution mode
            DuckDBQueryExecutor.ExecutionMode mode = parseExecutionMode(duckDbMode);

            // Format column indices as zero-padded strings (e.g., "05" instead of "5")
            String xColStr = String.format("%02d", schema.getxColumn());
            String yColStr = String.format("%02d", schema.getyColumn());
            // Create DuckDB executor with the appropriate mode and validation filters
            DuckDBQueryExecutor executor = new DuckDBQueryExecutor(schema.getCsv(), mode, "column" + xColStr,
                    "column" + yColStr, schema.getValidationFilters(), schema.getNullstr());

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

                    csvWriter.addValue(schema.getCsv());
                    csvWriter.addValue(duckDbMode);
                    csvWriter.addValue(i);
                    csvWriter.addValue(result.getRowCount());
                    csvWriter.addValue(i == 0 ? result.getExecutionTimeSeconds() + tableCreationTimeMs / 1000.0
                            : result.getExecutionTimeSeconds());
                    csvWriter.addValue(result.getQuery());
                    csvWriter.addValue(0);
                    csvWriter.addValue(result.getMeasureStats());
                    csvWriter.addValue(result.getMeasureStats() != null ? result.getMeasureStats().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().sum())).toString()
                            : null);
                    csvWriter.writeValuesToRow();
                } catch (Exception e) {
                    LOG.warn("Error executing query {}: {}", i, e.getMessage());
                    csvWriter.addValue(schema.getCsv());
                    csvWriter.addValue(duckDbMode);
                    csvWriter.addValue(i);
                    csvWriter.addValue(-1);
                    csvWriter.addValue(-1);
                    csvWriter.addValue(-1);
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
        switch (mode) {
            case "directCSV":
                return DuckDBQueryExecutor.ExecutionMode.DIRECT_CSV;
            case "table":
                return DuckDBQueryExecutor.ExecutionMode.TABLE;
            case "spatialIndex":
                return DuckDBQueryExecutor.ExecutionMode.SPATIAL_INDEX;
            default:
                throw new IllegalArgumentException(
                        "Invalid duckDbMode: " + mode + ". Valid modes are: directCSV, table, spatialIndex");
        }
    }

    /**
     * Generates query sequence and saves it to a file.
     * Each query is serialized as a string representation.
     * This ensures reproducibility when running from the saved sequence.
     */
    private void generateAndSaveQuerySequence() throws IOException {
        requireScenario("generateAndSaveQuerySequence");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        // Build initial query from scenario config
        Rectangle rect = scenarioConfig.getQ0().toRectangle();
        Map<Integer, String> categoricalFilters = scenarioConfig.getQ0().getFilters();
        Query q0 = new Query(rect, categoricalFilters, groupBy != null ? Arrays.asList(groupBy) : null,
                schema.getMeasureCols());
        List<Query> sequence = generateQuerySequence(q0, schema);

        // Save queries to file
        try (FileWriter writer = new FileWriter(outFile)) {
            for (int i = 0; i < sequence.size(); i++) {
                Query query = sequence.get(i);
                writer.write(query.toSerializedString());
                writer.write("\n");
            }
        }

        LOG.info("Generated and saved {} queries to {}", sequence.size(), outFile);
    }

}
