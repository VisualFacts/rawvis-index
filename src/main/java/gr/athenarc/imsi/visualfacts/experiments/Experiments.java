package gr.athenarc.imsi.visualfacts.experiments;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import gr.athenarc.imsi.visualfacts.DataValidationFilter;
import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.Valinor;
import gr.athenarc.imsi.visualfacts.config.IndexConfig;
import gr.athenarc.imsi.visualfacts.experiments.config.ExperimentConfig;
import gr.athenarc.imsi.visualfacts.experiments.config.ExperimentConfigLoader;
import gr.athenarc.imsi.visualfacts.experiments.config.ExplorationScenarioConfig;
import gr.athenarc.imsi.visualfacts.experiments.config.InitialQueryConfig;
import gr.athenarc.imsi.visualfacts.experiments.config.WorkloadConfig;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor.QueryResult;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBSQLQueryGenerator;
import gr.athenarc.imsi.visualfacts.experiments.util.PhasedQuerySequenceGenerator;
import gr.athenarc.imsi.visualfacts.experiments.util.QuerySequenceGenerator;
import gr.athenarc.imsi.visualfacts.experiments.util.SQLQueryGenerator;
import gr.athenarc.imsi.visualfacts.experiments.util.SpatialReservoir;
import gr.athenarc.imsi.visualfacts.experiments.util.ExtentCalibrator;
import gr.athenarc.imsi.visualfacts.experiments.util.SyntheticDatasetGenerator;
import gr.athenarc.imsi.visualfacts.experiments.util.UniformRandomQueryGenerator;
import gr.athenarc.imsi.visualfacts.experiments.util.ClusteredQueryGenerator;
import gr.athenarc.imsi.visualfacts.query.AggregateType;
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

    @Parameter(names = "-initMode", description = "Grid initialization mode: omit or null for uniform grid, 'queryBiased' for denser sub-tiles near q0")
    private String initMode;

    @Parameter(names = "-duckDbMode", description = "DuckDB execution mode: directCSV, table, spatialIndex")
    private String duckDbMode;

    @Parameter(names = "-resolution", description = "Number of partitions per axis for the initial uniform grid (G×G cells). Default: 100")
    private Integer resolution;

    @Parameter(names = "-subtileRatio", description = "Fraction of G² cells that get query-biased sub-tiling (0.0–1.0). Default: 0.2")
    private Double subtileRatio;

    @Parameter(names = "-run", description = "Run number for experiments")
    private Integer run;

    @Parameter(names = "-sort", description = "Sort mode")
    private String sort;

    @Parameter(names = "-numMeasures", description = "Number of measure columns to use (uses first N from config). If not specified, uses all.")
    private Integer numMeasures;

    @Parameter(names = "--measureMem", description = "Measure index memory after every query in the sequence")
    private boolean measureMem = false;

    @Parameter(names = "--measureMaxDepth", description = "Measure index max depth after every query in the sequence")
    private boolean measureMaxDepth = false;

    @Parameter(names = "--samplingOnly", description = "VALINOR-S baseline: disable aggregate metadata reuse, use plain sampling")
    private boolean samplingOnly = false;

    @Parameter(names = "-maxQueries", description = "Truncate the generated query sequence to the first N queries. " +
            "If unset or <= 0, runs the full sequence as configured in the scenario YAML. " +
            "Use to run baselines (e.g. DuckDB, PilotDB) on a shorter prefix while letting Valinor consume the full workload; " +
            "the truncated sequence is the deterministic prefix of the full one, so first-N queries are byte-identical across systems.")
    private Integer maxQueries;

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

        // Apply index parameter overrides
        if (resolution != null && resolution > 0) {
            IndexConfig.RESOLUTION = resolution;
            LOG.info("Overriding RESOLUTION to {}", resolution);
        }
        if (subtileRatio != null) {
            IndexConfig.SUBTILE_RATIO = subtileRatio;
            LOG.info("Overriding SUBTILE_RATIO to {}", subtileRatio);
        }
    }

    /**
     * Validates that a scenario has been loaded.
     * Call this at the start of any command that requires schema/scenario
     * configuration.
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
            case "printQuerySequenceCount":
                printQuerySequenceCount();
                break;
            case "generatePilotDBSqlFile":
                generatePilotDBSqlFile();
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
        Valinor valinor = null;

        try {

            csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
            if (addHeader) {
                if (measureMem) {
                    csvWriter.writeHeaders("csv", "errorBound", "initMode", "i", "op", "bbox",
                            "Leaf tiles",
                            "Overlapped tiles",
                            "Fully Contained Tiles With Stats", "Fully Contained Tiles Without Stats",
                            "I/Os", "Time (sec)", "Total Count", "Query Result",
                            "run", "Init Timing",
                            "Index Mem Deep Size (bytes)");
                } else {
                    csvWriter.writeHeaders("csv", "errorBound", "initMode", "i", "op", "bbox",
                            "Leaf tiles",
                            "Overlapped tiles",
                            "Fully Contained Tiles With Stats", "Fully Contained Tiles Without Stats",
                            "I/Os", "Time (sec)", "Total Count", "Query Result",
                            "run", "Init Timing");
                }
            }

            Stopwatch stopwatch;

            valinor = new Valinor(schema, 0, false, initMode);

            List<Query> sequence = generateQuerySequence(schema);

            int totalQueries = sequence.size();
            for (int i = 0; i < totalQueries; i++) {
                Query query = sequence.get(i);
                LOG.trace("Executing query {}: {}", i, query);

                stopwatch = Stopwatch.createStarted();
                QueryResults queryResults = valinor.executeQuery(query);
                stopwatch.stop();

                // Memory measurement — outside query time
                long indexMemBytes = -1;
                if (measureMem) {
                    indexMemBytes = valinor.measureDeepSizeBytes();
                }

                csvWriter.addValue(schema.getCsv());
                csvWriter.addValue(0);
                csvWriter.addValue(initMode);
                csvWriter.addValue(i);
                csvWriter.addValue(query.getUserOpType());
                csvWriter.addValue(formatBbox(query.getRect()));
                csvWriter.addValue(valinor.getLeafTileCount());
                csvWriter.addValue(queryResults.getTileCount());
                csvWriter.addValue(queryResults.getFullyContainedTileCount());
                csvWriter.addValue(queryResults.getFullyContainedTileWithoutStatsCount());
                csvWriter.addValue(queryResults.getIoCount());
                csvWriter.addValue(stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9));
                csvWriter.addValue(queryResults.getTotalCount());
                csvWriter.addValue(queryResults.getStats());
                csvWriter.addValue(run);
                // Init timing breakdown (only for query 0)
                csvWriter.addValue(i == 0 && valinor.getInitTimingBreakdown() != null
                        ? valinor.getInitTimingBreakdown().toString() : "");
                if (measureMem) {
                    csvWriter.addValue(indexMemBytes);
                }
                csvWriter.writeValuesToRow();
                csvWriter.flush();
                LOG.trace("Finished query {} in {} sec", i, stopwatch.elapsed(TimeUnit.NANOSECONDS) / 1_000_000_000.0);
                if ((i + 1) % 50 == 0 || i + 1 == totalQueries) {
                    LOG.info("Progress: {}/{} queries completed", i + 1, totalQueries);
                }
            }
        } finally {
            if (valinor != null) {
                try {
                    valinor.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close Valinor", e);
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
        Valinor index = null;
        try {
            CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
            csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
            if (measureMem) {
                csvWriter.writeHeaders("csv", "errorBound", "initMode", "i", "op", "bbox",
                        "Leaf tiles",
                        "Overlapped tiles", "Fully Contained Tiles With Stats", "Fully Contained Tiles Without Stats",
                        "Sampling Tiles", "Sampling Rate", "Sampling Rounds", "I/Os", "Time (sec)",
                        "Total Count", "Query Result",
                        "Error Bound", "run", "Init Timing",
                        "Index Mem Deep Size (bytes)");
            } else {
                csvWriter.writeHeaders("csv", "errorBound", "initMode", "i", "op", "bbox",
                        "Leaf tiles",
                        "Overlapped tiles", "Fully Contained Tiles With Stats", "Fully Contained Tiles Without Stats",
                        "Sampling Tiles", "Sampling Rate", "Sampling Rounds", "I/Os", "Time (sec)",
                        "Total Count", "Query Result",
                        "Error Bound", "run", "Init Timing");
            }

            Stopwatch stopwatch;

            index = new Valinor(schema, errorBound, samplingOnly, initMode);

            List<Query> sequence = generateQuerySequence(schema);

            int totalQueries = sequence.size();
            for (int i = 0; i < totalQueries; i++) {
                Query query = sequence.get(i);
                LOG.trace("Executing query {}: {}", i, query);

                stopwatch = Stopwatch.createStarted();
                ApproximateQueryResults queryResults = (ApproximateQueryResults) index.executeQuery(query);
                stopwatch.stop();

                // Memory measurement — outside query time
                long indexMemBytes = -1;
                if (measureMem) {
                    indexMemBytes = index.measureDeepSizeBytes();
                }

                csvWriter.addValue(schema.getCsv());
                csvWriter.addValue(errorBound);
                csvWriter.addValue(initMode);
                csvWriter.addValue(i);
                csvWriter.addValue(query.getUserOpType());
                csvWriter.addValue(formatBbox(query.getRect()));
                csvWriter.addValue(index.getLeafTileCount());
                csvWriter.addValue(queryResults.getTileCount());
                csvWriter.addValue(queryResults.getFullyContainedTileCount());
                csvWriter.addValue(queryResults.getFullyContainedTileWithoutStatsCount());
                csvWriter.addValue(queryResults.getSamplingTileCount());
                csvWriter.addValue(queryResults.getSamplingRate());
                csvWriter.addValue(queryResults.getSamplingRounds());
                csvWriter.addValue(queryResults.getIoCount());
                csvWriter.addValue(stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9));
                csvWriter.addValue(queryResults.getTotalCount());
                // Query Result: {measureCol={sum=[lo, hi]}, ...}
                csvWriter.addValue(formatApproxQueryResult(queryResults));
                csvWriter.addValue(queryResults.getErrorBounds());
                csvWriter.addValue(run);
                // Init timing breakdown (only for query 0)
                csvWriter.addValue(i == 0 && index.getInitTimingBreakdown() != null
                        ? index.getInitTimingBreakdown().toString() : "");
                if (measureMem) {
                    csvWriter.addValue(indexMemBytes);
                }
                csvWriter.writeValuesToRow();
                csvWriter.flush();
                LOG.trace("Finished query {} in {} sec", i, stopwatch.elapsed(TimeUnit.NANOSECONDS) / 1_000_000_000.0);
                if ((i + 1) % 50 == 0 || i + 1 == totalQueries) {
                    LOG.info("Progress: {}/{} queries completed", i + 1, totalQueries);
                }
            }
        } finally {
            if (index != null) {
                try {
                    index.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close Valinor", e);
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

    private List<Query> generateQuerySequence(Schema schema) throws IOException {
        List<Query> sequence = generateFullQuerySequence(schema);
        if (maxQueries != null && maxQueries > 0 && maxQueries < sequence.size()) {
            LOG.info("Truncating query sequence from {} to first {} queries (-maxQueries override)",
                    sequence.size(), maxQueries);
            sequence = new ArrayList<>(sequence.subList(0, maxQueries));
        }
        return sequence;
    }

    private void printQuerySequenceCount() throws IOException {
        requireScenario("printQuerySequenceCount");
        System.out.println("QUERY_SEQUENCE_COUNT=" + generateQuerySequence(schema).size());
    }

    private List<Query> generateFullQuerySequence(Schema schema) throws IOException {
        // Non-exploration workload (random) takes precedence.
        if (scenarioConfig.hasWorkload()) {
            WorkloadConfig wc = scenarioConfig.getWorkload();
            String type = wc.getType().toLowerCase();
            LOG.info("Using '{}' workload generator: {}", type, wc);
            switch (type) {
                case "random":
                    return buildRandomWorkload(wc, schema);
                case "clustered":
                    return buildClusteredWorkload(wc, schema);
                default:
                    throw new IllegalArgumentException("Unknown workload type: " + wc.getType()
                            + " (supported: random, clustered)");
            }
        }

        InitialQueryConfig q0Config = scenarioConfig.getQ0();
        Preconditions.checkArgument(q0Config != null,
            "Exploratory scenarios require q0, but scenario '%s' has none", scenario);
        Query q0 = new Query(q0Config.toRectangle(), schema.getMeasureCols());

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
        double zoomFactor = scenarioConfig.getZoomFactor();

        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(minShift, maxShift,
                zoomFactor, scenarioConfig.getDirectionWeights());
        return sequenceGenerator.generateQuerySequence(q0, seqCount, schema);
    }

    private List<Query> buildRandomWorkload(WorkloadConfig wc, Schema schema) throws IOException {
        Rectangle bounds = schema.getBounds();
        double sigma = wc.getSelectivity();
        String mode = wc.getExtentMode() == null
                ? WorkloadConfig.EXTENT_MODE_CALIBRATED
                : wc.getExtentMode();
        SpatialReservoir reservoir = null;
        double[] extent;
        if (WorkloadConfig.EXTENT_MODE_CLOSED_FORM.equalsIgnoreCase(mode)) {
            extent = ExtentCalibrator.closedForm(sigma, bounds);
            LOG.info("Random workload (closedForm): σ={} → sx={}, sy={} (bounds={})",
                    sigma, extent[0], extent[1], bounds);
        } else if (WorkloadConfig.EXTENT_MODE_CALIBRATED.equalsIgnoreCase(mode)) {
            int reservoirSize = wc.getReservoirSize() != null ? wc.getReservoirSize() : 100_000;
            Path cacheDir = Paths.get("experiments", "query_sequences", "reservoirs");
            reservoir = SpatialReservoir.loadOrBuild(
                    schema, scenarioConfig.getDataset(), cacheDir, reservoirSize, wc.getSeed());
            extent = ExtentCalibrator.calibrate(reservoir, bounds, sigma);
            LOG.info("Random workload (calibrated): σ={} → sx={}, sy={} (reservoir size={})",
                    sigma, extent[0], extent[1], reservoir.size());
        } else {
            throw new IllegalArgumentException("Unknown extentMode: " + mode
                    + " (supported: calibrated, closedForm)");
        }
        if (reservoir != null) {
            ExtentCalibrator.SelectivityStats stats =
                    ExtentCalibrator.stats(reservoir, bounds, extent[0], extent[1]);
            LOG.info("Random workload realised per-query selectivity (over reservoir): {}", stats);
        }
        return new UniformRandomQueryGenerator(wc.getSeed(), extent[0], extent[1], bounds, reservoir)
                .generateQuerySequence(wc.getSeqCount(), schema);
    }

    /**
     * Clustered (Gaussian Mixture Foci) workload. Centres are sampled from
     * {@code N(focus, σ_f · I)} per query. Extent (sx, sy) is calibrated
     * exactly as for {@link #buildRandomWorkload}, so the only difference
     * from the uniform-random workload is the centre distribution.
     */
    private List<Query> buildClusteredWorkload(WorkloadConfig wc, Schema schema) throws IOException {
        Rectangle bounds = schema.getBounds();
        double sigma = wc.getSelectivity();
        String mode = wc.getExtentMode() == null
                ? WorkloadConfig.EXTENT_MODE_CALIBRATED
                : wc.getExtentMode();
        SpatialReservoir reservoir = null;
        double[] extent;
        if (WorkloadConfig.EXTENT_MODE_CLOSED_FORM.equalsIgnoreCase(mode)) {
            extent = ExtentCalibrator.closedForm(sigma, bounds);
            LOG.info("Clustered workload (closedForm): σ={} → sx={}, sy={} (bounds={})",
                    sigma, extent[0], extent[1], bounds);
        } else if (WorkloadConfig.EXTENT_MODE_CALIBRATED.equalsIgnoreCase(mode)) {
            int reservoirSize = wc.getReservoirSize() != null ? wc.getReservoirSize() : 100_000;
            Path cacheDir = Paths.get("experiments", "query_sequences", "reservoirs");
            reservoir = SpatialReservoir.loadOrBuild(
                    schema, scenarioConfig.getDataset(), cacheDir, reservoirSize, wc.getSeed());
            extent = ExtentCalibrator.calibrate(reservoir, bounds, sigma);
            LOG.info("Clustered workload (calibrated): σ={} → sx={}, sy={} (reservoir size={})",
                    sigma, extent[0], extent[1], reservoir.size());
        } else {
            throw new IllegalArgumentException("Unknown extentMode: " + mode
                    + " (supported: calibrated, closedForm)");
        }

        double[][] foci = wc.parseFocusCenters();
        LOG.info("Clustered workload: numFoci={}, focusSpreadFraction={}, foci={}",
                wc.getNumFoci(), wc.getFocusSpreadFraction(),
                foci == null ? "<sampled from bounds with seed>" : java.util.Arrays.deepToString(foci));

        return new ClusteredQueryGenerator(wc.getSeed(), extent[0], extent[1], bounds,
                wc.getNumFoci(), wc.getFocusSpreadFraction(), foci)
                .generateQuerySequence(wc.getSeqCount(), schema);
    }

    private static String formatBbox(Rectangle rect) {
        if (rect == null) return "";
        return String.format("%s,%s,%s,%s",
                rect.getXRange().lowerEndpoint(), rect.getXRange().upperEndpoint(),
                rect.getYRange().lowerEndpoint(), rect.getYRange().upperEndpoint());
    }

    private String formatApproxQueryResult(ApproximateQueryResults queryResults) {
        StringBuilder sb = new StringBuilder("{");
        Map<Integer, double[]> cis = queryResults.getSumConfidenceIntervals();
        Map<Integer, double[]> countCIs = queryResults.getCountConfidenceIntervals();
        Map<Integer, double[]> meanCIs = queryResults.getMeanConfidenceIntervals();
        boolean first = true;
        if (cis != null) {
            for (Map.Entry<Integer, double[]> entry : cis.entrySet()) {
                if (!first) sb.append(", ");
                first = false;
                int col = entry.getKey();
                double[] ci = entry.getValue();
                sb.append(col).append("={sum=[").append(ci[0]).append(", ").append(ci[1]).append("]");
                if (countCIs != null && countCIs.containsKey(col)) {
                    double[] countCI = countCIs.get(col);
                    sb.append(", count=[").append(countCI[0]).append(", ").append(countCI[1]).append("]");
                }
                if (meanCIs != null && meanCIs.containsKey(col)) {
                    double[] meanCI = meanCIs.get(col);
                    sb.append(", mean=[").append(meanCI[0]).append(", ").append(meanCI[1]).append("]");
                }
                sb.append("}");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private void timeDuckDBQueries() throws IOException {
        requireScenario("timeDuckDBQueries");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);

        if (addHeader) {
            csvWriter.writeHeaders("csv", "version", "i", "rowCount", "Time (sec)", "Query", "errorBound",
                    "Query Result", "Init Timing");
        }

        try {
            List<Query> sequence = generateQuerySequence(schema);

            // Determine execution mode
            DuckDBQueryExecutor.ExecutionMode mode = parseExecutionMode(duckDbMode);

            // Format column indices as zero-padded strings (e.g., "05" instead of "5")
            String xColStr = String.format("%02d", schema.getxColumn());
            String yColStr = String.format("%02d", schema.getyColumn());
            // Create DuckDB executor with the appropriate mode and validation filters
            DuckDBQueryExecutor executor;
            if (mode == DuckDBQueryExecutor.ExecutionMode.TABLE_PROJECTED) {
                // Collect all column indices needed: x, y, measures, validation filter columns
                TreeSet<Integer> neededCols = new TreeSet<>();
                neededCols.add(schema.getxColumn());
                neededCols.add(schema.getyColumn());
                neededCols.addAll(schema.getMeasureCols());
                if (schema.getValidationFilters() != null) {
                    for (DataValidationFilter f : schema.getValidationFilters()) {
                        neededCols.add(f.getFilterColumn());
                    }
                }
                LOG.info("TABLE_PROJECTED: projecting {} columns: {}", neededCols.size(), neededCols);
                executor = new DuckDBQueryExecutor(schema.getCsv(), mode, "column" + xColStr,
                        "column" + yColStr, schema.getValidationFilters(), schema.getNullstr(),
                        schema.getHasHeader(), new ArrayList<>(neededCols));
            } else {
                executor = new DuckDBQueryExecutor(schema.getCsv(), mode, "column" + xColStr,
                        "column" + yColStr, schema.getValidationFilters(), schema.getNullstr(),
                        schema.getHasHeader(), null);
            }

            // Log initialization timing metrics
            long tableCreationTimeMs = executor.getTableCreationTimeNanos() / 1_000_000;
            long indexCreationTimeMs = executor.getIndexCreationTimeNanos() / 1_000_000;
            LOG.info("DuckDB initialization timings - Mode: {}, Table creation: {}ms, Index creation: {}ms",
                    duckDbMode, tableCreationTimeMs, indexCreationTimeMs);

            LOG.info("Executing DuckDB queries in {} mode", duckDbMode);

            int totalQueries = sequence.size();
            for (int i = 0; i < totalQueries; i++) {
                Query query = sequence.get(i);
                LOG.trace("Executing query {}", i);
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
                    // Init timing breakdown (only for query 0)
                    if (i == 0) {
                        double q0Sec = result.getExecutionTimeSeconds();
                        csvWriter.addValue(String.format("{tableCreation=%.3f, indexCreation=%.3f, q0=%.3f, total=%.3f}",
                                tableCreationTimeMs / 1000.0, indexCreationTimeMs / 1000.0, q0Sec,
                                tableCreationTimeMs / 1000.0 + indexCreationTimeMs / 1000.0 + q0Sec));
                    } else {
                        csvWriter.addValue("");
                    }
                    csvWriter.writeValuesToRow();
                    csvWriter.flush();
                } catch (Exception e) {
                    LOG.warn("Error executing query {}: {}", i, e.getMessage());
                    csvWriter.addValue(schema.getCsv());
                    csvWriter.addValue(duckDbMode);
                    csvWriter.addValue(i);
                    csvWriter.addValue(-1);
                    csvWriter.addValue(-1);
                    csvWriter.addValue(-1);
                    csvWriter.addValue(-1);
                    csvWriter.addValue("ERROR: " + e.getMessage());
                    csvWriter.addValue("");
                    csvWriter.writeValuesToRow();
                    csvWriter.flush();
                }
                if ((i + 1) % 50 == 0 || i + 1 == totalQueries) {
                    LOG.info("Progress: {}/{} queries completed", i + 1, totalQueries);
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
            case "tableProjected":
                return DuckDBQueryExecutor.ExecutionMode.TABLE_PROJECTED;
            case "spatialIndex":
                return DuckDBQueryExecutor.ExecutionMode.SPATIAL_INDEX;
            default:
                throw new IllegalArgumentException(
                        "Invalid duckDbMode: " + mode + ". Valid modes are: directCSV, table, tableProjected, spatialIndex");
        }
    }

    /**
     * Generates a SQL file for PilotDB execution.
     * Line 1: metadata comment with measure column indices.
     * Line 2: CREATE TABLE (projected, with validation filters applied).
     * Lines 3+: SELECT queries with PilotDB-compatible aggregates (sum, avg).
     */
    private void generatePilotDBSqlFile() throws IOException {
        requireScenario("generatePilotDBSqlFile");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        List<Query> sequence = generateQuerySequence(schema);

        // Determine column naming format via DuckDB introspection
        String readCsvOptions = DuckDBSQLQueryGenerator.buildReadCsvOptions(schema.getHasHeader(), schema.getNullstr());
        int csvColumnCount;
        try {
            csvColumnCount = DuckDBQueryExecutor.getCSVColumnCountForFile(schema.getCsv(), readCsvOptions);
        } catch (Exception e) {
            throw new IOException("Failed to determine CSV column count", e);
        }
        boolean useTwoDigit = csvColumnCount > 10;

        // Format column names
        String xColStr = DuckDBSQLQueryGenerator.formatColumnName(schema.getxColumn(), useTwoDigit);
        String yColStr = DuckDBSQLQueryGenerator.formatColumnName(schema.getyColumn(), useTwoDigit);
        List<String> measureColNames = schema.getMeasureCols().stream()
                .map(col -> DuckDBSQLQueryGenerator.formatColumnName(col, useTwoDigit))
                .collect(Collectors.toList());

        // Collect needed columns for projection (x, y, measures, validation filter columns)
        TreeSet<Integer> neededCols = new TreeSet<>();
        neededCols.add(schema.getxColumn());
        neededCols.add(schema.getyColumn());
        neededCols.addAll(schema.getMeasureCols());
        if (schema.getValidationFilters() != null) {
            for (DataValidationFilter f : schema.getValidationFilters()) {
                neededCols.add(f.getFilterColumn());
            }
        }

        // Build CREATE TABLE (TABLE_PROJECTED style)
        String createSql = DuckDBSQLQueryGenerator.buildCreateProjectedTableSQL(
                "data_table", schema.getCsv(), readCsvOptions,
                schema.getValidationFilters(), useTwoDigit, new ArrayList<>(neededCols));

        // Write SQL file
        try (FileWriter writer = new FileWriter(outFile)) {
            // Metadata: measure column indices (used by Python for result formatting)
            writer.write("-- measures: " + schema.getMeasureCols().stream()
                    .map(String::valueOf).collect(Collectors.joining(",")) + "\n");
            // CREATE TABLE
            writer.write(createSql);
            writer.write("\n");
            // SELECT queries
            for (Query query : sequence) {
                List<Range<Double>> ranges = new ArrayList<>();
                Rectangle rectangle = query.getRect();
                if (rectangle != null) {
                    ranges.add(rectangle.getXRange());
                    ranges.add(rectangle.getYRange());
                }
                String selectSql = SQLQueryGenerator.getSQLUniAggQuery(
                        "data_table", ranges, measureColNames, null, AggregateType.PILOTDB, xColStr, yColStr);
                writer.write(selectSql);
                writer.write("\n");
            }
        }

        LOG.info("Generated PilotDB SQL file with {} queries to {}", sequence.size(), outFile);
    }

    private void generateAndSaveQuerySequence() throws IOException {
        requireScenario("generateAndSaveQuerySequence");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        List<Query> sequence = generateQuerySequence(schema);
        File file = new File(outFile);
        File parent = file.getParentFile();
        if (parent != null) {
            parent.mkdirs();
        }
        try (FileWriter writer = new FileWriter(file, false)) {
            for (Query query : sequence) {
                Rectangle rect = query.getRect();
                writer.write(String.format("(%s..%s),(%s..%s)|{}|[]|%s|%s%n",
                        rect.getXRange().lowerEndpoint(), rect.getXRange().upperEndpoint(),
                        rect.getYRange().lowerEndpoint(), rect.getYRange().upperEndpoint(),
                        query.getMeasureCols().stream().map(String::valueOf).collect(Collectors.joining(",")),
                        query.getUserOpType()));
            }
        }
        LOG.info("Generated query sequence with {} queries to {}", sequence.size(), outFile);
    }

}
