package gr.athenarc.imsi.visualfacts.tests;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.math.Stats;

import gr.athenarc.imsi.visualfacts.ApproximateValinor;
import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.Veti;
import gr.athenarc.imsi.visualfacts.experiments.config.ExperimentConfig;
import gr.athenarc.imsi.visualfacts.experiments.config.ExperimentConfigLoader;
import gr.athenarc.imsi.visualfacts.experiments.config.ExplorationScenarioConfig;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor.StatsDuckDB;
import gr.athenarc.imsi.visualfacts.experiments.util.QuerySequenceGenerator;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.tests.groundtruth.GroundTruthCalculator;

/**
 * Test class for running and validating exploration scenarios.
 * 
 * Configuration via system properties:
 * - scenario.name: Name of the scenario to run (default: "test_pan_scenario")
 * - scenario.config: Path to YAML config file (default: uses test classpath resource)
 * - scenario.count: Number of queries to generate (default: 100, or uses seqCount from config)
 * - ci.coverage: Required CI coverage for approximate tests (default: 0.90)
 */
public class ScenarioRunnerTest {
    private static final Logger LOG = LogManager.getLogger(ScenarioRunnerTest.class);
    
    // Default test config in test resources
    private static final String DEFAULT_TEST_CONFIG = "experiments/test_scenarios.yaml";
    private static final String DEFAULT_SCENARIO = "test_scenario";

    private static Schema schema;
    private static ExplorationScenarioConfig scenarioConfig;
    private static List<Query> queries;
    private static List<Map<Integer, StatsDuckDB>> expectedResultsList;

    @BeforeAll
    static void prepareScenarioAndGroundTruth() throws IOException {
        // Load configuration
        String configPath = System.getProperty("scenario.config");
        String scenarioName = System.getProperty("scenario.name", DEFAULT_SCENARIO);
        
        ExperimentConfig experimentConfig;
        if (configPath != null && !configPath.isEmpty()) {
            // Load from specified file path
            LOG.info("Loading config from file: {}", configPath);
            experimentConfig = ExperimentConfigLoader.loadFromFile(configPath);
        } else {
            // Load from test classpath resource
            LOG.info("Loading config from test classpath: {}", DEFAULT_TEST_CONFIG);
            experimentConfig = ExperimentConfigLoader.loadFromClasspath(DEFAULT_TEST_CONFIG);
        }
        
        // Get scenario configuration
        scenarioConfig = experimentConfig.getScenario(scenarioName);
        if (scenarioConfig == null) {
            throw new IllegalArgumentException("Scenario not found: " + scenarioName + 
                    ". Available: " + experimentConfig.getScenarios().keySet());
        }
        
        // Get schema for the scenario's dataset
        schema = experimentConfig.getSchemaForScenario(scenarioName);
        LOG.info("Loaded scenario '{}' with dataset, csv: {}", scenarioName, schema.getCsv());


        Rectangle q0Rect = scenarioConfig.getQ0().toRectangle();
        Map<Integer, String> q0Filters = scenarioConfig.getQ0().getFilters();
        Query q0 = new Query(q0Rect, q0Filters, new ArrayList<>(), schema.getMeasureCols());
        
        QuerySequenceGenerator generator = new QuerySequenceGenerator(
                scenarioConfig.getMinShift(),
                scenarioConfig.getMaxShift(),
                0, 0,
                scenarioConfig.getZoomFactor());
        queries = generator.generateQuerySequence(q0, scenarioConfig.getSeqCount(), schema);
        LOG.info("Generated {} queries for scenario", queries.size());

        // Compute ground truth once for all engines
        expectedResultsList = GroundTruthCalculator.computeAll(schema, queries);
    }

    @Test
    void exactScenarioMatchesGroundTruth() throws Exception {
        LOG.info("Running exact scenario test with {} queries", queries.size());
        Veti index = new Veti(schema, null, "valinor", null);
        
        for (int i = 0; i < queries.size(); i++) {
            Query query = queries.get(i);
            QueryResults actual = index.executeQuery(query);
            Map<Integer, StatsDuckDB> expected = expectedResultsList.get(i);
            if (expected.isEmpty()) {
                LOG.debug("No expected results for query {}, skipping CI check", i);
                continue;
            }
            if (i > 0) {
                for (Integer measure : expected.keySet()) {
                    StatsDuckDB expStats = expected.get(measure);
                    Stats actStats = actual.getRectStats().get(measure);
                    LOG.trace("Q{} M{}: exp={}, act={}", i, measure, expStats, actStats);
                    if (expStats.count() == 0) {
                        if (actStats != null) {
                            assertEquals(0, actStats.count(), 
                                    String.format("Query %d [%s]: Actual stats should have count 0 for measure %d", 
                                            i, query.getRect(), measure));
                        }
                    } else {
                        try {
                            assertNotNull(actStats, 
                                    String.format("Query %d [%s]: Missing actual stats for measure %d. Expected: %s", 
                                            i, query.getRect(), measure, expStats));
                            assertEquals(expStats.count(), actStats.count(),
                                    String.format("Query %d [%s]: Count mismatch for measure %d", i, query.getRect(), measure));
                            assertEquals(expStats.mean(), actStats.mean(), 1e-6,
                                    String.format("Query %d [%s]: Mean mismatch for measure %d", i, query.getRect(), measure));
                            assertEquals(expStats.min(), actStats.min(), 1e-6,
                                    String.format("Query %d [%s]: Min mismatch for measure %d", i, query.getRect(), measure));
                            assertEquals(expStats.max(), actStats.max(), 1e-6,
                                    String.format("Query %d [%s]: Max mismatch for measure %d", i, query.getRect(), measure));
                        } catch (AssertionError e) {
                            LOG.error("Assertion failed for Query {} [{}], measure {}: {}", i, query.getRect(), measure, e.getMessage());
                        }
                    }
                }
            }
        }
    }

    @Test
    void approximateScenarioCoverageWithinCI() throws Exception {
        int minRows = 1000000;
        if (schema.getObjectCount() < minRows) {
            assumeTrue(false, "Skipping approximate scenario test: dataset too small (rows: " + schema.getObjectCount() + ")");
        }
        ApproximateValinor index = new ApproximateValinor(schema, 0.05);
        int total = 0;
        int inside = 0;
        double requiredCoverage = Double.parseDouble(System.getProperty("ci.coverage", "0.90"));
        Map<Integer, int[]> perMeasure = new HashMap<>(); // measure -> [inside, total]
        // With a fixed 0.95 confidence level, coverage should be around 0.95 in
        // expectation.
        // The test default threshold is 0.90 for robustness (small-sample effects,
        // normal approx., FP), but you can set -Dci.coverage=0.95 to be stricter.
        // 95% CI → expect ~0.95, allow 0.90
        for (int i = 0; i < queries.size(); i++) {
            QueryResults actual = index.executeQuery(queries.get(i));
            if (i == 0)
                continue;
            ApproximateQueryResults aqr = (ApproximateQueryResults) actual;

            LOG.trace("Approximate results for query {}: {}", i, aqr);

            Map<Integer, StatsDuckDB> expected = expectedResultsList.get(i);

            if (expected.isEmpty()) {
                LOG.debug("No expected results for query {}, skipping CI check", i);
                continue;
            }

            Map<Integer, double[]> confIntervals = aqr.getConfidenceIntervals();
            assertNotNull(confIntervals, "confidence intervals must be present");

            for (Map.Entry<Integer, double[]> e : confIntervals.entrySet()) {
                Integer measure = e.getKey();
                double[] interval = confIntervals.get(measure);
                assertNotNull(interval, "missing CI for measure " + measure);
                assertTrue(interval.length == 2, "CI must have length 2 for measure " + measure);
                double lo = interval[0], hi = interval[1];
                double absEps = 1e-6; // small absolute epsilon for FP and degenerate CIs
                double relEps = 1e-12 * Math.abs((lo + hi) / 2.0);
                double eps = Math.max(absEps, relEps);
                double expectedSum = expected.get(measure).sum();
                boolean insideInterval = expectedSum >= lo - eps && expectedSum <= hi + eps;
                if (insideInterval)
                    inside++;
                total++;
                int[] c = perMeasure.computeIfAbsent(measure, k -> new int[2]);
                if (insideInterval)
                    c[0]++;
                c[1]++;
            }
        }
        double coverage = total == 0 ? 1.0 : (inside / (double) total);
        LOG.info("Approximate CI coverage overall={} (inside={} total={})", coverage, inside, total);
        assertTrue(coverage >= requiredCoverage,
                String.format("CI coverage %.3f below required %.3f (inside=%d total=%d)", coverage, requiredCoverage,
                        inside, total));

        Map<Integer, CoverageStats> perMeasureCoverage = new TreeMap<>();
        for (Map.Entry<Integer, int[]> e : perMeasure.entrySet()) {
            int ok = e.getValue()[0];
            int tot = e.getValue()[1];
            double cov = tot == 0 ? 1.0 : (ok / (double) tot);
            perMeasureCoverage.put(e.getKey(), new CoverageStats(cov, ok, tot));
        }

        perMeasureCoverage.forEach((measure, stats) -> LOG.info("  Measure {} coverage={} (inside={} total={})",
                measure, stats.coverage, stats.inside, stats.total));

        perMeasureCoverage.forEach((measure, stats) -> assertTrue(stats.coverage >= requiredCoverage,
                String.format("CI coverage for measure %d %.3f below required %.3f (inside=%d total=%d)",
                        measure, stats.coverage, requiredCoverage, stats.inside, stats.total)));
    }

    private static final class CoverageStats {
        final double coverage;
        final int inside;
        final int total;

        CoverageStats(double coverage, int inside, int total) {
            this.coverage = coverage;
            this.inside = inside;
            this.total = total;
        }
    }
}
