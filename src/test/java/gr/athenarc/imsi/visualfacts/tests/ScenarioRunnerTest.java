package gr.athenarc.imsi.visualfacts.tests;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.math.Stats;

import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.Valinor;
import gr.athenarc.imsi.visualfacts.experiments.config.ExperimentConfig;
import gr.athenarc.imsi.visualfacts.experiments.config.ExperimentConfigLoader;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor.StatsDuckDB;
import gr.athenarc.imsi.visualfacts.experiments.util.UniformRandomQueryGenerator;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.tests.groundtruth.GroundTruthCalculator;
import gr.athenarc.imsi.visualfacts.tests.util.ProgressBar;

/**
 * Test class for running and validating Valinor against ground truth using
 * independent uniform random range queries.
 *
 * Configuration via system properties:
 * - dataset.name:   Dataset to use (default: "test_synth10_10M", fallback: "test_synth10_100K")
 * - dataset.config:  Path to YAML config file (default: test classpath resource)
 * - query.count:    Number of random queries (default: 500)
 * - query.seed:     Random seed for query generation (default: 42)
 * - query.selectivity: Target area selectivity (default: 0.01 = 1%)
 * - ci.coverage:    Required CI coverage for approximate tests (default: 0.90)
 */
public class ScenarioRunnerTest {
    private static final Logger LOG = LogManager.getLogger(ScenarioRunnerTest.class);

    private static final String DEFAULT_TEST_CONFIG = "experiments/test_scenarios.yaml";
    private static final String DEFAULT_DATASET = "test_synth10_10M";
    private static final String FALLBACK_DATASET  = "test_synth10_100K";

    private static Schema schema;
    private static List<Query> queries;
    private static List<Map<Integer, StatsDuckDB>> expectedResultsList;

    @BeforeAll
    static void prepareQueriesAndGroundTruth() throws IOException {
        String configPath = System.getProperty("dataset.config");
        String datasetName = System.getProperty("dataset.name", DEFAULT_DATASET);

        ExperimentConfig experimentConfig;
        if (configPath != null && !configPath.isEmpty()) {
            LOG.info("Loading config from file: {}", configPath);
            experimentConfig = ExperimentConfigLoader.loadFromFile(configPath);
        } else {
            LOG.info("Loading config from test classpath: {}", DEFAULT_TEST_CONFIG);
            experimentConfig = ExperimentConfigLoader.loadFromClasspath(DEFAULT_TEST_CONFIG);
        }

        schema = experimentConfig.getSchemaForDataset(datasetName);

        // Fall back to the bundled classpath dataset if the requested CSV is missing
        String csvPath = schema.getCsv();
        if (!csvPath.startsWith("classpath:")) {
            java.io.File csvFile = new java.io.File(csvPath);
            if (!csvFile.exists()) {
                LOG.info("CSV not found at {}; falling back to dataset '{}'", csvPath, FALLBACK_DATASET);
                schema = experimentConfig.getSchemaForDataset(FALLBACK_DATASET);
            }
        }
        LOG.info("Using dataset csv: {}", schema.getCsv());

        int queryCount = Integer.parseInt(System.getProperty("query.count", "500"));
        long seed = Long.parseLong(System.getProperty("query.seed", "42"));
        double selectivity = Double.parseDouble(System.getProperty("query.selectivity", "0.01"));

        UniformRandomQueryGenerator generator = new UniformRandomQueryGenerator(seed, selectivity);
        queries = generator.generate(queryCount, schema);
        LOG.info("Generated {} random queries (seed={}, selectivity={})", queries.size(), seed, selectivity);

        expectedResultsList = GroundTruthCalculator.computeAll(schema, queries);
    }

    @Test
    void exactMatchesGroundTruth() throws Exception {
        LOG.info("Running exact test with {} queries", queries.size());
        Valinor index = new Valinor(schema);

        int total = queries.size();
        for (int i = 0; i < total; i++) {
            Query query = queries.get(i);
            QueryResults actual = index.executeQuery(query);
            ProgressBar.print("Exact", i + 1, total);
            // First query initializes the index (full data scan); skip validation
            if (i == 0) continue;
            Map<Integer, StatsDuckDB> expected = expectedResultsList.get(i);
            if (expected.isEmpty()) {
                continue;
            }
            for (Integer measure : expected.keySet()) {
                StatsDuckDB expStats = expected.get(measure);
                Stats actStats = actual.getStats().get(measure);
                if (expStats.count() == 0) {
                    if (actStats != null) {
                        assertEquals(0, actStats.count(),
                                String.format("Q%d [%s]: count should be 0 for measure %d",
                                        i, query.getRect(), measure));
                    }
                } else {
                    assertNotNull(actStats,
                            String.format("Q%d [%s]: missing stats for measure %d. Expected: %s",
                                    i, query.getRect(), measure, expStats));
                    assertEquals(expStats.count(), actStats.count(),
                            String.format("Q%d [%s]: count mismatch for measure %d", i, query.getRect(), measure));
                    assertEquals(expStats.mean(), actStats.mean(), 1e-6,
                            String.format("Q%d [%s]: mean mismatch for measure %d", i, query.getRect(), measure));
                    assertEquals(expStats.min(), actStats.min(), 1e-6,
                            String.format("Q%d [%s]: min mismatch for measure %d", i, query.getRect(), measure));
                    assertEquals(expStats.max(), actStats.max(), 1e-6,
                            String.format("Q%d [%s]: max mismatch for measure %d", i, query.getRect(), measure));
                }
            }
        }
        ProgressBar.finish();
    }

    @Test
    void approximateCoverageWithinCI() throws Exception {
        // Needs enough points per tile for the CLT normal approximation to hold;
        // sparse datasets produce near-empty tiles where the nominal 95% level breaks down.
        assumeTrue(schema.getObjectCount() >= 1000000,
                "Skipping approximate test: dataset too small for reliable CLT approximation (" + schema.getObjectCount() + " rows, need >= 1M)");

        Valinor index = new Valinor(schema, 0.05);
        double requiredCoverage = Double.parseDouble(System.getProperty("ci.coverage", "0.90"));

        Map<String, int[]> overallByType = new HashMap<>();
        Map<String, Map<Integer, int[]>> perMeasureByType = new HashMap<>();
        overallByType.put("sum", new int[2]);
        overallByType.put("count", new int[2]);
        overallByType.put("mean", new int[2]);
        perMeasureByType.put("sum", new HashMap<>());
        perMeasureByType.put("count", new HashMap<>());
        perMeasureByType.put("mean", new HashMap<>());

        // 95% CI → expect ~0.95 coverage. 500 independent queries × 8 measures
        // gives ~4000 checks per aggregate type. Threshold 0.90 is conservative
        // enough for small-sample / normal-approximation effects.
        int total = queries.size();
        for (int i = 0; i < total; i++) {
            QueryResults actual = index.executeQuery(queries.get(i));
            ProgressBar.print("Approximate", i + 1, total);
            // First query initializes the index; skip validation
            if (i == 0) continue;
            ApproximateQueryResults aqr = (ApproximateQueryResults) actual;

            Map<Integer, StatsDuckDB> expected = expectedResultsList.get(i);
            if (expected.isEmpty()) {
                continue;
            }

            // --- SUM CI coverage ---
            Map<Integer, double[]> sumCIs = aqr.getSumConfidenceIntervals();
            assertNotNull(sumCIs, "SUM CIs must be present for query " + i);
            for (Map.Entry<Integer, double[]> e : sumCIs.entrySet()) {
                Integer measure = e.getKey();
                double[] ci = e.getValue();
                assertNotNull(ci, "missing SUM CI for measure " + measure);
                assertEquals(2, ci.length, "SUM CI must have length 2 for measure " + measure);
                boolean inside = isInsideCI(ci, expected.get(measure).sum());
                tally(overallByType.get("sum"), inside);
                tally(perMeasureByType.get("sum").computeIfAbsent(measure, k -> new int[2]), inside);
            }

            // --- COUNT CI coverage ---
            Map<Integer, double[]> countCIs = aqr.getCountConfidenceIntervals();
            assertNotNull(countCIs, "COUNT CIs must be present for query " + i);
            for (Map.Entry<Integer, double[]> e : countCIs.entrySet()) {
                Integer measure = e.getKey();
                double[] ci = e.getValue();
                assertNotNull(ci, "missing COUNT CI for measure " + measure);
                assertEquals(2, ci.length, "COUNT CI must have length 2 for measure " + measure);
                boolean inside = isInsideCI(ci, expected.get(measure).count());
                tally(overallByType.get("count"), inside);
                tally(perMeasureByType.get("count").computeIfAbsent(measure, k -> new int[2]), inside);
            }

            // --- MEAN CI coverage ---
            Map<Integer, double[]> meanCIs = aqr.getMeanConfidenceIntervals();
            assertNotNull(meanCIs, "MEAN CIs must be present for query " + i);
            for (Map.Entry<Integer, double[]> e : meanCIs.entrySet()) {
                Integer measure = e.getKey();
                double[] ci = e.getValue();
                assertNotNull(ci, "missing MEAN CI for measure " + measure);
                assertEquals(2, ci.length, "MEAN CI must have length 2 for measure " + measure);
                // Skip NaN intervals (zero-count queries have undefined mean)
                if (Double.isNaN(ci[0]) || Double.isNaN(ci[1])) continue;
                double expectedMean = expected.get(measure).mean();
                if (Double.isNaN(expectedMean)) continue;
                boolean inside = isInsideCI(ci, expectedMean);
                tally(overallByType.get("mean"), inside);
                tally(perMeasureByType.get("mean").computeIfAbsent(measure, k -> new int[2]), inside);
            }
        }
        ProgressBar.finish();

        // Assert coverage per aggregate type, overall and per measure
        for (String type : new String[] { "sum", "count", "mean" }) {
            int[] ov = overallByType.get(type);
            double coverage = ov[1] == 0 ? 1.0 : (ov[0] / (double) ov[1]);
            LOG.info("{} CI coverage overall={} (inside={} total={})", type.toUpperCase(), coverage, ov[0], ov[1]);
            assertTrue(coverage >= requiredCoverage,
                    String.format("%s CI coverage %.3f below required %.3f (inside=%d total=%d)",
                            type.toUpperCase(), coverage, requiredCoverage, ov[0], ov[1]));

            Map<Integer, CoverageStats> perMeasure = new TreeMap<>();
            for (Map.Entry<Integer, int[]> e : perMeasureByType.get(type).entrySet()) {
                int ok = e.getValue()[0], tot = e.getValue()[1];
                perMeasure.put(e.getKey(), new CoverageStats(tot == 0 ? 1.0 : (ok / (double) tot), ok, tot));
            }
            perMeasure.forEach((m, s) -> assertTrue(s.coverage >= requiredCoverage,
                    String.format("%s CI coverage for measure %d %.3f below required %.3f (inside=%d total=%d)",
                            type.toUpperCase(), m, s.coverage, requiredCoverage, s.inside, s.total)));
        }
    }

    private static boolean isInsideCI(double[] interval, double expected) {
        double lo = interval[0], hi = interval[1];
        double absEps = 1e-3;
        double relEps = 1e-8 * Math.abs((lo + hi) / 2.0);
        double eps = Math.max(absEps, relEps);
        return expected >= lo - eps && expected <= hi + eps;
    }

    private static void tally(int[] counter, boolean inside) {
        if (inside) counter[0]++;
        counter[1]++;
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
