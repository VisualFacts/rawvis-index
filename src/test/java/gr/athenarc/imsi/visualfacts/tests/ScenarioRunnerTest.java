package gr.athenarc.imsi.visualfacts.tests;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Range;
import com.google.common.math.Stats;

import gr.athenarc.imsi.visualfacts.ApproximateValinor;
import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.Veti;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor.StatsDuckDB;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.tests.groundtruth.GroundTruthCalculator;
import gr.athenarc.imsi.visualfacts.tests.util.TestScenarioGenerator;

public class ScenarioRunnerTest {
    private static int csvRowCount;
    private static final Logger LOG = LogManager.getLogger(ScenarioRunnerTest.class);

    private static Path csvPath;
    private static Schema schema;
    private static List<Query> queries;
    private static List<Map<Integer, StatsDuckDB>> expectedResultsList;

    @BeforeAll
    static void prepareScenarioAndGroundTruth() throws URISyntaxException {
        String csvPathStr = System.getProperty("csv.path");
        if (csvPathStr == null) {
            csvPath = Paths.get(ScenarioRunnerTest.class.getClassLoader()
                    .getResource("data/data_10_cols_1K.csv").toURI());
            LOG.info("Using default test CSV: data/data_10_cols_1K.csv");
        } else {
            csvPath = Paths.get(csvPathStr);
            LOG.info("Using test CSV: {}", csvPathStr);
        }

        // Count rows in CSV file (excluding header if present)
        csvRowCount = 0;
        try (java.io.BufferedReader reader = java.nio.file.Files.newBufferedReader(csvPath)) {
            while (reader.readLine() != null) {
                csvRowCount++;
            }
        } catch (Exception e) {
            LOG.warn("Could not count rows in test CSV: {}", e.toString());
        }

        schema = new Schema(csvPath.toString(), ',',
                0, 1, Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9),
                new Rectangle(Range.closed(0f, 1000f), Range.closed(0f, 1000f)),
                csvRowCount,
                Collections.emptyList());
        schema.setHasHeader(false);

        int count = Integer.getInteger("scenario.count", 100);
        queries = TestScenarioGenerator.generate(schema, count);

        // compute ground truth once for all engines
        expectedResultsList = GroundTruthCalculator.computeAll(schema, queries);
    }

    // @Test
    void exactScenarioMatchesGroundTruth() throws Exception {
        Veti index = new Veti(schema, null, "valinor", null);
        for (int i = 0; i < queries.size(); i++) {
            QueryResults actual = index.executeQuery(queries.get(i));
            Map<Integer, StatsDuckDB> expected = expectedResultsList.get(i);
            if (i > 0) {
                for (Integer measure : expected.keySet()) {
                    StatsDuckDB expStats = expected.get(measure);
                    Stats actStats = actual.getRectStats().get(measure);
                    LOG.debug("Query {} Measure {}: expected {}, actual {}",
                            i, measure, expStats, actStats);
                    if (expStats.count() == 0) {
                        if (actStats != null) {
                            assertEquals(0, actStats.count(), "Actual stats should have count 0 for measure " + measure
                                    + " with expected count 0 in query " + i);
                        }
                    } else {
                        assertNotNull(actStats, "Missing actual stats for measure " + measure);
                        assertEquals(expStats.count(), actStats.count(),
                                "Count mismatch for measure " + measure + " in query " + i);
                        assertEquals(expStats.mean(), actStats.mean(), 1e-6,
                                "Mean mismatch for measure " + measure + " in query " + i);
                        assertEquals(expStats.min(), actStats.min(), 1e-6,
                                "Min mismatch for measure " + measure + " in query " + i);
                        assertEquals(expStats.max(), actStats.max(), 1e-6,
                                "Max mismatch for measure " + measure + " in query " + i);
                    }
                }
            }
        }
    }

    @Test
    void approximateScenarioCoverageWithinCI() throws Exception {
        int minRows = 1000000;
        if (csvRowCount < minRows) {
            assumeTrue(false, "Skipping approximate scenario test: dataset too small (rows: " + csvRowCount + ")");
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
