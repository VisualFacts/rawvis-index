package gr.athenarc.imsi.visualfacts.tests;

import static gr.athenarc.imsi.visualfacts.tests.assertions.ResultsComparator.assertClose;
import static org.junit.jupiter.api.Assertions.*;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Range;
import com.google.common.math.Stats;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.query.ApproximateQueryResults;
import gr.athenarc.imsi.visualfacts.ApproximateValinor;
import gr.athenarc.imsi.visualfacts.Veti;
import gr.athenarc.imsi.visualfacts.config.IndexConfig;
import gr.athenarc.imsi.visualfacts.tests.groundtruth.GroundTruthCalculator;
import gr.athenarc.imsi.visualfacts.tests.util.TestScenarioGenerator;

public class ScenarioRunnerTest {

    private static final Logger LOG = LogManager.getLogger(ScenarioRunnerTest.class);

    private static Path csvPath;
    private static Schema schema;
    private static List<Query> queries;
    private static List<QueryResults> expectedResultsList;

    @BeforeAll
    static void prepareScenarioAndGroundTruth() throws URISyntaxException {
        IndexConfig.GRID_SIZE = Integer.getInteger("grid.size", 50); // e.g. smaller grid for tests with smaller datasets, important for approximate queries

        csvPath = Paths.get(ScenarioRunnerTest.class.getClassLoader()
                .getResource("data/data_10_cols_1M.csv").toURI());
        schema = new Schema(csvPath.toString(), ',',
                0, 1, Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9),
                new Rectangle(Range.closed(0f, 1000f), Range.closed(0f, 1000f)),
                1000000,
                Collections.emptyList());
        schema.setHasHeader(false);

        int count = Integer.getInteger("scenario.count", 100);
        queries = TestScenarioGenerator.generate(schema, count);

        // compute ground truth once for all engines
        expectedResultsList = GroundTruthCalculator.computeAll(schema, queries);
    }

    @Test
    void exactScenarioMatchesGroundTruth() throws Exception {
        Veti engine = new Veti(schema, null, "valinor", null);
        for (int i = 0; i < queries.size(); i++) {
            QueryResults actual = engine.executeQuery(queries.get(i));
            if (i == 0) continue; // warm-up
            QueryResults expected = expectedResultsList.get(i);
            double relTol = 1e-6; // small FP tolerance
            assertClose(expected, actual, relTol);
        }
    }

    @Test
    void approximateScenarioCoverageWithinCI() throws Exception {
        ApproximateValinor engine = new ApproximateValinor(schema, 0.05);
        int total = 0;
        int inside = 0;
        double requiredCoverage = Double.parseDouble(System.getProperty("ci.coverage", "0.90")); 
        Map<Integer, int[]> perMeasure = new HashMap<>(); // measure -> [inside, total]
        boolean verbose = Boolean.parseBoolean(System.getProperty("ci.verbose", "true"));
        // With a fixed 0.95 confidence level, coverage should be around 0.95 in expectation.
        // The test default threshold is 0.90 for robustness (small-sample effects, normal approx., FP), but you can set -Dci.coverage=0.95 to be stricter.
        // 95% CI → expect ~0.95, allow 0.90
        for (int i = 0; i < queries.size(); i++) {
            QueryResults actual = engine.executeQuery(queries.get(i));
            if (i == 0) continue;
            ApproximateQueryResults aqr = (ApproximateQueryResults) actual;
            QueryResults expected = expectedResultsList.get(i);
            Map<Integer, Stats> expRect = expected.getRectStats();

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
                double expectedSum = expRect.get(measure).sum();
                boolean insideInterval = expectedSum >= lo - eps && expectedSum <= hi + eps;
                if (insideInterval) inside++;
                total++;
                int[] c = perMeasure.computeIfAbsent(measure, k -> new int[2]);
                if (insideInterval) c[0]++;
                c[1]++;
                if (verbose) {
                    LOG.debug("q={} measure={} expected={} interval=[{},{}] inside={} eps={}",
                            i, measure, expectedSum, lo, hi, insideInterval, eps);
                }
            }
        }
        double coverage = total == 0 ? 1.0 : (inside / (double) total);
        LOG.info("Approximate CI coverage overall={} (inside={} total={})", coverage, inside, total);
        assertTrue(coverage >= requiredCoverage,
                String.format("CI coverage %.3f below required %.3f (inside=%d total=%d)", coverage, requiredCoverage, inside, total));

        Map<Integer, CoverageStats> perMeasureCoverage = new TreeMap<>();
        for (Map.Entry<Integer, int[]> e : perMeasure.entrySet()) {
            int ok = e.getValue()[0];
            int tot = e.getValue()[1];
            double cov = tot == 0 ? 1.0 : (ok / (double) tot);
            perMeasureCoverage.put(e.getKey(), new CoverageStats(cov, ok, tot));
        }

        perMeasureCoverage.forEach((measure, stats) ->
                LOG.info("  Measure {} coverage={} (inside={} total={})", measure, stats.coverage, stats.inside, stats.total));

        perMeasureCoverage.forEach((measure, stats) ->
                assertTrue(stats.coverage >= requiredCoverage,
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
