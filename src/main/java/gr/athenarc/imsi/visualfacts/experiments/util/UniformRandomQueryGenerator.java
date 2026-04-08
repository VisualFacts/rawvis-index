package gr.athenarc.imsi.visualfacts.experiments.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.query.Query;

/**
 * Generates independent random range queries uniformly distributed across the
 * data space, with a fixed extent such that the average query selectivity
 * matches the target.
 *
 * <p>For a 2-D space with bounds [xLo, xHi] × [yLo, yHi] and target
 * selectivity s, the query extent per dimension is:
 * <pre>
 *   xExtent = (xHi - xLo) · √s
 *   yExtent = (yHi - yLo) · √s
 * </pre>
 * giving area ratio = (xExtent · yExtent) / totalArea = s.
 *
 * <p>Corner placement is uniform in [lo, hi − extent] per dimension.
 *
 * <p>Deterministic: same (seed, selectivity, count, schema) → same sequence.
 */
public class UniformRandomQueryGenerator {

    private static final double DEFAULT_SELECTIVITY = 0.01; // 1%

    private final long seed;
    private final double selectivity;

    public UniformRandomQueryGenerator(long seed) {
        this(seed, DEFAULT_SELECTIVITY);
    }

    public UniformRandomQueryGenerator(long seed, double selectivity) {
        this.seed = seed;
        this.selectivity = selectivity;
    }

    /**
     * Generates {@code count} independent random range queries.
     */
    public List<Query> generate(int count, Schema schema) {
        Random random = new Random(seed);
        Rectangle bounds = schema.getBounds();
        List<Integer> measureCols = schema.getMeasureCols();

        double xLo = bounds.getXRange().lowerEndpoint();
        double xHi = bounds.getXRange().upperEndpoint();
        double yLo = bounds.getYRange().lowerEndpoint();
        double yHi = bounds.getYRange().upperEndpoint();

        double xExtent = (xHi - xLo) * Math.sqrt(selectivity);
        double yExtent = (yHi - yLo) * Math.sqrt(selectivity);

        List<Query> queries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            double x = xLo + random.nextDouble() * (xHi - xLo - xExtent);
            double y = yLo + random.nextDouble() * (yHi - yLo - yExtent);
            Rectangle rect = new Rectangle(
                    Range.open(x, x + xExtent),
                    Range.open(y, y + yExtent));
            queries.add(new Query(rect, measureCols));
        }
        return queries;
    }

    public long getSeed() {
        return seed;
    }

    public double getSelectivity() {
        return selectivity;
    }
}
