package gr.athenarc.imsi.visualfacts.experiments.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.query.Query;

/**
 * Generates a sequence of independent random range queries with a single
 * fixed extent.
 *
 * <p>Two center-sampling strategies are supported:
 * <ul>
 *   <li>If a {@link SpatialReservoir} is supplied, query centres are picked
 *       from the reservoir (so the workload follows the data distribution).
 *       A seeded partial Fisher-Yates shuffle is used so the same seed
 *       always produces the same sequence; if {@code count} exceeds the
 *       reservoir size, indices are reused cyclically after one full pass.</li>
 *   <li>If the reservoir is {@code null}, centres are sampled uniformly from
 *       the data bounding box (closed-form path; appropriate for uniform
 *       synthetic data).</li>
 * </ul>
 *
 * <p>Boundary policy: rectangles are <em>shifted inward</em> when they would
 * overflow the schema bounds. Area is preserved, so the mean per-query
 * selectivity calibrated by {@link ExtentCalibrator} is preserved at runtime.
 *
 * <p>Determinism: same {@code (seed, sx, sy, bounds, reservoir, count, schema)}
 * always yields the same query list. The seed is fixed across runs so all
 * competitors see identical streams.
 */
public class UniformRandomQueryGenerator {

    private final long seed;
    private final double sx;
    private final double sy;
    private final Rectangle bounds;
    private final SpatialReservoir reservoir;

    /**
     * @param seed       PRNG seed (fixed across runs and systems).
     * @param sx         fixed x-extent of every query rectangle.
     * @param sy         fixed y-extent of every query rectangle.
     * @param bounds     schema bounds; rectangles are shifted inward to fit.
     * @param reservoir  optional data sample whose points are reused as
     *                   query centres; pass {@code null} for uniform-domain
     *                   centres.
     */
    public UniformRandomQueryGenerator(long seed, double sx, double sy,
                                       Rectangle bounds, SpatialReservoir reservoir) {
        Preconditions.checkArgument(sx > 0, "sx must be > 0, got %s", sx);
        Preconditions.checkArgument(sy > 0, "sy must be > 0, got %s", sy);
        Preconditions.checkNotNull(bounds, "bounds is required");
        double wx = bounds.getXRange().upperEndpoint() - bounds.getXRange().lowerEndpoint();
        double wy = bounds.getYRange().upperEndpoint() - bounds.getYRange().lowerEndpoint();
        Preconditions.checkArgument(sx <= wx, "sx %s exceeds x span %s", sx, wx);
        Preconditions.checkArgument(sy <= wy, "sy %s exceeds y span %s", sy, wy);
        this.seed = seed;
        this.sx = sx;
        this.sy = sy;
        this.bounds = bounds;
        this.reservoir = reservoir;
    }

    public List<Query> generateQuerySequence(int count, Schema schema) {
        Preconditions.checkArgument(count >= 1, "count must be >= 1");
        Random rng = new Random(seed);
        List<Integer> measureCols = schema.getMeasureCols();

        double xLo = bounds.getXRange().lowerEndpoint();
        double xHi = bounds.getXRange().upperEndpoint();
        double yLo = bounds.getYRange().lowerEndpoint();
        double yHi = bounds.getYRange().upperEndpoint();
        double halfX = sx / 2.0;
        double halfY = sy / 2.0;

        // Build a deterministic index order from the reservoir, if present.
        // Partial Fisher-Yates over [0..n) gives the first min(count,n) picks
        // without replacement; further picks reuse the order cyclically.
        int[] order = null;
        if (reservoir != null) {
            int n = reservoir.size();
            Preconditions.checkArgument(n > 0, "reservoir is empty");
            int len = Math.max(count, n);
            order = new int[len];
            for (int i = 0; i < len; i++) order[i] = i % n;
            int picks = Math.min(count, n);
            for (int i = 0; i < picks; i++) {
                int j = i + rng.nextInt(n - i);
                int tmp = order[i];
                order[i] = order[j];
                order[j] = tmp;
            }
        }

        double[] rxs = reservoir != null ? reservoir.xs() : new double[0];
        double[] rys = reservoir != null ? reservoir.ys() : new double[0];

        List<Query> queries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            double cx, cy;
            if (order != null) {
                int idx = order[i];
                cx = rxs[idx];
                cy = rys[idx];
            } else {
                cx = xLo + rng.nextDouble() * (xHi - xLo);
                cy = yLo + rng.nextDouble() * (yHi - yLo);
            }
            // Shift inward so the fixed-area rectangle stays in bounds.
            if (cx - halfX < xLo) cx = xLo + halfX;
            else if (cx + halfX > xHi) cx = xHi - halfX;
            if (cy - halfY < yLo) cy = yLo + halfY;
            else if (cy + halfY > yHi) cy = yHi - halfY;

            Rectangle rect = new Rectangle(
                Range.open(cx - halfX, cx + halfX),
                Range.open(cy - halfY, cy + halfY));
            queries.add(new Query(rect, measureCols, UserOpType.R));
        }
        return queries;
    }

    public List<Query> generate(int count, Schema schema) {
        return generateQuerySequence(count, schema);
    }

    public long getSeed() { return seed; }
    public double getSx() { return sx; }
    public double getSy() { return sy; }
}
