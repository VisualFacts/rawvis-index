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
 * Generates a sequence of independent random range queries whose centres are
 * sampled from a Gaussian-mixture spatial distribution (Gaussian Mixture Foci,
 * GMF). All queries share the same fixed extent (sx, sy) — calibration of
 * (sx, sy) for a target average selectivity is performed externally by
 * {@link ExtentCalibrator}, identically to {@link UniformRandomQueryGenerator},
 * so the only difference between the two workloads is the centre distribution.
 *
 * <p>Workload knobs:
 * <ul>
 *   <li>{@code numFoci K} — number of Gaussian foci (centres). Default 1.</li>
 *   <li>{@code focusSpreadFraction σ_f} — Gaussian standard deviation per axis,
 *       expressed as a fraction of the dataset extent (so independent of
 *       absolute units).</li>
 *   <li>{@code focusCenters} — explicit list of focus centres; if {@code null}
 *       or empty, K centres are sampled uniformly inside the bounds with the
 *       same seed.</li>
 * </ul>
 *
 * <p>Per query: pick a focus uniformly at random (equal-weight foci), then
 * sample (cx, cy) from {@code N(focus, σ_f · I)}. Centres outside bounds
 * are resampled (rejection sampling) up to a small cap, then clamped to
 * bounds. The fixed-extent rectangle is finally shifted inward to fit,
 * area-preservingly.
 *
 * <p>Determinism: same {@code (seed, numFoci, σ_f, focusCenters, sx, sy,
 * bounds, count)} always yields the same query list.
 */
public class ClusteredQueryGenerator {

    private final long seed;
    private final double sx;
    private final double sy;
    private final Rectangle bounds;
    private final int numFoci;
    private final double sigmaX;
    private final double sigmaY;
    private final double[][] focusCenters;   // may be null → sampled below

    public ClusteredQueryGenerator(long seed, double sx, double sy, Rectangle bounds,
                                   int numFoci, double focusSpreadFraction,
                                   double[][] explicitFocusCenters) {
        Preconditions.checkArgument(sx > 0, "sx must be > 0, got %s", sx);
        Preconditions.checkArgument(sy > 0, "sy must be > 0, got %s", sy);
        Preconditions.checkNotNull(bounds, "bounds is required");
        Preconditions.checkArgument(numFoci >= 1, "numFoci must be >= 1, got %s", numFoci);
        Preconditions.checkArgument(focusSpreadFraction > 0,
                "focusSpreadFraction must be > 0, got %s", focusSpreadFraction);
        double wx = bounds.getXRange().upperEndpoint() - bounds.getXRange().lowerEndpoint();
        double wy = bounds.getYRange().upperEndpoint() - bounds.getYRange().lowerEndpoint();
        Preconditions.checkArgument(sx <= wx, "sx %s exceeds x span %s", sx, wx);
        Preconditions.checkArgument(sy <= wy, "sy %s exceeds y span %s", sy, wy);

        this.seed = seed;
        this.sx = sx;
        this.sy = sy;
        this.bounds = bounds;
        this.numFoci = numFoci;
        this.sigmaX = focusSpreadFraction * wx;
        this.sigmaY = focusSpreadFraction * wy;

        // If explicit centres are supplied, use them; otherwise we'll sample
        // them lazily in generate() with the same RNG so the seed is honoured.
        if (explicitFocusCenters != null && explicitFocusCenters.length > 0) {
            Preconditions.checkArgument(explicitFocusCenters.length == numFoci,
                    "explicitFocusCenters length %s != numFoci %s",
                    explicitFocusCenters.length, numFoci);
            this.focusCenters = new double[numFoci][2];
            for (int i = 0; i < numFoci; i++) {
                double fx = explicitFocusCenters[i][0];
                double fy = explicitFocusCenters[i][1];
                Preconditions.checkArgument(
                        bounds.getXRange().contains(fx) && bounds.getYRange().contains(fy),
                        "focus centre (%s, %s) outside bounds %s", fx, fy, bounds);
                this.focusCenters[i][0] = fx;
                this.focusCenters[i][1] = fy;
            }
        } else {
            this.focusCenters = null;
        }
    }

    public List<Query> generateQuerySequence(int count, Schema schema) {
        Preconditions.checkArgument(count >= 1, "count must be >= 1");
        List<Integer> measureCols = schema.getMeasureCols();

        double xLo = bounds.getXRange().lowerEndpoint();
        double xHi = bounds.getXRange().upperEndpoint();
        double yLo = bounds.getYRange().lowerEndpoint();
        double yHi = bounds.getYRange().upperEndpoint();
        double halfX = sx / 2.0;
        double halfY = sy / 2.0;

        double[][] centers = sampleCenters(
                seed, bounds, numFoci, sigmaX, sigmaY, focusCenters, count);

        List<Query> queries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            double cx = centers[i][0];
            double cy = centers[i][1];
            // Shift inward so the fixed-area rectangle stays in bounds
            // (area-preserving; mean per-query selectivity is preserved).
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

    /**
     * Samples {@code count} raw query centres from the Gaussian-mixture-foci
     * distribution, without applying the per-query inward shift (which depends
     * on the fixed extent). Determinism: same {@code (seed, bounds, numFoci,
     * focusSpreadFraction, explicitFocusCenters, count)} always yields the
     * same prefix of centres, so calibration and query generation stay in
     * lockstep when called with matching parameters.
     *
     * <p>This is the single source of truth for clustered-centre sampling and
     * is reused by {@link ExtentCalibrator} so that workload-aware extent
     * calibration uses the exact same centre distribution as the runtime
     * workload.
     */
    public static double[][] sampleCenters(long seed, Rectangle bounds, int numFoci,
                                           double focusSpreadFraction,
                                           double[][] explicitFocusCenters, int count) {
        Preconditions.checkNotNull(bounds, "bounds is required");
        Preconditions.checkArgument(numFoci >= 1, "numFoci must be >= 1, got %s", numFoci);
        Preconditions.checkArgument(focusSpreadFraction > 0,
                "focusSpreadFraction must be > 0, got %s", focusSpreadFraction);
        double wx = bounds.getXRange().upperEndpoint() - bounds.getXRange().lowerEndpoint();
        double wy = bounds.getYRange().upperEndpoint() - bounds.getYRange().lowerEndpoint();
        return sampleCenters(seed, bounds, numFoci,
                focusSpreadFraction * wx, focusSpreadFraction * wy,
                explicitFocusCenters, count);
    }

    private static double[][] sampleCenters(long seed, Rectangle bounds, int numFoci,
                                            double sigmaX, double sigmaY,
                                            double[][] explicitFocusCenters, int count) {
        Preconditions.checkArgument(count >= 1, "count must be >= 1");
        Random rng = new Random(seed);
        double xLo = bounds.getXRange().lowerEndpoint();
        double xHi = bounds.getXRange().upperEndpoint();
        double yLo = bounds.getYRange().lowerEndpoint();
        double yHi = bounds.getYRange().upperEndpoint();

        // Materialise focus centres (either supplied or sampled now from the
        // same seeded RNG, so subsequent query draws stay deterministic).
        double[][] foci;
        if (explicitFocusCenters != null && explicitFocusCenters.length > 0) {
            Preconditions.checkArgument(explicitFocusCenters.length == numFoci,
                    "explicitFocusCenters length %s != numFoci %s",
                    explicitFocusCenters.length, numFoci);
            foci = explicitFocusCenters;
        } else {
            foci = new double[numFoci][2];
            for (int k = 0; k < numFoci; k++) {
                foci[k][0] = xLo + rng.nextDouble() * (xHi - xLo);
                foci[k][1] = yLo + rng.nextDouble() * (yHi - yLo);
            }
        }

        final int MAX_REJECTIONS = 32;
        double[][] centers = new double[count][2];
        for (int i = 0; i < count; i++) {
            int k = rng.nextInt(numFoci);
            double cx = 0.0, cy = 0.0;
            int attempts = 0;
            while (true) {
                cx = foci[k][0] + rng.nextGaussian() * sigmaX;
                cy = foci[k][1] + rng.nextGaussian() * sigmaY;
                if (cx >= xLo && cx <= xHi && cy >= yLo && cy <= yHi) break;
                if (++attempts >= MAX_REJECTIONS) {
                    // Hard clamp as a fallback (rare).
                    cx = Math.min(Math.max(cx, xLo), xHi);
                    cy = Math.min(Math.max(cy, yLo), yHi);
                    break;
                }
            }
            centers[i][0] = cx;
            centers[i][1] = cy;
        }
        return centers;
    }

    public List<Query> generate(int count, Schema schema) {
        return generateQuerySequence(count, schema);
    }

    public long getSeed() { return seed; }
    public double getSx() { return sx; }
    public double getSy() { return sy; }
    public int getNumFoci() { return numFoci; }
    public double getSigmaX() { return sigmaX; }
    public double getSigmaY() { return sigmaY; }
}
