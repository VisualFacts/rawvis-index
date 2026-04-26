package gr.athenarc.imsi.visualfacts.experiments.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gr.athenarc.imsi.visualfacts.Rectangle;

/**
 * Computes the fixed-extent (sx, sy) for a random workload.
 *
 * <p>Two modes:
 * <ul>
 *   <li>{@link #closedForm} — uses the analytical formula
 *       {@code sx = √σ · Wx, sy = √σ · Wy}. Exact only when the data is
 *       uniform on the bounding box (synth datasets).</li>
 *   <li>{@link #calibrate} — binary-searches the extent so that the mean
 *       selectivity over reservoir-centred queries (estimated from the
 *       reservoir itself) matches the target σ. This is the data-aware path
 *       used for real datasets: a single fixed extent whose <em>average</em>
 *       selectivity equals the target.</li>
 * </ul>
 *
 * <p>The aspect ratio of (sx, sy) is fixed to the domain aspect ratio
 * ({@code sy / sx = Wy / Wx}) so queries look natural on long/thin domains.
 *
 * <p>Boundary policy at runtime is "shift inward" (area preserved → mean
 * selectivity preserved). The estimator below applies the same shift when
 * counting hits, so the calibrated extent matches what queries actually
 * sweep at run time.
 */
public final class ExtentCalibrator {

    private static final Logger LOG = LogManager.getLogger(ExtentCalibrator.class);
    private static final int GRID_SIZE = 256;
    private static final int MAX_ITERS = 40;
    private static final double TOLERANCE = 0.01; // 1% relative

    private ExtentCalibrator() {}

    /** Closed-form extent assuming uniform data on the bounding box. */
    public static double[] closedForm(double sigma, Rectangle bounds) {
        check(sigma);
        double wx = width(bounds);
        double wy = height(bounds);
        double a = Math.sqrt(sigma);
        return new double[]{a * wx, a * wy};
    }

    /**
     * Binary-searches a single (sx, sy) — both scaled linearly with α and the
     * domain aspect ratio — such that the reservoir-estimated mean
     * selectivity equals {@code targetSigma} within {@link #TOLERANCE}.
     */
    public static double[] calibrate(SpatialReservoir reservoir, Rectangle bounds, double targetSigma) {
        check(targetSigma);
        if (reservoir.size() < 2) {
            LOG.warn("Reservoir too small ({}) to calibrate; falling back to closed form.", reservoir.size());
            return closedForm(targetSigma, bounds);
        }

        double wx = width(bounds);
        double wy = height(bounds);

        // Build the index once; reuse across iterations.
        Grid g = new Grid(reservoir, bounds, GRID_SIZE);

        // Seed bounds with the closed-form alpha; refine outward if needed.
        double alphaSeed = Math.sqrt(targetSigma);
        double lo = 0.0;
        double hi = Math.min(1.0, alphaSeed * 4.0);
        // Ensure hi gives sel >= target; otherwise widen.
        double hiSel = meanSel(g, reservoir, bounds, hi * wx, hi * wy);
        int widenGuard = 0;
        while (hiSel < targetSigma && hi < 1.0 && widenGuard++ < 6) {
            hi = Math.min(1.0, hi * 1.5);
            hiSel = meanSel(g, reservoir, bounds, hi * wx, hi * wy);
        }
        if (hiSel < targetSigma) {
            LOG.warn("Even α={} yields mean selectivity {} < target {}; using α=1.", hi, hiSel, targetSigma);
            return new double[]{wx, wy};
        }

        double bestAlpha = (lo + hi) / 2.0;
        double bestSel = Double.NaN;
        for (int iter = 0; iter < MAX_ITERS; iter++) {
            double alpha = (lo + hi) / 2.0;
            double sx = alpha * wx;
            double sy = alpha * wy;
            double estimated = meanSel(g, reservoir, bounds, sx, sy);
            bestAlpha = alpha;
            bestSel = estimated;
            double rel = Math.abs(estimated - targetSigma) / targetSigma;
            LOG.debug("calibrate iter={} α={} sx={} sy={} sel={} (rel err {})",
                    iter, alpha, sx, sy, estimated, rel);
            if (rel < TOLERANCE) break;
            if (estimated < targetSigma) lo = alpha;
            else hi = alpha;
        }
        LOG.info("ExtentCalibrator: target σ={} → α={} (sx={}, sy={}), achieved mean σ={}",
                targetSigma, bestAlpha, bestAlpha * wx, bestAlpha * wy, bestSel);
        return new double[]{bestAlpha * wx, bestAlpha * wy};
    }

    /**
     * Computes statistics (mean / std / min / max) of the per-query selectivity
     * over reservoir-centred queries with the given fixed extent. Used for
     * paper-friendly logging. Selectivity is reported as a fraction in [0,1].
     */
    public static SelectivityStats stats(SpatialReservoir reservoir, Rectangle bounds, double sx, double sy) {
        Grid g = new Grid(reservoir, bounds, GRID_SIZE);
        return statsInternal(g, reservoir, bounds, sx, sy);
    }

    public static final class SelectivityStats {
        public final double mean;
        public final double std;
        public final double min;
        public final double max;
        public SelectivityStats(double mean, double std, double min, double max) {
            this.mean = mean; this.std = std; this.min = min; this.max = max;
        }
        @Override public String toString() {
            return String.format("mean=%.6f std=%.6f min=%.6f max=%.6f", mean, std, min, max);
        }
    }

    // --------------------------------------------------------------------- //

    private static double meanSel(Grid g, SpatialReservoir r, Rectangle bounds, double sx, double sy) {
        return statsInternal(g, r, bounds, sx, sy).mean;
    }

    private static SelectivityStats statsInternal(Grid g, SpatialReservoir r, Rectangle bounds,
                                                  double sx, double sy) {
        double[] xs = r.xs();
        double[] ys = r.ys();
        int n = xs.length;
        double xLo = bounds.getXRange().lowerEndpoint();
        double xHi = bounds.getXRange().upperEndpoint();
        double yLo = bounds.getYRange().lowerEndpoint();
        double yHi = bounds.getYRange().upperEndpoint();
        double halfX = sx / 2.0;
        double halfY = sy / 2.0;

        double sumSel = 0.0;
        double sumSqSel = 0.0;
        double minSel = Double.POSITIVE_INFINITY;
        double maxSel = Double.NEGATIVE_INFINITY;

        for (int i = 0; i < n; i++) {
            double cx = xs[i];
            double cy = ys[i];
            // Shift inward to preserve area at the boundary (matches runtime).
            if (cx - halfX < xLo) cx = xLo + halfX;
            else if (cx + halfX > xHi) cx = xHi - halfX;
            if (cy - halfY < yLo) cy = yLo + halfY;
            else if (cy + halfY > yHi) cy = yHi - halfY;

            double xMin = cx - halfX;
            double xMax = cx + halfX;
            double yMin = cy - halfY;
            double yMax = cy + halfY;

            int hits = g.countInside(xs, ys, xMin, xMax, yMin, yMax);
            double sel = (double) hits / n;
            sumSel += sel;
            sumSqSel += sel * sel;
            if (sel < minSel) minSel = sel;
            if (sel > maxSel) maxSel = sel;
        }
        double mean = sumSel / n;
        double var = Math.max(0.0, sumSqSel / n - mean * mean);
        return new SelectivityStats(mean, Math.sqrt(var), minSel, maxSel);
    }

    private static double width(Rectangle b) {
        return b.getXRange().upperEndpoint() - b.getXRange().lowerEndpoint();
    }

    private static double height(Rectangle b) {
        return b.getYRange().upperEndpoint() - b.getYRange().lowerEndpoint();
    }

    private static void check(double sigma) {
        if (!(sigma > 0 && sigma < 1)) {
            throw new IllegalArgumentException("selectivity must be in (0, 1), got " + sigma);
        }
    }

    /** Minimal CSR-style 2-D grid index over the reservoir for fast range counting. */
    private static final class Grid {
        final int g;
        final double xLo, yLo, cellW, cellH;
        final int[] starts;   // length g*g + 1
        final int[] order;    // length n
        Grid(SpatialReservoir r, Rectangle bounds, int g) {
            this.g = g;
            this.xLo = bounds.getXRange().lowerEndpoint();
            this.yLo = bounds.getYRange().lowerEndpoint();
            double wx = bounds.getXRange().upperEndpoint() - xLo;
            double wy = bounds.getYRange().upperEndpoint() - yLo;
            this.cellW = wx / g;
            this.cellH = wy / g;
            int n = r.size();
            int[] cellOf = new int[n];
            int[] counts = new int[g * g];
            double[] xs = r.xs();
            double[] ys = r.ys();
            for (int i = 0; i < n; i++) {
                int gx = clampInt((int) ((xs[i] - xLo) / cellW), 0, g - 1);
                int gy = clampInt((int) ((ys[i] - yLo) / cellH), 0, g - 1);
                int c = gy * g + gx;
                cellOf[i] = c;
                counts[c]++;
            }
            this.starts = new int[g * g + 1];
            for (int i = 0; i < g * g; i++) starts[i + 1] = starts[i] + counts[i];
            int[] cursor = new int[g * g];
            this.order = new int[n];
            for (int i = 0; i < n; i++) {
                int c = cellOf[i];
                order[starts[c] + cursor[c]++] = i;
            }
        }

        int countInside(double[] xs, double[] ys, double xMin, double xMax, double yMin, double yMax) {
            int gxLo = clampInt((int) ((xMin - xLo) / cellW), 0, g - 1);
            int gxHi = clampInt((int) ((xMax - xLo) / cellW), 0, g - 1);
            int gyLo = clampInt((int) ((yMin - yLo) / cellH), 0, g - 1);
            int gyHi = clampInt((int) ((yMax - yLo) / cellH), 0, g - 1);
            int hits = 0;
            for (int gy = gyLo; gy <= gyHi; gy++) {
                int rowBase = gy * g;
                for (int gx = gxLo; gx <= gxHi; gx++) {
                    int c = rowBase + gx;
                    int s = starts[c];
                    int e = starts[c + 1];
                    for (int k = s; k < e; k++) {
                        int p = order[k];
                        double x = xs[p];
                        double y = ys[p];
                        if (x >= xMin && x <= xMax && y >= yMin && y <= yMax) hits++;
                    }
                }
            }
            return hits;
        }
    }

    private static int clampInt(int v, int lo, int hi) {
        return v < lo ? lo : (v > hi ? hi : v);
    }
}
