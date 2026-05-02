package gr.athenarc.imsi.visualfacts;

import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.math.Stats;
import com.google.common.math.StatsAccumulator;

import gr.athenarc.imsi.visualfacts.query.Query;

/**
 * Per-stratum sample-size planner using stratified Neyman + FPC allocation.
 *
 * <p>For each measure column, the planner solves the closed-form
 * variance-constrained Neyman optimum:
 * <pre>
 *   n_total = (Sum_h N_h S_h)^2 / (V* + Sum_h N_h S_h^2)
 *   n_h     = n_total * (N_h S_h) / (Sum_g N_g S_g)
 *   V*      = (eps * |T_anticipated| / z)^2
 * </pre>
 * with iterative saturation when {@code n_h > N_h}.  Per-stratum sizes are
 * computed independently for SUM and COUNT, and the per-node maximum across
 * all measures and aggregates becomes that node's target sample count.
 *
 * <p>Variance priors are drawn via {@link Tile#getPrior(int, OutlierIndex)}
 * (own complete stats, then nearest ancestor with frozen exact stats), with
 * outliers analytically subtracted in both cases.  Global per-measure stats
 * are used as a final fallback.
 *
 * <p>For round 2+ the caller supplies the observed per-stratum
 * {@link StatsAccumulator}s and observed totals; the same closed form is
 * re-solved, and the iterator only requests the delta over the round-1
 * targets.
 *
 * <p>See {@code docs/stratified_sampling_approach.md} for the full
 * derivation.
 */
public final class SampleAllocator {

    /** Floor for |T_anticipated| to keep V* > 0 when the answer is near zero. */
    private static final double T_ANTICIPATED_FLOOR_FRAC = 1e-6;

    /** Minimum samples per stratum (CLT / variance-estimability floor). */
    private static final int MIN_SAMPLES_PER_NODE = 2;

    private final Schema schema;
    private final OutlierIndex outlierIndex;
    private final StatsAccumulator[] globalMeasureStats;
    private final double errorThreshold;
    private final double zScore;

    public SampleAllocator(Schema schema, OutlierIndex outlierIndex,
            StatsAccumulator[] globalMeasureStats,
            double errorThreshold, double zScore) {
        this.schema = schema;
        this.outlierIndex = outlierIndex;
        this.globalMeasureStats = globalMeasureStats;
        this.errorThreshold = errorThreshold;
        this.zScore = zScore;
    }

    /** Plan round 1: priors only (no observed sample stats yet). */
    public Map<QueryNode, Integer> planInitial(List<QueryNode> samplingNodes,
            Query query, Map<Integer, Double> exactSumPerMeasure,
            Map<Integer, Long> exactCountPerMeasure) {
        return plan(samplingNodes, query, exactSumPerMeasure, exactCountPerMeasure,
                false, null, null);
    }

    /**
     * Plan round 2+: re-solves with observed per-stratum sample stats (read
     * directly from each {@link QueryNode}'s {@code sampleStatsAccumulators})
     * and observed (estimated) totals.  Returns absolute per-node targets;
     * the caller is responsible for converting them into deltas relative to
     * the round-1 plan.
     *
     * @param observedSumEstimate per measure col -> currently estimated SUM (incl. exact + outlier + sample contribution)
     * @param observedCountEstimate per measure col -> currently estimated COUNT (incl. exact + outlier + sample)
     */
    public Map<QueryNode, Integer> planAdaptive(List<QueryNode> samplingNodes,
            Query query, Map<Integer, Double> exactSumPerMeasure,
            Map<Integer, Long> exactCountPerMeasure,
            Map<Integer, Double> observedSumEstimate,
            Map<Integer, Double> observedCountEstimate) {
        return plan(samplingNodes, query, exactSumPerMeasure, exactCountPerMeasure,
                true, observedSumEstimate, observedCountEstimate);
    }

    private Map<QueryNode, Integer> plan(List<QueryNode> samplingNodes,
            Query query,
            Map<Integer, Double> exactSumPerMeasure,
            Map<Integer, Long> exactCountPerMeasure,
            boolean useObserved,
            Map<Integer, Double> observedSumEstimate,
            Map<Integer, Double> observedCountEstimate) {

        Map<QueryNode, Integer> targets = new IdentityHashMap<>();
        if (samplingNodes == null || samplingNodes.isEmpty()) return targets;

        int H = samplingNodes.size();
        QueryNode[] nodes = samplingNodes.toArray(new QueryNode[0]);
        double[] N = new double[H];
        for (int h = 0; h < H; h++) {
            N[h] = nodes[h].getIntersectionCount();
            // Floor to MIN_SAMPLES_PER_NODE (or N_h if smaller); the per-measure
            // planners may raise this further.
            targets.put(nodes[h], Math.min((int) N[h], MIN_SAMPLES_PER_NODE));
        }

        for (Integer measureCol : query.getMeasureCols()) {
            int midx = schema.getMeasureIndex(measureCol);

            // Gather per-stratum priors (mean_nn, stdev_nn, p_nn).
            double[] muNN = new double[H];
            double[] sigNN = new double[H];
            double[] pNN = new double[H];
            for (int h = 0; h < H; h++) {
                double[] prior = priorOrFallback(nodes[h], midx, measureCol, useObserved);
                muNN[h] = prior[0];
                sigNN[h] = prior[1];
                pNN[h] = prior[2];
            }

            // ---- SUM allocation (null-as-zero variance) ----
            double[] S_sum = new double[H];
            for (int h = 0; h < H; h++) {
                double s2 = nullAsZeroVariance(muNN[h], sigNN[h], pNN[h]);
                if (useObserved) {
                    double priorS2 = priorNullAsZeroVariance(nodes[h], midx);
                    if (priorS2 > s2) s2 = priorS2;
                }
                S_sum[h] = Math.sqrt(Math.max(0.0, s2));
            }
            double exactSum = exactSumPerMeasure.getOrDefault(measureCol, 0.0);
            double Tsum;
            if (observedSumEstimate != null && observedSumEstimate.containsKey(measureCol)) {
                Tsum = observedSumEstimate.get(measureCol);
            } else {
                Tsum = exactSum;
                for (int h = 0; h < H; h++) Tsum += N[h] * pNN[h] * muNN[h];
            }
            int[] n_sum = neymanFPC(N, S_sum, Tsum, sumTargetFloor(N));

            // ---- COUNT allocation (Bernoulli on null indicator) ----
            double[] S_cnt = new double[H];
            for (int h = 0; h < H; h++) {
                S_cnt[h] = Math.sqrt(Math.max(0.0, pNN[h] * (1.0 - pNN[h])));
            }
            double exactCount = exactCountPerMeasure.getOrDefault(measureCol, 0L).doubleValue();
            double Tcnt;
            if (observedCountEstimate != null && observedCountEstimate.containsKey(measureCol)) {
                Tcnt = observedCountEstimate.get(measureCol);
            } else {
                Tcnt = exactCount;
                for (int h = 0; h < H; h++) Tcnt += N[h] * pNN[h];
            }
            int[] n_cnt = neymanFPC(N, S_cnt, Tcnt, countTargetFloor(N));

            // Per-stratum max across SUM and COUNT, and across measures.
            for (int h = 0; h < H; h++) {
                int wanted = Math.max(n_sum[h], n_cnt[h]);
                Integer cur = targets.get(nodes[h]);
                if (cur == null || wanted > cur) targets.put(nodes[h], wanted);
            }
        }

        // Final clamp: never exceed N_h, never below 0.
        for (int h = 0; h < H; h++) {
            int t = targets.get(nodes[h]);
            int Nh = (int) N[h];
            if (t > Nh) t = Nh;
            if (t < 0) t = 0;
            targets.put(nodes[h], t);
        }
        return targets;
    }

    /**
     * Closed-form Neyman + FPC with iterative saturation.
     * Returns per-stratum sample counts that satisfy
     * {@code z * sqrt(Var) <= eps * |T|} when the priors are correct.
     */
    private int[] neymanFPC(double[] N, double[] S, double T, double targetFloor) {
        int H = N.length;
        int[] n_h = new int[H];
        boolean[] sat = new boolean[H];

        // V* — variance budget from anticipated total.
        double Tabs = Math.abs(T);
        double floor = Math.max(0.0, targetFloor);
        if (Tabs < floor) Tabs = floor;
        double Vstar = Math.pow(errorThreshold * Tabs / zScore, 2);

        // Saturation iteration. Each pass solves the closed form over the
        // currently unsaturated strata; any stratum whose solution exceeds
        // N_h is saturated (n_h := N_h, FPC := 0) and dropped from the next
        // iteration. Converges in at most H iterations; usually 1–2.
        while (true) {
            double sumNS = 0.0, sumNS2 = 0.0;
            for (int h = 0; h < H; h++) {
                if (sat[h]) continue;
                sumNS += N[h] * S[h];
                sumNS2 += N[h] * S[h] * S[h];
            }
            // Degenerate: every unsaturated stratum has zero variance prior
            // (e.g. all NULLs or constant column). Floor to MIN_SAMPLES.
            if (sumNS <= 0.0) {
                for (int h = 0; h < H; h++) {
                    if (!sat[h]) n_h[h] = Math.min((int) N[h], MIN_SAMPLES_PER_NODE);
                }
                break;
            }
            double n_total = (sumNS * sumNS) / (Vstar + sumNS2);
            boolean changed = false;
            for (int h = 0; h < H; h++) {
                if (sat[h]) continue;
                double n_raw = n_total * (N[h] * S[h]) / sumNS;
                int n = (int) Math.ceil(n_raw);
                if (n < MIN_SAMPLES_PER_NODE) n = Math.min((int) N[h], MIN_SAMPLES_PER_NODE);
                if (n >= (int) N[h]) {
                    sat[h] = true;
                    n_h[h] = (int) N[h];
                    changed = true;
                } else {
                    n_h[h] = n;
                }
            }
            if (!changed) break;
        }
        return n_h;
    }

    /**
     * Returns {@code [meanNonNull, stdevNonNull, nonNullRatio]} for stratum
     * {@code node} on measure index {@code midx} / measure column
     * {@code measureCol}.  Resolution order:
     * observed sample stats (round 2+) → tile/ancestor exact prior →
     * global per-measure stats fallback.
     */
    private double[] priorOrFallback(QueryNode node, int midx, int measureCol, boolean useObserved) {
        // 1) Observed sample stats with sufficient count win (round 2+).
        //    The SUM allocation later floors the full null-as-zero variance
        //    against the tile prior; mean, stdev, and pNN stay observed here
        //    so COUNT allocation continues to depend only on observed pNN.
        if (useObserved) {
            StatsAccumulator s = node.getSampleStatsAcc(measureCol);
            if (s != null && s.count() >= 2) {
                int sampledRows = node.getSampledPointCount();
                int nNN = (int) s.count();
                double pNN = sampledRows > 0 ? (double) nNN / sampledRows : 0.0;
                return new double[] { s.mean(), s.sampleStandardDeviation(), pNN };
            }
        }
        // 2) Tile / ancestor exact prior.
        double[] prior = node.getTile().getPrior(midx, outlierIndex);
        if (prior != null) return prior;
        // 3) Global stats fallback.
        if (globalMeasureStats != null && midx >= 0 && midx < globalMeasureStats.length) {
            StatsAccumulator g = globalMeasureStats[midx];
            if (g != null && g.count() >= 2) {
                double pNN = globalNonNullRatio(g);
                return new double[] { g.mean(), g.sampleStandardDeviation(), pNN };
            }
        }
        // No information at all — return a neutral, non-zero prior so that
        // Neyman degenerates gracefully (proportional allocation with min).
        return new double[] { 0.0, 1.0, 1.0 };
    }

    private double priorNullAsZeroVariance(QueryNode node, int midx) {
        double[] prior = node.getTile().getPrior(midx, outlierIndex);
        if (prior == null) return 0.0;
        return nullAsZeroVariance(prior[0], prior[1], prior[2]);
    }

    private static double nullAsZeroVariance(double muNN, double sigNN, double pNN) {
        double s2 = pNN * sigNN * sigNN + pNN * (1.0 - pNN) * muNN * muNN;
        return Math.max(0.0, s2);
    }

    private double globalMagnitudeFloor() {
        if (globalMeasureStats == null) return 1.0;
        double m = 0.0;
        for (StatsAccumulator s : globalMeasureStats) {
            if (s != null && s.count() > 0) {
                m = Math.max(m, Math.abs(s.mean()));
            }
        }
        return m > 0 ? m : 1.0;
    }

    private double sumTargetFloor(double[] N) {
        return Math.max(1e-12, T_ANTICIPATED_FLOOR_FRAC * globalMagnitudeFloor() * totalPopulation(N));
    }

    private double countTargetFloor(double[] N) {
        return Math.max(1.0, T_ANTICIPATED_FLOOR_FRAC * totalPopulation(N));
    }

    private static double totalPopulation(double[] N) {
        double total = 0.0;
        for (double n : N) total += n;
        return Math.max(1.0, total);
    }

    private double globalNonNullRatio(StatsAccumulator stats) {
        int objectCount = schema != null ? schema.getObjectCount() : 0;
        if (stats == null || objectCount <= 0) return 0.5;
        double p = stats.count() / (double) objectCount;
        if (p <= 0.0) return 0.5;
        if (p >= 1.0) return 1.0;
        return p;
    }

    /** Convenience: computes {@code Σ n_h / Σ N_h} for reporting. */
    public static double effectiveSamplingRate(List<QueryNode> nodes,
            Map<QueryNode, Integer> targets) {
        long totalN = 0, totalSamples = 0;
        for (QueryNode q : nodes) {
            totalN += q.getIntersectionCount();
            Integer t = targets.get(q);
            if (t != null) totalSamples += t;
        }
        if (totalN == 0) return 0.0;
        return Math.min(1.0, (double) totalSamples / (double) totalN);
    }

    /** Convenience: computes the cumulative sample fraction actually available to estimators. */
    public static double actualSamplingRate(List<QueryNode> nodes) {
        long totalN = 0, totalSamples = 0;
        for (QueryNode q : nodes) {
            totalN += q.getIntersectionCount();
            totalSamples += q.getSampledPointCount();
        }
        if (totalN == 0) return 0.0;
        return Math.min(1.0, (double) totalSamples / (double) totalN);
    }

    // unused but kept for downstream debugging
    @SuppressWarnings("unused")
    private static double safeStdev(Stats s) {
        return (s != null && s.count() >= 2) ? s.sampleStandardDeviation() : 0.0;
    }

    @SuppressWarnings("unused")
    private static Map<Integer, Double> emptyDoubleMap() { return new HashMap<>(); }

    @SuppressWarnings("unused")
    private static Map<Integer, Long> emptyLongMap() { return new HashMap<>(); }

    @SuppressWarnings("unused")
    private static int[] copyOf(int[] a) { return Arrays.copyOf(a, a.length); }
}
