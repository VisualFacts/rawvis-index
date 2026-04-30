package gr.athenarc.imsi.visualfacts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.math.StatsAccumulator;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

/**
 * Outlier-aware AQP support structure.
 *
 * <p>The outlier index identifies a small set {@code O} of rows (|O| ≤ K) whose
 * extreme measure values dominate the variance of one or more aggregates.
 * Those rows are extracted into an in-memory value matrix, removed from the
 * per-tile sampling population, and contribute exact (closed-form) sums at
 * query time.  The trimmed population has dramatically lower CV, which
 * tightens approximate-query confidence intervals at the cost of a small
 * up-front memory footprint (~88 bytes per outlier × M measures).
 *
 * <p>Lifecycle (orchestrated by {@link Valinor}):
 * <ol>
 *   <li><b>Phase 1 — collect</b>: per-thread top-K min-heaps are populated
 *       inline during the parallel CSV scan via {@link #offer}.  Each
 *       candidate carries the full M-vector of measure values so that
 *       Phase 3 needs no second I/O pass.</li>
 *   <li><b>Phase 2 — merge</b>: per-thread heaps are merged into a single
 *       global pool of unique candidates ({@link #mergeCandidates}).</li>
 *   <li><b>Phase 3 — score & top-K select</b>: each candidate gets a single
 *       static "outlierness" score = max over measures of its squared
 *       z-score; the top-K by score are retained ({@link #selectByScore}).
 *       This is O(|pool| · M + |pool| · log K) — orders of magnitude faster
 *       than the previous K-iteration greedy and produces equivalent
 *       results on heavy-tailed data where one measure dominates.</li>
 *   <li><b>Phase 5 — partition by tile</b>: a single linear pass over all
 *       tile rows assigns each selected outlier to its tile via byte-offset
 *       lookup ({@link #partitionByTile}).</li>
 * </ol>
 *
 * <p>This class is created only when {@code IndexConfig.OUTLIER_K > 0}.  The
 * pre-outlier code path is fully bypassed when the index is {@code null}.
 */
public final class OutlierIndex {

    private static final Logger LOG = LogManager.getLogger(OutlierIndex.class);

    /** Number of outliers to retain globally. */
    private final int k;
    /** Number of measures (M). */
    private final int measureCount;
    /** Number of scanner threads (T). */
    private final int numThreads;

    /**
     * Per-thread, per-measure bounded min-heaps of size up to {@link #k}.
     * Heap is ordered by {@code |value|} ascending so the smallest
     * absolute-value element sits at the root and is the first to be evicted
     * when a new larger candidate arrives.
     *
     * <p>Indexing: {@code heaps[threadIdx][measureIdx]}.
     */
    private final PriorityQueue<Candidate>[][] heaps;

    /** Comparator: smallest |value| first (root = candidate to evict). */
    private static final Comparator<Candidate> MIN_BY_ABS =
            Comparator.comparingDouble(c -> Math.abs(c.value));

    /**
     * Set after {@link #selectByScore}: the chosen outlier rows in stable
     * order.  Each row's full M-vector is stored in {@link #outlierMatrix}
     * (one row per selected outlier).
     */
    private List<Candidate> selected;

    /** Selected outlier values: outlierMatrix[i][m] = vector[m] for outlier i. */
    private double[][] outlierMatrix;

    /**
     * Map from byte offset → index into {@link #selected} / {@link #outlierMatrix}.
     * Built in {@link #selectByScore}; consulted in {@link #partitionByTile}.
     */
    private Long2IntOpenHashMap offsetToOutlierIdx;

    /** Initial uncapped CV per measure (logged once after Phase 3). */
    private double[] initialCV;
    /** Post-outlier uncapped CV per measure (logged once after Phase 3). */
    private double[] postOutlierCV;

    /**
     * One row of the heap: a candidate outlier with all per-measure values
     * captured inline.  Total size ≈ 24 (header) + 8 (value) + 8 (offset) +
     * 16 + 8M (vector) ≈ 56 + 8M bytes.  For M = 8: ~120 B per candidate.
     */
    public static final class Candidate {
        public final double value;     // value of THIS measure (the one that pushed it onto its heap)
        public final long byteOffset;  // unique row identifier across the whole CSV
        public final double[] vector;  // length-M vector of all measures

        public Candidate(double value, long byteOffset, double[] vector) {
            this.value = value;
            this.byteOffset = byteOffset;
            this.vector = vector;
        }
    }

    @SuppressWarnings("unchecked")
    public OutlierIndex(int k, int measureCount, int numThreads) {
        if (k <= 0) {
            throw new IllegalArgumentException("OutlierIndex requires k > 0 (got " + k + ")");
        }
        this.k = k;
        this.measureCount = measureCount;
        this.numThreads = numThreads;
        this.heaps = (PriorityQueue<Candidate>[][]) new PriorityQueue[numThreads][measureCount];
        for (int t = 0; t < numThreads; t++) {
            for (int m = 0; m < measureCount; m++) {
                // Capacity hint k + 1: we add then poll so heap briefly grows by 1.
                heaps[t][m] = new PriorityQueue<>(k + 1, MIN_BY_ABS);
            }
        }
    }

    public int getK() { return k; }
    public int getMeasureCount() { return measureCount; }

    /**
     * Phase 1: thread-local insertion.  Called from
     * {@link ParallelCsvScanner} for every (row, measure) with a non-NaN
     * value.  Each thread's heap is private — no synchronization needed.
     *
     * <p>A single allocation (the wrapper {@link Candidate} + the inline
     * vector array) happens only when the candidate is actually competitive
     * (heap not full, OR strictly larger than the current root).  The
     * caller is responsible for materializing the {@code vector} array
     * exactly once per row, since it is shared across all measures of that
     * row.
     */
    public void offer(int threadIdx, int measureIdx, double value, long byteOffset, double[] vector) {
        PriorityQueue<Candidate> heap = heaps[threadIdx][measureIdx];
        double absV = Math.abs(value);
        if (heap.size() < k) {
            heap.offer(new Candidate(value, byteOffset, vector));
        } else {
            // Peek at the current minimum-|value| element; replace only if strictly larger.
            Candidate root = heap.peek();
            if (absV > Math.abs(root.value)) {
                heap.poll();
                heap.offer(new Candidate(value, byteOffset, vector));
            }
        }
    }

    /**
     * Phase 2: merge per-thread, per-measure heaps into a single deduplicated
     * candidate pool.  Returns the deduplicated list (one entry per byte
     * offset; the inline vector is the same regardless of which heap the
     * candidate came from).
     */
    public List<Candidate> mergeCandidates() {
        // Use a hashmap byteOffset → Candidate to deduplicate (a single row may
        // appear in multiple per-measure heaps — but its vector is identical).
        Long2IntOpenHashMap seen = new Long2IntOpenHashMap();
        seen.defaultReturnValue(-1);
        List<Candidate> pool = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            for (int m = 0; m < measureCount; m++) {
                for (Candidate c : heaps[t][m]) {
                    if (seen.putIfAbsent(c.byteOffset, pool.size()) == -1) {
                        pool.add(c);
                    }
                }
            }
        }
        // Free heap memory eagerly — vectors are now owned by `pool`.
        for (int t = 0; t < numThreads; t++) {
            Arrays.fill(heaps[t], null);
        }
        LOG.info("OutlierIndex Phase 2 merge: {} unique candidates from {} per-thread heaps",
                pool.size(), numThreads * measureCount);
        return pool;
    }

    /**
     * Phase 3: single-pass score-based selection.
     *
     * <p>Each candidate is assigned a static "outlierness" score equal to
     * the maximum over measures of its squared z-score:
     * <pre>
     *   score(c) = max_m  ((c.vector[m] - μ_m) / σ_m)²
     * </pre>
     * The top-K candidates by score are retained.  Squared z-score is the
     * per-row contribution to global variance (modulo the constant 1/N), so
     * removing the top-K by max-z² rows directly maximizes variance reduction
     * on whichever measure is currently the bottleneck.  Z-score normalization
     * makes scores comparable across measures with different units and scales.
     *
     * <p>Complexity: O(|pool| · M) for scoring + O(|pool| · log K) for the
     * size-K min-heap top-K extraction.  For K=100 000 and |pool|=8 000 000
     * this is ~3 seconds — versus ~10 hours for the previous K-iteration
     * greedy whose cost was O(K² · T·M).
     *
     * <p>Records initial and post-trim uncapped CVs in {@link #initialCV} /
     * {@link #postOutlierCV} for logging by {@link Valinor}.
     *
     * @param globalStats per-measure global StatsAccumulator (must already
     *                    aggregate every leaf tile's stats)
     * @param pool        deduplicated candidate pool from {@link #mergeCandidates}
     */
    public void selectByScore(StatsAccumulator[] globalStats, List<Candidate> pool) {
        final int M = measureCount;

        // 1) Cache (mean, std) per measure for z-score normalization.
        double[] mean = new double[M];
        double[] std  = new double[M];
        long[]   N    = new long[M];
        double[] sum   = new double[M];
        double[] sumSq = new double[M];
        for (int m = 0; m < M; m++) {
            StatsAccumulator s = globalStats[m];
            if (s == null || s.count() < 2) {
                mean[m] = 0; std[m] = 0; N[m] = 0; sum[m] = 0; sumSq[m] = 0;
                continue;
            }
            long n = s.count();
            mean[m] = s.mean();
            std[m]  = s.sampleStandardDeviation();
            double var = s.populationVariance();
            N[m]   = n;
            sum[m] = mean[m] * n;
            sumSq[m] = n * (var + mean[m] * mean[m]);
        }
        initialCV = computeCVs(N, sum, sumSq);

        // 2) Score each candidate: max_m ((v_m - μ_m) / σ_m)²
        //    NaN values contribute zero; measures with std==0 are skipped
        //    (no row can be an outlier on a constant column).
        final int poolSize = pool.size();
        double[] scores = new double[poolSize];
        for (int p = 0; p < poolSize; p++) {
            double[] v = pool.get(p).vector;
            double best = 0.0;
            for (int m = 0; m < M; m++) {
                if (std[m] == 0) continue;
                double x = v[m];
                if (Double.isNaN(x)) continue;
                double z = (x - mean[m]) / std[m];
                double z2 = z * z;
                if (z2 > best) best = z2;
            }
            scores[p] = best;
        }

        // 3) Top-K extraction via bounded min-heap of pool indices.
        //    Heap root = currently smallest-score retained candidate.
        final double[] scoresRef = scores;
        PriorityQueue<Integer> topK = new PriorityQueue<>(
                Math.min(k, poolSize) + 1,
                (a, b) -> Double.compare(scoresRef[a], scoresRef[b]));
        for (int p = 0; p < poolSize; p++) {
            if (topK.size() < k) {
                topK.offer(p);
            } else if (scoresRef[p] > scoresRef[topK.peek()]) {
                topK.poll();
                topK.offer(p);
            }
        }

        // 4) Materialize selected list, offsetToOutlierIdx, outlierMatrix.
        //    Iteration order is heap order, which is fine (no semantic meaning).
        selected = new ArrayList<>(topK.size());
        offsetToOutlierIdx = new Long2IntOpenHashMap(topK.size());
        offsetToOutlierIdx.defaultReturnValue(-1);
        outlierMatrix = new double[topK.size()][];
        int idx = 0;
        for (int p : topK) {
            Candidate c = pool.get(p);
            selected.add(c);
            offsetToOutlierIdx.put(c.byteOffset, idx);
            outlierMatrix[idx] = c.vector;
            idx++;
        }

        // 5) Compute post-outlier CVs by subtracting selected rows from running stats.
        for (Candidate c : selected) {
            double[] v = c.vector;
            for (int m = 0; m < M; m++) {
                double x = v[m];
                if (Double.isNaN(x) || N[m] == 0) continue;
                N[m]   -= 1;
                sum[m] -= x;
                sumSq[m] -= x * x;
            }
        }
        postOutlierCV = computeCVs(N, sum, sumSq);

        LOG.info("OutlierIndex Phase 3 score-based: selected {}/{} outliers from pool of {}",
                selected.size(), k, poolSize);
    }

    /** Helper: compute uncapped CV per measure from running sums. */
    private static double[] computeCVs(long[] N, double[] sum, double[] sumSq) {
        int M = N.length;
        double[] out = new double[M];
        for (int m = 0; m < M; m++) {
            if (N[m] < 2) { out[m] = 0.0; continue; }
            double mean = sum[m] / N[m];
            double var = sumSq[m] / N[m] - mean * mean;
            if (var < 0) var = 0;
            out[m] = (mean == 0) ? Double.POSITIVE_INFINITY : Math.sqrt(var) / Math.abs(mean);
        }
        return out;
    }

    public double[] getInitialCV()      { return initialCV; }
    public double[] getPostOutlierCV()  { return postOutlierCV; }
    public int getSelectedCount()       { return selected == null ? 0 : selected.size(); }
    public double[][] getOutlierMatrix() { return outlierMatrix; }

    /**
     * Phase 5: walk every tile and mark its {@code outlierBitSet} for every
     * row whose byte offset belongs to a selected outlier.  This is a single
     * O(N) pass with a HashSet lookup per row (~30 ns) — acceptable for
     * datasets up to ~10^9 rows.
     *
     * <p>For each marked row we also record its global outlier index in
     * {@code tile.outlierIdxs[localPos]}, so query-time code can fetch
     * {@code outlierMatrix[outlierIdxs[pos]][m]} without any further lookup.
     */
    public void partitionByTile(Iterable<?> leafTiles) {
        if (selected == null || selected.isEmpty()) return;
        int totalAssigned = 0;
        for (Object obj : leafTiles) {
            Tile tile = (Tile) obj;
            if (!tile.hasPoints()) continue;
            int n = tile.getSize();
            java.util.BitSet bits = null;
            int[] idxs = null;
            for (int i = 0; i < n; i++) {
                int outIdx = offsetToOutlierIdx.get(tile.getOffset(i));
                if (outIdx >= 0) {
                    if (bits == null) {
                        bits = new java.util.BitSet(n);
                        idxs = new int[n];
                        Arrays.fill(idxs, -1);
                    }
                    bits.set(i);
                    idxs[i] = outIdx;
                    totalAssigned++;
                }
            }
            if (bits != null) {
                tile.setOutlierData(bits, idxs);
            }
        }
        LOG.info("OutlierIndex Phase 5 partition: assigned {}/{} outliers to tiles",
                totalAssigned, selected.size());
    }

    /**
     * Returns the per-measure value of the outlier at the given global
     * outlier index, or NaN if the outlier had no value for that measure.
     */
    public double getOutlierValue(int outlierIdx, int measureIdx) {
        return outlierMatrix[outlierIdx][measureIdx];
    }
}
