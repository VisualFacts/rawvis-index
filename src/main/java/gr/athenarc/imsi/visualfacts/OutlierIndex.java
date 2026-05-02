package gr.athenarc.imsi.visualfacts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.math.StatsAccumulator;

import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
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
 * up-front memory footprint.
 *
 * <p><b>Construction (single CSV pass):</b>
 * <ol>
 *   <li><b>Phase 1 — collect</b>: each scanner thread maintains a single
 *       bounded min-heap of capacity {@link #k}, ordered by an admission
 *       score equal to {@code max_m z²} computed from per-thread
 *       Welford-running per-measure stats.  Vectors are allocated only on
 *       admission, so rejected rows incur zero allocation.  Periodically
 *       (every {@link #SIGMA_CHECK_INTERVAL} rows after the heap fills) the
 *       thread checks whether any per-measure σ has drifted by more than
 *       {@link #REHEAPIFY_THRESHOLD}; if so, every heap entry is re-scored
 *       against the updated stats and the heap is rebuilt.  Reheapify events
 *       are tracked as aggregate diagnostics and exposed in init timing rather
 *       than logged individually from scanner threads.</li>
 *   <li><b>Phase 2 — merge</b>: per-thread heaps are concatenated into a
 *       single candidate pool ({@link #mergeCandidates}).</li>
 *   <li><b>Phase 3 — select</b>: every candidate is re-scored against the
 *       <i>exact</i> global per-measure stats and the top-K survive
 *       ({@link #selectByScore}).</li>
 *   <li><b>Phase 5 — partition</b>: a single linear pass over all tile rows
 *       assigns each selected outlier to its tile via byte-offset lookup
 *       ({@link #partitionByTile}).</li>
 * </ol>
 *
 * <p><b>Memory class:</b> {@code O(T·K)} candidates during scan (one heap of
 * size K per thread), versus the previous {@code O(T·M·K)} pool with full
 * vectors stored on every (thread, measure) heap.
 *
 * <p>This class is created only when {@code IndexConfig.OUTLIER_K > 0}.  The
 * pre-outlier code path is fully bypassed when the index is {@code null}.
 */
public final class OutlierIndex {

    private static final Logger LOG = LogManager.getLogger(OutlierIndex.class);

    /** Rows between σ-drift checks, evaluated only when the per-thread heap is full. */
    private static final int SIGMA_CHECK_INTERVAL = 8192;
    /** Relative change in σ_m on any measure that triggers a reheapify. */
    private static final double REHEAPIFY_THRESHOLD = 0.10;

    /** Number of outliers to retain globally. */
    private final int k;
    /** Number of measures (M). */
    private final int measureCount;
    /** Number of scanner threads (T). */
    private final int numThreads;

    /**
     * One bounded min-heap per scanner thread, capacity = {@link #k}, ordered
     * by {@link Candidate#value} ascending so the smallest-score (most
     * evictable) candidate sits at the root.
     */
    private final PriorityQueue<Candidate>[] heaps;

    /** Comparator: smallest admission score first (root = candidate to evict). */
    private static final Comparator<Candidate> MIN_BY_SCORE =
            Comparator.comparingDouble(c -> c.value);

    // -------- Per-thread Welford running stats (M values per thread) --------
    private final long[][]   nCount;        // [T][M]
    private final double[][] meanRun;       // [T][M]
    private final double[][] m2Sum;         // [T][M]  sum of squared deviations
    private final double[][] sigmaSnap;     // [T][M]  σ at last reheapify (0 if never)
    private final long[]     rowsSinceCheck;// [T]
    private final long[]     reheapifyCount;// [T]
    private final long[]     reheapifyNanos;// [T]
    private final double[]   maxSigmaChange;// [T]  max relative σ change observed on actual reheapify

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

    /**
     * Heap entry: candidate outlier with full M-vector.  The {@code value}
     * field is the admission score (max_m z² with current per-thread running
     * stats); it is mutated by reheapify to reflect the latest stats and
     * is finally overwritten / ignored by Phase 3 (which re-scores against
     * exact global stats).  External code reads only {@link #byteOffset}
     * and {@link #vector}.
     */
    public static final class Candidate {
        public double value;          // mutable: admission score under current per-thread stats
        public final long byteOffset; // unique row identifier across the whole CSV
        public final double[] vector; // length-M vector of all measures (allocated on admission)

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
        this.heaps = (PriorityQueue<Candidate>[]) new PriorityQueue[numThreads];
        this.nCount         = new long[numThreads][measureCount];
        this.meanRun        = new double[numThreads][measureCount];
        this.m2Sum          = new double[numThreads][measureCount];
        this.sigmaSnap      = new double[numThreads][measureCount];
        this.rowsSinceCheck = new long[numThreads];
        this.reheapifyCount = new long[numThreads];
        this.reheapifyNanos = new long[numThreads];
        this.maxSigmaChange = new double[numThreads];
        for (int t = 0; t < numThreads; t++) {
            // Capacity hint k + 1: we add then poll so heap briefly grows by 1.
            heaps[t] = new PriorityQueue<>(k + 1, MIN_BY_SCORE);
        }
    }

    public int getK() { return k; }
    public int getMeasureCount() { return measureCount; }

    /**
     * Phase 1: per-row admission against the per-thread top-K heap.
     *
     * <p>Steps:
     * <ol>
     *   <li>Welford-update per-measure running (n, mean, m2) for every
     *       non-NaN measure.</li>
     *   <li>Compute the row's admission score
     *       {@code maxScore = max_m ((x_m − μ_m) / σ_m)²} using the freshly
     *       updated stats (measures with {@code n < 2} or {@code σ = 0} are
     *       ignored).</li>
     *   <li>If the heap is not yet full, allocate the row's M-vector and
     *       insert.  Otherwise, compare against the heap root: if strictly
     *       larger, evict the root, allocate the M-vector, insert.  No
     *       allocation occurs for rejected rows.</li>
     *   <li>Once the heap is full, increment a row-counter; every
     *       {@link #SIGMA_CHECK_INTERVAL} rows, evaluate σ-drift and trigger
     *       a reheapify if any measure's σ has changed by more than
     *       {@link #REHEAPIFY_THRESHOLD}.</li>
     * </ol>
     *
     * <p>No synchronization: each thread owns its own heap and stats arrays.
     *
     * @param threadIdx        scanner thread index in {@code [0, T)}
     * @param row              the parsed row (caller-owned, not retained)
     * @param measurePositions index into {@code row} for each measure m;
     *                         negative means "measure not present in row"
     * @param byteOffset       unique row identifier
     */
    public void offer(int threadIdx, double[] row, int[] measurePositions, long byteOffset) {
        final int M = measureCount;
        final long[]   nT  = nCount[threadIdx];
        final double[] muT = meanRun[threadIdx];
        final double[] m2T = m2Sum[threadIdx];

        // 1) Welford update + 2) admission score under current per-thread stats.
        double maxScore = 0.0;
        for (int m = 0; m < M; m++) {
            int pos = measurePositions[m];
            if (pos < 0) continue;
            double x = row[pos];
            if (Double.isNaN(x)) continue;
            long n = ++nT[m];
            double delta  = x - muT[m];
            muT[m] += delta / n;
            double delta2 = x - muT[m];
            m2T[m] += delta * delta2;
            if (n >= 2) {
                double var = m2T[m] / (n - 1);
                if (var > 0.0) {
                    double z2 = (delta2 * delta2) / var;
                    if (z2 > maxScore) maxScore = z2;
                }
            }
        }

        // 3) Admission against per-thread heap.
        PriorityQueue<Candidate> heap = heaps[threadIdx];
        if (heap.size() < k) {
            double[] vector = materializeVector(row, measurePositions, M);
            heap.offer(new Candidate(maxScore, byteOffset, vector));
        } else {
            Candidate root = heap.peek();
            if (maxScore > root.value) {
                heap.poll();
                double[] vector = materializeVector(row, measurePositions, M);
                heap.offer(new Candidate(maxScore, byteOffset, vector));
            }
        }

        // 4) Periodic σ-drift check (only meaningful once heap is full;
        //    floor is 0 while heap.size() < k, so admission is exact).
        if (heap.size() >= k) {
            long c = ++rowsSinceCheck[threadIdx];
            if (c >= SIGMA_CHECK_INTERVAL) {
                rowsSinceCheck[threadIdx] = 0L;
                maybeReheapify(threadIdx);
            }
        }
    }

    /** Allocate and fill a length-M vector from the row; missing measures → NaN. */
    private static double[] materializeVector(double[] row, int[] measurePositions, int M) {
        double[] v = new double[M];
        for (int m = 0; m < M; m++) {
            int pos = measurePositions[m];
            v[m] = (pos >= 0) ? row[pos] : Double.NaN;
        }
        return v;
    }

    /**
     * Re-score every entry of thread {@code threadIdx}'s heap against the
     * current per-thread Welford stats and rebuild the heap, but only if
     * any measure's σ has changed by more than {@link #REHEAPIFY_THRESHOLD}
     * relative to the snapshot taken at the previous reheapify (or since
     * the heap first filled).  Logs each reheapify event with diagnostic
    * counters.
     */
    private void maybeReheapify(int threadIdx) {
        final int M = measureCount;
        final long[]   nT  = nCount[threadIdx];
        final double[] muT = meanRun[threadIdx];
        final double[] m2T = m2Sum[threadIdx];
        final double[] snap = sigmaSnap[threadIdx];

        double[] sigCur = new double[M];
        double maxRelChange = 0.0;
        boolean anyFirstTime = false;
        for (int m = 0; m < M; m++) {
            if (nT[m] < 2) { sigCur[m] = 0.0; continue; }
            double var = m2T[m] / (nT[m] - 1);
            sigCur[m] = (var > 0.0) ? Math.sqrt(var) : 0.0;
            if (snap[m] > 0.0) {
                double rel = Math.abs(sigCur[m] - snap[m]) / snap[m];
                if (rel > maxRelChange) maxRelChange = rel;
            } else if (sigCur[m] > 0.0) {
                anyFirstTime = true;
            }
        }
        // First-time snapshot always triggers (no prior baseline to compare against).
        if (!anyFirstTime && maxRelChange < REHEAPIFY_THRESHOLD) return;

        long t0 = System.nanoTime();
        PriorityQueue<Candidate> oldHeap = heaps[threadIdx];
        Candidate[] arr = oldHeap.toArray(new Candidate[0]);
        for (Candidate c : arr) {
            double[] v = c.vector;
            double newScore = 0.0;
            for (int m = 0; m < M; m++) {
                if (sigCur[m] == 0.0) continue;
                double x = v[m];
                if (Double.isNaN(x)) continue;
                double z = (x - muT[m]) / sigCur[m];
                double z2 = z * z;
                if (z2 > newScore) newScore = z2;
            }
            c.value = newScore;
        }
        PriorityQueue<Candidate> rebuilt = new PriorityQueue<>(k + 1, MIN_BY_SCORE);
        Collections.addAll(rebuilt, arr);
        heaps[threadIdx] = rebuilt;

        // Update snapshot for next drift comparison.
        System.arraycopy(sigCur, 0, snap, 0, M);
        reheapifyCount[threadIdx]++;
        reheapifyNanos[threadIdx] += System.nanoTime() - t0;
        if (maxRelChange > maxSigmaChange[threadIdx]) {
            maxSigmaChange[threadIdx] = maxRelChange;
        }
    }

    /** Total number of reheapifies across all threads (diagnostic). */
    public long getTotalReheapifies() {
        long t = 0L;
        for (long c : reheapifyCount) t += c;
        return t;
    }

    /** Total time spent reheapifying across scanner threads, in nanoseconds. */
    public long getTotalReheapifyNanos() {
        long t = 0L;
        for (long ns : reheapifyNanos) t += ns;
        return t;
    }

    /** Maximum number of reheapifies performed by a single scanner thread. */
    public long getMaxThreadReheapifies() {
        long max = 0L;
        for (long c : reheapifyCount) {
            if (c > max) max = c;
        }
        return max;
    }

    /** Largest relative σ change that triggered a reheapify, as a percentage. */
    public double getMaxReheapifySigmaChangePct() {
        double max = 0.0;
        for (double rel : maxSigmaChange) {
            if (rel > max) max = rel;
        }
        return max * 100.0;
    }

    /**
     * Phase 2: merge per-thread heaps into a single candidate pool.  Since
     * Phase 1 now offers each CSV row exactly once to exactly one per-thread
     * heap, byte offsets are already unique; the old hash-based deduplication
     * was only needed when the same row could be present in multiple per-
     * measure heaps.  Per-thread heap arrays are released eagerly.
     */
    public List<Candidate> mergeCandidates() {
        int totalCandidates = 0;
        for (int t = 0; t < numThreads; t++) {
            totalCandidates += heaps[t].size();
        }
        List<Candidate> pool = new ArrayList<>(totalCandidates);
        for (int t = 0; t < numThreads; t++) {
            pool.addAll(heaps[t]);
        }
        // Free heap memory eagerly — vectors are now owned by `pool`.
        Arrays.fill(heaps, null);
        long totalReheap = getTotalReheapifies();
        LOG.info("OutlierIndex Phase 2 merge: {} candidates from {} per-thread heaps; {} reheapifies total",
                pool.size(), numThreads, totalReheap);
        return pool;
    }

    /**
     * Phase 3: single-pass score-based selection using <i>exact</i> global
     * stats (passed in from {@link Valinor#computeGlobalMeasureStats()}).
     *
     * <p>Each candidate is assigned its true outlierness score equal to the
     * maximum over measures of its squared z-score:
     * <pre>
     *   score(c) = max_m  ((c.vector[m] − μ_m) / σ_m)²
     * </pre>
     * The top-K candidates by exact score are retained.  This re-scoring
     * step corrects any approximation introduced during Phase-1 admission
     * (which used per-thread <i>running</i> stats).
     *
     * <p>Complexity: O(|pool| · M) for scoring + O(|pool| · log K) for the
     * size-K min-heap top-K extraction.
     *
     * @param globalStats per-measure global StatsAccumulator (must already
     *                    aggregate every leaf tile's stats)
    * @param pool        candidate pool from {@link #mergeCandidates}
     */
    public void selectByScore(StatsAccumulator[] globalStats, List<Candidate> pool) {
        final int M = measureCount;

        // 1) Cache (mean, std) per measure for z-score normalization.
        double[] mean = new double[M];
        double[] std  = new double[M];
        for (int m = 0; m < M; m++) {
            StatsAccumulator s = globalStats[m];
            if (s == null || s.count() < 2) {
                mean[m] = 0;
                std[m] = 0;
                continue;
            }
            mean[m] = s.mean();
            std[m]  = s.sampleStandardDeviation();
        }

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

        // 3) Top-K extraction via bounded primitive-int min-heap of pool indices
        //    (fastutil — no Integer boxing, no per-offer allocation).  Heap root
        //    = currently smallest-score retained candidate.
        final double[] scoresRef = scores;
        IntComparator cmp = (a, b) -> Double.compare(scoresRef[a], scoresRef[b]);
        IntHeapPriorityQueue topK = new IntHeapPriorityQueue(Math.min(k, poolSize) + 1, cmp);
        for (int p = 0; p < poolSize; p++) {
            if (topK.size() < k) {
                topK.enqueue(p);
            } else if (scoresRef[p] > scoresRef[topK.firstInt()]) {
                topK.dequeueInt();
                topK.enqueue(p);
            }
        }

        // 4) Materialize selected list, offsetToOutlierIdx, outlierMatrix.
        //    Iteration order is heap order, which is fine (no semantic meaning).
        final int kept = topK.size();
        selected = new ArrayList<>(kept);
        offsetToOutlierIdx = new Long2IntOpenHashMap(kept);
        offsetToOutlierIdx.defaultReturnValue(-1);
        outlierMatrix = new double[kept][];
        for (int idx = 0; idx < kept; idx++) {
            int p = topK.dequeueInt();
            Candidate c = pool.get(p);
            selected.add(c);
            offsetToOutlierIdx.put(c.byteOffset, idx);
            outlierMatrix[idx] = c.vector;
        }
        LOG.info("OutlierIndex Phase 3 score-based: selected {}/{} outliers from pool of {}",
                selected.size(), k, poolSize);
    }

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
        // Snapshot tiles into a List (parallelStream needs a sized source) and
        // run the per-tile scan in parallel.  Each tile owns disjoint state
        // (its own bitset and idx array), so no cross-tile synchronization is
        // required; only the global counter uses LongAdder.
        List<Tile> tiles = new ArrayList<>();
        for (Object obj : leafTiles) tiles.add((Tile) obj);
        LongAdder totalAssigned = new LongAdder();
        // Read-only snapshot of the lookup map; Long2IntOpenHashMap.get is
        // safe for concurrent reads as long as no thread is mutating it,
        // which is the case after selectByScore returns.
        final Long2IntOpenHashMap idxMap = offsetToOutlierIdx;
        tiles.parallelStream().forEach(tile -> {
            if (!tile.hasPoints()) return;
            int n = tile.getSize();
            java.util.BitSet bits = null;
            int[] idxs = null;
            int local = 0;
            for (int i = 0; i < n; i++) {
                int outIdx = idxMap.get(tile.getOffset(i));
                if (outIdx >= 0) {
                    if (bits == null) {
                        bits = new java.util.BitSet(n);
                        idxs = new int[n];
                        Arrays.fill(idxs, -1);
                    }
                    bits.set(i);
                    if (idxs == null) {
                        throw new IllegalStateException("Outlier index array was not initialized");
                    }
                    idxs[i] = outIdx;
                    local++;
                }
            }
            if (bits != null) {
                tile.setOutlierData(bits, idxs);
                totalAssigned.add(local);
            }
        });
        LOG.info("OutlierIndex Phase 5 partition: assigned {}/{} outliers to tiles ({} leaf tiles, parallel)",
                totalAssigned.sum(), selected.size(), tiles.size());
    }

    /**
     * Returns the per-measure value of the outlier at the given global
     * outlier index, or NaN if the outlier had no value for that measure.
     */
    public double getOutlierValue(int outlierIdx, int measureIdx) {
        return outlierMatrix[outlierIdx][measureIdx];
    }
}
