package gr.athenarc.imsi.visualfacts;

import java.util.Arrays;

/**
 * Shared backing store for point data (x, y, file-offset).
 * All tiles reference slices of the same arrays, avoiding per-tile duplication.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>Phase 1 (CSV scan): points written sequentially via {@link #set(int, double, double, long)}</li>
 *   <li>Phase 1.5: prefix-sums computed from per-tile counts</li>
 *   <li>Phase 2 ({@link #partition}): per-array sequential scatter (stable, cache-friendly)</li>
 *   <li>Phase 3: each tile wired to its slice via {@link TreeNode#setSlice}</li>
 * </ol>
 */
public class SharedPointStore {

    private double[] xs;
    private double[] ys;
    private long[] offsets;
    private int capacity;

    public SharedPointStore(int capacity) {
        this.capacity = capacity;
        this.xs = new double[capacity];
        this.ys = new double[capacity];
        this.offsets = new long[capacity];
    }

    public int getCapacity() { return capacity; }

    public double getX(int i) { return xs[i]; }
    public double getY(int i) { return ys[i]; }
    public long getOffset(int i) { return offsets[i]; }

    /** Writes a point at position {@code i} (used during Phase 1 sequential scan). */
    public void set(int i, double x, double y, long offset) {
        xs[i] = x;
        ys[i] = y;
        offsets[i] = offset;
    }

    /**
     * Stable per-array sequential scatter partition.
     * After completion, tile {@code t}'s points occupy
     * {@code [starts[t], starts[t] + counts[t])}.
     * <p>
     * Scatters one array at a time: sequential read of the source array,
     * writing to per-tile cursor positions. Between passes the old array
     * is replaced and becomes eligible for GC, so at most one old + one new
     * array of the same type coexist.
     * <p>
     * Stable: elements within each tile preserve their original CSV/file order,
     * so no subsequent sort-by-offset is needed.
     *
     * @param tileIds   per-point tile ID (read-only during scatter)
     * @param n         number of valid points (elements [0, n) are partitioned)
     * @param starts    prefix-sum array: starts[t] = first index for tile t
     * @param numTiles  number of distinct tiles
     */
    public void partition(int[] tileIds, int n, int[] starts, int numTiles) {
        int[] cursors;

        // Pass 1: scatter xs
        cursors = Arrays.copyOf(starts, numTiles);
        double[] newXs = new double[n];
        for (int i = 0; i < n; i++) {
            newXs[cursors[tileIds[i]]++] = xs[i];
        }
        this.xs = newXs; // old xs[] now unreachable

        // Pass 2: scatter ys
        cursors = Arrays.copyOf(starts, numTiles);
        double[] newYs = new double[n];
        for (int i = 0; i < n; i++) {
            newYs[cursors[tileIds[i]]++] = ys[i];
        }
        this.ys = newYs; // old ys[] now unreachable

        // Pass 3: scatter offsets
        cursors = Arrays.copyOf(starts, numTiles);
        long[] newOffsets = new long[n];
        for (int i = 0; i < n; i++) {
            newOffsets[cursors[tileIds[i]]++] = offsets[i];
        }
        this.offsets = newOffsets; // old offsets[] now unreachable

        this.capacity = n;
    }

    /**
     * Sub-partitions a slice {@code [sliceStart, sliceStart+sliceLen)} in-place.
     * Used by {@link QuadTreeTile#split()} to partition a parent's slice
     * among quadrant children. Uses cycle chasing (only ~thousands of elements,
     * so cache effects are negligible).
     *
     * @param sliceStart start index of the slice in the shared arrays
     * @param sliceLen   number of elements in the slice
     * @param subIds     per-element sub-partition ID (0..numParts-1), length = sliceLen;
     *                   overwritten during partition
     * @param subStarts  absolute start indices for each sub-partition
     *                   (pre-computed as prefix sums relative to sliceStart)
     * @param numParts   number of sub-partitions (typically 4)
     */
    public void subPartition(int sliceStart, int sliceLen, int[] subIds, int[] subStarts, int numParts) {
        // Convert subIds to absolute target positions
        int[] cursors = Arrays.copyOf(subStarts, numParts);
        for (int i = 0; i < sliceLen; i++) {
            subIds[i] = cursors[subIds[i]]++;
        }
        // Apply permutation in-place via cycle chasing
        for (int i = 0; i < sliceLen; i++) {
            int absI = sliceStart + i;
            while (subIds[i] != absI) {
                int absJ = subIds[i];
                int j = absJ - sliceStart;
                swap(absI, absJ);
                int tmp = subIds[i]; subIds[i] = subIds[j]; subIds[j] = tmp;
            }
        }
    }

    /** Swaps elements at absolute positions a and b across all three arrays. */
    private void swap(int a, int b) {
        double tx = xs[a]; xs[a] = xs[b]; xs[b] = tx;
        double ty = ys[a]; ys[a] = ys[b]; ys[b] = ty;
        long to = offsets[a]; offsets[a] = offsets[b]; offsets[b] = to;
    }
}
