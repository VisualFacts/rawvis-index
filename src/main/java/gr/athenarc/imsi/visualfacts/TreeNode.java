package gr.athenarc.imsi.visualfacts;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;

import com.google.common.math.StatsAccumulator;

import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;

public class TreeNode {

    private static int counter;

    private final short label;

    // Flat parallel arrays for point storage — replaces List<Point>.
    // During init: two-phase (incrementCount → allocateExact → insertAtCursor).
    // During split: count per quadrant → setPoints with exact size → insertAtCursor.
    private float[] xs;
    private float[] ys;
    private long[] offsets;
    private int size;

    private Short2ObjectMap<TreeNode> children;
    private StatsAccumulator[] statsArray;
    private BitSet sampledTracker;
    
    public TreeNode(short label) {
        this.label = label;
        counter++;
    }

    public static int getInstanceCount() {
        return counter;
    }

    public void adjustStats(int measureIndex, int measureCount, double value) {
        if (statsArray == null) {
            statsArray = new StatsAccumulator[measureCount];
        }
        StatsAccumulator stats = statsArray[measureIndex];
        if (stats == null) {
            stats = new StatsAccumulator();
            statsArray[measureIndex] = stats;
        }
        stats.add(value);
    }

    /**
     * Checks if the current TreeNode has statistics available for a specific measure.
     *
     * @param measure the measure to check statistics for
     * @return {@code true} if the stats object for the given measure is not null and its count is equal
     *         to the number of points, otherwise {@code false}.
     */
    public boolean hasStats(int measureIndex) {
        if (size == 0 || statsArray == null) {
            return false;
        }
        if (measureIndex < 0 || measureIndex >= statsArray.length) {
            return false;
        }
        StatsAccumulator stats = statsArray[measureIndex];
        return stats != null && stats.count() == size;
    }

    // ---- Two-phase init support ----

    /**
     * Phase 1: just count, no array allocation.
     */
    public void incrementCount() {
        size++;
    }

    /**
     * Between phases: allocate exact-sized arrays based on accumulated count,
     * then reset size to 0 as a write cursor for phase 2.
     */
    public void allocateExact() {
        xs = new float[size];
        ys = new float[size];
        offsets = new long[size];
        size = 0;
    }

    /**
     * Phase 2: insert point at current cursor position. No bounds check.
     * Arrays must have been allocated via {@link #allocateExact()}.
     */
    public void insertAtCursor(float x, float y, long offset) {
        xs[size] = x;
        ys[size] = y;
        offsets[size] = offset;
        size++;
    }

    /**
     * Bulk-sets the point arrays with exact sizes. Used during split redistribute
     * where counts are known. Takes ownership of the passed arrays.
     * Set size=0 when using as a cursor target with insertAtCursor.
     */
    public void setPoints(float[] xs, float[] ys, long[] offsets, int size) {
        this.xs = xs;
        this.ys = ys;
        this.offsets = offsets;
        this.size = size;
    }

    // ---- Indexed access ----

    public float getX(int i) { return xs[i]; }
    public float getY(int i) { return ys[i]; }
    public long getOffset(int i) { return offsets[i]; }
    public int getSize() { return size; }

    /** Returns true if this node has point data. */
    public boolean hasPoints() { return xs != null && size > 0; }

    public StatsAccumulator getStats(int measureIndex) {
        if (statsArray == null || measureIndex < 0 || measureIndex >= statsArray.length) {
            return null;
        }
        return statsArray[measureIndex];
    }

    public TreeNode getChild(short label) {
        return children != null ? children.get(label) : null;
    }

    public TreeNode getOrAddChild(short label) {
        if (children == null) {
            children = new Short2ObjectOpenHashMap();
        }
        TreeNode child = getChild(label);
        if (child == null) {
            child = new TreeNode(label);
            children.put(label, child);
        }
        return child;
    }

    public short getLabel() {
        return label;
    }

    public Collection<TreeNode> getChildren() {
        return children == null ? null : children.values();
    }

    @Override
    public String toString() {
        return "TreeNode{" +
                "label=" + label +
                ", size=" + size +
                ", children=" + children +
                ", statsArray=" + Arrays.toString(statsArray) +
                '}';
    }

    public void convertToNonleaf() {
        xs = null;
        ys = null;
        offsets = null;
        size = 0;
        statsArray = null;
    }

    public StatsAccumulator[] getStatsArray() {
        return statsArray;
    }

    public BitSet getSampledTracker() {
        return sampledTracker;
    }

    public void setSampledTracker(BitSet sampledTracker) {
        this.sampledTracker = sampledTracker;
    }

}
