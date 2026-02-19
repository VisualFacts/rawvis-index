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

    // Point data: references a slice [start, start+size) of a SharedPointStore.
    // During Phase 1 (counting), store is null and only size is used (via incrementCount).
    // After Phase 3 (wiring), store is set and getX/getY/getOffset delegate to it.
    private SharedPointStore store;
    private int start;
    private int size;

    private Short2ObjectMap<TreeNode> children;
    private StatsAccumulator[] statsArray;
    private int[] statsPointCount; // per-measure count of processed points (incl. NaN)
    private BitSet sampledTracker;
    
    public TreeNode(short label) {
        this.label = label;
        counter++;
    }

    public static int getInstanceCount() {
        return counter;
    }

    public void adjustStats(int measureIndex, int measureCount, double value) {
        // Always count this point as processed, even if NaN.
        // This lets hasStats() distinguish "fully accumulated" from "only sampled."
        if (statsPointCount == null) {
            statsPointCount = new int[measureCount];
        }
        statsPointCount[measureIndex]++;

        if (Double.isNaN(value)) return;
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
        if (size == 0 || statsPointCount == null) {
            return false;
        }
        if (measureIndex < 0 || measureIndex >= statsPointCount.length) {
            return false;
        }
        // Check that every point has been processed for this measure,
        // not just the non-NaN ones. statsPointCount is incremented for
        // every point (including NaN), while stats.count() only reflects
        // non-NaN values.
        return statsPointCount[measureIndex] == size;
    }

    // ---- Phase 1: counting ----

    /**
     * Phase 1: just count, no array allocation.
     */
    public void incrementCount() {
        size++;
    }

    // ---- Slice wiring (Phase 3) ----

    /**
     * Wires this node to a slice of the shared point store.
     * Called after the global partition (or sub-partition during split)
     * has placed this tile's points at contiguous positions [start, start+size).
     */
    public void setSlice(SharedPointStore store, int start, int size) {
        this.store = store;
        this.start = start;
        this.size = size;
    }

    // ---- Indexed access ----

    public double getX(int i) { return store.getX(start + i); }
    public double getY(int i) { return store.getY(start + i); }
    public long getOffset(int i) { return store.getOffset(start + i); }
    public int getSize() { return size; }
    public int getStart() { return start; }
    public SharedPointStore getStore() { return store; }

    /** Returns true if this node has point data wired to a shared store. */
    public boolean hasPoints() { return store != null && size > 0; }

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
        store = null;
        start = 0;
        size = 0;
        statsArray = null;
        statsPointCount = null;
    }

    public StatsAccumulator[] getStatsArray() {
        return statsArray;
    }

    public int[] getStatsPointCount() {
        return statsPointCount;
    }

    public BitSet getSampledTracker() {
        return sampledTracker;
    }

    public void setSampledTracker(BitSet sampledTracker) {
        this.sampledTracker = sampledTracker;
    }

}
