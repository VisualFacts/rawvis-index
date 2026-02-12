package gr.athenarc.imsi.visualfacts;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

import com.google.common.math.StatsAccumulator;

import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;

public class TreeNode {

    private static int counter;

    private final short label;
    protected List<Point> points;
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
     *         to the size of the points list, otherwise {@code false}.
     */
    public boolean hasStats(int measureIndex) {
        if (points == null || statsArray == null) {
            return false;
        }
        if (measureIndex < 0 || measureIndex >= statsArray.length) {
            return false;
        }
        StatsAccumulator stats = statsArray[measureIndex];
        // todo: check what happens in case of null value for an object
        return stats != null && stats.count() == points.size();
    }

    public TreeNode addPoint(Point point) {
        if (points == null) {
            points = new ArrayList<>();
        }
        points.add(point);
        return this;
    }

    public List<Point> getPoints() {
        return points;
    }

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
                ", children=" + children +
                ", statsArray=" + java.util.Arrays.toString(statsArray) +
                '}';
    }

    public void convertToNonleaf() {
        points = null;
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
