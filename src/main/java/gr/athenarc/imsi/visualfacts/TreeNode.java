package gr.athenarc.imsi.visualfacts;

import com.google.common.math.PairedStatsAccumulator;
import com.google.common.math.StatsAccumulator;
import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TreeNode {

    private static int counter;

    private final short label;
    protected List<Point> points;
    private Short2ObjectMap<TreeNode> children;

    private PairedStatsAccumulator stats;

    public TreeNode(short label) {
        this.label = label;
        counter++;
    }

    public static int getInstanceCount() {
        return counter;
    }

    public void adjustStats(float value0, float value1) {
        if (stats == null) {
            stats = new PairedStatsAccumulator();
        }
        stats.add(value0, value1);
    }

    public boolean hasStats() {
        return stats != null;
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

    public PairedStatsAccumulator getStats() {
        return stats;
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
                ", stats=" + stats +
                '}';
    }

    public void convertToNonleaf() {
        points = null;
        stats = null;
    }

}
