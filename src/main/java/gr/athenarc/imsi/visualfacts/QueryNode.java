package gr.athenarc.imsi.visualfacts;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;
import com.google.common.math.StatsAccumulator;

public class QueryNode implements Iterable<Point> {
    private static final Logger LOG = LogManager.getLogger(QueryNode.class);

    private Map<Integer, Short> groupByValues;
    private TreeNode node;
    private Tile tile;
    private ContainmentExaminer containmentExaminer;
    private List<CategoricalColumn> unknownCatAttrs;

    public int intersectionCount = 0;

    // BitSet for tracking which points are inside the query
    private BitSet queryPointsBitSet;

    // Sampling-based statistics
    private StatsAccumulator sampleStatsAcc = new StatsAccumulator();
    private BitSet sampledTracker;

    public QueryNode(TreeNode node, Tile tile, ContainmentExaminer containmentExaminer,
            Map<Integer, Short> groupByValues, List<CategoricalColumn> unknownCatAttrs) {
        this.groupByValues = groupByValues;
        this.node = node;
        this.tile = tile;
        this.containmentExaminer = containmentExaminer;
        this.unknownCatAttrs = unknownCatAttrs;

        // Initialize BitSet with the size of points in the node
        this.sampledTracker = new BitSet(node.getPoints().size()); // All bits default to false (unsampled)

        computeQueryIntersection();
    }

    /**
     * Iterates over the points in the tile, checks against the containment
     * examiner,
     * and sets up the BitSet marking points inside the query.
     */
    private void computeQueryIntersection() {
        List<Point> points = node.getPoints();
        queryPointsBitSet = new BitSet(points.size());

        // If the tile is fully contained, all points belong to the query
        if (containmentExaminer == null) {
            queryPointsBitSet.set(0, points.size()); // Mark all points
            intersectionCount = points.size();
            return;
        }

        // Otherwise, check containment for each point
        intersectionCount = 0;
        for (int i = 0; i < points.size(); i++) {
            if (containmentExaminer.contains(points.get(i))) {
                queryPointsBitSet.set(i); // Mark this point as inside the query
                intersectionCount++;
            }
        }
    }

    public void addSampleValue(double value) {
        sampleStatsAcc.add(value);
    }

    public Map<Integer, Short> getGroupByValues() {
        return groupByValues;
    }

    public TreeNode getNode() {
        return node;
    }

    public Tile getTile() {
        return tile;
    }

    public ContainmentExaminer getContainmentExaminer() {
        return containmentExaminer;
    }

    public List<CategoricalColumn> getUnknownCatAttrs() {
        return unknownCatAttrs;
    }

    public boolean isFullyContained() {
        return containmentExaminer == null;
    }

    public int getIntersectionCount() {
        return intersectionCount;
    }

    public StatsAccumulator getSampleStatsAcc() {
        return sampleStatsAcc;
    }

    public BitSet getSampledTracker() {
        return sampledTracker;
    }

    @Override
    public Iterator<Point> iterator() {
        return new NodePointsIterator(this);
    }

    @Override
    public String toString() {
        return "QueryNode [node=" + node + ", tile=" + tile + ", containmentExaminer=" + containmentExaminer
                + ", intersectionCount=" + intersectionCount + ", queryPointsBitSet=" + queryPointsBitSet
                + ", sampleStatsAcc.count=" + sampleStatsAcc.count() + ", sampleStatsAcc.sum=" + sampleStatsAcc.sum()
                + ", sampledTracker=" + sampledTracker + "]";
    }

    public BitSet getQueryPointsBitSet() {
        return queryPointsBitSet;
    }
}