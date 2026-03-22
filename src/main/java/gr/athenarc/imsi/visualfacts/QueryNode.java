package gr.athenarc.imsi.visualfacts;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.math.StatsAccumulator;

import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;

public class QueryNode {
    private static final Logger LOG = LogManager.getLogger(QueryNode.class);
    private Tile tile;
    private ContainmentExaminer containmentExaminer;
    private Schema schema;

    public int intersectionCount = 0;

    // BitSet for tracking which points are inside the query
    private BitSet queryPointsBitSet;

    // Sampling-based statistics for multiple measures
    private Map<Integer, StatsAccumulator> sampleStatsAccumulators;
    private BitSet sampledTracker;

    public QueryNode(Tile tile, ContainmentExaminer containmentExaminer, Query query, Schema schema) {
        this.tile = tile;
        this.containmentExaminer = containmentExaminer;
        this.schema = schema;

        // Initialize StatsAccumulators for each measure
        this.sampleStatsAccumulators = new HashMap<>();
        for (Integer measure : query.getMeasureCols()) {
            this.sampleStatsAccumulators.put(measure, new StatsAccumulator());
        }

        // Initialize BitSet with the size of points in the tile
        if (tile.getSampledTracker() != null && containmentExaminer == null) {
            // Add stats for each measure from the tile's stats
            for (Integer measure : query.getMeasureCols()) {
                StatsAccumulator tileStats = tile.getStats(schema.getMeasureIndex(measure));
                if (tileStats != null) {
                    StatsAccumulator accumulator = sampleStatsAccumulators.get(measure);
                    accumulator.addAll(tileStats.snapshot());
                }
            }
            this.sampledTracker = tile.getSampledTracker();
        } else {
            this.sampledTracker = new BitSet(tile.getSize());
        }
        computeQueryIntersection();
    }

    /**
     * Iterates over the points in the tile, checks against the containment
     * examiner, and sets up the BitSet marking points inside the query.
     */
    private void computeQueryIntersection() {
        int size = tile.getSize();
        queryPointsBitSet = new BitSet(size);

        // If the tile is fully contained, all points belong to the query
        if (containmentExaminer == null) {
            queryPointsBitSet.set(0, size);
            intersectionCount = size;
            return;
        }

        // Otherwise, check containment for each point using flat array access
        intersectionCount = 0;
        for (int i = 0; i < size; i++) {
            if (containmentExaminer.contains(tile.getX(i), tile.getY(i))) {
                queryPointsBitSet.set(i);
                intersectionCount++;
            }
        }
    }

    public void addSampleValue(int measureCol, double value) {
        sampleStatsAccumulators.get(measureCol).add(value);
    }

    public Tile getTile() {
        return tile;
    }

    public ContainmentExaminer getContainmentExaminer() {
        return containmentExaminer;
    }

    public boolean isFullyContained() {
        return containmentExaminer == null;
    }

    public int getIntersectionCount() {
        return intersectionCount;
    }

    public StatsAccumulator getSampleStatsAcc(int measureCol) {
        return sampleStatsAccumulators.get(measureCol);
    }

    public BitSet getSampledTracker() {
        return sampledTracker;
    }

    @Override
    public String toString() {
        return "QueryNode [tile=" + tile + ", containmentExaminer=" + containmentExaminer
                + ", intersectionCount=" + intersectionCount + ", queryPointsBitSet=" + queryPointsBitSet
                + ", sampleStatsAccumulators=" + sampleStatsAccumulators
                + ", sampledTracker=" + sampledTracker + "]";
    }

    public BitSet getQueryPointsBitSet() {
        return queryPointsBitSet;
    }
}