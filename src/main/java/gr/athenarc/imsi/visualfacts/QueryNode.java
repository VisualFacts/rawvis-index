package gr.athenarc.imsi.visualfacts;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.math.StatsAccumulator;

import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;

public class QueryNode {
    private Tile tile;
    private ContainmentExaminer containmentExaminer;

    public int intersectionCount = 0;

    // BitSet for tracking which points are inside the query
    private BitSet queryPointsBitSet;

    // Sampling-based statistics for multiple measures
    private Map<Integer, StatsAccumulator> sampleStatsAccumulators;
    private BitSet sampledTracker;

    // ---- Outlier-aware AQP support (set only when the tile has outliers) ----
    //
    // {@code inQueryOutliers} is the intersection of {@code tile.outlierBitSet}
    // with {@code queryPointsBitSet} \u2014 i.e. the positions in this tile that
    // are BOTH outliers AND inside the query.  Those positions are removed
    // from the sampling pool (their bits are pre-cleared from
    // {@code queryPointsBitSet}) and {@link #intersectionCount} is reduced
    // accordingly.  Their per-measure contributions are added back as exact
    // (closed-form) sums by the CI computation.
    //
    // When the tile has no outliers, this field is null and the rest of the
    // class behaves byte-identically to the pre-outlier code path.
    private BitSet inQueryOutliers;

    public QueryNode(Tile tile, ContainmentExaminer containmentExaminer, Query query, Schema schema) {
        this.tile = tile;
        this.containmentExaminer = containmentExaminer;

        // Initialize StatsAccumulators for each schema measure. Approximate
        // sampling reads all schema measures so fully-contained tile stats can
        // keep adapting even when a query asks for only a subset of measures.
        this.sampleStatsAccumulators = new HashMap<>();
        for (Integer measure : schema.getMeasureCols()) {
            this.sampleStatsAccumulators.put(measure, new StatsAccumulator());
        }

        // Initialize BitSet with the size of points in the tile
        boolean fullyContained = containmentExaminer == null;
        boolean hasReusableSamples = (tile.getSampledTracker() != null && fullyContained);
        boolean hasCompleteStats = fullyContained && query.getMeasureCols().stream()
            .allMatch(measure -> tile.hasStats(schema.getMeasureIndex(measure)));
        if (hasReusableSamples) {
            // Add stats for each measure from the tile's cached sample stats.
            for (Integer measure : schema.getMeasureCols()) {
                StatsAccumulator tileStats = tile.getStats(schema.getMeasureIndex(measure));
                if (tileStats != null) {
                    StatsAccumulator accumulator = sampleStatsAccumulators.get(measure);
                    accumulator.addAll(Objects.requireNonNull(tileStats.snapshot()));
                }
            }
            this.sampledTracker = tile.getSampledTracker();
        } else {
            this.sampledTracker = new BitSet(tile.getSize());
        }
        computeQueryIntersection();
        // Outlier removal is only meaningful for nodes that will be sampled.
        // FC+stats nodes contribute via tile.getStats() (which already includes
        // outlier values, since tile-level stats were built from the FULL
        // population at scan time) and bypass the sample-based estimator.
        // Touching them would corrupt totalCount (intersectionCount is read by
        // executeApproximateQuery for FC+stats nodes too) without any benefit.
        if (!hasCompleteStats) {
            applyOutlierRemoval();
        }
    }

    /**
     * Outlier-aware AQP: if this tile has selected outliers, pre-remove
     * the in-query subset from {@link #queryPointsBitSet} and reduce
     * {@link #intersectionCount} accordingly.  The removed positions are
     * remembered in {@link #inQueryOutliers} so that the CI computation can
     * add their exact contributions back as a closed-form sum.
     *
     * <p>Important: when the tile has frozen stats or this node is the
     * "fully-contained-with-stats" branch, the existing tile.statsArray
     * already aggregated outlier values at scan time; no adjustment is
     * needed there.  Outlier removal only affects the sampling estimator,
     * not the exact-stats short-circuit.
     */
    private void applyOutlierRemoval() {
        BitSet tileOutliers = tile.getOutlierBitSet();
        if (tileOutliers == null || tileOutliers.isEmpty()) return;
        BitSet inQuery = (BitSet) tileOutliers.clone();
        inQuery.and(queryPointsBitSet);
        if (inQuery.isEmpty()) return;
        this.inQueryOutliers = inQuery;
        // Remove outlier bits from the sampling-eligible population.  Both
        // selection paths in SamplingNodePointsIterator skip outliers via
        // standard set algebra:
        //   \u2022 Fisher-Yates path: enumerates queryPointsBitSet \\ sampledTracker,
        //     so outliers (already removed below) cannot be picked.
        //   \u2022 Rejection path: explicitly checks tile.getOutlierBitSet() in
        //     SamplingNodePointsIterator (since it samples uniformly in
        //     [0, tileSize) without consulting queryPointsBitSet).
        // sampledTracker is intentionally NOT polluted with outlier bits; the
        // true sample count (m') is computed against the trimmed query bitset.
        queryPointsBitSet.andNot(inQuery);
        intersectionCount -= inQuery.cardinality();
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

    public int getSampledPointCount() {
        BitSet sampledInQuery = (BitSet) sampledTracker.clone();
        sampledInQuery.and(queryPointsBitSet);
        return sampledInQuery.cardinality();
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

    /**
     * Returns the bitset of in-query outlier positions for this tile, or null
     * if the tile contains no outliers (or none fall inside the query).
     * The CI computation iterates over these bits to add the deterministic
     * (closed-form) outlier sums to the sampling estimator.
     */
    public BitSet getInQueryOutliers() {
        return inQueryOutliers;
    }
}