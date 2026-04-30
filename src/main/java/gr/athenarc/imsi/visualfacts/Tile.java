package gr.athenarc.imsi.visualfacts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.math.Stats;
import com.google.common.math.StatsAccumulator;

import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;

public abstract class Tile {

    private static final Logger LOG = LogManager.getLogger(Tile.class);

    protected Rectangle bounds;

    // ---- Point data ----
    // References a slice [start, start+size) of a SharedPointStore.
    // During Phase 1 (counting), store is null and only size is used (via incrementCount/setSize).
    // After Phase 3 (wiring), store is set and getX/getY/getOffset delegate to it.
    private SharedPointStore store;
    private int start;
    private int size;

    private StatsAccumulator[] statsArray;
    private int[] statsPointCount; // per-measure count of processed points (incl. NaN)
    private BitSet sampledTracker;

    // ---- Outlier-aware AQP support (set only when IndexConfig.OUTLIER_K > 0) ----
    //
    // outlierBitSet[i] is true if the i-th row of this tile (i.e., position
    // [start+i] in the shared store) was selected as a global outlier.
    // outlierIdxs[i] is the corresponding index into the global outlier value
    // matrix, or -1 if the row is not an outlier.  Both are null for tiles
    // that contain zero outliers (the common case for a small global K), so
    // tiles with no outliers pay zero memory.
    private BitSet outlierBitSet;
    private int[] outlierIdxs;

    /**
     * Frozen stats from a tile that has been split. These are exact aggregate
     * statistics captured before the split destroyed the point data.
     * Only set when ALL measures had complete stats (count == pointCount).
     */
    private Stats[] frozenStats;
    private int frozenPointCount;


    public Tile(Rectangle bounds) {
        this.bounds = bounds;
    }

    public abstract Tile getLeafTile(double x, double y);

    public Rectangle getBounds() {
        return bounds;
    }

    public abstract List getLeafTiles();

    public abstract List<Tile> getOverlappedLeafTiles(Query query);

    /**
     * Like getOverlappedLeafTiles but always recurses to actual leaf tiles,
     * ignoring the frozen-stats short-circuit. Use this after split() to
     * ensure children are visited even if the parent has frozen stats.
     */
    public abstract List<Tile> getOverlappedActualLeafTiles(Query query);

    public abstract void split();

    public abstract int getMaxDepth();

    public abstract int getLeafTileCount();

    // ---- Point data methods ----

    public void adjustStats(int measureIndex, int measureCount, double value) {
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
     * Checks if this tile has complete statistics for a specific measure.
     *
     * @param measureIndex the measure to check statistics for
     * @return {@code true} if every point has been processed for this measure
     */
    public boolean hasStats(int measureIndex) {
        if (size == 0 || statsPointCount == null) {
            return false;
        }
        if (measureIndex < 0 || measureIndex >= statsPointCount.length) {
            return false;
        }
        return statsPointCount[measureIndex] == size;
    }

    /** Phase 1: just count, no array allocation. */
    public void incrementCount() {
        size++;
    }

    /**
     * Sets the point count directly (used by parallel scanner merge, which
     * computes per-tile counts across all threads and applies them in bulk).
     */
    public void setSize(int count) {
        this.size = count;
    }

    /**
     * Bulk-sets pre-built statistics from the parallel scanner.
     * Replaces any existing statsArray and statsPointCount.
     */
    public void setPrebuiltStats(StatsAccumulator[] stats, int[] pointCounts) {
        this.statsArray = stats;
        this.statsPointCount = pointCounts;
    }

    /**
     * Wires this tile to a slice of the shared point store.
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

    /** Returns true if this tile has point data wired to a shared store. */
    public boolean hasPoints() { return store != null && size > 0; }

    public StatsAccumulator getStats(int measureIndex) {
        if (statsArray == null || measureIndex < 0 || measureIndex >= statsArray.length) {
            return null;
        }
        return statsArray[measureIndex];
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

    /**
     * Outlier-aware AQP: bit i is set iff the i-th row of this tile was
     * selected by {@link OutlierIndex} as a global outlier.  Returns
     * {@code null} if the tile contains zero outliers (the common case when
     * the K-cap is small relative to the dataset).
     */
    public BitSet getOutlierBitSet() {
        return outlierBitSet;
    }

    /**
     * Outlier-aware AQP: for tiles with at least one outlier,
     * {@code outlierIdxs[localPos]} returns the index into the global
     * outlier value matrix (or -1 for non-outlier rows).  Returns
     * {@code null} if the tile contains zero outliers.
     */
    public int[] getOutlierIdxs() {
        return outlierIdxs;
    }

    /** Set by {@link OutlierIndex#partitionByTile}; tiles without outliers leave both fields null. */
    public void setOutlierData(BitSet outlierBitSet, int[] outlierIdxs) {
        this.outlierBitSet = outlierBitSet;
        this.outlierIdxs = outlierIdxs;
    }

    /**
     * Clears point data when this tile becomes a non-leaf (after split).
     * The parent's points have been sub-partitioned into children.
     */
    public void clearPointData() {
        store = null;
        start = 0;
        size = 0;
        statsArray = null;
        statsPointCount = null;
    }

    // ---- Query node creation ----

    /**
     * Returns a single QueryNode wrapping this tile for the given query.
     */
    public List<QueryNode> getQueryNodes(Query query, ContainmentExaminer containmentExaminer, Schema schema) {
        List<QueryNode> queryNodes = new ArrayList<>();
        if (hasPoints()) {
            queryNodes.add(new QueryNode(this, containmentExaminer, query, schema));
        }
        return queryNodes;
    }

    // ---- Frozen stats (for split short-circuit) ----

    /**
     * Freezes the current tile's complete stats before splitting.
     * Only freezes when:
     * - tile has point data
     * - ALL measures have complete stats (count == point count)
     *
     * This enables short-circuiting subtree traversal for future queries
     * that fully contain this tile.
     */
    public void freezeStats() {
        if (!hasPoints()) return;

        if (statsPointCount == null || statsPointCount.length == 0) return;

        int pointCount = size;
        int measureCount = statsPointCount.length;
        Stats[] candidate = new Stats[measureCount];
        for (int i = 0; i < measureCount; i++) {
            if (statsPointCount[i] != pointCount) {
                return; // Not all points processed for this measure — don't freeze
            }
            candidate[i] = (statsArray != null && i < statsArray.length && statsArray[i] != null)
                    ? statsArray[i].snapshot()
                    : Stats.of(); // empty stats: count=0, sum=0
        }
        frozenStats = candidate;
        frozenPointCount = pointCount;
        LOG.trace("Froze exact stats for tile {} ({} points, {} measures)",
                bounds, pointCount, statsPointCount.length);
    }

    /**
     * Returns true if this tile has frozen exact stats from a prior split.
     * When true, all measures are guaranteed to have complete stats.
     */
    public boolean hasFrozenStats() {
        return frozenStats != null;
    }

    /**
     * Returns the frozen Stats snapshot for the given measure index,
     * or null if not available.
     */
    public Stats getFrozenStats(int measureIndex) {
        if (frozenStats == null || measureIndex < 0 || measureIndex >= frozenStats.length) {
            return null;
        }
        return frozenStats[measureIndex];
    }

    public int getFrozenPointCount() {
        return frozenPointCount;
    }

    @Override
    public String toString() {
        return "Tile{" +
                "bounds=" + bounds +
                ", size=" + size +
                ", stats=" + Arrays.toString(statsArray) +
                ", frozenStats=" + (frozenStats != null ? "yes(" + frozenPointCount + " pts)" : "no") +
                '}';
    }
}