package gr.athenarc.imsi.visualfacts.query;

import com.google.common.collect.ImmutableList;
import com.google.common.math.PairedStats;
import com.google.common.math.PairedStatsAccumulator;
import gr.athenarc.imsi.visualfacts.Point;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryResults {

    private Query query;

    private Map<ImmutableList<String>, PairedStatsAccumulator> stats;

    //private Map<String, PairedStatsAccumulator> stats;

    private PairedStatsAccumulator rectStats;

    private List<Point> points;

    private int fullyContainedTileCount;

    private int tileCount;

    private int expandedNodeCount;

    private int ioCount;

    public QueryResults(Query query) {
        this.query = query;
        this.stats = new HashMap<>();
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public Map<ImmutableList<String>, PairedStats> getStats() {
        return stats.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().snapshot()));
    }

    public void adjustStats(ImmutableList<String> groupByValues, float measureValue0, float measureValue1) {
        stats.computeIfAbsent(groupByValues, (v) -> new PairedStatsAccumulator()).add(measureValue0, measureValue1);
    }

    public void adjustStats(ImmutableList<String> groupByValues, PairedStats stats) {
        this.stats.computeIfAbsent(groupByValues, (v) -> new PairedStatsAccumulator()).addAll(stats);
    }


    public int getFullyContainedTileCount() {
        return fullyContainedTileCount;
    }

    public void setFullyContainedTileCount(int fullyContainedTileCount) {
        this.fullyContainedTileCount = fullyContainedTileCount;
    }

    public int getTileCount() {
        return tileCount;
    }

    public void setTileCount(int tileCount) {
        this.tileCount = tileCount;
    }

    public int getIoCount() {
        return ioCount;
    }

    public void setIoCount(int ioCount) {
        this.ioCount = ioCount;
    }

    public int getExpandedNodeCount() {
        return expandedNodeCount;
    }

    public void setExpandedNodeCount(int expandedNodeCount) {
        this.expandedNodeCount = expandedNodeCount;
    }

    public List<Point> getPoints() {
        return points;
    }

    public void setPoints(List<Point> points) {
        this.points = points;
    }

    public PairedStatsAccumulator getRectStats() {
        return rectStats;
    }

    public void setRectStats(PairedStatsAccumulator rectStats) {
        this.rectStats = rectStats;
    }


    @Override
    public String toString() {
        return "QueryResults{" +
                "query=" + query +
                ", stats=" + stats +
                ", rectStats=" + rectStats +
                ", points=" + points +
                ", fullyContainedTileCount=" + fullyContainedTileCount +
                ", tileCount=" + tileCount +
                ", expandedNodeCount=" + expandedNodeCount +
                ", ioCount=" + ioCount +
                '}';
    }
}
