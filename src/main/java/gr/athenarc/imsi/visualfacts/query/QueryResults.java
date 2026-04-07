package gr.athenarc.imsi.visualfacts.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.math.Stats;
import com.google.common.math.StatsAccumulator;

public class QueryResults {

    private Query query;

    private Map<Integer, StatsAccumulator> stats;

    private List<double[]> points;

    private int fullyContainedTileCount;

    private int fullyContainedTileWithoutStatsCount;

    private int samplingTileCount;

    private double samplingRate;

    private int tileCount;

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

    public Map<Integer, Stats> getStats() {
        return stats.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().snapshot()));
    }

    public void adjustStats(Integer measure, double measureValue) {
        stats.computeIfAbsent(measure, m -> new StatsAccumulator())
                .add(measureValue);
    }

    public void adjustStats(Integer measure, Stats stats) {
        this.stats.computeIfAbsent(measure, m -> new StatsAccumulator())
                .addAll(stats);
    }

    public int getFullyContainedTileCount() {
        return fullyContainedTileCount;
    }

    public void setFullyContainedTileCount(int fullyContainedTileCount) {
        this.fullyContainedTileCount = fullyContainedTileCount;
    }

    

    public int getFullyContainedTileWithoutStatsCount() {
        return fullyContainedTileWithoutStatsCount;
    }

    public void setFullyContainedTileWithoutStatsCount(int fullyContainedTileWithoutStatsCount) {
        this.fullyContainedTileWithoutStatsCount = fullyContainedTileWithoutStatsCount;
    }

    

    public int getSamplingTileCount() {
        return samplingTileCount;
    }

    public void setSamplingTileCount(int samplingTileCount) {
        this.samplingTileCount = samplingTileCount;
    }

    

    public double getSamplingRate() {
        return samplingRate;
    }

    public void setSamplingRate(double samplingRate) {
        this.samplingRate = samplingRate;
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

    public List<double[]> getPoints() {
        return points;
    }

    public void setPoints(List<double[]> points) {
        this.points = points;
    }

    @Override
    public String toString() {
        return "QueryResults{" +
                "query=" + query +
                ", stats=" + stats +
                ", points=" + points +
                ", fullyContainedTileCount=" + fullyContainedTileCount +
                ", tileCount=" + tileCount +
                ", ioCount=" + ioCount +
                '}';
    }
}
