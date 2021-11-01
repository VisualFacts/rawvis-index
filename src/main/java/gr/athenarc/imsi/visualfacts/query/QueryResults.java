package gr.athenarc.imsi.visualfacts.query;

import gr.athenarc.imsi.visualfacts.Point;
import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.DedupVizOutput;

import java.util.List;

public class QueryResults {

    private Query query;

    private Stats stats;

    private List<Point> points;

    private int fullyContainedTileCount;

    private int tileCount;

    private int expandedNodeCount;

    private int ioCount;

    private DedupVizOutput dedupVizOutput;

    private Stats cleanedStats;

    public QueryResults(Query query) {
        this.query = query;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
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

    public DedupVizOutput getDedupVizOutput() {
        return dedupVizOutput;
    }

    public void setDedupVizOutput(DedupVizOutput dedupVizOutput) {
        this.dedupVizOutput = dedupVizOutput;
    }

    public Stats getStats() {
        return stats;
    }

    public void setStats(Stats stats) {
        this.stats = stats;
    }

    public Stats getCleanedStats() {
        return cleanedStats;
    }

    public void setCleanedStats(Stats cleanedStats) {
        this.cleanedStats = cleanedStats;
    }
}
