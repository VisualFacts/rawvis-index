package gr.athenarc.imsi.visualfacts.query;

import java.util.Map;

public class ApproximateQueryResults extends QueryResults {
    // Confidence intervals for each measure (key: measure column index)
    private Map<Integer, double[]> confidenceIntervals;

    // Error bounds for each measure (key: measure column index)
    private Map<Integer, Double> errorBounds;

    public ApproximateQueryResults(Query query) {
        super(query);
    }

    public Map<Integer, double[]> getConfidenceIntervals() {
        return confidenceIntervals;
    }

    public void setConfidenceIntervals(Map<Integer, double[]> confidenceIntervals) {
        this.confidenceIntervals = confidenceIntervals;
    }

    public Map<Integer, Double> getErrorBounds() {
        return errorBounds;
    }

    public void setErrorBounds(Map<Integer, Double> errorBounds) {
        this.errorBounds = errorBounds;
    }

    @Override
    public String toString() {
        return "QueryResults{" +
                "query=" + getQuery() +
                ", confidenceIntervals=" + confidenceIntervals +
                ", errorBounds=" + errorBounds +
                ", stats=" + getStats() +
                ", rectStats=" + getRectStats() +
                ", ioCount=" + getIoCount() +
                '}';
    }
}
