package gr.athenarc.imsi.visualfacts.query;

import java.util.Arrays;
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
        String ciStr = "{";
        if (confidenceIntervals != null && !confidenceIntervals.isEmpty()) {
            ciStr += confidenceIntervals.entrySet().stream()
                    .map(e -> e.getKey() + ":" + Arrays.toString(e.getValue()))
                    .reduce((a, b) -> a + ", " + b).orElse("");
        }
        ciStr += "}";
        return "ApproximateQueryResults{" +
                "query=" + getQuery() +
                ", confidenceIntervals=" + ciStr +
                ", errorBounds=" + errorBounds +
                ", ioCount=" + getIoCount() +
                '}';
    }
}
