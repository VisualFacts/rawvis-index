package gr.athenarc.imsi.visualfacts.query;

import java.util.Arrays;
import java.util.Map;

public class ApproximateQueryResults extends QueryResults {
    // SUM confidence intervals for each measure (key: measure column index)
    private Map<Integer, double[]> sumConfidenceIntervals;

    // COUNT confidence intervals for each measure (key: measure column index)
    // Represents the estimated non-null count as [lower, upper] bounds.
    private Map<Integer, double[]> countConfidenceIntervals;

    // MEAN confidence intervals for each measure (key: measure column index)
    // Computed via delta method on the ratio SUM/COUNT.
    private Map<Integer, double[]> meanConfidenceIntervals;

    // Error bounds for each measure (key: measure column index)
    private Map<Integer, Double> errorBounds;

    // Number of sampling rounds needed to achieve the error threshold
    private int samplingRounds;

    public ApproximateQueryResults(Query query) {
        super(query);
    }

    public Map<Integer, double[]> getSumConfidenceIntervals() {
        return sumConfidenceIntervals;
    }

    public void setSumConfidenceIntervals(Map<Integer, double[]> sumConfidenceIntervals) {
        this.sumConfidenceIntervals = sumConfidenceIntervals;
    }

    public Map<Integer, double[]> getCountConfidenceIntervals() {
        return countConfidenceIntervals;
    }

    public void setCountConfidenceIntervals(Map<Integer, double[]> countConfidenceIntervals) {
        this.countConfidenceIntervals = countConfidenceIntervals;
    }

    public Map<Integer, double[]> getMeanConfidenceIntervals() {
        return meanConfidenceIntervals;
    }

    public void setMeanConfidenceIntervals(Map<Integer, double[]> meanConfidenceIntervals) {
        this.meanConfidenceIntervals = meanConfidenceIntervals;
    }

    public Map<Integer, Double> getErrorBounds() {
        return errorBounds;
    }

    public void setErrorBounds(Map<Integer, Double> errorBounds) {
        this.errorBounds = errorBounds;
    }

    public int getSamplingRounds() {
        return samplingRounds;
    }

    public void setSamplingRounds(int samplingRounds) {
        this.samplingRounds = samplingRounds;
    }

    @Override
    public String toString() {
        String ciStr = "{";
        if (sumConfidenceIntervals != null && !sumConfidenceIntervals.isEmpty()) {
            ciStr += sumConfidenceIntervals.entrySet().stream()
                    .map(e -> e.getKey() + ":" + Arrays.toString(e.getValue()))
                    .reduce((a, b) -> a + ", " + b).orElse("");
        }
        ciStr += "}";
        return "ApproximateQueryResults{" +
                "query=" + getQuery() +
                ", sumConfidenceIntervals=" + ciStr +
                ", errorBounds=" + errorBounds +
                ", ioCount=" + getIoCount() +
                '}';
    }
}
