package gr.athenarc.imsi.visualfacts.query;

import java.util.Arrays;

public class ApproximateQueryResults extends QueryResults {
    private double errorBound;
    private double[] confidenceInterval;

    public ApproximateQueryResults(Query query) {
        super(query);
    }

    public void setErrorBound(double errorBound) {
        this.errorBound = errorBound;
    }

    public double getErrorBound() {
        return errorBound;
    }

    public double[] getConfidenceInterval() {
        return confidenceInterval;
    }

    @Override
    public String toString() {
        return "ApproximateQueryResults [errorBound=" + errorBound + ", confidenceInterval="
                + Arrays.toString(confidenceInterval) + "]";
    }

    public void setConfidenceInterval(double[] confidenceInterval) {
        this.confidenceInterval = confidenceInterval;
    }

}
