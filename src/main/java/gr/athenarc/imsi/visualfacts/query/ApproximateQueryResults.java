package gr.athenarc.imsi.visualfacts.query;

import java.util.Arrays;
import java.util.Map;

public class ApproximateQueryResults extends QueryResults {
    /**
     * Sampling stop reason for approximate queries.
     * <p>
     * This is orthogonal to the confidence-interval contents themselves: the
     * intervals describe the returned estimate, while this status explains how
     * the engine decided to stop sampling.
     */
    public enum SamplingStatus {
        /**
         * Normal stop: the requested relative CI half-width target was met by
         * adaptive sampling before resorting to residual exactification.
         */
        CONVERGED("converged"),
        /**
         * Valid but expensive stop: adaptive sampling stopped being useful, so
         * Valinor exactified all residual sampling nodes in one final pass.
         */
        EXACTIFIED_CONVERGED("exactified_converged"),
        /**
         * Defensive failure state: Valinor exhausted every residual sampling
         * node and still did not pass the convergence check.
         * <p>
         * With the current CI code this should be rare, because fully sampled
         * residual nodes contribute zero sampling variance. Reaching this state
         * therefore points to a numerical edge case, a bookkeeping mismatch, or
         * a future change that keeps a non-zero uncertainty floor even at full
         * residual sampling.
         */
        EXHAUSTED_UNCONVERGED("exhausted_unconverged");

        private final String csvValue;

        SamplingStatus(String csvValue) {
            this.csvValue = csvValue;
        }

        @Override
        public String toString() {
            return csvValue;
        }
    }

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

    private Map<Integer, Double> sumErrorBounds;

    private Map<Integer, Double> countErrorBounds;

    private Map<Integer, Double> meanErrorBounds;

    private boolean converged = true;

    // Default for exact-mode style callers; approximate execution overwrites
    // this with the stop reason observed in Valinor.executeApproximateQuery.
    private SamplingStatus samplingStatus = SamplingStatus.CONVERGED;

    private String samplingStopReason = "converged";

    private double preExactificationErrorBound = Double.NaN;

    private long samplingSeed;

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

    public Map<Integer, Double> getSumErrorBounds() {
        return sumErrorBounds;
    }

    public void setSumErrorBounds(Map<Integer, Double> sumErrorBounds) {
        this.sumErrorBounds = sumErrorBounds;
    }

    public Map<Integer, Double> getCountErrorBounds() {
        return countErrorBounds;
    }

    public void setCountErrorBounds(Map<Integer, Double> countErrorBounds) {
        this.countErrorBounds = countErrorBounds;
    }

    public Map<Integer, Double> getMeanErrorBounds() {
        return meanErrorBounds;
    }

    public void setMeanErrorBounds(Map<Integer, Double> meanErrorBounds) {
        this.meanErrorBounds = meanErrorBounds;
    }

    public boolean isConverged() {
        return converged;
    }

    public void setConverged(boolean converged) {
        this.converged = converged;
    }

    public SamplingStatus getSamplingStatus() {
        return samplingStatus;
    }

    public void setSamplingStatus(SamplingStatus samplingStatus) {
        this.samplingStatus = samplingStatus;
    }

    public String getSamplingStopReason() {
        return samplingStopReason;
    }

    public void setSamplingStopReason(String samplingStopReason) {
        this.samplingStopReason = samplingStopReason;
    }

    public double getPreExactificationErrorBound() {
        return preExactificationErrorBound;
    }

    public void setPreExactificationErrorBound(double preExactificationErrorBound) {
        this.preExactificationErrorBound = preExactificationErrorBound;
    }

    public long getSamplingSeed() {
        return samplingSeed;
    }

    public void setSamplingSeed(long samplingSeed) {
        this.samplingSeed = samplingSeed;
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
                ", converged=" + converged +
                ", samplingStatus=" + samplingStatus +
                ", ioCount=" + getIoCount() +
                '}';
    }
}
