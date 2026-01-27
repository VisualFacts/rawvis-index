package gr.athenarc.imsi.visualfacts.experiments.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Configuration class for exploration scenarios in experiment YAML files.
 * An exploration scenario defines a sequence of queries simulating user exploration behavior.
 */
public class ExplorationScenarioConfig {

    @JsonProperty("dataset")
    private String dataset;

    @JsonProperty("q0")
    private InitialQueryConfig q0;

    @JsonProperty("minShift")
    private int minShift;

    @JsonProperty("maxShift")
    private int maxShift;

    @JsonProperty("zoomFactor")
    private float zoomFactor = 1.0f;

    @JsonProperty("seqCount")
    private int seqCount;

    @JsonProperty("directionWeights")
    private Map<String, Double> directionWeights;

    // Default constructor for Jackson
    public ExplorationScenarioConfig() {
    }

    // Getters and Setters

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public InitialQueryConfig getQ0() {
        return q0;
    }

    public void setQ0(InitialQueryConfig q0) {
        this.q0 = q0;
    }

    public int getMinShift() {
        return minShift;
    }

    public void setMinShift(int minShift) {
        this.minShift = minShift;
    }

    public int getMaxShift() {
        return maxShift;
    }

    public void setMaxShift(int maxShift) {
        this.maxShift = maxShift;
    }

    public float getZoomFactor() {
        return zoomFactor;
    }

    public void setZoomFactor(float zoomFactor) {
        this.zoomFactor = zoomFactor;
    }

    public int getSeqCount() {
        return seqCount;
    }

    public void setSeqCount(int seqCount) {
        this.seqCount = seqCount;
    }

    public Map<String, Double> getDirectionWeights() {
        return directionWeights;
    }

    public void setDirectionWeights(Map<String, Double> directionWeights) {
        this.directionWeights = directionWeights;
    }

    @Override
    public String toString() {
        return "ExplorationScenarioConfig{" +
                "dataset='" + dataset + '\'' +
                ", q0=" + q0 +
                ", minShift=" + minShift +
                ", maxShift=" + maxShift +
                ", zoomFactor=" + zoomFactor +
                ", seqCount=" + seqCount +
                ", directionWeights=" + directionWeights +
                '}';
    }
}
