package gr.athenarc.imsi.visualfacts.experiments.config;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for a single phase in a phased exploration scenario.
 * 
 * Supported operations:
 * - "pan": viewport shifts in a random direction, size stays constant
 * - "zoom_to_point": viewport shrinks toward a target point (cursor-anchored zoom-in)
 * - "zoom_out": viewport grows from its center (center-anchored zoom-out)
 */
public class PhaseConfig {

    @JsonProperty("name")
    private String name;

    @JsonProperty("operation")
    private String operation;

    @JsonProperty("count")
    private int count;

    // Pan parameters
    @JsonProperty("minShift")
    private int minShift;

    @JsonProperty("maxShift")
    private int maxShift;

    @JsonProperty("directionWeights")
    private Map<String, Double> directionWeights;

    /** Optional bounds to clamp panning within (format: "xLow:xHigh,yLow:yHigh") */
    @JsonProperty("clampToBounds")
    private String clampToBounds;

    // Zoom parameters
    /** For zoom_to_point: the target point (format: "x,y") */
    @JsonProperty("target")
    private String target;

    /** Zoom factor per step. For zoom_to_point: 0.5 = 2x zoom-in. For zoom_out: 2.0 = 2x zoom-out. */
    @JsonProperty("zoomFactor")
    private float zoomFactor = 0.5f;

    public PhaseConfig() {
    }

    // Getters and Setters

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
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

    public Map<String, Double> getDirectionWeights() {
        return directionWeights;
    }

    public void setDirectionWeights(Map<String, Double> directionWeights) {
        this.directionWeights = directionWeights;
    }

    public String getClampToBounds() {
        return clampToBounds;
    }

    public void setClampToBounds(String clampToBounds) {
        this.clampToBounds = clampToBounds;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public float getZoomFactor() {
        return zoomFactor;
    }

    public void setZoomFactor(float zoomFactor) {
        this.zoomFactor = zoomFactor;
    }

    /**
     * Parses the target string "x,y" into a float array [x, y].
     */
    public float[] getTargetPoint() {
        if (target == null || target.isEmpty()) {
            return null;
        }
        String[] parts = target.split(",");
        return new float[] { Float.parseFloat(parts[0].trim()), Float.parseFloat(parts[1].trim()) };
    }

    @Override
    public String toString() {
        return "PhaseConfig{" +
                "name='" + name + '\'' +
                ", operation='" + operation + '\'' +
                ", count=" + count +
                '}';
    }
}
