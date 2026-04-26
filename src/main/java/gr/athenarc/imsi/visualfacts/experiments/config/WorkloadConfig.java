package gr.athenarc.imsi.visualfacts.experiments.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for a non-exploration workload (random).
 *
 * <p>Query centres are
 * sampled from the data distribution (via a cached {@code SpatialReservoir})
 * and every query has the same fixed extent, calibrated so that the
 * <em>average</em> per-query selectivity equals {@link #selectivity}.
 *
 * <p>Knobs:
 * <ul>
 *   <li>{@code selectivity} — target mean σ (e.g. {@code 0.01} for 1%).</li>
 *   <li>{@code extentMode} — {@code calibrated} (default; data-aware) or
 *       {@code closedForm} (uses {@code √σ·W}, exact only on uniform data).</li>
 *   <li>{@code reservoirSize} — number of points sampled from the dataset
 *       to act as the centre pool and the calibration estimator.</li>
 *   <li>{@code seed} — controls both reservoir sampling and query order;
 *       fixed across runs so all systems execute identical streams.</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkloadConfig {

    public static final String EXTENT_MODE_CALIBRATED = "calibrated";
    public static final String EXTENT_MODE_CLOSED_FORM = "closedForm";

    @JsonProperty("type")
    private String type;

    @JsonProperty("seqCount")
    private int seqCount = 300;

    @JsonProperty("selectivity")
    private double selectivity = 0.01;

    @JsonProperty("seed")
    private long seed = 0L;

    @JsonProperty("extentMode")
    private String extentMode;

    @JsonProperty("reservoirSize")
    private Integer reservoirSize;

    public WorkloadConfig() {
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public int getSeqCount() { return seqCount; }
    public void setSeqCount(int seqCount) { this.seqCount = seqCount; }

    public double getSelectivity() { return selectivity; }
    public void setSelectivity(double selectivity) { this.selectivity = selectivity; }

    public long getSeed() { return seed; }
    public void setSeed(long seed) { this.seed = seed; }

    public String getExtentMode() { return extentMode; }
    public void setExtentMode(String extentMode) { this.extentMode = extentMode; }

    public Integer getReservoirSize() { return reservoirSize; }
    public void setReservoirSize(Integer reservoirSize) { this.reservoirSize = reservoirSize; }

    @Override
    public String toString() {
        return "WorkloadConfig{type='" + type + "', seqCount=" + seqCount
            + ", selectivity=" + selectivity + ", seed=" + seed
            + ", extentMode=" + extentMode + ", reservoirSize=" + reservoirSize + "}";
    }
}
