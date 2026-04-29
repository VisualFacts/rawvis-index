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

    // ----- Clustered (GMF) workload knobs -----
    // Used only when type == "clustered".

    @JsonProperty("numFoci")
    private int numFoci = 1;

    /** Gaussian σ per axis as a fraction of the dataset extent. */
    @JsonProperty("focusSpreadFraction")
    private double focusSpreadFraction = 0.05;

    /**
     * Optional explicit focus centres, formatted as a list of "x,y" strings.
     * If null/empty, foci are sampled uniformly inside the dataset bounds with
     * the same seed (so the choice is reproducible).
     */
    @JsonProperty("focusCenters")
    private java.util.List<String> focusCenters;

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

    public int getNumFoci() { return numFoci; }
    public void setNumFoci(int numFoci) { this.numFoci = numFoci; }

    public double getFocusSpreadFraction() { return focusSpreadFraction; }
    public void setFocusSpreadFraction(double focusSpreadFraction) {
        this.focusSpreadFraction = focusSpreadFraction;
    }

    public java.util.List<String> getFocusCenters() { return focusCenters; }
    public void setFocusCenters(java.util.List<String> focusCenters) {
        this.focusCenters = focusCenters;
    }

    /**
     * Parses {@link #focusCenters} into a {@code double[K][2]} array, or
     * returns {@code null} if no explicit centres were configured.
     */
    public double[][] parseFocusCenters() {
        if (focusCenters == null || focusCenters.isEmpty()) return null;
        double[][] out = new double[focusCenters.size()][2];
        for (int i = 0; i < focusCenters.size(); i++) {
            String[] parts = focusCenters.get(i).split(",");
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "focusCenters[" + i + "] must be 'x,y', got '" + focusCenters.get(i) + "'");
            }
            out[i][0] = Double.parseDouble(parts[0].trim());
            out[i][1] = Double.parseDouble(parts[1].trim());
        }
        return out;
    }

    @Override
    public String toString() {
        return "WorkloadConfig{type='" + type + "', seqCount=" + seqCount
            + ", selectivity=" + selectivity + ", seed=" + seed
            + ", extentMode=" + extentMode + ", reservoirSize=" + reservoirSize
            + ", numFoci=" + numFoci + ", focusSpreadFraction=" + focusSpreadFraction
            + ", focusCenters=" + focusCenters + "}";
    }
}
