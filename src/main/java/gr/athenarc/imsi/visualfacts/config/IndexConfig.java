package gr.athenarc.imsi.visualfacts.config;

/**
 * Index construction parameters.
 * <p>
 * Defaults can be overridden at runtime via CLI parameters in
 * {@code Experiments} ({@code -resolution}, {@code -subtileRatio}).
 */
public final class IndexConfig {

    /** Partitions per axis for the initial uniform grid (R×R cells). */
    public static int RESOLUTION = 500;

    /** Fraction of R² cells allocated additionally for query-biased sub-tiling (0.0–1.0). */
    public static double SUBTILE_RATIO = 0.2;

    /** Point count above which a partial tile is split during query processing. */
    public static int THRESHOLD = 100;

    /**
     * Bytes per tileId element in the point store arrays.
     * Must match the primitive type used for tileId storage:
     * {@code Integer.BYTES} (4) when tileIds are {@code int[]},
     * {@code Short.BYTES} (2) when tileIds are {@code short[]}.
     */
    public static final int TILE_ID_BYTES = Integer.BYTES;

    // ==================== Outlier-aware AQP (Phase 1) ====================
    //
    // The optional outlier index identifies the K rows (across the entire
    // dataset) whose extreme measure values dominate the variance of one or
    // more aggregates.  Those rows are extracted into a small in-memory
    // value matrix, removed from the per-tile sampling population, and
    // contribute exact (closed-form) sums at query time.  The remaining
    // sampling population is the "trimmed" tail with much lower CV, which
    // tightens approximate-query confidence intervals dramatically.
    //
    // Set OUTLIER_K = 0 (default) to completely disable the feature; in that
    // case Valinor's behavior is byte-identical to the pre-outlier code path
    // (no extra heaps, no extra tile state, no query-time bookkeeping).

    /** Number of outliers to extract globally; 0 disables outlier-aware AQP. */
    public static int OUTLIER_K = 0;

    private IndexConfig() {}
}