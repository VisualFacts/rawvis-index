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

    private IndexConfig() {}
}