package gr.athenarc.imsi.visualfacts.config;

/**
 * Index construction parameters.
 * <p>
 * Defaults can be overridden at runtime via CLI parameters in
 * {@code Experiments} ({@code -resolution}, {@code -subtileRatio}).
 */
public final class IndexConfig {

    /** Partitions per axis for the initial uniform grid (R×R cells). */
    public static int RESOLUTION = 100;

    /** Fraction of R² cells allocated additionally for query-biased sub-tiling (0.0–1.0). */
    public static double SUBTILE_RATIO = 0.2;

    /** Point count above which a partial tile is split during query processing. */
    public static int THRESHOLD = 200;

    private IndexConfig() {}
}