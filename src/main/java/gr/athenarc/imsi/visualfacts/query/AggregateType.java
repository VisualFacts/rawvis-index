package gr.athenarc.imsi.visualfacts.query;

import java.util.EnumSet;

/**
 * Types of aggregate functions supported in SQL query generation.
 */
public enum AggregateType {
    COUNT("count"),
    MIN("min"),
    MAX("max"),
    SUM("sum"),
    AVG("avg"),
    SUM_OF_SQUARES("sum_of_squares");

    private final String sqlAlias;

    AggregateType(String sqlAlias) {
        this.sqlAlias = sqlAlias;
    }

    public String getSqlAlias() {
        return sqlAlias;
    }

    /**
     * All aggregate types.
     */
    public static final EnumSet<AggregateType> ALL = EnumSet.allOf(AggregateType.class);

    /**
     * Basic aggregates without sum of squares (count, min, max, sum, avg).
     */
    public static final EnumSet<AggregateType> BASIC = EnumSet.of(COUNT, MIN, MAX, SUM, AVG);

    /**
     * Only count.
     */
    public static final EnumSet<AggregateType> COUNT_ONLY = EnumSet.of(COUNT);

    /**
     * Stats needed for variance/stddev calculation (count, sum, sum_of_squares).
     */
    public static final EnumSet<AggregateType> VARIANCE_STATS = EnumSet.of(COUNT, SUM, SUM_OF_SQUARES);

    /**
     * PilotDB-compatible aggregates (only linear aggregates that support error bounds).
     */
    public static final EnumSet<AggregateType> PILOTDB = EnumSet.of(SUM, AVG);
}
