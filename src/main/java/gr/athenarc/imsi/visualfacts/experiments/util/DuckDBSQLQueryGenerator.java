package gr.athenarc.imsi.visualfacts.experiments.util;

import java.util.List;

import gr.athenarc.imsi.visualfacts.DataValidationFilter;

/**
 * DuckDB-specific SQL generation: column naming, read_csv_auto options,
 * CREATE TABLE statements, and validation-filter WHERE clauses.
 * <p>
 * Extends {@link SQLQueryGenerator} which provides generic SELECT / aggregate
 * query generation. Both {@link DuckDBQueryExecutor} and
 * {@link gr.athenarc.imsi.visualfacts.experiments.Experiments} call these
 * static methods so that SQL construction is never duplicated.
 */
public class DuckDBSQLQueryGenerator extends SQLQueryGenerator {

    // ---- Column naming ----

    /**
     * Format a 0-based column index as DuckDB's auto-generated name.
     *
     * @param colIdx      zero-based column index
     * @param useTwoDigit true → "column05", false → "column5"
     */
    public static String formatColumnName(int colIdx, boolean useTwoDigit) {
        return useTwoDigit
                ? "column" + String.format("%02d", colIdx)
                : "column" + colIdx;
    }

    /**
     * Re-format an existing column name (e.g. "column5", "column05", or just "5")
     * with the appropriate zero-padding convention.
     */
    public static String formatColumnName(String colName, boolean useTwoDigit) {
        String numberPart = colName;
        if (colName.startsWith("column")) {
            numberPart = colName.substring(6);
        }
        try {
            return formatColumnName(Integer.parseInt(numberPart), useTwoDigit);
        } catch (NumberFormatException e) {
            return colName; // keep as-is if unparseable
        }
    }

    // ---- read_csv_auto options ----

    /**
     * Build the options string passed to DuckDB's {@code read_csv_auto()}.
     */
    public static String buildReadCsvOptions(boolean hasHeader, String nullstr) {
        StringBuilder opts = new StringBuilder("ignore_errors = true");
        if (hasHeader) {
            // header=false; skip=1 discards the actual header row so it isn't ingested as data.
            opts.append(", header = false, skip = 1");
        }
        if (nullstr != null && !nullstr.isEmpty()) {
            // Use a list so DuckDB still treats empty fields as NULL alongside
            // the custom null string (e.g. 'X' in eBird OBSERVATION COUNT).
            opts.append(", nullstr = ['', '").append(nullstr).append("']");
        }
        return opts.toString();
    }

    // ---- Validation-filter WHERE clause ----

    /**
     * Build a {@code " WHERE col >= 0 AND col <= 1440"} clause that keeps only
     * valid rows by <em>negating</em> the validation filters (which describe
     * <em>invalid</em> conditions).
     *
     * @return the clause including the leading {@code " WHERE "}, or {@code ""}
     *         if there are no filters
     */
    public static String buildValidationFilterWhereClause(
            List<DataValidationFilter> filters, boolean useTwoDigit) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" WHERE ");
        String sep = "";
        for (DataValidationFilter filter : filters) {
            String colName = formatColumnName(filter.getFilterColumn(), useTwoDigit);
            double value = filter.getFilterPredicate().getConstant();
            String negatedOp = negateOperator(filter.getFilterPredicate().getOperator());
            sb.append(sep).append(colName).append(" ").append(negatedOp).append(" ").append(value);
            sep = " AND ";
        }
        return sb.toString();
    }

    // ---- CREATE TABLE statements ----

    /**
     * {@code CREATE TABLE <table> AS SELECT * FROM read_csv_auto(...) [WHERE ...];}
     */
    public static String buildCreateTableSQL(String tableName, String csvPath,
            String readCsvOptions, List<DataValidationFilter> filters, boolean useTwoDigit) {
        return String.format("CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', %s)%s;",
                tableName, csvPath, readCsvOptions,
                buildValidationFilterWhereClause(filters, useTwoDigit));
    }

    /**
     * {@code CREATE TABLE <table> AS SELECT col05, col06 FROM read_csv_auto(...) [WHERE ...];}
     *
     * @param projectedColumns sorted column indices to include
     */
    public static String buildCreateProjectedTableSQL(String tableName, String csvPath,
            String readCsvOptions, List<DataValidationFilter> filters, boolean useTwoDigit,
            List<Integer> projectedColumns) {
        StringBuilder selectCols = new StringBuilder();
        String sep = "";
        for (int colIdx : projectedColumns) {
            selectCols.append(sep).append(formatColumnName(colIdx, useTwoDigit));
            sep = ", ";
        }
        return String.format("CREATE TABLE %s AS SELECT %s FROM read_csv_auto('%s', %s)%s;",
                tableName, selectCols, csvPath, readCsvOptions,
                buildValidationFilterWhereClause(filters, useTwoDigit));
    }

    /**
     * {@code CREATE TABLE <table> AS SELECT *, ST_Point(x,y) AS geometry FROM read_csv_auto(...) [WHERE ...];}
     */
    public static String buildCreateSpatialTableSQL(String tableName, String csvPath,
            String readCsvOptions, List<DataValidationFilter> filters, boolean useTwoDigit,
            String xCol, String yCol) {
        return String.format(
                "CREATE TABLE %s AS SELECT *, ST_Point(%s::DOUBLE, %s::DOUBLE) AS geometry FROM read_csv_auto('%s', %s)%s;",
                tableName, xCol, yCol, csvPath, readCsvOptions,
                buildValidationFilterWhereClause(filters, useTwoDigit));
    }

    /**
     * {@code CREATE INDEX <idx> ON <table> USING RTREE (geometry);}
     */
    public static String buildCreateSpatialIndexSQL(String tableName) {
        return String.format("CREATE INDEX spatial_index_%s ON %s USING RTREE (geometry);",
                tableName, tableName);
    }
}
