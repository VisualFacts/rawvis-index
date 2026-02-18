package gr.athenarc.imsi.visualfacts.experiments.util;

import java.util.EnumSet;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.DataValidationFilter;
import gr.athenarc.imsi.visualfacts.Filter;
import gr.athenarc.imsi.visualfacts.query.AggregateType;
import gr.athenarc.imsi.visualfacts.query.FilterOperator;

public class SQLQueryGenerator {

    private static final Logger LOG = LogManager.getLogger(SQLQueryGenerator.class);

    /**
     * @param tableName
     * @param ranges    the ranges to be applied to the given cols
     * @param cols      the names of the columns to be used in the where clause and in the select clause
     * @return a select all sql range query
     */
    public static String getSQLSelectQuery(String tableName, List<Range<Float>> ranges, String... cols) {
        String query = "select ";
        String sep = "";
        for (String col : cols) {
            query += sep + col;
            sep = ",";
        }
        query += " from " + tableName + " where " + generateWhereClause(ranges, cols) + ";";
        return query;
    }


    /**
     * @param tableName
     * @param ranges    the ranges to be applied to the given cols
     * @param cols      the names of the columns to be used in the where clause
     * @return a count all sql range query
     */
    public static String getSQLCountQuery(String tableName, List<Range<Float>> ranges, String... cols) {
        String query = "select ";
        query += "count(*) from " + tableName + " where " + generateWhereClause(ranges, cols) + ";";
        return query;
    }

    /**
     * Generate a uni-variate aggregation query for a single aggregation column (backwards compatible).
     */
    public static String getSQLUniAggQuery(String tableName, List<Range<Float>> ranges, String aggCol, String... cols) {
        return getSQLUniAggQuery(tableName, ranges, java.util.Arrays.asList(aggCol), cols);
    }

    /**
     * Generate a uni-variate aggregation query for multiple aggregation columns.
     * For each aggregation column we produce count/min/max/sum/avg/sum_of_squares with distinct aliases.
     */
    public static String getSQLUniAggQuery(String tableName, List<Range<Float>> ranges, List<String> aggCols, String... cols) {
        return getSQLUniAggQuery(tableName, ranges, aggCols, null, AggregateType.ALL, cols);
    }

    /**
     * Generate a uni-variate aggregation query for multiple aggregation columns with validation filters.
     * Validation filters exclude rows that match (e.g., "column12 < 0 OR column12 > 400" excludes outliers).
     */
    public static String getSQLUniAggQuery(String tableName, List<Range<Float>> ranges, List<String> aggCols, 
                                           List<DataValidationFilter> validationFilters, String... cols) {
        return getSQLUniAggQuery(tableName, ranges, aggCols, validationFilters, AggregateType.ALL, cols);
    }

    /**
     * Generate a uni-variate aggregation query for multiple aggregation columns with validation filters
     * and a configurable subset of aggregate functions.
     */
    public static String getSQLUniAggQuery(String tableName, List<Range<Float>> ranges, List<String> aggCols, 
                                           List<DataValidationFilter> validationFilters,
                                           EnumSet<AggregateType> aggregates, String... cols) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        String sep = "";
        for (String aggCol : aggCols) {
            String aliasBase = aggCol.replaceAll("[^A-Za-z0-9]", "_");
            sb.append(sep);
            sep = appendAggregates(sb, aggCol, aliasBase, aggregates);
        }
        sb.append(" from ").append(tableName).append(" where ").append(generateWhereClause(ranges, cols));
        
        // Add validation filter exclusions
        String validationClause = generateValidationFilterClause(validationFilters);
        if (!validationClause.isEmpty()) {
            sb.append(" AND ").append(validationClause);
        }
        
        sb.append(";");
        return sb.toString();
    }

    /**
     * Appends aggregate functions for a column based on the specified aggregate types.
     * Casts columns to FLOAT to match Valinor's float precision.
     * @return the separator to use for the next column (", " if any aggregates were added)
     */
    private static String appendAggregates(StringBuilder sb, String aggCol, String aliasBase, 
                                            EnumSet<AggregateType> aggregates) {
        // Cast to FLOAT to match Valinor's float precision
        String castCol = "CAST(" + aggCol + " AS FLOAT)";
        String innerSep = "";
        if (aggregates.contains(AggregateType.COUNT)) {
            sb.append(innerSep).append("count(").append(castCol).append(") as count_").append(aliasBase);
            innerSep = ", ";
        }
        if (aggregates.contains(AggregateType.MIN)) {
            sb.append(innerSep).append("min(").append(castCol).append(") as min_").append(aliasBase);
            innerSep = ", ";
        }
        if (aggregates.contains(AggregateType.MAX)) {
            sb.append(innerSep).append("max(").append(castCol).append(") as max_").append(aliasBase);
            innerSep = ", ";
        }
        if (aggregates.contains(AggregateType.SUM)) {
            sb.append(innerSep).append("sum(").append(castCol).append(") as sum_").append(aliasBase);
            innerSep = ", ";
        }
        if (aggregates.contains(AggregateType.AVG)) {
            sb.append(innerSep).append("avg(").append(castCol).append(") as avg_").append(aliasBase);
            innerSep = ", ";
        }
        if (aggregates.contains(AggregateType.SUM_OF_SQUARES)) {
            sb.append(innerSep).append("sum(").append(castCol).append(" * ").append(castCol)
              .append(") as sum_of_squares_").append(aliasBase);
            innerSep = ", ";
        }
        return innerSep.isEmpty() ? "" : ", ";
    }

    /**
     * DuckDB-specific spatial query for one aggregation column (backwards compatible).
     */
    public static String getDuckDBSQLSpatialUniAggQuery(String tableName, List<Range<Float>> ranges, String aggCol) {
        return getDuckDBSQLSpatialUniAggQuery(tableName, ranges, java.util.Arrays.asList(aggCol));
    }

    /**
     * DuckDB-specific spatial query for multiple aggregation columns.
     * Uses ST_Within with ST_MakeEnvelope to leverage the R-tree index on the geometry column.
     */
    public static String getDuckDBSQLSpatialUniAggQuery(String tableName, List<Range<Float>> ranges, java.util.List<String> aggCols) {
        return getDuckDBSQLSpatialUniAggQuery(tableName, ranges, aggCols, null, AggregateType.ALL);
    }

    /**
     * DuckDB-specific spatial query for multiple aggregation columns with validation filters.
     */
    public static String getDuckDBSQLSpatialUniAggQuery(String tableName, List<Range<Float>> ranges, 
                                                         java.util.List<String> aggCols,
                                                         List<DataValidationFilter> validationFilters) {
        return getDuckDBSQLSpatialUniAggQuery(tableName, ranges, aggCols, validationFilters, AggregateType.ALL);
    }

    /**
     * DuckDB-specific spatial query for multiple aggregation columns with validation filters
     * and a configurable subset of aggregate functions.
     */
    public static String getDuckDBSQLSpatialUniAggQuery(String tableName, List<Range<Float>> ranges, 
                                                         java.util.List<String> aggCols,
                                                         List<DataValidationFilter> validationFilters,
                                                         EnumSet<AggregateType> aggregates) {
        if (ranges.size() < 2) {
            throw new IllegalArgumentException("Spatial queries require at least 2 ranges (x and y)");
        }

        Range<Float> xRange = ranges.get(0);
        Range<Float> yRange = ranges.get(1);

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        String sep = "";
        for (String aggCol : aggCols) {
            String aliasBase = aggCol.replaceAll("[^A-Za-z0-9]", "_");
            sb.append(sep);
            sep = appendAggregates(sb, aggCol, aliasBase, aggregates);
        }
        sb.append(" FROM ").append(tableName).append(" ")
          .append("WHERE ST_Within(geometry, ST_MakeEnvelope(")
          .append(xRange.lowerEndpoint()).append(", ")
          .append(yRange.lowerEndpoint()).append(", ")
          .append(xRange.upperEndpoint()).append(", ")
          .append(yRange.upperEndpoint()).append("))");

        // Add validation filter exclusions
        String validationClause = generateValidationFilterClause(validationFilters);
        if (!validationClause.isEmpty()) {
            sb.append(" AND ").append(validationClause);
        }
        
        sb.append(";");

        return sb.toString();
    }

    public static String getSQLBiAggQuery(String tableName, List<Range<Float>> ranges, String aggCol1, String aggCol2, String... cols) {
        String query = "select ";
        query += "count(" + aggCol1 + ") as count1, min(" + aggCol1 + ") as min1, max(" + aggCol1 + ") as max1, sum(" + aggCol1 + ") as sum1, avg(" + aggCol1 + ") as avg1, sum(" + aggCol1 + " * " + aggCol1 + ") as squared1, " +
                "count(" + aggCol2 + ") as count2, min(" + aggCol2 + ") as min2, max(" + aggCol2 + ") as max2, sum(" + aggCol2 + ") as sum2, avg(" + aggCol2 + ") as avg2, sum(" + aggCol2 + " * " + aggCol2 + ") as squared2, sum(" + aggCol1 + " * " + aggCol2 + ") as squared12  from " + tableName + " where " + generateWhereClause(ranges, cols) + ";";
        return query;
    }

    private static String generateWhereClause(List<Range<Float>> ranges, String... cols) {
        String whereClause = "";
        int i = 0;
        for (Range<Float> range : ranges) {
            String col = cols[i];
            // Cast to FLOAT to match Valinor's float precision (avoids float vs double discrepancies)
            whereClause += "CAST(" + col + " AS FLOAT) > CAST(" + range.lowerEndpoint() + " AS FLOAT) AND " +
                           "CAST(" + col + " AS FLOAT) < CAST(" + range.upperEndpoint() + " AS FLOAT)";
            if (i < ranges.size() - 1) {
                whereClause += " AND ";
            }
            i++;
        }
        return whereClause;
    }

    /**
     * Generates a WHERE clause that EXCLUDES rows matching the validation filters.
     * Validation filters define invalid data (e.g., "column12 < 0" means values < 0 are invalid).
     * The returned clause ensures only VALID rows are included (NOT matching the filter conditions).
     * 
     * @param validationFilters list of filters defining invalid data conditions
     * @return SQL clause string, or empty string if no filters
     */
    private static String generateValidationFilterClause(List<DataValidationFilter> validationFilters) {
        if (validationFilters == null || validationFilters.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("(");
        String sep = "";
        for (DataValidationFilter filter : validationFilters) {
            // Format column name with two-digit padding to match DuckDB column naming
            String colName = "column" + String.format("%02d", filter.getFilterColumn());
            double value = filter.getFilterPredicate().getConstant();
            
            // NEGATE the filter: if filter says "< 0" (invalid), we want "NOT (col < 0)" i.e., "col >= 0"
            String negatedOperator = negateOperator(filter.getFilterPredicate().getOperator());
            
            sb.append(sep).append(colName).append(" ").append(negatedOperator).append(" ").append(value);
            // Validation filters are OR'd (any match = invalid), so negation becomes AND
            sep = " AND ";
        }
        sb.append(")");
        return sb.toString();
    }

    private static String operatorToSQL(FilterOperator operator) {
        switch (operator) {
            case LESS_THAN: return "<";
            case LESS_THAN_OR_EQUAL: return "<=";
            case GREATER_THAN: return ">";
            case GREATER_THAN_OR_EQUAL: return ">=";
            case EQUAL: return "=";
            case NOT_EQUAL: return "!=";
            default: throw new IllegalArgumentException("Unknown operator: " + operator);
        }
    }

    private static String negateOperator(FilterOperator operator) {
        switch (operator) {
            case LESS_THAN: return ">=";
            case LESS_THAN_OR_EQUAL: return ">";
            case GREATER_THAN: return "<=";
            case GREATER_THAN_OR_EQUAL: return "<";
            case EQUAL: return "!=";
            case NOT_EQUAL: return "=";
            default: throw new IllegalArgumentException("Unknown operator: " + operator);
        }
    }

    public static String getSQLFilterQuery(String tableName, Filter filter, List<Range<Float>> ranges, String... cols) {
        String query = "select ";
        query += "count(*) from " + tableName + " where " + generateWhereClause(ranges, cols);
        query += " AND col" + filter.getFilterColumn() + (filter.getFilterPredicate().getOperator().equals(FilterOperator.LESS_THAN) ? " < " : " > ")
                + filter.getFilterPredicate().getConstant() + ";";
        return query;
    }
}