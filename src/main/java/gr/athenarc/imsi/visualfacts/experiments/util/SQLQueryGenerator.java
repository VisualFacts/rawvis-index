package gr.athenarc.imsi.visualfacts.experiments.util;

import java.util.List;

import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.Filter;
import gr.athenarc.imsi.visualfacts.query.FilterOperator;

public class SQLQueryGenerator {

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

    public static String getSQLUniAggQuery(String tableName, List<Range<Float>> ranges, String aggCol, String... cols) {
        String query = "select ";
        query += "count(" + aggCol + ") as count, min(" + aggCol + ") as min, max(" + aggCol + ") as max, sum(" + aggCol + ") as sum, avg(" + aggCol + ") as avg, sum(" + aggCol + " * " + aggCol + ") as sum_of_squares  from " + tableName + " where " + generateWhereClause(ranges, cols) + ";";
        return query;
    }

    /**
     * Generate a DuckDB-specific spatial query using ST_Within and ST_MakeEnvelope for R-tree index usage.
     * This query uses the geometry column and spatial index for efficient filtering.
     * 
     * @param tableName
     * @param ranges    the x and y ranges (first two ranges should be x and y)
     * @param aggCol    the aggregation column
     * @return a spatial range query using geometry and R-tree index
     */
    public static String getDuckDBSQLSpatialUniAggQuery(String tableName, List<Range<Float>> ranges, String aggCol) {
        if (ranges.size() < 2) {
            throw new IllegalArgumentException("Spatial queries require at least 2 ranges (x and y)");
        }
        
        Range<Float> xRange = ranges.get(0);
        Range<Float> yRange = ranges.get(1);
        
        String query = "SELECT count(" + aggCol + ") as count, " +
                      "min(" + aggCol + ") as min, " +
                      "max(" + aggCol + ") as max, " +
                      "sum(" + aggCol + ") as sum, " +
                      "avg(" + aggCol + ") as avg, " +
                      "sum(" + aggCol + " * " + aggCol + ") as sum_of_squares " +
                      "FROM " + tableName + " " +
                      "WHERE ST_Within(geometry, ST_MakeEnvelope(" + 
                      xRange.lowerEndpoint() + ", " + 
                      yRange.lowerEndpoint() + ", " + 
                      xRange.upperEndpoint() + ", " + 
                      yRange.upperEndpoint() + "));";
        return query;
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
            whereClause += col + " > " + range.lowerEndpoint() + " AND " + col + " < " + range.upperEndpoint();
            if (i < ranges.size() - 1) {
                whereClause += " AND ";
            }
            i++;
        }
        return whereClause;
    }

    public static String getSQLFilterQuery(String tableName, Filter filter, List<Range<Float>> ranges, String... cols) {
        String query = "select ";
        query += "count(*) from " + tableName + " where " + generateWhereClause(ranges, cols);
        query += " AND col" + filter.getFilterColumn() + (filter.getFilterPredicate().getOperator().equals(FilterOperator.LESS_THAN) ? " < " : " > ")
                + filter.getFilterPredicate().getConstant() + ";";
        return query;
    }
}