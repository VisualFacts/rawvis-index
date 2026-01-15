package gr.athenarc.imsi.visualfacts.experiments.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.query.Query;

/**
 * Utility class for executing SQL queries in DuckDB with three different
 * strategies:
 * 1. Direct query on CSV file
 * 2. Query on table created from CSV
 * 3. Query on table with spatial R-tree index
 */
public class DuckDBQueryExecutor {

    /**
     * Wrapper class for DuckDB aggregation statistics that mimics Guava's Stats
     * interface.
     * This class holds summary statistics computed directly from SQL aggregation
     * functions.
     */
    public static class StatsDuckDB {
        private final long count;
        private final double min;
        private final double max;
        private final double sum;
        private final double mean;
        private final double sumOfSquares;

        public StatsDuckDB(long count, double min, double max, double sum, double mean, double sumOfSquares) {
            this.count = count;
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.mean = mean;
            this.sumOfSquares = sumOfSquares;
        }

        public long count() {
            return count;
        }

        public double min() {
            return min;
        }

        public double max() {
            return max;
        }

        public double sum() {
            return sum;
        }

        public double mean() {
            return mean;
        }

        public double sumOfSquares() {
            return sumOfSquares;
        }

        public double populationStandardDeviation() {
            if (count <= 0)
                return 0.0;
            double variance = (sumOfSquares / count) - (mean * mean);
            return Math.sqrt(Math.max(0, variance));
        }

        public double sampleStandardDeviation() {
            if (count <= 1)
                return 0.0;
            double variance = (sumOfSquares - (sum * sum / count)) / (count - 1);
            return Math.sqrt(Math.max(0, variance));
        }

        @Override
        public String toString() {
            return String.format(
                    "StatsDuckDB{count=%d, min=%.4f, max=%.4f, sum=%.4f, mean=%.4f, sumOfSquares=%.4f, populationStandardDeviation=%.4f, sampleStandardDeviation=%.4f}",
                    count, min, max, sum, mean, sumOfSquares, populationStandardDeviation(), sampleStandardDeviation());
        }
    }

    private static final Logger LOG = LogManager.getLogger(DuckDBQueryExecutor.class);

    public enum ExecutionMode {
        DIRECT_CSV,
        TABLE,
        SPATIAL_INDEX
    }

    private Connection connection;
    private String csvPath;
    private String tableName = "data_table";
    private ExecutionMode mode;
    private String xCol;
    private String yCol;
    private boolean initialized = false;
    private long tableCreationTimeNanos = 0;
    private long indexCreationTimeNanos = 0;
    private Integer datasetColumnCount = null; // Cache the dataset column count

    public DuckDBQueryExecutor(String csvPath) throws Exception {
        this.csvPath = csvPath;
        initializeConnection();
    }

    public DuckDBQueryExecutor(String csvPath, ExecutionMode mode, String xCol, String yCol) throws Exception {
        this.csvPath = csvPath;
        this.mode = mode;
        this.xCol = xCol;
        this.yCol = yCol;
        initializeConnection();
        initializeResources();
    }

    private void initializeConnection() throws Exception {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            connection = DriverManager.getConnection("jdbc:duckdb::memory:");
            LOG.info("DuckDB connection established");
        } catch (Exception e) {
            LOG.error("Failed to initialize DuckDB connection", e);
            throw e;
        }
    }

    /**
     * Initialize resources (table and/or index) based on the execution mode.
     * This is called once during construction to set up all necessary structures.
     */
    private void initializeResources() throws Exception {
        if (initialized) {
            return;
        }

        try {
            switch (mode) {
                case DIRECT_CSV:
                    LOG.info("Direct CSV mode: no table/index creation needed");
                    break;
                case TABLE:
                    LOG.info("Table mode: creating table from CSV");
                    createTableFromCSV();
                    break;
                case SPATIAL_INDEX:
                    LOG.info("Spatial Index mode: creating table with geometry and R-tree index");
                    createTableWithSpatialIndex();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown execution mode: " + mode);
            }
            initialized = true;
            LOG.info("Resources initialized successfully for mode: {}", mode);
        } catch (Exception e) {
            LOG.error("Failed to initialize resources", e);
            throw e;
        }
    }


    private void createTableFromCSV() throws Exception {
        long startTime = System.nanoTime();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET memory_limit = '8GB'");
            String createTableQuery = String.format(
                    "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', ignore_errors = true);",
                    tableName, csvPath);
            LOG.info("Creating table from CSV: {}", createTableQuery);
            stmt.execute(createTableQuery);
            LOG.info("Table {} created successfully", tableName);
        }
        long endTime = System.nanoTime();
        tableCreationTimeNanos = endTime - startTime;
        LOG.info("Table creation time: {} s", tableCreationTimeNanos / 1_000_000_000.0);
    }

    private void createTableWithSpatialIndex() throws Exception {
        long tableStartTime = System.nanoTime();
        try (Statement stmt = connection.createStatement()) {
            // Load spatial extension
            LOG.info("Loading SPATIAL extension");
            stmt.execute("INSTALL SPATIAL;");
            stmt.execute("LOAD SPATIAL;");
            LOG.info("SPATIAL extension loaded");

            stmt.execute("SET memory_limit = '8GB'");
            // Create table from CSV with geometry column in a single statement
            String createTableWithGeomQuery = String.format(
                    "CREATE TABLE %s AS SELECT *, ST_Point(%s::DOUBLE, %s::DOUBLE) AS geometry FROM read_csv_auto('%s', ignore_errors = true);",
                    tableName, xCol, yCol, csvPath);
            LOG.info("Creating table from CSV with geometry column: {}", createTableWithGeomQuery);
            stmt.execute(createTableWithGeomQuery);
            LOG.info("Table {} created with geometry column", tableName);
        }
        long tableEndTime = System.nanoTime();
        tableCreationTimeNanos = tableEndTime - tableStartTime;

        long indexStartTime = System.nanoTime();
        try (Statement stmt = connection.createStatement()) {
            // Create R-tree spatial index on the geometry column
            String indexName = "spatial_index_" + tableName;
            String createIndexQuery = String.format(
                    "CREATE INDEX %s ON %s USING RTREE (geometry);",
                    indexName, tableName);
            LOG.info("Creating R-tree spatial index on geometry column: {}", createIndexQuery);
            stmt.execute(createIndexQuery);
            LOG.info("Spatial index {} created successfully", indexName);
        }
        long indexEndTime = System.nanoTime();
        indexCreationTimeNanos = indexEndTime - indexStartTime;
    }

    private List<Range<Float>> extractRangesFromQuery(Query query) {
        List<Range<Float>> ranges = new ArrayList<>();
        Rectangle rectangle = query.getRect();
        if (rectangle != null) {
            ranges.add(rectangle.getXRange());
            ranges.add(rectangle.getYRange());
        }
        return ranges;
    }

    /**
     * Execute query based on the configured execution mode.
     * This method handles mode distinction and delegates to appropriate execution
     * strategy.
     */
    public QueryResult executeQuery(Query query) throws Exception {
        if (mode == null) {
            throw new IllegalStateException("Execution mode not set. Use constructor with mode parameter.");
        }

        LOG.info("Executing query in {} mode", mode);
        List<Range<Float>> ranges = extractRangesFromQuery(query);

        // Determine column naming format based on actual dataset column count
        int columnCount = getDatasetColumnCount();
        boolean useTwoDigitFormat = columnCount > 10;

        List<String> measureColsString = query.getMeasureCols().stream().map(col -> {
            if (useTwoDigitFormat) {
                return "column" + String.format("%02d", col);
            } else {
                return "column" + col;
            }
        }).collect(Collectors.toList());

        // Format xCol and yCol with the same naming convention
        String formattedXCol = formatColumnName(xCol, useTwoDigitFormat);
        String formattedYCol = formatColumnName(yCol, useTwoDigitFormat);

        switch (mode) {
            case DIRECT_CSV:
                return executeQueryDirectCSV(ranges, measureColsString, formattedXCol, formattedYCol);
            case TABLE:
                return executeQueryWithTable(ranges, measureColsString, formattedXCol, formattedYCol);
            case SPATIAL_INDEX:
                return executeQueryWithSpatialIndex(ranges, measureColsString, formattedXCol, formattedYCol);
            default:
                throw new IllegalArgumentException("Unknown execution mode: " + mode);
        }
    }

    /**
     * Version 1: Run query directly on CSV file
     */
    public QueryResult executeQueryDirectCSV(List<Range<Float>> ranges, List<String> aggCols, String xCol, String yCol)
            throws Exception {

        String query = SQLQueryGenerator.getSQLUniAggQuery("\"" + csvPath + "\"", ranges, aggCols, xCol, yCol);

        LOG.trace("Executing direct CSV query: {}", query);
        return executeQueryWithTiming(query);
    }

    /**
     * Version 2: Run query on pre-created table
     */
    public QueryResult executeQueryWithTable(List<Range<Float>> ranges, List<String> aggCols, String xCol, String yCol)
            throws Exception {
        String query = SQLQueryGenerator.getSQLUniAggQuery(tableName, ranges, aggCols, xCol, yCol);

        LOG.trace("Executing table query: {}", query);
        return executeQueryWithTiming(query);
    }

    /**
     * Version 3: Run query on table with spatial R-tree index
     */
    public QueryResult executeQueryWithSpatialIndex(List<Range<Float>> ranges, List<String> aggCols, String xCol,
            String yCol) throws Exception {
        String query = SQLQueryGenerator.getDuckDBSQLSpatialUniAggQuery(tableName, ranges, aggCols);

        LOG.trace("Executing spatial index query: {}", query);
        return executeQueryWithTiming(query);
    }

    private QueryResult executeQueryWithTiming(String query) throws Exception {
        QueryResult result = new QueryResult();
        result.setQuery(query);

        long startTime = System.nanoTime();

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {

            Map<Integer, StatsDuckDB> measureStats = new HashMap<>();

            while (rs.next()) {
                result.incrementRowCount();

                // Process each aggregation column's statistics
                // Assuming column names follow pattern: count_*, min_*, max_*, sum_*, avg_*,
                // sum_of_squares_*
                Map<Integer, Double[]> statsData = new HashMap<>(); // [count, min, max, sum, avg, sum_sq]

                for (int colIdx = 1; colIdx <= rs.getMetaData().getColumnCount(); colIdx++) {
                    String colName = rs.getMetaData().getColumnName(colIdx);

                    // Extract measure index from column name
                    Integer measureIdx = extractMeasureIndex(colName);
                    if (measureIdx == null)
                        continue;

                    Double[] data = statsData.computeIfAbsent(measureIdx, k -> new Double[6]);
                    double value = rs.getDouble(colIdx);

                    if (colName.startsWith("count_")) {
                        data[0] = value;
                    } else if (colName.startsWith("min_")) {
                        data[1] = value;
                    } else if (colName.startsWith("max_")) {
                        data[2] = value;
                    } else if (colName.startsWith("sum_") && !colName.startsWith("sum_of_squares_")) {
                        data[3] = value;
                    } else if (colName.startsWith("avg_")) {
                        data[4] = value;
                    } else if (colName.startsWith("sum_of_squares_")) {
                        data[5] = value;
                    }
                }

                // Build StatsDuckDB objects from collected data
                for (Map.Entry<Integer, Double[]> entry : statsData.entrySet()) {
                    Integer measureIdx = entry.getKey();
                    Double[] data = entry.getValue();

                    if (data[0] != null && data[3] != null && data[5] != null) {
                        long count = data[0].longValue();
                        double sum = data[3];
                        double sumOfSquares = data[5];
                        double mean = mean(sum, count);
                        double min = data[1] != null ? data[1] : Double.NaN;
                        double max = data[2] != null ? data[2] : Double.NaN;

                        StatsDuckDB stats = new StatsDuckDB(count, min, max, sum, mean, sumOfSquares);
                        measureStats.put(measureIdx, stats);
                    }
                }
            }

            result.setMeasureStats(measureStats);

            long endTime = System.nanoTime();
            result.setExecutionTimeNanos(endTime - startTime);

            LOG.debug("DuckDB Query executed successfully. Rows: {}, Time: {} ns",
                    result.getRowCount(), result.getExecutionTimeNanos());
            LOG.trace("DuckDB query results: {}", measureStats);
        } catch (Exception e) {
            long endTime = System.nanoTime();
            result.setExecutionTimeNanos(endTime - startTime);
            result.setError(e.getMessage());
            LOG.error("Query execution failed", e);
            throw e;
        }

        return result;
    }

    private Integer extractMeasureIndex(String columnName) {
        // Extract measure index from column names like: count_column0, min_column1,
        // ..., count_column09, count_column10, etc.
        String[] parts = columnName.split("_");
        if (parts.length >= 2) {
            String columnPart = parts[parts.length - 1]; // Get last part after split
            if (columnPart.startsWith("column")) {
                try {
                    // Extract everything after "column"
                    String numberPart = columnPart.substring(6);
                    return Integer.parseInt(numberPart);
                } catch (NumberFormatException e) {
                    LOG.debug("Could not parse measure index from column: {}", columnName);
                }
            }
        }
        return null;
    }

    private double mean(double sum, long count) {
        return count > 0 ? sum / count : 0.0;
    }

    /**
     * Format a column name (or column index) with the appropriate naming
     * convention.
     * Converts strings like "0", "1", "column0" to "column0" or "column00" format.
     */
    private String formatColumnName(String colName, boolean useTwoDigitFormat) {
        // Extract the numeric part if it's just a number or already formatted
        String numberPart = colName;
        if (colName.startsWith("column")) {
            numberPart = colName.substring(6);
        }

        try {
            int colIndex = Integer.parseInt(numberPart);
            if (useTwoDigitFormat) {
                return "column" + String.format("%02d", colIndex);
            } else {
                return "column" + colIndex;
            }
        } catch (NumberFormatException e) {
            LOG.debug("Could not parse column index from: {}, using as-is", colName);
            return colName; // Return original if parsing fails
        }
    }

    /**
     * Get the column count of the dataset.
     * For TABLE and SPATIAL_INDEX modes, queries the actual table.
     * For DIRECT_CSV mode, infers from the CSV by reading the first row.
     * Result is cached for efficiency.
     */
    private int getDatasetColumnCount() throws Exception {
        if (datasetColumnCount != null) {
            return datasetColumnCount;
        }

        try {
            if (mode == ExecutionMode.DIRECT_CSV) {
                // For direct CSV, we need to count columns from the CSV file
                datasetColumnCount = getCSVColumnCount();
            } else {
                // For TABLE and SPATIAL_INDEX modes, query the table schema
                datasetColumnCount = getTableColumnCount();
            }
            LOG.debug("Dataset has {} columns", datasetColumnCount);
            return datasetColumnCount;
        } catch (Exception e) {
            LOG.warn("Failed to determine column count, defaulting to 10", e);
            datasetColumnCount = 10;
            return datasetColumnCount;
        }
    }

    /**
     * Get the number of columns in the table using DuckDB's information schema.
     */
    private int getTableColumnCount() throws Exception {
        String query = String.format(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '%s';",
                tableName);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 10; // default
    }

    /**
     * Get the number of columns in the CSV file by reading the header.
     */
    private int getCSVColumnCount() throws Exception {
        String query = String.format(
                "SELECT COUNT(*) FROM (SELECT * FROM read_csv_auto('%s', ignore_errors = true) LIMIT 0) AS t;",
                csvPath);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            // This approach gets column count by checking metadata
            if (rs.next()) {
                return rs.getMetaData().getColumnCount();
            }
        }
        return 10; // default
    }

    public long getTableCreationTimeNanos() {
        return tableCreationTimeNanos;
    }

    public long getIndexCreationTimeNanos() {
        return indexCreationTimeNanos;
    }

    public void close() throws Exception {
        try {
            // Drop table and index before closing connection
            if (initialized && mode != ExecutionMode.DIRECT_CSV) {
                dropResources();
            }
        } catch (Exception e) {
            LOG.warn("Error dropping resources during cleanup", e);
        }

        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("DuckDB connection closed");
        }
    }

    /**
     * Drop the table and index created during initialization.
     */
    private void dropResources() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            if (mode == ExecutionMode.SPATIAL_INDEX) {
                // Drop index first
                String indexName = "spatial_index_" + tableName;
                try {
                    String dropIndexQuery = String.format("DROP INDEX IF EXISTS %s;", indexName);
                    LOG.info("Dropping R-tree spatial index: {}", dropIndexQuery);
                    stmt.execute(dropIndexQuery);
                    LOG.info("Spatial index {} dropped successfully", indexName);
                } catch (Exception e) {
                    LOG.debug("Index {} may not exist or already dropped", indexName);
                }
            }

            // Drop table
            try {
                String dropTableQuery = String.format("DROP TABLE IF EXISTS %s;", tableName);
                LOG.info("Dropping table: {}", dropTableQuery);
                stmt.execute(dropTableQuery);
                LOG.info("Table {} dropped successfully", tableName);
            } catch (Exception e) {
                LOG.debug("Table {} may not exist or already dropped", tableName);
            }

            LOG.info("Resources cleaned up successfully");
        }
    }

    /**
     * Inner class to hold query results with statistical aggregations
     */
    public static class QueryResult {
        private String query;
        private long rowCount;
        private long executionTimeNanos;
        private String error;
        private Map<Integer, StatsDuckDB> measureStats = new HashMap<>();

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public long getRowCount() {
            return rowCount;
        }

        public void setRowCount(long rowCount) {
            this.rowCount = rowCount;
        }

        public void incrementRowCount() {
            this.rowCount++;
        }

        public long getExecutionTimeNanos() {
            return executionTimeNanos;
        }

        public void setExecutionTimeNanos(long executionTimeNanos) {
            this.executionTimeNanos = executionTimeNanos;
        }

        public double getExecutionTimeSeconds() {
            return executionTimeNanos / Math.pow(10d, 9);
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public boolean hasError() {
            return error != null;
        }

        public Map<Integer, StatsDuckDB> getMeasureStats() {
            return measureStats;
        }

        public void setMeasureStats(Map<Integer, StatsDuckDB> measureStats) {
            this.measureStats = measureStats;
        }

        @Override
        public String toString() {
            return String.format(
                    "DuckDBQueryResult{measureStats=%s}", measureStats);
        }

    }
}
