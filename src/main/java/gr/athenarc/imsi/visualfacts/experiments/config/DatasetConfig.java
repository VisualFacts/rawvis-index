package gr.athenarc.imsi.visualfacts.experiments.config;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import gr.athenarc.imsi.visualfacts.DataValidationFilter;
import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.experiments.util.QueryUtils;
import gr.athenarc.imsi.visualfacts.query.FilterOperator;
import gr.athenarc.imsi.visualfacts.query.FilterPredicate;

/**
 * Configuration class for dataset definitions in experiment YAML files.
 * Maps to the datasets section and can be converted to a Schema object.
 */
public class DatasetConfig {

    @JsonProperty("csv")
    private String csv;

    @JsonProperty("hasHeader")
    private boolean hasHeader = false;

    @JsonProperty("delimiter")
    private String delimiter = ",";

    @JsonProperty("xColumn")
    private int xColumn;

    @JsonProperty("yColumn")
    private int yColumn;

    @JsonProperty("measureCols")
    private List<Integer> measureCols = new ArrayList<>();

    @JsonProperty("bounds")
    private String bounds;

    @JsonProperty("objectCount")
    private int objectCount;

    @JsonProperty("validationFilters")
    private List<String> validationFilters = new ArrayList<>();

    // Default constructor for Jackson
    public DatasetConfig() {
    }

    /**
     * Converts this config to a Schema object.
     * If csv path starts with "classpath:", resolves it from the classpath.
     */
    public Schema toSchema() {
        Rectangle boundsRect = QueryUtils.convertToRectangle(bounds);
        List<DataValidationFilter> filters = parseValidationFilters();
        String resolvedCsv = resolveCsvPath(csv);
        char delimChar = parseDelimiter(delimiter);
        Schema schema = new Schema(resolvedCsv, delimChar, xColumn, yColumn, measureCols, boundsRect, objectCount, filters);
        schema.setHasHeader(hasHeader);
        return schema;
    }

    /**
     * Parses a delimiter string, handling escape sequences like \t for tab.
     */
    private char parseDelimiter(String delim) {
        if (delim == null || delim.isEmpty()) {
            return ',';
        }
        if (delim.equals("\\t")) {
            return '\t';
        }
        if (delim.equals("\\n")) {
            return '\n';
        }
        if (delim.equals("\\r")) {
            return '\r';
        }
        return delim.charAt(0);
    }

    /**
     * Resolves the CSV path. If it starts with "classpath:", looks up the resource
     * from the classpath and returns its absolute file path.
     */
    private String resolveCsvPath(String csvPath) {
        if (csvPath == null) {
            return null;
        }
        if (csvPath.startsWith("classpath:")) {
            String resourcePath = csvPath.substring("classpath:".length());
            URL resource = getClass().getClassLoader().getResource(resourcePath);
            if (resource == null) {
                throw new IllegalArgumentException("Classpath resource not found: " + resourcePath);
            }
            try {
                return Paths.get(resource.toURI()).toString();
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid classpath resource URI: " + resourcePath, e);
            }
        }
        return csvPath;
    }

    private List<DataValidationFilter> parseValidationFilters() {
        List<DataValidationFilter> filters = new ArrayList<>();
        for (String filterStr : validationFilters) {
            if (filterStr == null || filterStr.trim().isEmpty()) {
                continue;
            }
            filters.add(parseValidationFilter(filterStr.trim()));
        }
        return filters;
    }

    private DataValidationFilter parseValidationFilter(String value) {
        // Extract column, operator, and value
        String[] parts = value.split("(?=[<>=!])", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid filter format: " + value);
        }

        int columnIndex = Integer.parseInt(parts[0].trim());
        String operatorWithValue = parts[1].trim();

        FilterOperator operator = extractOperator(operatorWithValue);
        double constant = Double.parseDouble(operatorWithValue.replaceAll("[^0-9.\\-]", ""));

        return new DataValidationFilter(columnIndex, new FilterPredicate(operator, constant));
    }

    private FilterOperator extractOperator(String condition) {
        if (condition.startsWith("<=")) return FilterOperator.LESS_THAN_OR_EQUAL;
        if (condition.startsWith(">=")) return FilterOperator.GREATER_THAN_OR_EQUAL;
        if (condition.startsWith("==")) return FilterOperator.EQUAL;
        if (condition.startsWith("!=")) return FilterOperator.NOT_EQUAL;
        if (condition.startsWith("<")) return FilterOperator.LESS_THAN;
        if (condition.startsWith(">")) return FilterOperator.GREATER_THAN;
        throw new IllegalArgumentException("Invalid operator in condition: " + condition);
    }

    // Getters and Setters

    public String getCsv() {
        return csv;
    }

    public void setCsv(String csv) {
        this.csv = csv;
    }

    public boolean isHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public int getxColumn() {
        return xColumn;
    }

    public void setxColumn(int xColumn) {
        this.xColumn = xColumn;
    }

    public int getyColumn() {
        return yColumn;
    }

    public void setyColumn(int yColumn) {
        this.yColumn = yColumn;
    }

    public List<Integer> getMeasureCols() {
        return measureCols;
    }

    public void setMeasureCols(List<Integer> measureCols) {
        this.measureCols = measureCols;
    }

    public String getBounds() {
        return bounds;
    }

    public void setBounds(String bounds) {
        this.bounds = bounds;
    }

    public int getObjectCount() {
        return objectCount;
    }

    public void setObjectCount(int objectCount) {
        this.objectCount = objectCount;
    }

    public List<String> getValidationFilters() {
        return validationFilters;
    }

    public void setValidationFilters(List<String> validationFilters) {
        this.validationFilters = validationFilters;
    }

    @Override
    public String toString() {
        return "DatasetConfig{" +
                "csv='" + csv + '\'' +
                ", hasHeader=" + hasHeader +
                ", delimiter='" + delimiter + '\'' +
                ", xColumn=" + xColumn +
                ", yColumn=" + yColumn +
                ", measureCols=" + measureCols +
                ", bounds='" + bounds + '\'' +
                ", objectCount=" + objectCount +
                ", validationFilters=" + validationFilters +
                '}';
    }
}
