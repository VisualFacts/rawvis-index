package gr.athenarc.imsi.visualfacts;

import com.univocity.parsers.csv.CsvParserSettings;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Schema {
    private final String csv;
    private boolean hasHeader = false;
    private final int xColumn;
    private final int yColumn;
    private final List<Integer> measureCols;
    private final Map<Integer, Integer> measureColToIndex = new HashMap<>();
    private final Rectangle bounds;
    private final Map<Integer, CategoricalColumn> categoricalColumns = new HashMap();
    private Character delimiter = ',';
    private int objectCount;

    private List<DataValidationFilter> validationFilters;

    public Schema(String csv, Character delimiter, int xColumn, int yColumn, List<Integer> measureCols, Rectangle bounds, int objectCount, List<DataValidationFilter> validationFilters) {
        this.csv = csv;
        this.delimiter = delimiter;
        this.xColumn = xColumn;
        this.yColumn = yColumn;
        this.measureCols = measureCols;
        for (int i = 0; i < measureCols.size(); i++) {
            measureColToIndex.put(measureCols.get(i), i);
        }
        this.bounds = bounds;
        this.objectCount = objectCount;
        this.validationFilters = validationFilters;
    }

    public boolean getHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    public String getCsv() {
        return csv;
    }

    public Short getColValue(String[] parsedRow, int col) {
        String rawValue = parsedRow[col];
        return categoricalColumns.get(col).getValueKey(rawValue);
    }

    public Rectangle getBounds() {
        return bounds;
    }

    public int getxColumn() {
        return xColumn;
    }

    public int getyColumn() {
        return yColumn;
    }

    public List<Integer> getMeasureCols() {
        return measureCols;
    }

    public int getMeasureIndex(int colNumber) {
        return measureColToIndex.get(colNumber);
    }

    public int getMeasureCount() {
        return measureCols.size();
    }

    public List<CategoricalColumn> getCategoricalColumns() {
        return categoricalColumns.values().stream().sorted(Comparator.comparingInt(CategoricalColumn::getIndex)).collect(Collectors.toList());
    }

    public void setCategoricalColumns(List<CategoricalColumn> categoricalCols) {
        for (CategoricalColumn categoricalColumn : categoricalCols) {
            categoricalColumns.put(categoricalColumn.getIndex(), categoricalColumn);
        }
    }

    public CategoricalColumn getCategoricalColumn(int colIndex) {
        return categoricalColumns.get(colIndex);
    }

    public int getObjectCount() {
        return objectCount;
    }

    public CsvParserSettings createCsvParserSettings() {
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(delimiter);
        parserSettings.setIgnoreLeadingWhitespaces(false);
        parserSettings.setIgnoreTrailingWhitespaces(false);
        return parserSettings;
    }

    

    public List<DataValidationFilter> getValidationFilters() {
        return validationFilters;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "csv='" + csv + '\'' +
                ", hasHeader=" + hasHeader +
                ", xColumn=" + xColumn +
                ", yColumn=" + yColumn +
                ", measureCols=" + measureCols +
                ", bounds=" + bounds +
                ", categoricalColumns=" + categoricalColumns +
                ", delimiter=" + delimiter +
                ", objectCount=" + objectCount +
                '}';
    }
}
