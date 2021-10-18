package gr.athenarc.imsi.visualfacts;

import com.univocity.parsers.csv.CsvParserSettings;

import java.util.*;
import java.util.stream.Collectors;

public class Schema {
    private final String csv;
    private boolean hasHeader = false;
    private final int xColumn;
    private final int yColumn;
    private final Integer measureCol0;
    private final Integer measureCol1;
    private final Rectangle bounds;
    private final Map<Integer, CategoricalColumn> categoricalColumns = new HashMap();
    private Set<Integer> dedupCols;
    private Character delimiter = ',';
    private int objectCount;
    private int idColumn;

    public Schema(String csv, Character delimiter, int xColumn, int yColumn, Integer measureCol0, Integer measureCol1, Rectangle bounds, int objectCount, int idColumn) {
        this.csv = csv;
        this.delimiter = delimiter;
        this.xColumn = xColumn;
        this.yColumn = yColumn;
        this.measureCol0 = measureCol0;
        this.measureCol1 = measureCol1;
        this.bounds = bounds;
        this.objectCount = objectCount;
        this.idColumn = idColumn;
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

    public int getidColumn() {
        return idColumn;
    }

    public Integer getMeasureCol0() {
        return measureCol0;
    }

    public Integer getMeasureCol1() {
        return measureCol1;
    }

    public List<CategoricalColumn> getCategoricalColumns() {
        return categoricalColumns.values().stream().sorted(Comparator.comparingInt(CategoricalColumn::getIndex)).collect(Collectors.toList());
    }

    public void setCategoricalColumns(List<CategoricalColumn> categoricalCols) {
        for (CategoricalColumn categoricalColumn : categoricalCols) {
            categoricalColumns.put(categoricalColumn.getIndex(), categoricalColumn);
        }
    }

    public Set<Integer> getDedupCols() {
        return dedupCols;
    }

    public void setDedupCols(Set<Integer> dedupCols) {
        this.dedupCols = dedupCols;
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
        parserSettings.getFormat().setQuote('“');
        parserSettings.setIgnoreLeadingWhitespaces(false);
        parserSettings.setIgnoreTrailingWhitespaces(false);
        return parserSettings;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "csv='" + csv + '\'' +
                ", hasHeader=" + hasHeader +
                ", xColumn=" + xColumn +
                ", yColumn=" + yColumn +
                ", measureCol0=" + measureCol0 +
                ", measureCol1=" + measureCol1 +
                ", bounds=" + bounds +
                ", categoricalColumns=" + categoricalColumns +
                ", dedupCols=" + dedupCols +
                ", delimiter=" + delimiter +
                ", objectCount=" + objectCount +
                ", idColumn=" + idColumn +
                '}';
    }
}
