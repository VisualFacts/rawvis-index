package gr.athenarc.imsi.visualfacts.util.csv;

import java.io.IOException;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * CsvRowReader backed by Univocity CsvParser.
 */
public class UnivocityCsvDoubleRowReader implements CsvDoubleRowReader {
    private CsvParser parser;
    private long lastOffset;

    @Override
    public void open(CsvReaderConfig config) throws IOException {
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setDelimiter(config.getDelimiter());
        settings.setHeaderExtractionEnabled(config.isSkipHeader());
        int[] cols = config.getSelectedColumns();
        if (cols != null) {
            Integer[] boxed = new Integer[cols.length];
            for (int i = 0; i < cols.length; i++) {
                boxed[i] = cols[i];
            }
            settings.selectIndexes(boxed);
        }
        settings.setColumnReorderingEnabled(true);

        parser = new CsvParser(settings);
        if (config.getFile() != null) {
            parser.beginParsing(config.getFile(), config.getCharset());
        }
        lastOffset = 0L;
    }

    @Override
    public double[] nextRow() {
        if (parser == null) {
            return null;
        }
        lastOffset = parser.getContext().currentChar() - 1;
        String[] row = parser.parseNext();
        if (row == null) {
            return null;
        }
        double[] doubleRow = new double[row.length];
        for (int i = 0; i < row.length; i++) {
            if (row[i] == null || row[i].isEmpty()) {
                doubleRow[i] = Double.NaN;
            } else {
                doubleRow[i] = Double.parseDouble(row[i]);
            }
        }
        return doubleRow;
    }

    @Override
    public long currentOffset() {
        return lastOffset;
    }

    @Override
    public void close() throws IOException {
        if (parser != null) {
            parser.stopParsing();
            parser = null;
        }
    }

    @Override
    public long maxRowLength() {
        throw new UnsupportedOperationException("Unimplemented method 'maxRowLength'");
    }
}
