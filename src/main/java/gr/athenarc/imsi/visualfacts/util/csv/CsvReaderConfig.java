package gr.athenarc.imsi.visualfacts.util.csv;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Configuration object describing how a CSV file should be read.
 */
public final class CsvReaderConfig {
    private final File file;
    private final Charset charset;
    private final int[] selectedColumns;
    private final boolean headerExtraction;
    private final char delimiter;

    public CsvReaderConfig(File file, Charset charset, int[] selectedColumns,
                           boolean headerExtraction, char delimiter) {
        this.file = file;
        this.charset = charset;
        this.selectedColumns = selectedColumns == null ? null : Arrays.copyOf(selectedColumns, selectedColumns.length);
        this.headerExtraction = headerExtraction;
        this.delimiter = delimiter;
    }

    public File getFile() {
        return file;
    }

    public Charset getCharset() {
        return charset;
    }

    public int[] getSelectedColumns() {
        return selectedColumns == null ? null : Arrays.copyOf(selectedColumns, selectedColumns.length);
    }

    public boolean isHeaderExtraction() {
        return headerExtraction;
    }

    public char getDelimiter() {
        return delimiter;
    }
}
