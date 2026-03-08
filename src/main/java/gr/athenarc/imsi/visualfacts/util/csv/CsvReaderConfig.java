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
    private final boolean skipHeader;
    private final char delimiter;
    private final long startOffset;  // byte offset to start from (0 = beginning)
    private final long endOffset;    // exclusive end byte; -1 = read to EOF

    public CsvReaderConfig(File file, Charset charset, int[] selectedColumns,
                           boolean skipHeader, char delimiter) {
        this(file, charset, selectedColumns, skipHeader, delimiter, 0, -1);
    }

    public CsvReaderConfig(File file, Charset charset, int[] selectedColumns,
                           boolean skipHeader, char delimiter,
                           long startOffset, long endOffset) {
        this.file = file;
        this.charset = charset;
        this.selectedColumns = selectedColumns == null ? null : Arrays.copyOf(selectedColumns, selectedColumns.length);
        this.skipHeader = skipHeader;
        this.delimiter = delimiter;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
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

    public char getDelimiter() {
        return delimiter;
    }

    public boolean isSkipHeader() {
        return skipHeader;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    /** Returns true if this config describes a chunk-based read (non-zero start offset or bounded end). */
    public boolean isChunkBased() {
        return startOffset > 0 || endOffset >= 0;
    }
}
