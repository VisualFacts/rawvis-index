package gr.athenarc.imsi.visualfacts.util.csv;

import java.io.Closeable;
import java.io.IOException;

/**
 * Abstraction over CSV parsing so different implementations (Univocity/custom) can be plugged in.
 */
public interface CsvRowReader extends Closeable {

    /**
     * Opens the reader for the specified configuration.
     */
    void open(CsvReaderConfig config) throws IOException;

    /**
     * Reads the next row from the underlying CSV stream.
     *
     * @return an array with the selected column values or {@code null} if EOF.
     */
    String[] nextRow() throws IOException;

    /**
     * Parses a single CSV line provided as a string.
     */
    String[] parseLine(String line) throws IOException;

    /**
     * Byte offset of the most recently returned row.
     */
    long currentOffset();

    @Override
    void close() throws IOException;
}
