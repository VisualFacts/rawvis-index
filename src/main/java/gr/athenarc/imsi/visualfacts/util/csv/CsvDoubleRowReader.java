package gr.athenarc.imsi.visualfacts.util.csv;

import java.io.Closeable;
import java.io.IOException;

public interface CsvDoubleRowReader extends Closeable {
    void open(CsvReaderConfig config) throws IOException;
    double[] nextRow() throws IOException;   // returns reused array; null on EOF
    long currentOffset();
    void close() throws IOException;
}
