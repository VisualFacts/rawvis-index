package gr.athenarc.imsi.visualfacts.util.csv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Row-by-row CSV reader that returns doubles (assumes all selected columns are
 * numeric).
 *
 * Backed by zsv via JNI:
 * - Uses nextBatchDoubles() to parse rows in native code in batches.
 * - Exposes a row-by-row API: nextRow() returns a reused double[].
 * - Exposes current row byte offset via currentOffset().
 *
 * Missing/invalid cells are returned as Double.NaN using a native-produced
 * presence buffer
 * (presentB1), without paying a full-batch NaN initialization cost.
 */
public final class ZsvCsvDoubleRowReader implements CsvDoubleRowReader {

    private static final int DEFAULT_MAX_ROWS_PER_BATCH = 131072;  // 128K rows per JNI call

    private CsvReaderConfig config;

    private long handle;

    private int selCount;
    private double[] rowBuffer;
    private long currentOffset = -1;

    // Batch sizing
    private int maxRowsPerBatch = DEFAULT_MAX_ROWS_PER_BATCH;

    // Batch buffers (direct, reused)
    private ByteBuffer offsets8; // long[maxRows]
    private ByteBuffer valuesF8; // double[maxRows * k]
    private ByteBuffer presentB1; // byte[maxRows * k] (0=missing/invalid, 1=present)

    // Current batch state
    private int rowsInBatch = 0;
    private int batchRowIndex = 0;

    public ZsvCsvDoubleRowReader() {
    }

    public ZsvCsvDoubleRowReader withMaxRowsPerBatch(int maxRowsPerBatch) {
        if (maxRowsPerBatch <= 0) {
            throw new IllegalArgumentException("maxRowsPerBatch must be > 0");
        }
        this.maxRowsPerBatch = maxRowsPerBatch;
        return this;
    }

    @Override
    public void open(CsvReaderConfig config) throws IOException {
        close();

        if (config == null) {
            throw new IOException("CsvReaderConfig must not be null");
        }
        if (config.getFile() == null) {
            throw new IOException("CsvReaderConfig.file must not be null");
        }

        int[] selectedCols = config.getSelectedColumns();
        if (selectedCols == null || selectedCols.length == 0) {
            throw new IOException("CsvReaderConfig.selectedColumns must not be null/empty");
        }

        this.config = config;
        this.selCount = selectedCols.length;
        this.rowBuffer = new double[selCount];

        allocateBatchBuffers();

        this.currentOffset = -1;
        this.rowsInBatch = 0;
        this.batchRowIndex = 0;

        boolean skipHeader = config.isSkipHeader();

        this.handle = ZsvNative.open(
                config.getFile().getAbsolutePath(),
                (byte) config.getDelimiter(),
                skipHeader,
                selectedCols);

        if (this.handle == 0) {
            cleanupState();
            throw new IOException("Failed to open zsv native double reader (handle=0)");
        }
    }

    private void allocateBatchBuffers() throws IOException {
        // offsets: 8 * maxRows
        long offsetsBytesL = 8L * (long) maxRowsPerBatch;
        if (offsetsBytesL > Integer.MAX_VALUE) {
            throw new IOException("offsets buffer too large");
        }
        this.offsets8 = ByteBuffer.allocateDirect((int) offsetsBytesL).order(ByteOrder.nativeOrder());

        // values: 8 * (maxRows * k)
        long valueCount = (long) maxRowsPerBatch * (long) selCount;
        long valuesBytesL = 8L * valueCount;
        if (valuesBytesL > Integer.MAX_VALUE) {
            throw new IOException("values buffer too large");
        }
        this.valuesF8 = ByteBuffer.allocateDirect((int) valuesBytesL).order(ByteOrder.nativeOrder());

        // present: 1 * (maxRows * k)
        long presentBytesL = valueCount;
        if (presentBytesL > Integer.MAX_VALUE) {
            throw new IOException("present buffer too large");
        }
        this.presentB1 = ByteBuffer.allocateDirect((int) presentBytesL).order(ByteOrder.nativeOrder());
    }

    @Override
    public double[] nextRow() throws IOException {
        if (handle == 0) {
            throw new IOException("Reader is not opened. Call open(config) first.");
        }

        if (batchRowIndex >= rowsInBatch) {
            fetchNextBatch();
            if (rowsInBatch == 0) {
                currentOffset = -1;
                return null; // EOF
            }
        }

        final int r = batchRowIndex++;
        currentOffset = offsets8.getLong(r * 8);

        // Read this row's doubles from the batch buffers into rowBuffer.
        // Layout: idx = r*k + c
        final int base = r * selCount;
        for (int c = 0; c < selCount; c++) {
            int idx = base + c;

            byte p = presentB1.get(idx); // absolute read
            if (p == 0) {
                rowBuffer[c] = Double.NaN;
            } else {
                rowBuffer[c] = valuesF8.getDouble(idx * 8); // absolute read (byte offset)
            }
        }

        return rowBuffer;
    }

    private void fetchNextBatch() throws IOException {
        batchRowIndex = 0;

        // New JNI signature includes presentB1
        rowsInBatch = ZsvNative.nextBatchDoubles(handle, maxRowsPerBatch, offsets8, valuesF8, presentB1);
    }

    @Override
    public long currentOffset() {
        return currentOffset;
    }

    @Override
    public long maxRowLength() {
        if (handle == 0) return 0;
        return ZsvNative.getMaxRowLength(handle);
    }

    @Override
    public void close() throws IOException {
        if (handle != 0) {
            try {
                ZsvNative.close(handle);
            } finally {
                handle = 0;
            }
        }
        cleanupState();
    }

    private void cleanupState() {
        config = null;
        selCount = 0;
        rowBuffer = null;

        offsets8 = null;
        valuesF8 = null;
        presentB1 = null;

        rowsInBatch = 0;
        batchRowIndex = 0;
        currentOffset = -1;
    }
}
