package gr.athenarc.imsi.visualfacts.util.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import gr.athenarc.imsi.visualfacts.util.csv.ZsvNative;

/**
 * Pipelined random-access CSV row reader.
 * <p>
 * Given a set of file byte offsets (sorted ascending), reads the corresponding
 * CSV rows from disk in a single pipelined I/O pass, parses the requested
 * measure columns into doubles, and exposes them via typed accessors.
 * <p>
 * Internally backed by io_uring for high-throughput async I/O, but the API is
 * I/O-backend-agnostic. Manages reusable direct {@link ByteBuffer}s and the
 * native handle lifecycle.
 * <p>
 * Typical usage:
 * <pre>{@code
 * try (RandomAccessRowReader reader = new RandomAccessRowReader(csvPath)) {
 *     reader.readBatch(sortedOffsets, count, sortedMeasureCols, delimiter);
 *     for (int r = 0; r < reader.rowCount(); r++) {
 *         for (int m = 0; m < numMeasures; m++) {
 *             if (reader.isPresent(r, m)) {
 *                 double v = reader.getValue(r, m);
 *             }
 *         }
 *     }
 * }
 * }</pre>
 */
public class RandomAccessRowReader implements AutoCloseable {

    /**
     * Maximum rows per native batch call.  Keeps memory bounded regardless of
     * query size while still providing enough rows (~1600 unique 4KB pages at
     * ~20 rows/page) to keep the io_uring pipeline's 256 in-flight slots full.
     */
    public static final int BATCH_SIZE = 32_768;

    private long handle;

    // Reusable direct ByteBuffers — grown on demand, never shrunk
    private ByteBuffer offsetsBuf;
    private ByteBuffer valuesBuf;
    private ByteBuffer presentBuf;
    private int bufCapacity;   // current capacity in rows
    private int bufMeasures;   // number of measure columns the buffers were sized for

    // Result of the last readBatch call
    private int lastRowCount;
    private int lastMeasureCount;

    /**
     * Opens the file for pipelined random-access reading.
     *
     * @param path         absolute path to the CSV file
     * @param maxRowLength largest row length in bytes observed during the init
     *                     scan; determines the per-I/O read window so that every
     *                     row is guaranteed to fit regardless of page alignment
     * @throws RuntimeException wrapping IOException if the file cannot be opened
     */
    public RandomAccessRowReader(String path, int maxRowLength) {
        this.handle = ZsvNative.openUringReader(path, maxRowLength);
    }

    /**
     * Reads {@code rowCount} CSV rows at the given sorted file offsets, parses
     * the requested measure columns, and stores results in internal buffers
     * accessible via {@link #getValue} and {@link #isPresent}.
     *
     * @param sortedOffsets    primitive array of file byte offsets, sorted ascending
     * @param rowCount         number of valid entries in {@code sortedOffsets}
     * @param sortedMeasureCols sorted 0-based column indices to extract
     * @param delimiter        CSV delimiter byte (e.g. {@code ','})
     * @return number of rows read (same as {@code rowCount})
     */
    public int readBatch(long[] sortedOffsets, int rowCount, int[] sortedMeasureCols, byte delimiter) {
        if (rowCount == 0) {
            lastRowCount = 0;
            lastMeasureCount = sortedMeasureCols.length;
            return 0;
        }

        int nm = sortedMeasureCols.length;
        ensureBuffers(rowCount, nm);

        // Fill offsets buffer from primitive array
        offsetsBuf.clear();
        for (int i = 0; i < rowCount; i++) {
            offsetsBuf.putLong(sortedOffsets[i]);
        }

        // Issue pipelined async batch read (C++ side zeros presentBuf via memset)
        offsetsBuf.clear();
        valuesBuf.clear();
        presentBuf.clear();
        ZsvNative.readRowBatch(
                handle,
                offsetsBuf, rowCount,
                sortedMeasureCols, delimiter,
                valuesBuf, presentBuf);

        lastRowCount = rowCount;
        lastMeasureCount = nm;
        return rowCount;
    }

    /**
     * Returns the number of rows from the last {@link #readBatch} call.
     */
    public int rowCount() {
        return lastRowCount;
    }

    /**
     * Returns the parsed double value at the given row and measure position.
     * Only meaningful when {@link #isPresent(int, int)} returns {@code true}.
     *
     * @param rowIdx     row index (0-based, &lt; {@link #rowCount()})
     * @param measurePos position within the sorted measure columns array (0-based)
     */
    public double getValue(int rowIdx, int measurePos) {
        return valuesBuf.getDouble((rowIdx * lastMeasureCount + measurePos) * 8);
    }

    /**
     * Returns {@code true} if the value at the given row and measure position
     * was successfully parsed (not NaN / missing / unparseable).
     *
     * @param rowIdx     row index (0-based, &lt; {@link #rowCount()})
     * @param measurePos position within the sorted measure columns array (0-based)
     */
    public boolean isPresent(int rowIdx, int measurePos) {
        return presentBuf.get(rowIdx * lastMeasureCount + measurePos) != 0;
    }

    /**
     * Ensures the internal direct ByteBuffers are large enough for the given
     * row count and measure count. Allocates new (larger) buffers if necessary,
     * doubling capacity to amortise allocation cost.
     */
    private void ensureBuffers(int neededRows, int nm) {
        if (neededRows <= bufCapacity && nm == bufMeasures) return;
        int newRows = Math.min(BATCH_SIZE, Math.max(neededRows, bufCapacity * 2 + 256));
        offsetsBuf  = ByteBuffer.allocateDirect(newRows * 8).order(ByteOrder.nativeOrder());
        valuesBuf   = ByteBuffer.allocateDirect(newRows * nm * 8).order(ByteOrder.nativeOrder());
        presentBuf  = ByteBuffer.allocateDirect(newRows * nm);
        bufCapacity = newRows;
        bufMeasures = nm;
    }

    @Override
    public void close() {
        if (handle != 0) {
            ZsvNative.closeUringReader(handle);
            handle = 0;
        }
        offsetsBuf  = null;
        valuesBuf   = null;
        presentBuf  = null;
        bufCapacity = 0;
        bufMeasures = 0;
    }
}
