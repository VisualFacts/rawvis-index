package gr.athenarc.imsi.visualfacts.util.csv;

import java.nio.ByteBuffer;

public final class ZsvNative {
    static {
        System.loadLibrary("zsv_jni");
    }

    public static native long open(String path, byte delimiter, boolean skipHeader, int[] selectedCols, byte[] nullstr);

    /**
     * Opens a ZSV parser starting at a specific byte offset within the file.
     * Used for parallel chunk-based CSV scanning. The parser will stop after
     * consuming bytes up to {@code endOffset}. No header is skipped — the
     * caller must ensure {@code startOffset} points past any header line.
     * <p>
     * Byte offsets reported by {@link #nextBatchDoubles} are absolute (adjusted
     * by {@code startOffset}).
     *
     * @param path         absolute path to the CSV file
     * @param delimiter    column delimiter byte
     * @param startOffset  byte offset to start parsing from
     * @param endOffset    exclusive end byte offset; -1 = read to EOF
     * @param selectedCols zero-based column indices to extract
     * @return opaque native handle, or 0 on failure (exception thrown)
     */
    public static native long openAtOffset(String path, byte delimiter,
                                           long startOffset, long endOffset,
                                           int[] selectedCols, byte[] nullstr);

    public static native void close(long handle);

    /**
     * Returns the maximum row length (in bytes, excluding line terminator)
     * observed across all rows scanned so far by the given handle.
     * Must be called before {@link #close(long)}.
     */
    public static native long getMaxRowLength(long handle);

    public static native int nextBatchDoubles(
            long handle,
            int maxRows,
            ByteBuffer offsets8,
            ByteBuffer valuesF8,
            ByteBuffer presentB1 // byte[maxRows * k]
    );

    // ---- io_uring batch reader ----

    /**
     * Opens a file for pipelined random-access reading via io_uring.
     * Initialises the submission ring, allocates a 256-slot page-aligned buffer
     * pool, and registers both buffers and the file descriptor with the kernel
     * (reduces per-request overhead on every subsequent readRowBatch call).
     * posix_fadvise(FADV_RANDOM) is set so the OS does not waste bandwidth on
     * sequential readahead that would evict useful cached pages.
     * <p>
     * The {@code maxRowLength} parameter (obtained from {@link #getMaxRowLength}
     * during the init scan) determines the per-buffer read size so that every
     * row is guaranteed to fit within a single I/O completion regardless of
     * page alignment.
     *
     * @param path         absolute path to the CSV file
     * @param maxRowLength largest row length in bytes (from init scan)
     * @return opaque native handle, or 0 on failure (exception thrown)
     */
    public static native long openUringReader(String path, int maxRowLength);

    /**
     * Closes the io_uring ring, frees all buffers, and closes the file
     * descriptor opened by {@link #openUringReader}.
     *
     * @param handle value returned by {@link #openUringReader}
     */
    public static native void closeUringReader(long handle);

    /**
     * Reads {@code rowCount} CSV rows in a pipelined async batch and parses
     * the requested measure columns into the output buffers.
     *
     * <p>The offsets must be <em>sorted ascending</em> (as produced by the
     * KWayMerge iterator). Consecutive rows on the same 4 KB page are
     * deduplicated into a single I/O request. Up to 256 reads are kept
     * in-flight simultaneously so the SSD's internal command queue stays full.
     *
     * <p>All three ByteBuffers must be {@code ByteBuffer.allocateDirect}. The
     * caller is responsible for sizing:
     * <ul>
     *   <li>{@code offsets8}  — {@code rowCount * 8} bytes (int64_t per row)
     *   <li>{@code values8}   — {@code rowCount * numMeasures * 8} bytes (double per cell)
     *   <li>{@code present1}  — {@code rowCount * numMeasures} bytes (1 = valid, 0 = NaN/missing)
     * </ul>
     *
     * @param handle      value returned by {@link #openUringReader}
     * @param offsets8    sorted row file offsets (int64_t[rowCount])
     * @param rowCount    number of rows
     * @param measureCols sorted measure column indices (0-based, ascending)
     * @param delimiter   CSV column delimiter byte (e.g. ',' or '\t')
     * @param values8     output: parsed doubles, row-major [rowCount][numMeasures]
     * @param present1    output: presence flags, row-major [rowCount][numMeasures]
     */
    public static native void readRowBatch(
            long handle,
            ByteBuffer offsets8,
            int rowCount,
            int[] measureCols,
            byte delimiter,
            ByteBuffer values8,
            ByteBuffer present1,
            byte[] nullstr
    );

    private ZsvNative() {
    }
}
