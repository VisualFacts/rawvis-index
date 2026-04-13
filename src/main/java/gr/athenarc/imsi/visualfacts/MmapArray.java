package gr.athenarc.imsi.visualfacts;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Memory-mapped array of 8-byte elements (doubles or longs) backed by
 * pure JDK NIO {@link MappedByteBuffer}s.
 * <p>
 * Because a single {@code MappedByteBuffer} is limited to ~2 GiB
 * ({@code Integer.MAX_VALUE} bytes), the file is divided into segments
 * of up to {@link #SEGMENT_SIZE} bytes.
 * <p>
 * Element indices and count are {@code long} to support interleaved layouts
 * where the logical element count exceeds {@code Integer.MAX_VALUE}
 * (e.g. 3 × 1B = 3B elements for an interleaved x/y/offset array).
 * <p>
 * Thread safety: absolute-positioned reads are thread-safe (each call
 * computes the segment and offset independently). Writes at disjoint
 * positions from different threads are safe.
 */
public final class MmapArray implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(MmapArray.class);

    /** Segment size: 1 GiB — well under MappedByteBuffer's 2 GiB limit. */
    private static final long SEGMENT_SIZE = 1L << 30;  // 1 GiB

    /** Bit shift for dividing byte offset by SEGMENT_SIZE. */
    private static final int SEGMENT_SHIFT = 30;

    private MappedByteBuffer[] segments;
    private final Path filePath;
    private final long count;

    private MmapArray(Path filePath, long count, MappedByteBuffer[] segments) {
        this.filePath = filePath;
        this.count = count;
        this.segments = segments;
    }

    /**
     * Creates a new mmap file for {@code count} 8-byte elements.
     * The file is created at {@code file}, pre-sized to {@code count * 8} bytes,
     * and memory-mapped in 1 GiB segments.
     */
    public static MmapArray create(Path file, long count) throws IOException {
        long totalBytes = count * 8;

        // Delete any stale file from a previous crashed run
        Files.deleteIfExists(file);

        int numSegments = (int) ((totalBytes + SEGMENT_SIZE - 1) / SEGMENT_SIZE);
        if (numSegments == 0) numSegments = 1;
        MappedByteBuffer[] segs = new MappedByteBuffer[numSegments];

        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(totalBytes);
            FileChannel ch = raf.getChannel();
            for (int i = 0; i < numSegments; i++) {
                long offset = (long) i * SEGMENT_SIZE;
                long size = Math.min(SEGMENT_SIZE, totalBytes - offset);
                segs[i] = (MappedByteBuffer) ch.map(FileChannel.MapMode.READ_WRITE, offset, size)
                        .order(ByteOrder.nativeOrder());
            }
        }

        LOG.debug("Mmap created (NIO): {} ({} elements, {} GB, {} segments)",
                file.getFileName(), count,
                String.format("%.1f", totalBytes / (1024.0 * 1024 * 1024)),
                numSegments);
        return new MmapArray(file, count, segs);
    }

    /**
     * Opens an existing file as a read-write mmap array.
     * Use after writing the file via O_DIRECT to avoid page-fault storms
     * during the write phase.  Opened read-write because query-time
     * sub-partition ({@code swap}) needs write access.
     */
    public static MmapArray open(Path file, long count) throws IOException {
        long totalBytes = count * 8;
        int numSegments = (int) ((totalBytes + SEGMENT_SIZE - 1) / SEGMENT_SIZE);
        if (numSegments == 0) numSegments = 1;
        MappedByteBuffer[] segs = new MappedByteBuffer[numSegments];

        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            FileChannel ch = raf.getChannel();
            for (int i = 0; i < numSegments; i++) {
                long offset = (long) i * SEGMENT_SIZE;
                long size = Math.min(SEGMENT_SIZE, totalBytes - offset);
                segs[i] = (MappedByteBuffer) ch.map(FileChannel.MapMode.READ_WRITE, offset, size)
                        .order(ByteOrder.nativeOrder());
            }
        }

        LOG.debug("Mmap opened (NIO, read-write): {} ({} elements, {} GB, {} segments)",
                file.getFileName(), count,
                String.format("%.1f", totalBytes / (1024.0 * 1024 * 1024)),
                numSegments);
        return new MmapArray(file, count, segs);
    }

    public long getCount() { return count; }

    // ---- Double access (absolute element index) ----

    public double getDouble(long i) {
        long bytePos = i * 8;
        int seg = (int) (bytePos >>> SEGMENT_SHIFT);
        int off = (int) (bytePos & (SEGMENT_SIZE - 1));
        return segments[seg].getDouble(off);
    }

    public void putDouble(long i, double v) {
        long bytePos = i * 8;
        int seg = (int) (bytePos >>> SEGMENT_SHIFT);
        int off = (int) (bytePos & (SEGMENT_SIZE - 1));
        segments[seg].putDouble(off, v);
    }

    // ---- Long access (absolute element index) ----

    public long getLong(long i) {
        long bytePos = i * 8;
        int seg = (int) (bytePos >>> SEGMENT_SHIFT);
        int off = (int) (bytePos & (SEGMENT_SIZE - 1));
        return segments[seg].getLong(off);
    }

    public void putLong(long i, long v) {
        long bytePos = i * 8;
        int seg = (int) (bytePos >>> SEGMENT_SHIFT);
        int off = (int) (bytePos & (SEGMENT_SIZE - 1));
        segments[seg].putLong(off, v);
    }

    /**
     * Forces all dirty pages to disk.
     * Call after partition scatter is complete.
     */
    public void force() {
        if (segments != null) {
            for (MappedByteBuffer seg : segments) {
                if (seg != null) seg.force();
            }
        }
    }

    @Override
    public void close() {
        if (segments != null) {
            // MappedByteBuffers are unmapped by GC; no deterministic unmap in standard JDK.
            // Null out references so GC can reclaim.
            for (int i = 0; i < segments.length; i++) {
                segments[i] = null;
            }
            segments = null;
        }
        if (filePath != null) {
            try { Files.deleteIfExists(filePath); } catch (IOException ignored) { }
        }
    }
}
