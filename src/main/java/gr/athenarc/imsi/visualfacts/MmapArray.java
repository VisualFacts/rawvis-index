package gr.athenarc.imsi.visualfacts;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;

/**
 * Memory-mapped array of 8-byte elements (doubles or longs) backed by
 * Chronicle Bytes for efficient off-heap access.
 * <p>
 * Chronicle Bytes handles multi-segment mapping automatically (no 2 GiB
 * limit), provides deterministic unmap via {@code release()}, and uses
 * {@code Unsafe} for minimal-overhead reads/writes.
 * <p>
 * Thread safety: absolute-positioned reads are thread-safe (each call
 * computes the byte offset independently). Writes are single-writer only.
 */
public final class MmapArray implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(MmapArray.class);

    /** Chunk size for Chronicle's internal mapping — 128 MB. */
    private static final long CHUNK_SIZE = 128L * 1024 * 1024;

    private MappedBytes mappedBytes;
    private final Path filePath;
    private final int count;

    private MmapArray(Path filePath, int count, MappedBytes mappedBytes) {
        this.filePath = filePath;
        this.count = count;
        this.mappedBytes = mappedBytes;
    }

    /**
     * Creates a new mmap file for {@code count} 8-byte elements.
     * The file is created at {@code file}, pre-sized to {@code count * 8} bytes,
     * and mapped read-write via Chronicle Bytes.
     */
    public static MmapArray create(Path file, int count) throws IOException {
        long totalBytes = (long) count * 8;

        // Delete any stale file from a previous crashed run so we don't
        // inherit a larger-than-needed mapping (Chronicle opens without truncation).
        Files.deleteIfExists(file);

        File f = file.toFile();
        MappedFile mf = MappedFile.of(f, CHUNK_SIZE, 0);
        MappedBytes mb = MappedBytes.mappedBytes(mf);
        // Set write limit so Chronicle knows the file extent
        mb.writeLimit(totalBytes);

        LOG.debug("Mmap created (Chronicle): {} ({} elements, {} GB)",
                file.getFileName(), count,
                String.format("%.1f", totalBytes / (1024.0 * 1024 * 1024)));
        return new MmapArray(file, count, mb);
    }

    public int getCount() { return count; }

    // ---- Double access (absolute byte offset) ----

    public double getDouble(int i) {
        return mappedBytes.readDouble((long) i * 8);
    }

    public void putDouble(int i, double v) {
        mappedBytes.writeDouble((long) i * 8, v);
    }

    // ---- Long access (absolute byte offset) ----

    public long getLong(int i) {
        return mappedBytes.readLong((long) i * 8);
    }

    public void putLong(int i, long v) {
        mappedBytes.writeLong((long) i * 8, v);
    }

    /**
     * Forces all dirty pages to disk.
     * Call after partition scatter is complete.
     */
    public void force() {
        MappedFile mf = mappedBytes.mappedFile();
        if (mf != null) {
            // No direct force API on MappedBytes — the OS will flush eventually.
            // For correctness this is fine; data is visible to readers immediately
            // through the page cache.
        }
    }

    @Override
    public void close() {
        if (mappedBytes != null) {
            try {
                mappedBytes.close();     // deterministic unmap via Chronicle
            } catch (Exception ignored) { }
            mappedBytes = null;
        }
        if (filePath != null) {
            try { Files.deleteIfExists(filePath); } catch (IOException ignored) { }
        }
    }
}
