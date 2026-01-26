package gr.athenarc.imsi.visualfacts.util.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import net.openhft.chronicle.bytes.MappedBytes;

/**
 * Memory-mapped file reader optimized for random access patterns.
 * Uses Chronicle Bytes for efficient mmap handling of large files (>2GB).
 * 
 * Unlike explicit buffering which copies data from OS cache to a user buffer,
 * mmap provides direct access to OS page cache - ideal for sparse random reads.
 */
public class MappedFileReader implements Closeable {

    // Chunk size for mapping - Chronicle handles multiple chunks automatically
    private static final long CHUNK_SIZE = 64 * 1024 * 1024; // 64MB chunks
    
    private final MappedBytes mappedBytes;
    private final long fileSize;
    private final StringBuilder lineBuilder = new StringBuilder(256);
    
    private MappedFileReader(File file) throws IOException {
        this.fileSize = file.length();
        // Open in READ-ONLY mode to avoid modifying/padding the file
        this.mappedBytes = MappedBytes.readOnly(file);
        // Set the read limit to the actual file size so we can seek anywhere
        this.mappedBytes.readLimit(fileSize);
    }
    
    /**
     * Opens a file for memory-mapped reading.
     * 
     * @param file the file to open
     * @return a new MappedFileReader instance
     * @throws IOException if the file cannot be opened
     */
    public static MappedFileReader open(File file) throws IOException {
        return new MappedFileReader(file);
    }
    
    /**
     * Seeks to the specified position in the file.
     * With mmap, this is essentially free - just updates the read position.
     * 
     * @param position the byte offset to seek to
     */
    public void seek(long position) {
        if (position < 0 || position > fileSize) {
            throw new IllegalArgumentException("Position out of bounds: " + position);
        }
        mappedBytes.readPosition(position);
    }
    
    /**
     * Returns the current read position.
     */
    public long getPosition() {
        return mappedBytes.readPosition();
    }
    
    /**
     * Returns the file size.
     */
    public long length() {
        return fileSize;
    }
    
    /**
     * Reads a line from the current position.
     * Handles both \n and \r\n line endings.
     * 
     * @return the line (without line terminator), or null if EOF
     */
    public String readLine() {
        if (mappedBytes.readPosition() >= fileSize) {
            return null;
        }
        
        lineBuilder.setLength(0);
        long startPos = mappedBytes.readPosition();
        
        // Scan for end of line
        while (mappedBytes.readPosition() < fileSize) {
            int b = mappedBytes.readUnsignedByte();
            
            if (b == '\n') {
                return lineBuilder.toString();
            } else if (b == '\r') {
                // Check for \r\n
                if (mappedBytes.readPosition() < fileSize) {
                    int next = mappedBytes.peekUnsignedByte();
                    if (next == '\n') {
                        mappedBytes.readSkip(1); // consume the \n
                    }
                }
                return lineBuilder.toString();
            } else {
                lineBuilder.append((char) b);
            }
        }
        
        // EOF reached - return remaining content if any
        return lineBuilder.length() > 0 ? lineBuilder.toString() : null;
    }
    
    /**
     * Reads a byte at the current position and advances.
     */
    public byte readByte() {
        return mappedBytes.readByte();
    }
    
    /**
     * Checks if we're at end of file.
     */
    public boolean isEOF() {
        return mappedBytes.readPosition() >= fileSize;
    }
    
    @Override
    public void close() throws IOException {
        if (mappedBytes != null) {
            mappedBytes.close();
        }
    }
}
