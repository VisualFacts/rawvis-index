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
    
    // Buffer for double parsing (avoids allocation per call)
    private static final int MAX_DOUBLE_CHARS = 32;
    private final byte[] doubleBuffer = new byte[MAX_DOUBLE_CHARS];
    
    /**
     * Extracts specific columns as doubles directly from the mmap buffer.
     * This is optimized for wide rows where you only need a few columns.
     * Stops scanning immediately after the last needed column.
     * 
     * IMPORTANT: sortedColumnIndices must be sorted in ascending order!
     * The returned array is compact: result[i] corresponds to sortedColumnIndices[i].
     * 
     * @param sortedColumnIndices column indices to extract (must be sorted ascending)
     * @param delimiter the column delimiter byte (e.g., '\t' or ',')
     * @return double array with extracted values (NaN for missing/invalid values),
     *         indexed by position in sortedColumnIndices (not by original column index)
     */
    public double[] extractDoubles(int[] sortedColumnIndices, byte delimiter) {
        double[] result = new double[sortedColumnIndices.length];
        
        if (sortedColumnIndices.length == 0 || mappedBytes.readPosition() >= fileSize) {
            return result;
        }
        
        int maxCol = sortedColumnIndices[sortedColumnIndices.length - 1];
        int currentCol = 0;
        int nextTargetIdx = 0;
        int nextTargetCol = sortedColumnIndices[0];
        int bufPos = 0;
        
        while (mappedBytes.readPosition() < fileSize && currentCol <= maxCol) {
            byte b = mappedBytes.readByte();
            
            if (b == delimiter || b == '\n' || b == '\r') {
                // End of column
                if (currentCol == nextTargetCol) {
                    // This is a target column - parse the double
                    result[nextTargetIdx] = parseDoubleFromBytes(doubleBuffer, 0, bufPos);
                    nextTargetIdx++;
                    if (nextTargetIdx < sortedColumnIndices.length) {
                        nextTargetCol = sortedColumnIndices[nextTargetIdx];
                    } else {
                        // Got all columns - stop early (skip remaining ~84 columns for SDSS)
                        break;
                    }
                }
                
                if (b == '\n' || b == '\r') {
                    break; // End of row
                }
                
                currentCol++;
                bufPos = 0;
            } else {
                // Only buffer bytes for target columns
                if (currentCol == nextTargetCol && bufPos < MAX_DOUBLE_CHARS) {
                    doubleBuffer[bufPos++] = b;
                }
            }
        }
        
        return result;
    }
    
    /**
     * Fast double parser for simple decimal numbers.
     * Handles: 123, -123, 123.456, -123.456
     * Falls back to Double.parseDouble for scientific notation or edge cases.
     */
    private static double parseDoubleFromBytes(byte[] buf, int offset, int len) {
        if (len == 0) {
            return Double.NaN;
        }
        
        int pos = offset;
        int end = offset + len;
        boolean negative = false;
        
        // Handle sign
        if (buf[pos] == '-') {
            negative = true;
            pos++;
        } else if (buf[pos] == '+') {
            pos++;
        }
        
        if (pos >= end) {
            return Double.NaN;
        }
        
        // Parse integer part
        long intPart = 0;
        while (pos < end && buf[pos] >= '0' && buf[pos] <= '9') {
            intPart = intPart * 10 + (buf[pos] - '0');
            pos++;
        }
        
        // Parse fractional part
        double fracPart = 0;
        if (pos < end && buf[pos] == '.') {
            pos++;
            double divisor = 10;
            while (pos < end && buf[pos] >= '0' && buf[pos] <= '9') {
                fracPart += (buf[pos] - '0') / divisor;
                divisor *= 10;
                pos++;
            }
        }
        
        // Check for scientific notation - fall back to standard parser
        if (pos < end && (buf[pos] == 'e' || buf[pos] == 'E')) {
            try {
                return Double.parseDouble(new String(buf, offset, len, java.nio.charset.StandardCharsets.US_ASCII));
            } catch (NumberFormatException e) {
                return Double.NaN;
            }
        }
        
        // Check we consumed all characters
        if (pos != end) {
            // Unexpected characters - try standard parser
            try {
                return Double.parseDouble(new String(buf, offset, len, java.nio.charset.StandardCharsets.US_ASCII));
            } catch (NumberFormatException e) {
                return Double.NaN;
            }
        }
        
        double result = intPart + fracPart;
        return negative ? -result : result;
    }
    
    @Override
    public void close() throws IOException {
        if (mappedBytes != null) {
            mappedBytes.close();
        }
    }
}
