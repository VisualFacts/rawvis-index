package gr.athenarc.imsi.visualfacts.util.csv;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public final class CsvRowReaderImpl implements CsvRowReader {

    private static final int BUFFER_SIZE = 64 * 1024; // 64 KB read buffer

    private FileChannel channel;
    private Charset charset;
    private char delimiter;
    private byte delimiterByte;

    private int[] selectedColumns;
    private boolean headerExtraction;

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

    // Tracking offsets
    private long absoluteOffset = 0; // total bytes consumed so far
    private long rowStartOffset = 0; // byte offset of current row
    private long lastRowOffset = -1; // last returned offset

    private boolean eof = false;

    // Row accumulator
    private final ByteArrayOutputStream rowBytes = new ByteArrayOutputStream(4_096);

    @Override
    public void open(CsvReaderConfig config) throws IOException {
        this.charset = config.getCharset();
        this.delimiter = config.getDelimiter();
        this.delimiterByte = (byte) delimiter;
        this.selectedColumns = config.getSelectedColumns();
        this.headerExtraction = config.isHeaderExtraction();
        if (config.getFile() != null) {
            this.channel = FileChannel.open(config.getFile().toPath(), StandardOpenOption.READ);
            buffer.clear();
            EOFCheckRefill();

            absoluteOffset = 0;
            rowStartOffset = 0;
            lastRowOffset = -1;
            eof = false;

            if (headerExtraction) {
                // skip the first row
                nextRow();
            }
        }
    }

    @Override
    public String[] nextRow() throws IOException {
        if (channel == null || eof) {
            return null;
        }

        rowBytes.reset();
        rowStartOffset = absoluteOffset;
        boolean inQuotes = false;

        while (true) {
            if (!buffer.hasRemaining()) {
                if (!EOFCheckRefill()) {
                    // End of file, maybe we accumulated a partial last row?
                    if (rowBytes.size() == 0) {
                        eof = true;
                        return null;
                    }
                    // deliver last partial row
                    lastRowOffset = rowStartOffset;
                    return selectColumns(parseRowBytes());
                }
            }

            byte b = buffer.get();
            absoluteOffset++;

            if (inQuotes) {
                if (b == '"') {
                    // Could be end or escaped quote
                    if (!buffer.hasRemaining()) {
                        if (!EOFCheckRefill()) {
                            // final quote then eof
                            rowBytes.write(b);
                            lastRowOffset = rowStartOffset;
                            return selectColumns(parseRowBytes());
                        }
                    }
                    byte next = buffer.get(buffer.position());
                    if (next == '"') {
                        // Escaped quote
                        buffer.get();
                        absoluteOffset++;
                        rowBytes.write('"');
                    } else {
                        // End of quoted field
                        inQuotes = false;
                    }
                } else {
                    rowBytes.write(b);
                }
                continue;
            }

            // Not in quotes
            if (b == '"') {
                inQuotes = true;
            } else if (b == '\n') {
                lastRowOffset = rowStartOffset;
                return selectColumns(parseRowBytes());
            } else if (b == '\r') {
                // swallow optional following \n
                if (!buffer.hasRemaining()) {
                    if (EOFCheckRefill() && buffer.hasRemaining()) {
                        byte n = buffer.get(buffer.position());
                        if (n == '\n') {
                            buffer.get();
                            absoluteOffset++;
                        }
                    }
                } else {
                    byte n = buffer.get(buffer.position());
                    if (n == '\n') {
                        buffer.get();
                        absoluteOffset++;
                    }
                }
                lastRowOffset = rowStartOffset;
                return selectColumns(parseRowBytes());
            } else {
                rowBytes.write(b);
            }
        }
    }

    @Override
    public String[] parseLine(String line) throws IOException {
        // Standard RFC4180 parsing
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cur.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    cur.append(c);
                }
            } else {
                if (c == delimiter) {
                    out.add(cur.toString());
                    cur.setLength(0);
                } else if (c == '"') {
                    inQuotes = true;
                } else {
                    cur.append(c);
                }
            }
        }
        out.add(cur.toString());

        return selectColumns(out.toArray(new String[0]));
    }

    @Override
    public long currentOffset() {
        return lastRowOffset;
    }

    @Override
    public void close() throws IOException {
        if (channel != null)
            channel.close();
    }

    // ----------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------

    private boolean EOFCheckRefill() throws IOException {
        buffer.clear();
        int n = channel.read(buffer);
        buffer.flip();
        return n != -1;
    }

    private String[] parseRowBytes() {
        String line = new String(rowBytes.toByteArray(), charset);
        try {
            return parseLine(line);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String[] selectColumns(String[] fields) {
        return fields;
        // if (selectedColumns == null)
        //     return fields;

        // String[] out = new String[selectedColumns.length];
        // for (int i = 0; i < selectedColumns.length; i++) {
        //     int idx = selectedColumns[i];
        //     out[i] = (idx < fields.length ? fields[idx] : null);
        // }
        // return out;
    }
}
