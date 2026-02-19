package gr.athenarc.imsi.visualfacts.util.csv;

import java.nio.ByteBuffer;

public final class ZsvNative {
    static {
        System.loadLibrary("zsv_jni");
    }

    public static native long open(String path, byte delimiter, boolean skipHeader, int[] selectedCols);

    public static native void close(long handle);

    public static native int nextBatchDoubles(
            long handle,
            int maxRows,
            ByteBuffer offsets8,
            ByteBuffer valuesF8,
            ByteBuffer presentB1 // byte[maxRows * k]
    );

    private ZsvNative() {
    }
}
