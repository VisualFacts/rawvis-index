#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <ctype.h>

#ifdef __linux__
#include <fcntl.h>  // for posix_fadvise
#endif

#include <fast_float/fast_float.h>

extern "C"
{
#include "zsv.h"
#include "zsv/api.h"
}

typedef struct
{
    FILE *f;
    zsv_parser parser;

    int *sel;
    int sel_count;

    int skip_header;
    long row_index;
    int eof;
} reader_t;

static reader_t *handle_to_reader(jlong handle)
{
    return (reader_t *)(uintptr_t)handle;
}

static void throw_ioe(JNIEnv *env, const char *msg)
{
    jclass ioex = env->FindClass("java/io/IOException");
    if (ioex)
        env->ThrowNew(ioex, msg);
}

static void free_reader(reader_t *r)
{
    if (!r)
        return;

    if (r->parser)
    {
        zsv_finish(r->parser);
        zsv_delete(r->parser);
    }
    if (r->f)
        fclose(r->f);

    free(r->sel);
    free(r);
}

static inline int parse_float(const unsigned char *s, size_t len, float *out)
{
    // fast_float expects [begin, end)
    const char *begin = reinterpret_cast<const char *>(s);
    const char *end = begin + len;

    // Parse float, require full consumption (like your current strict parser)
    auto r = fast_float::from_chars(begin, end, *out);
    return (r.ec == std::errc() && r.ptr == end) ? 1 : 0;
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jlong JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_open(
    JNIEnv *env, jclass cls, jstring jpath, jbyte delimiter,
    jboolean skipHeader, jintArray jselCols)
{
    (void)cls;

    if (!jpath || !jselCols)
    {
        throw_ioe(env, "open(): path/selectedCols must not be null");
        return 0;
    }

    const char *path = env->GetStringUTFChars(jpath, NULL);
    if (!path)
        return 0;

    FILE *f = fopen(path, "rb");
    env->ReleaseStringUTFChars(jpath, path);

    if (!f)
    {
        throw_ioe(env, "Failed to open CSV file");
        return 0;
    }

    // I/O optimizations for large files
    setvbuf(f, NULL, _IOFBF, 8 * 1024 * 1024);  // 8MB read buffer
#ifdef __linux__
    posix_fadvise(fileno(f), 0, 0, POSIX_FADV_SEQUENTIAL);  // hint: sequential read
#endif

    reader_t *r = (reader_t *)calloc(1, sizeof(reader_t));
    if (!r)
    {
        fclose(f);
        jclass oom = env->FindClass("java/lang/OutOfMemoryError");
        if (oom)
            env->ThrowNew(oom, "Out of memory");
        return 0;
    }

    r->f = f;
    r->skip_header = skipHeader ? 1 : 0;
    r->row_index = 0;
    r->eof = 0;

    jsize selCount = env->GetArrayLength(jselCols);
    if (selCount <= 0)
    {
        free_reader(r);
        throw_ioe(env, "selectedCols must not be empty");
        return 0;
    }

    r->sel_count = (int)selCount;
    r->sel = (int *)malloc(sizeof(int) * (size_t)selCount);
    if (!r->sel)
    {
        free_reader(r);
        jclass oom = env->FindClass("java/lang/OutOfMemoryError");
        if (oom)
            env->ThrowNew(oom, "Out of memory");
        return 0;
    }

    jint *tmp = env->GetIntArrayElements(jselCols, NULL);
    if (!tmp)
    {
        free_reader(r);
        throw_ioe(env, "Failed to read selectedCols");
        return 0;
    }
    for (int i = 0; i < (int)selCount; i++)
        r->sel[i] = tmp[i];
    env->ReleaseIntArrayElements(jselCols, tmp, JNI_ABORT);

    struct zsv_opts opts;
    memset(&opts, 0, sizeof(opts));
    opts.stream = r->f;
    opts.delimiter = (char)delimiter;

    // NOTE: no_quotes=1 disables quoted CSV handling (fast, but not standard CSV).
    opts.no_quotes = 1;

    opts.buff = NULL;
    opts.buffsize = 0;
    opts.max_columns = ZSV_MAX_COLS_DEFAULT;
    opts.max_row_size = ZSV_ROW_MAX_SIZE_DEFAULT;

    r->parser = zsv_new(&opts);
    if (!r->parser)
    {
        free_reader(r);
        throw_ioe(env, "zsv_new failed");
        return 0;
    }

    return (jlong)(uintptr_t)r;
}

JNIEXPORT void JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_close(
    JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    free_reader(handle_to_reader(handle));
}


JNIEXPORT jint JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_nextBatchFloats(
    JNIEnv *env, jclass cls, jlong handle, jint maxRows,
    jobject jOffsets8, jobject jValuesF4, jobject jPresentB1)
{
    (void)cls;

    reader_t *r = handle_to_reader(handle);
    if (!r)
    {
        throw_ioe(env, "nextBatchFloats(): invalid handle");
        return 0;
    }
    if (r->eof)
        return 0;
    if (maxRows <= 0)
    {
        throw_ioe(env, "nextBatchFloats(): maxRows must be > 0");
        return 0;
    }
    if (!jOffsets8 || !jValuesF4 || !jPresentB1)
    {
        throw_ioe(env, "nextBatchFloats(): buffers must not be null");
        return 0;
    }

    int64_t *offsets = (int64_t *)env->GetDirectBufferAddress(jOffsets8);
    float *values = (float *)env->GetDirectBufferAddress(jValuesF4);
    uint8_t *present = (uint8_t *)env->GetDirectBufferAddress(jPresentB1);

    if (!offsets || !values || !present)
    {
        throw_ioe(env, "nextBatchFloats(): buffers must be direct ByteBuffers");
        return 0;
    }

    jlong capOffsets = env->GetDirectBufferCapacity(jOffsets8);
    jlong capValues = env->GetDirectBufferCapacity(jValuesF4);
    jlong capPresent = env->GetDirectBufferCapacity(jPresentB1);

    const int k = r->sel_count;

    const int64_t needOffsets = 8LL * (int64_t)maxRows;
    const int64_t needValues = 4LL * (int64_t)maxRows * (int64_t)k;
    const int64_t needPresent = 1LL * (int64_t)maxRows * (int64_t)k;

    if (capOffsets < needOffsets)
    {
        throw_ioe(env, "nextBatchFloats(): offsets8 capacity too small");
        return 0;
    }
    if (capValues < needValues)
    {
        throw_ioe(env, "nextBatchFloats(): valuesF4 capacity too small");
        return 0;
    }
    if (capPresent < needPresent)
    {
        throw_ioe(env, "nextBatchFloats(): presentB1 capacity too small");
        return 0;
    }

    int rowsRead = 0;

    while (rowsRead < maxRows)
    {
        enum zsv_status st = zsv_next_row(r->parser);

        if (st == zsv_status_row)
        {
            if (r->skip_header && r->row_index == 0)
            {
                r->row_index++;
                continue;
            }

            size_t cum = zsv_cum_scanned_length(r->parser);
            size_t row_len = zsv_row_length_raw_bytes(r->parser);
            offsets[rowsRead] = (int64_t)(cum - row_len);

            size_t cc = zsv_cell_count(r->parser);

            const int64_t base = (int64_t)rowsRead * (int64_t)k;

            // Clear presence flags for THIS row only (k bytes)
            memset(present + base, 0, (size_t)k);

            for (int c = 0; c < k; c++)
            {
                int col = r->sel[c];
                if (col < 0 || (size_t)col >= cc)
                    continue;

                struct zsv_cell cell = zsv_get_cell(r->parser, (size_t)col);
                if (!cell.str || cell.len == 0)
                    continue;

                float f;
                if (parse_float((const unsigned char *)cell.str, cell.len, &f))
                {
                    values[base + c] = f;
                    present[base + c] = 1;
                }
            }

            rowsRead++;
            r->row_index++;
            continue;
        }

        if (st == zsv_status_no_more_input || st == zsv_status_done)
        {
            r->eof = 1;
            break;
        }

        const unsigned char *desc = zsv_parse_status_desc(st);
        char buf[256];
        snprintf(buf, sizeof(buf), "zsv_next_row error: %s", desc ? (const char *)desc : "unknown");
        throw_ioe(env, buf);
        return 0;
    }

    return rowsRead;
}


#ifdef __cplusplus
}
#endif