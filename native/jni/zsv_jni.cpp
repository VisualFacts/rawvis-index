#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <ctype.h>

#ifdef __linux__
#include <fcntl.h>  // for posix_fadvise
#include <unistd.h>
#endif

#include <fast_float/fast_float.h>

// io_uring header must be included before any extern "C" scope because it
// contains C++ templates (barrier helpers). The HAVE_URING macro is set by
// CMake when liburing-dev is found.
#ifdef HAVE_URING
#include <liburing.h>
#include <sys/mman.h>
#endif

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
    size_t max_row_len;   // largest row_length_raw_bytes seen during scan
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

static inline int parse_double(const unsigned char *s, size_t len, double *out)
{
    // fast_float expects [begin, end)
    const char *begin = reinterpret_cast<const char *>(s);
    const char *end = begin + len;

    // Parse double, require full consumption (like your current strict parser)
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

JNIEXPORT jlong JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_getMaxRowLength(
    JNIEnv *env, jclass cls, jlong handle)
{
    (void)cls;
    reader_t *r = handle_to_reader(handle);
    if (!r) {
        throw_ioe(env, "getMaxRowLength(): invalid handle");
        return 0;
    }
    return (jlong)r->max_row_len;
}


JNIEXPORT jint JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_nextBatchDoubles(
    JNIEnv *env, jclass cls, jlong handle, jint maxRows,
    jobject jOffsets8, jobject jValuesF8, jobject jPresentB1)
{
    (void)cls;

    reader_t *r = handle_to_reader(handle);
    if (!r)
    {
        throw_ioe(env, "nextBatchDoubles(): invalid handle");
        return 0;
    }
    if (r->eof)
        return 0;
    if (maxRows <= 0)
    {
        throw_ioe(env, "nextBatchDoubles(): maxRows must be > 0");
        return 0;
    }
    if (!jOffsets8 || !jValuesF8 || !jPresentB1)
    {
        throw_ioe(env, "nextBatchDoubles(): buffers must not be null");
        return 0;
    }

    int64_t *offsets = (int64_t *)env->GetDirectBufferAddress(jOffsets8);
    double *values = (double *)env->GetDirectBufferAddress(jValuesF8);
    uint8_t *present = (uint8_t *)env->GetDirectBufferAddress(jPresentB1);

    if (!offsets || !values || !present)
    {
        throw_ioe(env, "nextBatchDoubles(): buffers must be direct ByteBuffers");
        return 0;
    }

    jlong capOffsets = env->GetDirectBufferCapacity(jOffsets8);
    jlong capValues = env->GetDirectBufferCapacity(jValuesF8);
    jlong capPresent = env->GetDirectBufferCapacity(jPresentB1);

    const int k = r->sel_count;

    const int64_t needOffsets = 8LL * (int64_t)maxRows;
    const int64_t needValues = 8LL * (int64_t)maxRows * (int64_t)k;
    const int64_t needPresent = 1LL * (int64_t)maxRows * (int64_t)k;

    if (capOffsets < needOffsets)
    {
        throw_ioe(env, "nextBatchDoubles(): offsets8 capacity too small");
        return 0;
    }
    if (capValues < needValues)
    {
        throw_ioe(env, "nextBatchDoubles(): valuesF8 capacity too small");
        return 0;
    }
    if (capPresent < needPresent)
    {
        throw_ioe(env, "nextBatchDoubles(): presentB1 capacity too small");
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
            if (row_len > r->max_row_len) r->max_row_len = row_len;

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

                double d;
                if (parse_double((const unsigned char *)cell.str, cell.len, &d))
                {
                    values[base + c] = d;
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

// ==============================================================
// io_uring batch reader
// ==============================================================
//
// Design summary:
//   - One persistent ring + buffer pool per opened file (handle).
//   - Per query: Java passes all row offsets at once (sorted, from KWayMerge).
//   - We group offsets by 4 KB page (dedup), then pipeline reads with QD=256.
//   - Each slot reads READ_SIZE=8192 bytes (2 pages) so rows that start near
//     the end of a page still have ample bytes for parsing — no cross-page
//     stitching needed.
//   - fast_float used for double parsing (same as initial CSV scan).
//   - Buffer and file registration attempted at open time; falls back to
//     unregistered reads gracefully if the kernel rejects them.
//   - posix_fadvise(FADV_RANDOM) disables OS readahead on this fd.

#ifdef HAVE_URING

#define URING_RING_SIZE   256
#define URING_POOL_SIZE   256
#define URING_PAGE_BITS   12
#define URING_PAGE_SIZE   (1u << URING_PAGE_BITS)   // 4096
#define URING_MIN_READ_SIZE  (URING_PAGE_SIZE * 2)   // 8192 — minimum, covers most CSVs

// Round up x to the next multiple of URING_PAGE_SIZE
static inline size_t ceil_to_page(size_t x) {
    return (x + URING_PAGE_SIZE - 1) & ~(size_t)(URING_PAGE_SIZE - 1);
}

typedef struct {
    struct io_uring   ring;
    int               fd;           // raw fd for this file
    int               reg_fd;       // registered fd index (0), or -1 if not registered
    uint8_t          *pool_mem;     // POOL_SIZE * read_size, page-aligned
    struct iovec      iovecs[URING_POOL_SIZE];
    int               free_list[URING_POOL_SIZE];
    int               free_count;
    bool              bufs_registered;
    size_t            read_size;    // dynamic: ceil_to_page(maxRowLen + PAGE_SIZE)
} uring_reader_t;

// ---- buffer pool helpers ----

static void uring_pool_init(uring_reader_t *r) {
    r->free_count = URING_POOL_SIZE;
    for (int i = 0; i < URING_POOL_SIZE; i++) {
        r->free_list[i] = URING_POOL_SIZE - 1 - i;
        r->iovecs[i].iov_base = r->pool_mem + (size_t)i * r->read_size;
        r->iovecs[i].iov_len  = r->read_size;
    }
}

static inline int  uring_alloc_buf(uring_reader_t *r)          { return r->free_list[--r->free_count]; }
static inline void uring_free_buf (uring_reader_t *r, int idx) { r->free_list[r->free_count++] = idx;  }

// ---- page grouping ----

typedef struct {
    int64_t page_start;
    int     row_start;   // first row index (into the flat offset array) on this page
    int     row_count;
} page_group_t;

// Build page groups from a sorted offset array. Returns number of unique pages.
static int build_page_groups(const int64_t *offsets, int n_rows, page_group_t *pages) {
    if (n_rows == 0) return 0;
    int num_pages  = 0;
    int64_t cur_pg = -1;
    for (int i = 0; i < n_rows; i++) {
        int64_t pg = offsets[i] & ~(int64_t)(URING_PAGE_SIZE - 1);
        if (pg != cur_pg) {
            if (num_pages > 0)
                pages[num_pages - 1].row_count = i - pages[num_pages - 1].row_start;
            pages[num_pages].page_start = pg;
            pages[num_pages].row_start  = i;
            pages[num_pages].row_count  = 0;
            num_pages++;
            cur_pg = pg;
        }
    }
    pages[num_pages - 1].row_count = n_rows - pages[num_pages - 1].row_start;
    return num_pages;
}

// ---- row parser (fast_float) ----

// Parses a single CSV row from the given buffer, extracting the measure columns
// specified by mcols[0..nm).  Returns true if the row was TRUNCATED — i.e. the
// buffer ended mid-row before all target columns were extracted.  In that case
// the caller must retry with a larger buffer.  When buf_is_eof is true, running
// past the buffer boundary is treated as a genuine end-of-file (last row with
// no trailing newline) and the pending field is flushed normally.

static bool parse_row_from_buf(
        const uint8_t *buf, size_t buf_len, size_t row_off,
        const int *mcols, int nm, char delim,
        double *out_vals, uint8_t *out_pres,
        bool buf_is_eof)
{
    size_t pos         = row_off;
    int    cur_col     = 0;
    int    tgt_idx     = 0;
    int    next_tgt    = mcols[0];
    int    max_col     = mcols[nm - 1];
    char   field[64];
    int    flen        = 0;
    bool   row_complete = false;

    while (pos < buf_len && cur_col <= max_col) {
        uint8_t c = buf[pos++];

        if (c == (uint8_t)delim || c == '\n' || c == '\r') {
            if (cur_col == next_tgt) {
                if (flen > 0) {
                    double d;
                    auto result = fast_float::from_chars(field, field + flen, d);
                    if (result.ec == std::errc() && result.ptr == field + flen) {
                        out_vals[tgt_idx] = d;
                        out_pres[tgt_idx] = 1;
                    }
                }
                // Always advance to next target (empty field -> present stays 0)
                if (++tgt_idx >= nm) { row_complete = true; break; }
                next_tgt = mcols[tgt_idx];
            }
            if (c == '\n' || c == '\r') { row_complete = true; break; }
            cur_col++;
            flen = 0;
        } else {
            if (cur_col == next_tgt && flen < 63)
                field[flen++] = (char)c;
        }
    }

    // Exited because cur_col > max_col -> all target columns are behind us
    if (!row_complete && cur_col > max_col) {
        row_complete = true;
    }

    // If buffer ran out mid-row: only flush the pending field when this buffer
    // reaches actual end-of-file (short read).  Otherwise the field is likely
    // truncated and would produce a silently wrong value.
    if (!row_complete && tgt_idx < nm && cur_col == next_tgt) {
        if (buf_is_eof && flen > 0) {
            double d;
            auto result = fast_float::from_chars(field, field + flen, d);
            if (result.ec == std::errc() && result.ptr == field + flen) {
                out_vals[tgt_idx] = d;
                out_pres[tgt_idx] = 1;
            }
            row_complete = true;
        }
        // else: truncated — caller must retry with a larger buffer
    }

    return !row_complete;  // true = truncated
}

// ---- JNI: openUringReader ----

JNIEXPORT jlong JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_openUringReader(
        JNIEnv *env, jclass cls, jstring jpath, jint maxRowLength)
{
    (void)cls;
    const char *path = env->GetStringUTFChars(jpath, NULL);
    if (!path) return 0;

    int fd = open(path, O_RDONLY);
    env->ReleaseStringUTFChars(jpath, path);
    if (fd < 0) {
        throw_ioe(env, "openUringReader: failed to open file");
        return 0;
    }

    // Disable OS readahead — random access pattern, readahead wastes bandwidth
    posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);

    // Compute read_size: must cover a full row even when it starts at the last
    // byte of a page.  ceil_to_page(maxRowLen + PAGE_SIZE) guarantees this.
    // Floor at URING_MIN_READ_SIZE (8 KB) for safety.
    size_t dyn_read_size = ceil_to_page((size_t)maxRowLength + URING_PAGE_SIZE);
    if (dyn_read_size < URING_MIN_READ_SIZE) dyn_read_size = URING_MIN_READ_SIZE;

    uring_reader_t *r = (uring_reader_t *)calloc(1, sizeof(uring_reader_t));
    if (!r) { close(fd); throw_ioe(env, "openUringReader: OOM"); return 0; }
    r->fd        = fd;
    r->reg_fd    = -1;
    r->read_size = dyn_read_size;

    // Allocate page-aligned buffer pool
    void *mem = NULL;
    if (posix_memalign(&mem, URING_PAGE_SIZE, (size_t)URING_POOL_SIZE * r->read_size) != 0) {
        free(r); close(fd); throw_ioe(env, "openUringReader: posix_memalign failed"); return 0;
    }
    r->pool_mem = (uint8_t *)mem;
    uring_pool_init(r);

    // Init ring
    if (io_uring_queue_init(URING_RING_SIZE, &r->ring, 0) < 0) {
        free(r->pool_mem); free(r); close(fd);
        throw_ioe(env, "openUringReader: io_uring_queue_init failed");
        return 0;
    }

    // Attempt buffer registration (reduces per-request overhead)
    r->bufs_registered = (io_uring_register_buffers(&r->ring, r->iovecs, URING_POOL_SIZE) == 0);

    // Attempt file registration
    int fds[1] = { fd };
    if (io_uring_register_files(&r->ring, fds, 1) == 0) {
        r->reg_fd = 0;
    }

    return (jlong)(uintptr_t)r;
}

// ---- JNI: closeUringReader ----

JNIEXPORT void JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_closeUringReader(
        JNIEnv *env, jclass cls, jlong handle)
{
    (void)env; (void)cls;
    if (!handle) return;
    uring_reader_t *r = (uring_reader_t *)(uintptr_t)handle;
    io_uring_queue_exit(&r->ring);
    free(r->pool_mem);
    close(r->fd);
    free(r);
}

// ---- JNI: readRowBatch ----
//
// Inputs:
//   offsets8  — direct ByteBuffer, int64_t[rowCount], sorted file offsets
//   measureCols — sorted measure column indices (0-based)
//   delimiter
// Outputs (caller-allocated direct ByteBuffers, zero-filled before call):
//   values8   — double[rowCount * numMeasures]   (NaN-free: only filled where present=1)
//   present1  — byte[rowCount * numMeasures]      (1 = value valid, 0 = NaN/missing)

JNIEXPORT void JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_readRowBatch(
        JNIEnv *env, jclass cls, jlong handle,
        jobject jOffsets, jint rowCount,
        jintArray jMeasureCols, jbyte delimiter,
        jobject jValues, jobject jPresent)
{
    (void)cls;
    if (!handle || rowCount <= 0) return;

    uring_reader_t *r = (uring_reader_t *)(uintptr_t)handle;

    // Resolve buffers
    int64_t  *offsets = (int64_t  *)env->GetDirectBufferAddress(jOffsets);
    double   *values  = (double   *)env->GetDirectBufferAddress(jValues);
    uint8_t  *present = (uint8_t  *)env->GetDirectBufferAddress(jPresent);
    if (!offsets || !values || !present) {
        throw_ioe(env, "readRowBatch: buffers must be direct ByteBuffers");
        return;
    }

    // Validate buffer capacities
    jlong capOff  = env->GetDirectBufferCapacity(jOffsets);
    jlong capVal  = env->GetDirectBufferCapacity(jValues);
    jlong capPres = env->GetDirectBufferCapacity(jPresent);

    // Get measure columns
    int nm = env->GetArrayLength(jMeasureCols);
    if (nm <= 0) return;

    if (capOff < (jlong)rowCount * 8) {
        throw_ioe(env, "readRowBatch: offsets buffer too small");
        return;
    }
    if (capVal < (jlong)rowCount * (jlong)nm * 8) {
        throw_ioe(env, "readRowBatch: values buffer too small");
        return;
    }
    if (capPres < (jlong)rowCount * (jlong)nm) {
        throw_ioe(env, "readRowBatch: present buffer too small");
        return;
    }
    jint *jmc = env->GetIntArrayElements(jMeasureCols, NULL);
    int *mcols = (int *)malloc((size_t)nm * sizeof(int));
    if (!mcols) { env->ReleaseIntArrayElements(jMeasureCols, jmc, JNI_ABORT); throw_ioe(env, "readRowBatch: OOM"); return; }
    for (int i = 0; i < nm; i++) mcols[i] = (int)jmc[i];
    env->ReleaseIntArrayElements(jMeasureCols, jmc, JNI_ABORT);

    // Zero output
    memset(present, 0, (size_t)rowCount * (size_t)nm);

    // Build page groups
    page_group_t *pages = (page_group_t *)malloc((size_t)rowCount * sizeof(page_group_t));
    if (!pages) { free(mcols); throw_ioe(env, "readRowBatch: OOM (pages)"); return; }
    int num_pages = build_page_groups(offsets, rowCount, pages);

    // io_uring pipeline
    int next_submit    = 0;
    int completed      = 0;
    char delim         = (char)delimiter;
    size_t rs          = r->read_size;   // dynamic, computed from maxRowLength at open()

    // Track which buf_idx is used by each in-flight page.
    // Encoded in sqe user_data as: (uint64_t)buf_idx << 32 | page_idx
    while (completed < num_pages) {

        // ---- submit phase ----
        int to_submit = 0;
        while (r->free_count > 0 && next_submit < num_pages) {
            int bi = uring_alloc_buf(r);
            struct io_uring_sqe *sqe = io_uring_get_sqe(&r->ring);
            if (!sqe) {
                // Ring full — submit what we have first
                uring_free_buf(r, bi);
                break;
            }

            if (r->bufs_registered) {
                int use_fd = (r->reg_fd >= 0) ? r->reg_fd : r->fd;
                io_uring_prep_read_fixed(sqe, use_fd,
                    r->pool_mem + (size_t)bi * rs,
                    rs,
                    (uint64_t)pages[next_submit].page_start,
                    bi);
                if (r->reg_fd >= 0) sqe->flags |= IOSQE_FIXED_FILE;
            } else {
                io_uring_prep_read(sqe, r->fd,
                    r->pool_mem + (size_t)bi * rs,
                    rs,
                    (uint64_t)pages[next_submit].page_start);
            }
            // Encode buf_idx (high 32 bits) and page_idx (low 32 bits) into void* user_data.
            // io_uring_sqe_set_data64 is liburing >=2.2; use void* cast for 2.1 compat.
            uintptr_t ud_enc = ((uintptr_t)(uint32_t)bi << 32) | (uint32_t)next_submit;
            io_uring_sqe_set_data(sqe, (void*)ud_enc);
            next_submit++;
            to_submit++;
        }
        if (to_submit > 0) {
            int sr = io_uring_submit(&r->ring);
            if (sr < 0 && sr != -EAGAIN) {
                // Non-recoverable submit error — bail out
                free(pages);
                free(mcols);
                char errbuf[128];
                snprintf(errbuf, sizeof(errbuf), "readRowBatch: io_uring_submit failed (%d)", sr);
                throw_ioe(env, errbuf);
                return;
            }
        }

        // ---- completion phase ----
        // Must wait if pool is drained or nothing more to submit
        bool must_wait = (r->free_count == 0) || (next_submit >= num_pages);
        struct io_uring_cqe *cqe = NULL;
        int ret;
        if (must_wait) {
            ret = io_uring_wait_cqe(&r->ring, &cqe);
        } else {
            ret = io_uring_peek_cqe(&r->ring, &cqe);
            if (ret != 0) continue;  // nothing ready yet — go submit more
        }

        if (ret == 0 && cqe) {
            do {
                uintptr_t ud  = (uintptr_t)io_uring_cqe_get_data(cqe);
                int bi        = (int)(ud >> 32);
                int pi        = (int)(ud & 0xFFFFFFFFu);
                int bytes_got = cqe->res;
                io_uring_cqe_seen(&r->ring, cqe);

                if (bytes_got > 0) {
                    uint8_t         *buf = r->pool_mem + (size_t)bi * rs;
                    page_group_t    *pg  = &pages[pi];
                    size_t           blen = (size_t)bytes_got;
                    bool             is_eof = ((size_t)bytes_got < rs);

                    for (int ri = pg->row_start; ri < pg->row_start + pg->row_count; ri++) {
                        size_t roff = (size_t)(offsets[ri] - pg->page_start);
                        int    base = ri * nm;
                        parse_row_from_buf(buf, blen, roff,
                                           mcols, nm, delim,
                                           values + base, present + base,
                                           is_eof);
                    }
                }
                // Failed reads (bytes_got < 0) are silently skipped — present stays 0

                uring_free_buf(r, bi);
                completed++;
            } while (io_uring_peek_cqe(&r->ring, &cqe) == 0);
        }
    }

    free(pages);
    free(mcols);
}

#else  // !HAVE_URING — stub implementations that throw at runtime

JNIEXPORT jlong JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_openUringReader(
        JNIEnv *env, jclass cls, jstring jpath, jint maxRowLength)
{
    (void)cls; (void)jpath; (void)maxRowLength;
    jclass ex = env->FindClass("java/lang/UnsupportedOperationException");
    if (ex) env->ThrowNew(ex, "io_uring not available (build without HAVE_URING)");
    return 0;
}

JNIEXPORT void JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_closeUringReader(
        JNIEnv *env, jclass cls, jlong handle)
{ (void)env; (void)cls; (void)handle; }

JNIEXPORT void JNICALL
Java_gr_athenarc_imsi_visualfacts_util_csv_ZsvNative_readRowBatch(
        JNIEnv *env, jclass cls, jlong handle,
        jobject jOffsets, jint rowCount,
        jintArray jMeasureCols, jbyte delimiter,
        jobject jValues, jobject jPresent)
{
    (void)cls; (void)handle; (void)jOffsets; (void)rowCount;
    (void)jMeasureCols; (void)delimiter; (void)jValues; (void)jPresent;
    jclass ex = env->FindClass("java/lang/UnsupportedOperationException");
    if (ex) env->ThrowNew(ex, "io_uring not available (build without HAVE_URING)");
}

#endif  // HAVE_URING


#ifdef __cplusplus
}
#endif