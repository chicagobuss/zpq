/*
 * libzpq: zpq's query engine behind a C ABI. ABI version 1 (symbol version LIBZPQ_1).
 *
 * A request is the JSON the zpq Lambda takes (see docs/libzpq.md): "inputs" (local paths or s3:// URLs), and an
 * "aggregate" / "group_by", or an "output_url" to write to, with "filter", "select", "columns", "max_memory" and the
 * rest as the Lambda reads them. Or use {"sql":"SELECT ...", "parallelism":3}: SQL excludes structured query
 * fields and returns {"ok":true,"ndjson":"..."}, containing the CLI's exact NDJSON rows. SQL request/response
 * shapes are EXPERIMENTAL in 0.5.0, with no stability guarantee. See docs/libzpq.md.
 * Structured requests keep the Lambda response. Query failures return an {"error": ...} document.
 * An answer returned in the response (SQL rows, GROUP BY rows) is at most the request's "max_result_bytes" bytes,
 * the larger of the query's budget and 1 GiB by default. Past it the response is {"error":"result_too_large", ...}
 * naming the cap: raise "max_result_bytes", or write the answer to an "output_url" instead.
 *
 * Ownership: every char * libzpq hands out (a response, an error message) is malloc'd, NUL-terminated, owned by the
 * caller from then on, and freed with zpq_free. zpq_strerror's strings are static: never free them. libzpq keeps no
 * pointer to anything the caller passes in after the call returns.
 *
 * Threads: zpq_run may be called from any number of threads at once on one context. zpq_open, zpq_free,
 * zpq_abi_version and zpq_strerror are thread-safe, and so is zpq_shutdown, which may be called any number of times.
 * zpq_close must be called once per context, and no call on the context may start once zpq_close has begun: it frees
 * the context. To close a context other threads may still be calling, call zpq_shutdown (new calls, and queries
 * waiting for their budget, get ZPQ_ECLOSED), wait until the host's own calls have returned, then call zpq_close.
 * zpq_run does its work on one of the context's own threads, with an 8 MiB stack, so the calling thread needs no
 * particular stack size. Signals: libzpq's sockets never raise SIGPIPE. While any context is open, SIGIO and SIGPIPE
 * have a do-nothing handler unless the host installs its own; closing the last context restores what the host had.
 * While a query has a local input mapped, libzpq handles SIGBUS: a fault in its mapping (the file truncated under it)
 * fails that query, and any other goes to the handler installed before. When no input is mapped, as after the last
 * zpq_close, the handler before is back, unless the host has installed its own since.
 *
 * Environment: each zpq_run reads the process environment when it is called (S3 credentials, endpoint, region, as
 * the CLI reads them). Do not change it with setenv while a call is running.
 *
 * Failure: libzpq never aborts the process for a bad request, a bad file or a failing network: those are error
 * responses. A broken internal invariant still aborts, as it would the CLI.
 */
#ifndef ZPQ_H
#define ZPQ_H

#include <stddef.h>
#include <stdint.h>

/* Arrow's stable C structures (Apache-2.0), with their canonical guards. */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE
#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4
struct ArrowSchema {
    const char *format, *name, *metadata;
    int64_t flags, n_children;
    struct ArrowSchema **children, *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};
struct ArrowArray {
    int64_t length, null_count, offset, n_buffers, n_children;
    const void **buffers;
    struct ArrowArray **children, *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};
#endif
#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE
struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *);
    int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *);
    const char *(*get_last_error)(struct ArrowArrayStream *);
    void (*release)(struct ArrowArrayStream *);
    void *private_data;
};
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct zpq_ctx zpq_ctx;

/* Status codes. Stable: a value never changes meaning, and new ones are only added. */
typedef enum zpq_status {
    ZPQ_OK = 0,         /* Done. zpq_run: *out_json holds the response, which may be an {"error": ...} document. */
    ZPQ_EINVAL = -1,    /* The call itself was malformed: a NULL pointer, a bad length, or bad zpq_open options. */
    ZPQ_ENOMEM = -2,    /* Out of memory before a result could be built. No message. */
    ZPQ_ECLOSED = -3,   /* zpq_shutdown or zpq_close has been called on this context. */
    ZPQ_EINTERNAL = -4, /* A failure that is not the caller's: a thread that would not start, a pool or TLS set-up. */
} zpq_status;

/*
 * Open a context. options_json may be NULL or "" for the defaults, else a JSON object:
 *   "max_memory":     the context's ceiling on its queries' declared budgets: bytes, or a size string ("2GB").
 *                     Default: the memory available, as the CLI's default budget, so one query alone runs as it would
 *                     under `zpq query`. A request that names no "max_memory" takes the whole ceiling.
 *   "budget_wait_ms": how long a query waits for its memory budget when its request names no "budget_wait_ms".
 *                     Default: as long as it takes.
 *   "preconnect":     true to open connections to the S3 endpoint now (ZPQ_S3_PRECONNECT of them, default 4, at most
 *                     8), as the Lambda does during init. It resolves the endpoint on the calling thread and starts
 *                     one thread per connection; without S3 credentials in the environment it does nothing. Failures
 *                     are ignored. Default: false.
 * On ZPQ_OK, *out_ctx is the context. On any failure *out_ctx is NULL, and *out_error (when out_error is not NULL)
 * is an {"error", "reason"} JSON message to free with zpq_free, or NULL for ZPQ_ENOMEM.
 * Returns ZPQ_OK, ZPQ_EINVAL (out_ctx NULL, or bad options), ZPQ_ENOMEM or ZPQ_EINTERNAL.
 */
int zpq_open(const char *options_json, zpq_ctx **out_ctx, char **out_error);

/*
 * Run one request (request_json, len bytes, need not be NUL-terminated; NULL only with len 0).
 * Returns ZPQ_OK whenever a response was produced, including a query that failed (bad SQL, a missing column, an S3
 * 403, a refused memory budget): the response is then an {"error": ...} document, exactly as the Lambda returns it.
 * *out_json is the response, to free with zpq_free; *out_len (when out_len is not NULL) its length without the NUL.
 * The other codes are for the call itself: ZPQ_EINVAL (ctx or out_json NULL, NULL request with len > 0, len too
 * large), ZPQ_ECLOSED (the context is shutting down; a query waiting for its budget is woken with it), ZPQ_ENOMEM,
 * ZPQ_EINTERNAL. With those, *out_json is an {"error", "reason"} message to free with zpq_free when one could be
 * built (never for ZPQ_ENOMEM), else NULL.
 * Memory: the query reserves its budget (its "max_memory", else the ceiling) from the context's ceiling before it
 * starts, in arrival order, waiting while it does not fit; "budget_wait_ms" in the request bounds that wait. A
 * budget larger than the ceiling, or a wait that runs out, is an {"error":"budget", ...} response, and a budget of 0
 * a bad request. The ceiling bounds declared budgets, not memory: the engine sizes GROUP BY state and read-ahead
 * against a query's budget but not every allocation, so a query can use more than it declared (docs/libzpq.md).
 */
int zpq_run(zpq_ctx *ctx, const char *request_json, size_t len, char **out_json, size_t *out_len);

/*
 * Arrow C stream result, additive in LIBZPQ_1. This function selects Arrow; request_json uses the existing SQL or
 * structured projection/filter/aggregate request. output_url and unsupported nested/logical types are refused.
 * ZPQ_OK means *out_stream owns a live stream. Query failures (including before its first batch) are reported by
 * get_schema/get_next as errno codes, with get_last_error's UTF-8 message. EOF is 0 with out.release == NULL.
 * Call failures use zpq_status and an optional zpq_free-owned *out_error. Failure clears out_stream.
 * Serialize callbacks on a stream. Separate streams may run concurrently. Release the stream before zpq_close;
 * release cancels further production and joins its producer. Each schema/batch is owned independently, survives
 * stream/context release and is freed by its own release callback, on any thread. Keep libzpq loaded until then.
 * max_result_bytes caps cumulative Arrow buffer bytes, excluding schemas/structures, before handing out a batch.
 * Its default is max(request budget, 1 GiB). Earlier batches remain valid on error. Retained arrays are caller-held
 * memory; the bounded producer queue is separate from them. max_memory retains the existing query-budget meaning.
 */
int zpq_run_arrow(zpq_ctx *ctx, const char *request_json, size_t len,
                  struct ArrowArrayStream *out_stream, char **out_error);

/* Free a response or message from zpq_open or zpq_run. NULL is a no-op. Any thread. */
void zpq_free(char *json);

/*
 * Shut a context down without freeing it: new calls get ZPQ_ECLOSED, and queries waiting for their memory budget
 * are woken and return ZPQ_ECLOSED. Running queries are not cancelled: each runs to its end, and one stalled on S3
 * ends only by the S3 timeouts (ZPQ_S3_CONNECT_TIMEOUT_MS, ZPQ_S3_HANDSHAKE_TIMEOUT_MS, ZPQ_S3_FIRST_BYTE_TIMEOUT_MS,
 * ZPQ_S3_IDLE_TIMEOUT_MS; 5, 5, 10 and 10 s by default) over its retries, which can take a minute or more. The
 * context stays valid until zpq_close. Returns at once; any thread, any number of times. NULL is a no-op.
 */
void zpq_shutdown(zpq_ctx *ctx);

/*
 * Close a context: zpq_shutdown, then wait for the running queries to finish, join any pre-connect handshakes still
 * in flight (up to about one second), and free the context. Call it once, when no other call on the context can
 * still start (see Threads); the context is gone when it returns. NULL is a no-op.
 */
void zpq_close(zpq_ctx *ctx);

/* The ABI version this library implements: 1. */
int zpq_abi_version(void);

/* A static description of a status code ("unknown status" for any other value). Never free it. */
const char *zpq_strerror(int status);

#ifdef __cplusplus
}
#endif

#endif /* ZPQ_H */
