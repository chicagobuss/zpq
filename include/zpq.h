/*
 * libzpq: zpq's query engine behind a C ABI.
 *
 * A request is the JSON the zpq Lambda takes (see docs/libzpq.md): "inputs" (local paths or s3:// URLs), and an
 * "aggregate" / "group_by", or an "output_url" to write to, with "filter", "select", "columns", "max_memory" and the
 * rest as the Lambda reads them. The response is the JSON the Lambda returns.
 *
 * Threads: zpq_run may be called from any number of threads at once on one context. Each call runs its query on
 * threads of its own; the context's S3 connections and footer cache are shared.
 *
 * Environment: each zpq_run reads the process environment when it is called (S3 credentials, endpoint, region, as
 * the CLI reads them). Do not change it with setenv while a call is running.
 */
#ifndef ZPQ_H
#define ZPQ_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct zpq_ctx zpq_ctx;

/* zpq_run's return codes. */
#define ZPQ_OK 0             /* *out_json holds the response. */
#define ZPQ_ERROR_RESPONSE 1 /* *out_json holds an {"error": ...} response: a bad request, or a query that failed. */
#define ZPQ_EINVAL (-1)      /* A NULL context or out_json, or a NULL request with a nonzero length. No response. */
#define ZPQ_ENOMEM (-2)      /* Out of memory before a response could be built. No response. */

/*
 * A context for any number of zpq_run calls. options_json may be NULL or "" for the defaults, else a JSON object:
 *   "max_memory": a query's memory budget when its request names none: bytes, or a size string ("2GB").
 *                 Default: derived from the memory available, as the Lambda's is.
 * Returns NULL on invalid options or when out of memory.
 */
zpq_ctx *zpq_open(const char *options_json);

/*
 * Run one request (request_json, len bytes, need not be NUL-terminated). On ZPQ_OK and ZPQ_ERROR_RESPONSE,
 * *out_json is a NUL-terminated response the caller frees with zpq_free, and *out_len (when out_len is not NULL)
 * its length without the NUL. Otherwise *out_json is NULL.
 */
int zpq_run(zpq_ctx *ctx, const char *request_json, size_t len, char **out_json, size_t *out_len);

/* Free a response from zpq_run. NULL is a no-op. */
void zpq_free(char *json);

/* Free a context. No zpq_run may be running on it. NULL is a no-op. */
void zpq_close(zpq_ctx *ctx);

/* The ABI version this library implements: 1. */
int zpq_abi_version(void);

#ifdef __cplusplus
}
#endif

#endif /* ZPQ_H */
