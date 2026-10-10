# libzpq: zpq behind a C ABI

`libzpq` runs zpq queries in-process. It takes the zpq Lambda's JSON request and returns the Lambda's JSON response.
A long-lived process, such as a service, a notebook or a benchmark harness, keeps one context open, so S3 connections
and parquet footers stay warm between queries and no process starts per query.

Status: spike (`spike/libzpq`), not part of a release.

## Build

```sh
zig build libzpq -Doptimize=ReleaseFast
# zig-out/lib/libzpq.so, zig-out/lib/libzpq.a, zig-out/include/zpq.h
```

- `libzpq.so` carries everything it links: BoringSSL, the codecs and libc++. At run time it needs the system C
  library alone: libc and the dynamic loader on x86_64; on aarch64 (glibc) it also declares libpthread and libdl,
  which are part of glibc. It needs no TLS, codec or C++ library from the system.
- It exports the five `zpq_*` functions and nothing else (`src/libzpq/libzpq.map`). Its BoringSSL cannot collide
  with a host's own OpenSSL, such as the one a Python process loads.
- The shared library's BoringSSL is compiled from source, position-independent, because the pinned prebuilts the
  CLI and Lambda link are not. That makes the first build slower.
- Linux only for now (x86_64 and aarch64 build): the export list is an ELF version script, and macOS is untried.
- `libzpq.a` holds zpq's own objects. A static link must add the codec, BoringSSL and libc++ archives the build
  produces; prefer the shared library.

## The C ABI (`include/zpq.h`)

```c
zpq_ctx *zpq_open(const char *options_json);   /* NULL or "" for defaults; NULL on bad options */
int zpq_run(zpq_ctx *, const char *request_json, size_t len, char **out_json, size_t *out_len);
void zpq_free(char *json);
void zpq_close(zpq_ctx *);
int zpq_abi_version(void);                      /* 1 */
```

- **Options.** An unknown option is an error.
  - `"max_memory"`: `1073741824` or `"4GB"`. Each query's budget when its request names none. By default the
    budget derives from available memory, as the Lambda's does.
  - `"preconnect"`: `true` opens connections to the S3 endpoint as the context opens (`ZPQ_S3_PRECONNECT` of them,
    default 4), as the Lambda does during init, so the first queries find them ready. Off by default: a host that
    never reads S3 opens no connections. Failures are ignored; the queries then connect on their own.
- **Request.** The Lambda's request (see [lambda_requests.md](lambda_requests.md)): `inputs`, `filter`,
  `aggregate`, `group_by`, `select`, `columns`, `output_url`, `output_codec`, `max_memory`, `bloom_filters`,
  `profile` and the rest. Inputs and `output_url` may be local paths as well as `s3://` URLs. A request must
  aggregate (`aggregate` / `group_by`) or write (`output_url`); anything else answers
  `{"error":"bad_request","reason":"NoOperation"}`.
- **Return codes.**
  - `ZPQ_OK` (0): `*out_json` is the response.
  - `ZPQ_ERROR_RESPONSE` (1): `*out_json` is an `{"error": ...}` response, for a malformed request or a query that
    failed.
  - `ZPQ_EINVAL` (-1): bad arguments.
  - `ZPQ_ENOMEM` (-2): out of memory.
  On 0 and 1 the response is NUL-terminated and the caller frees it with `zpq_free`. Every output pointer given is
  cleared before anything else, so a refused call leaves `*out_json` NULL and `*out_len` 0.
- **Threads.** Any number of `zpq_run` calls may run at once on one context, from its first call on: `zpq_open`
  sets the pool up before returning. Each query runs on threads of its own,
  as in the CLI, and shares the context's S3 connection pool and footer cache. The `"pool"` member of a response then
  reports the pool's running totals, not the one query's.
- **Environment.** Each call reads the process environment as it is called: S3 credentials, endpoint and region, as
  the CLI reads them. Don't change it with `setenv` while a call runs.
- **Memory.** `max_memory` bounds each query, not the context; concurrent queries each have their own budget. The
  footer cache and warm Bloom filters stay with the context until `zpq_close`.

### What libzpq does to the process

- **Signals.** While a context is open, SIGPIPE and SIGIO have no-op handlers (Zig's `std.Io.Threaded`), so a
  closed socket fails a write instead of killing the process; `zpq_close` restores the previous handlers. Open
  one context per process: closing two out of order can restore a stale handler.
- **Alternate signal stack.** Threads zpq starts install Zig's alternate signal stack (256 KiB each). Under
  AddressSanitizer, set `ASAN_OPTIONS=use_sigaltstack=0`, or ASan tries to unmap that stack as its own when the
  thread exits.
- **Process-wide state.** Beyond the context, the only process-wide state is a few caches, each set once and then
  only read: the TLS client contexts (CA stores) and the io_uring and gzip-backend probes.
- **Local files are memory-mapped.** A local input is read through `mmap`. If another process truncates the file
  while a query reads it, the next access past the new end raises SIGBUS, which kills the host process unless it
  handles that signal. Don't let files a query reads be truncated or rewritten in place while it runs (write a new
  file and rename it instead). S3 inputs are unaffected.
- **Panics.** A broken invariant inside zpq panics and aborts the process, as it would the CLI.

## Python

`python/zpq.py` is a ctypes binding, with no compiled extension. It loads `$ZPQ_LIB`, else `libzpq.so` next to it,
in `../zig-out/lib`, or on the loader path.

```python
import zpq
with zpq.Zpq(max_memory="4GB") as z:
    r = z.run({"inputs": ["s3://bucket/lineitem/*.parquet"], "aggregate": "count(*) AS n"})
    print(r["agg"])
```

`run` returns the response as a dict and raises `zpq.ZpqError` on an `{"error": ...}` response, unless
`check=False`. ctypes releases the GIL during the call, so threads run queries in parallel. `close` waits for the
calls in flight and refuses new ones (`zpq.ZpqClosed`), so the C context is never freed under a running query.

`python/zpq_service.py` is a stdlib HTTP service: one process and one context, with a thread per request.

```sh
ZPQ_LIB=zig-out/lib/libzpq.so python python/zpq_service.py --port 8099 --max-memory 4GB
curl -s localhost:8099/query -d '{"inputs": ["data.parquet"], "aggregate": "count(*) AS n"}'
```

`POST /query` answers 200 with a response, or 400 with an `{"error": ...}` one; `GET /health` answers
`{"ok": true}`.
- A missing `Content-Length` answers 411; one that is not a non-negative integer, 400; one over 16 MiB, 413. Each of
  these is answered without reading the body, and closes the connection.
- On shutdown (Ctrl-C, or `server.shutdown()` then `zpq_service.stop(server, z)` when embedded), the service:
  - stops accepting;
  - ends idle keep-alive connections;
  - joins every request thread, so queries in flight finish and answer;
  - only then frees the context.

  A call reaching the context after that answers 503 `{"error":"shutting_down"}`.

## Tests

`tests/libzpq/run.sh` builds the library (ReleaseSafe by default; `OPTIMIZE=` changes it) and runs:

- `tests/libzpq/test_abi.c`, the C ABI end to end:
  - options, round trip, a write read back, error responses and invalid arguments;
  - 16 threads × 40 mixed calls on one context, each answer checked, on a warm context and on 8 fresh ones whose
    first calls all start together;
  - resident memory over 10,000 calls (the second half must not grow it);
- the same program under AddressSanitizer and LeakSanitizer, with a tenth of the calls (the system compiler builds
  it; `zig cc` has no sanitizer runtime);
- `tests/libzpq/test_python.py`: the binding, 480 calls from 16 threads, and the HTTP service. It also checks
  `close` and service shutdown with a query in flight and a keep-alive connection idle (no request thread may
  still run when the context closes), and malformed `Content-Length` values.

`tests/libzpq/overhead.sh` compares a query through a warm context with the CLI, which starts a process per query,
in perf counters.

The handler is shared: the Lambda binary and libzpq call the same `src/service/handler.zig`, so the Lambda's
integration tests cover libzpq's request handling too.
