# libzpq: zpq behind a C ABI

`libzpq` runs zpq queries in-process. It takes the zpq Lambda's JSON request and returns the Lambda's JSON response.
A long-lived process, such as a service, a notebook or a benchmark harness, keeps one context open, so S3 connections
and parquet footers stay warm between queries and no process starts per query. Its answers are the CLI's and the
Lambda's, byte for byte (see Tests).

## Quick start

C:

```c
#include <stdio.h>
#include <string.h>
#include "zpq.h"

int main(void) {
    zpq_ctx *ctx;
    char *err = NULL;
    if (zpq_open("{\"max_memory\": \"4GB\"}", &ctx, &err) != ZPQ_OK) {
        fprintf(stderr, "zpq_open: %s\n", err ? err : "out of memory");
        zpq_free(err);
        return 1;
    }
    const char *req = "{\"inputs\": [\"data.parquet\"], \"aggregate\": \"count(*) AS n\"}";
    char *resp = NULL;
    int rc = zpq_run(ctx, req, strlen(req), &resp, NULL);
    if (rc == ZPQ_OK) printf("%s\n", resp); /* the Lambda's response, an {"error": ...} one included */
    else fprintf(stderr, "zpq_run: %s: %s\n", zpq_strerror(rc), resp ? resp : "");
    zpq_free(resp);
    zpq_close(ctx);
    return rc == ZPQ_OK ? 0 : 1;
}
```

```sh
zig build libzpq -Doptimize=ReleaseFast --prefix /usr/local   # installs lib/libzpq.so, lib/libzpq.a, include/zpq.h
cc -o q q.c -lzpq
```

Python (`python/zpq.py`, a ctypes binding with no compiled extension):

```python
import zpq
with zpq.Zpq(max_memory="4GB") as z:
    r = z.run({"inputs": ["s3://bucket/lineitem/*.parquet"], "aggregate": "count(*) AS n"})
    print(r["agg"])
```

## Build and install

```sh
zig build libzpq -Doptimize=ReleaseFast [-Dtarget=...] [--prefix DIR]
# DIR/lib/libzpq.so (libzpq.dylib on macOS), DIR/lib/libzpq.a, DIR/include/zpq.h; DIR defaults to zig-out
```

| `-Dtarget=` | library | needs at run time | status |
|---|---|---|---|
| (native, Linux x86_64 glibc) | `libzpq.so` | libc.so.6, at the host's glibc version | built and tested: C ABI, ASan, leak loop, Python, differential |
| `x86_64-linux-gnu.2.28` | `libzpq.so` | glibc ≥ 2.28 (libc, libpthread, libdl) | built; C ABI test run |
| `aarch64-linux-gnu.2.28` | `libzpq.so` | glibc ≥ 2.28 (libc, libpthread, libdl) | cross-built, exports checked |
| `x86_64-linux-musl` | `libzpq.so` | musl's libc.so | built; C ABI test run on Alpine 3.24 |
| `aarch64-linux-musl` | `libzpq.so` | musl's libc.so | cross-built, exports checked |
| `aarch64-macos` | `libzpq.dylib` | libSystem; macOS ≥ 15 by default | cross-built, exports checked; not run |
| `x86_64-macos` | `libzpq.dylib` | libSystem; macOS ≥ 15 by default | cross-built, exports checked; not run |

A native build links against the host's glibc and may need that version or later. For a library to ship to other
machines, name the oldest glibc in the target, as above (`x86_64-linux-gnu` alone also means 2.28). On macOS the
oldest release goes in the target the same way (`-Dtarget=aarch64-macos.13.0`); the default is the toolchain's.
Windows is not built.

- The library carries everything it links: BoringSSL, the codecs and libc++. At run time it needs the system C
  library alone, as in the table. It needs no TLS, codec or C++ library from the system.
- On Linux it exports the seven `zpq_*` functions and nothing else, under the symbol version `LIBZPQ_1`
  (`src/libzpq/libzpq.map`; `nm -D --defined-only libzpq.so` lists them). Its BoringSSL cannot collide with a host's
  own OpenSSL, such as the one a Python process loads.
- On macOS, which has no version script, every archive the dylib links is compiled with hidden symbols, so it exports
  the seven functions too. It adds only what a Mach-O library always carries (`__mh_dylib_header`, `__dso_handle`) and
  libc++'s weak `operator new` and `operator delete`, which allocate with `malloc` as the system's do.
- The shared library's BoringSSL is compiled from source, position-independent, because the pinned prebuilts the
  CLI and Lambda link are not. That makes the first build slower.
- `libzpq.a` holds zpq's own objects. A static link must add the codec, BoringSSL and libc++ archives the build
  produces; prefer the shared library.

## The C ABI (`include/zpq.h`)

| function | returns | the caller owns | threads |
|---|---|---|---|
| `int zpq_open(const char *options_json, zpq_ctx **out_ctx, char **out_error)` | a status | `*out_ctx` until `zpq_close`; `*out_error` (free with `zpq_free`) | any thread, any number at once |
| `int zpq_run(zpq_ctx *, const char *request_json, size_t len, char **out_json, size_t *out_len)` | a status | `*out_json` (free with `zpq_free`) | any number at once on one context |
| `void zpq_free(char *)` | | | any thread |
| `void zpq_shutdown(zpq_ctx *)` | | | any thread, any number of times |
| `void zpq_close(zpq_ctx *)` | | the context is gone when it returns | once per context, when no other call can start |
| `int zpq_abi_version(void)` | `1` | | any thread |
| `const char *zpq_strerror(int)` | a static string | nothing: never free it | any thread |

libzpq keeps no pointer to anything the caller passes in once the call returns: a request need not outlive its call,
nor be NUL-terminated (only `len` bytes are read).

**Status codes** (`zpq_status`, stable: a value never changes meaning, new ones are only added):

| code | value | meaning | message |
|---|---:|---|---|
| `ZPQ_OK` | 0 | `zpq_open`: the context is in `*out_ctx`. `zpq_run`: a response was produced, **including** a query that failed (bad SQL, a missing column, an S3 403, a refused memory budget): an `{"error": ...}` document exactly as the Lambda returns it | the response |
| `ZPQ_EINVAL` | -1 | the call itself was malformed: a NULL context or output, a NULL request with `len > 0`, a length too large, bad `zpq_open` options | `{"error":"invalid_argument","reason":...}` |
| `ZPQ_ENOMEM` | -2 | out of memory before a result could be built | none |
| `ZPQ_ECLOSED` | -3 | `zpq_shutdown` or `zpq_close` has been called on this context | `{"error":"closed","reason":"Closed"}` |
| `ZPQ_EINTERNAL` | -4 | a failure that is not the caller's: a thread that would not start, a pool or TLS set-up | `{"error":"internal","reason":...}` |

Every output pointer given is cleared before anything else. On a non-OK status `*out_ctx` is NULL, and the message
(when the caller asked for one and it could be built) is in `*out_error` / `*out_json`, to free with `zpq_free`;
`*out_len` describes responses only and stays 0.

**Options** (`zpq_open`, a JSON object; NULL or `""` for the defaults; an unknown option is `ZPQ_EINVAL`):

- `"max_memory"`: the context's memory ceiling (see Memory): bytes, or a size string (`"4GB"`). Default: the memory
  available, the CLI's default budget.
- `"budget_wait_ms"`: how long a query waits for its memory budget when its request names no `budget_wait_ms`.
  Default: as long as it takes.
- `"preconnect"`: `true` opens connections to the S3 endpoint now, as the Lambda does during init, so the first
  queries find them ready: `ZPQ_S3_PRECONNECT` of them (default 4, at most 8), one thread each, after resolving the
  endpoint on the calling thread. Without S3 credentials in the environment it does nothing; failures are ignored
  and the queries connect on their own. A `zpq_close` while handshakes are still in flight waits for them, at most
  about one second. Default: `false`, so a host that never reads S3 opens no connections.

**Requests and responses.** The Lambda's request (see [lambda_requests.md](lambda_requests.md)): `inputs`, `filter`,
`aggregate`, `group_by`, `select`, `columns`, `output_url`, `output_codec`, `max_memory`, `budget_wait_ms`,
`bloom_filters`, `profile` and the rest. Inputs and `output_url` may be local paths as well as `s3://` URLs. A request
must aggregate (`aggregate` / `group_by`) or write (`output_url`); anything else answers
`{"error":"bad_request","reason":"NoOperation"}`.

## Threads and contexts

- Any number of `zpq_run` calls may run at once on one context, from its first call on: `zpq_open` sets the pool up
  before returning. Each query runs on threads of its own, as in the CLI, and shares the context's S3 connection
  pool and footer cache. The `"pool"` member of a response then reports the pool's running totals.
- Closing is two steps, because `zpq_close` frees the context and no call may start once it has begun:
  1. `zpq_shutdown` refuses new calls and wakes the queries still waiting for their memory budget; both return
     `ZPQ_ECLOSED`. The context stays valid, so threads that are about to call get that status instead of touching
     freed memory. It returns at once, and may be called again.
  2. Once the host's own calls have all returned, `zpq_close` waits for any query still running and frees the
     context.

  The Python binding's `close` does both for its callers.
- Neither step cancels a running query: it runs to its end. One stalled on S3 ends only by the S3 timeouts
  (`ZPQ_S3_CONNECT_TIMEOUT_MS`, `ZPQ_S3_HANDSHAKE_TIMEOUT_MS`, `ZPQ_S3_FIRST_BYTE_TIMEOUT_MS`,
  `ZPQ_S3_IDLE_TIMEOUT_MS`: 5, 5, 10 and 10 s by default) over its retries, which can hold `zpq_close` for a minute
  or more. A host that needs a faster shutdown can lower those timeouts. Cancelling running queries is not in 0.5.0.
- One context per process is the intended use. Several work, but each sets and restores the SIGPIPE/SIGIO handlers
  (below), so close them in the reverse order they were opened.

## Memory

A context's `max_memory` is a ceiling its queries share. Each query reserves its whole budget before it starts: its
request's `max_memory`, else the whole ceiling (so one query alone runs as `zpq query` would, and queries that name no
budget run one at a time). It returns the budget when it ends.

- Reservations are granted in arrival order: a query waits while any query that arrived before it is still waiting,
  so a large query is never starved by small ones.
- A request asking for more than the ceiling is refused at once: `{"error":"budget","reason":"OverCeiling",...}`.
- A wait longer than `budget_wait_ms` (the request's, else the context's) ends with
  `{"error":"budget","reason":"WaitTimeout",...}`.

To run several queries at once, give each request a `max_memory` that leaves room for the others. The footer cache
and warm Bloom filters stay with the context until `zpq_close` and are not counted against the ceiling in 0.5.0.

## What libzpq does to the process

- **Signals.** While a context is open, SIGPIPE and SIGIO have no-op handlers (Zig's `std.Io.Threaded`), so a
  closed socket fails a write instead of killing the process; `zpq_close` restores the previous handlers.
- **Alternate signal stack.** Threads zpq starts install Zig's alternate signal stack (256 KiB each). Under
  AddressSanitizer, set `ASAN_OPTIONS=use_sigaltstack=0`, or ASan tries to unmap that stack as its own when the
  thread exits.
- **Environment.** Each call reads the process environment as it is called: S3 credentials, endpoint and region, as
  the CLI reads them. Don't change it with `setenv` while a call runs. A cleared environment (`clearenv()`) is an
  empty one.
- **Process-wide state.** Beyond the context, the only process-wide state is a few caches, each set once and then
  only read: the TLS client contexts (CA stores) and the io_uring and gzip-backend probes.
- **Local files are memory-mapped.** A local input is read through `mmap`. If another process truncates the file
  while a query reads it, the next access past the new end raises SIGBUS, which kills the host process unless it
  handles that signal. Don't let files a query reads be truncated or rewritten in place while it runs (write a new
  file and rename it instead). S3 inputs are unaffected.
- **Failures.** A bad request, a bad or hostile file, or a failing or hostile network peer is an error response, never
  a crash: the paths a request reaches are audited for panics, and the footer, page decoding and the expression and
  filter parsers are fuzzed. A broken internal invariant still aborts the process with a message, as it would the
  CLI; libzpq never unwinds into the caller. Build it `ReleaseSafe` to keep those checks, `ReleaseFast` for speed.

## Stable and experimental in 0.5.0

Stable: the seven functions, their signatures and ownership, the status codes and their meanings, the option names, and
the request and response schema the Lambda documents. ABI version 1 only grows: new options, request fields and
status codes may be added.

Experimental: the `"pool"` and `"profile"` members of a response (their fields may change), the Python binding's
API, and the HTTP service. The memory ceiling does not yet count the context's caches (see Memory).

## Python

`python/zpq.py` loads `$ZPQ_LIB`, else `libzpq.so` (`libzpq.dylib` on macOS) next to it, in `../zig-out/lib`, or on
the loader path. `zpq.Zpq(max_memory=None, library=None, *, preconnect=False, budget_wait_ms=None)` opens a context.
`run` returns the response as a dict and raises `zpq.ZpqError` on an `{"error": ...}` response, unless `check=False`;
a refused call raises `ValueError`, `MemoryError`, `zpq.ZpqClosed` or `RuntimeError` with its reason. ctypes releases
the GIL during the call, so threads run queries in parallel. `close` refuses new calls and ends those still waiting
for their memory budget (both `zpq.ZpqClosed`), then waits for the running ones, so the C context is never freed
under a running query.

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

`tests/libzpq/run.sh` builds the library and the binaries (ReleaseSafe by default; `OPTIMIZE=` changes it) and runs:

- `tests/libzpq/test_abi.c`, the C ABI end to end:
  - options and their refusals, the round trip, a write read back, error responses, and every refused call's
    status, message and cleared outputs;
  - 16 threads × 40 mixed calls on one context, each answer checked, on a warm context and on 8 fresh ones whose
    first calls all start together;
  - queries whose budgets add up past the ceiling, all answered; close and shutdown while queries wait for their
    budget;
  - 1,000 open/run/close cycles, and resident memory over 10,000 calls (the second half must not grow it);
- the same program under AddressSanitizer and LeakSanitizer, with a tenth of the calls (the system compiler builds
  it; `zig cc` has no sanitizer runtime);
- `tests/libzpq/test_python.py`: the binding, its options and queued budgets, 480 calls from 16 threads, and the
  HTTP service. It also checks `close` and service shutdown with a query in flight and a keep-alive connection idle
  (no request thread may still run when the context closes), and malformed `Content-Length` values;
- `tests/libzpq/differential.py`: the differential corpus (`tools/differential.py`) through libzpq, the CLI and the
  Lambda. Every filter × projection and filter × select writes the same file, byte for byte, and every
  filter × aggregate gives the same `"agg"` text.

`tests/libzpq/overhead.sh` compares a query through a warm context with the CLI, which starts a process per query,
in perf counters.
