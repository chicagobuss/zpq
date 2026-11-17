# Browser reactor

Build with Zig 0.17.1:

```
zig build zpq-wasm -Dtarget=wasm32-wasi -Doptimize=ReleaseFast
zig build zpq-wasm -Dtarget=wasm32-wasi -Dcpu=baseline+atomics+bulk_memory -Dwasm-threads=true -Doptimize=ReleaseFast
```

Both commands install `zig-out/bin/zpq-wasm.wasm`; preserve the serial artifact before building the threaded one.
Native targets keep their existing build graph. The reactor accepts caller-owned Parquet bytes and aggregate SQL
against `input`. It supplies neither filesystem nor network access. It is a single-request API: do not invoke it
concurrently. Returned response bytes remain valid until the next query. Free caller allocations using their exact
original length; do not free the response. The JSON request accepts `sql`, `threads` (1–8) and `max_memory` (up to
512 MiB, default 256 MiB). The query ledger excludes the caller's input buffer and serialized response.

Exports are `zpq_alloc`, `zpq_free`, `zpq_run`, `zpq_response_ptr`, `zpq_response_len`; the threaded build also exports
`wasi_thread_start` and `zpq_pool_threads`. Initialize the WASI reactor before calling it. `zpq_run` returns 0 for
success, 1 for a JSON error response, or -1 if it cannot allocate the error response. Requests must fit the ABI's
32-bit address space. Threaded instances share memory but have independent stacks/TLS; provide WASI thread-spawn
and a shared WebAssembly.Memory with initial 512 pages and maximum 16384 pages. Worker execution requires a
cross-origin isolated page. Zig's WASI heap is compiled separately and serialized across worker instances; wide
atomic counters use a 32-bit lock because Zig 0.17's atomic builtins reject 64-bit values on wasm32.

For a focused ReleaseSafe smoke, build the serial target with `-Doptimize=ReleaseSafe`, then:

```
node tools/wasm-smoke.mjs zig-out/bin/zpq-wasm.wasm sample.parquet 100000
```

The smoke uses Node's WASI reactor support and verifies the ABI against a supplied Parquet fixture. The site's
browser test must additionally cover all presets and the real shared-memory worker pool. The site owns its
content-stamped JavaScript, DuckDB assets, samples and COOP/COEP deployment policy; none are bundled here.
