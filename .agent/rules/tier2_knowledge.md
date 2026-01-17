---
trigger: always_on
---

# Tier 2: The Grimoire of Knowledge

**Hard-earned lessons from the bleeding edge.**

## Bleeding Edge Zig (0.16.x)
*   **Std Lib Volatility**: `std.http` and `std.net` are unstable. Avoid them for core logic. Use `std.ArrayListUnmanaged` for buffers to avoid implicit allocations.
*   **Comptime Power**: Use `comptime` to generate specialized decoders (e.g., bit-unpacking). Avoid runtime `if` switches in hot loops.
*   **Allocator Discipline**: Pass `std.mem.Allocator` explicitly. Use `ArenaAllocator` for per-request or per-row-group lifecycles to avoid fragmentation.

## The Async Stack (libxev + BoringSSL)
*   **Implicit State Trap**: BoringSSL requires explicit state initialization (`SSL_set_connect_state`). Do not rely on `SSL_read/write` to trigger handshakes.
*   **Memory BIOs**: Use `BIO_s_mem` to decouple crypto from I/O.
    *   *Read Path*: Socket -> Ring Buffer -> BIO_write -> SSL_read -> Application.
    *   *Write Path*: Application -> SSL_write -> BIO_read -> Ring Buffer -> Socket.
*   **Partial Writes**: `xev.write` may write fewer bytes than requested. **ALWAYS** loop until the entire buffer is drained.
*   **Backpressure**: Implement a bounded queue for outgoing writes. Do not blindly `write()` faster than the network can transmit.

## Workflow & Benchmarking
*   **Fair Comparisons**:
    *   ** Make sure your benchmark is apples-to-apples.  If one tools is reading a file locally and writing to s3, the other tool needs to be doing the exact same task.
    *   **Always** source `.env` before running benchmarks.
    *   **Use `just`**: `just build` and `just native, and just lambda` are the sources of truth.
*   **Reproducibility**:
    *   Pin the Zig compiler version (see `.zig-version`).
    *   # Standard parquet testing and benchmark file (snappy compressed, lots of types):
       ZPQ_BENCH_FILE=data/benchmark/benchmark_100mb.parquet
       # Also in S3 in s3://${AWS_S3_BUCKET}zpq_test_data/zpq_test_data/benchmark/

## DNS & Networking
*   **The Resolver Wars**: We use a 3-tier resolver stack:
    1.  **Fast Path**: In-memory LRU cache.
    2.  **deduplication**: SingleFlight (coalesce concurrent requests for same host).
    3.  **Transport**: `xev`-based async resolution (never block the loop).

## Observability & Logging
 - zpq shouldn't need fancy debug-mode compilation, just use releasefast builds and a normal debug/info/warn/error style logger in a consistent, unified fashion throughout the codebase, and set the log level appropriately with a cli arg when running the binary if you need/want more logging output.
 - for more detailed observability, you can use ebpf tools on linux or xccode when developing on macos