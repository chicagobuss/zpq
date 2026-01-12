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
    *   **Use `just`**: `just build` and `just bench` are the sources of truth.
*   **Reproducibility**:
    *   Pin the Zig compiler version (see `.zig-version`).
    *   Disable "Verify Certificate" only for local MinIO tests. Production **MUST** verify.

## DNS & Networking
*   **The Resolver Wars**: We use a 3-tier resolver stack:
    1.  **Fast Path**: In-memory LRU cache.
    2.  **deduplication**: SingleFlight (coalesce concurrent requests for same host).
    3.  **Transport**: `xev`-based async resolution (never block the loop).
