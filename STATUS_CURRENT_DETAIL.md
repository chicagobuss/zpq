# ZPQ Technical Context & Deep Dive

**Last Updated**: Dec 16, 2025
**Current State**: High-Performance "Bare Metal" S3 I/O Stack complete (Async, Evented, TLS-enabled). Integration with Parquet Core is next.

## 🧠 Lessons Learned: Working with Zig 0.16.x & ZPQ Workflow

### 1. Zig 0.16.x Breaking Changes & Patterns
*   **`std.Io` Overhaul**: `std.io` is now `std.Io`. The API is async-native.
    *   `Reader.read` is gone. Use `Reader.readVec` (returns bytes read) or `Reader.readSliceShort` (tries to fill buffer).
    *   `Writer` handles buffering differently.
*   **`std.crypto.tls.Client`**:
    *   **Manual Buffering**: Requires explicit `Options` with `read_buffer` (plaintext read) and `write_buffer` (plaintext write).
    *   **Socket Wrapping**: Wraps `std.Io.net.Stream`, which wraps the raw FD.
    *   **Error Handling**: Does NOT explicitly list `error.WouldBlock` in inferred return types, requiring `@as(anyerror, err)` casts to handle non-blocking flow correctly.
*   **`std.net` Removal**: Old `std.net` functions (like `getAddressList`) are moving/changing. Use `std.Io.net` or `std.posix` primitives where possible.

### 2. Workflow & Testing Strategy
*   **The Python Wrapper**: ALWAYS use `python3 tools/no_output_timeout.py zig build test-io` for I/O tests.
    *   **Why**: Zig test runner buffers output and can hang silently on deadlocks/timeouts. The wrapper ensures we see partial output and kills hung processes.
*   **Micro-Tests First**: Don't try to integrate complex systems immediately.
    *   *Example*: We built `test_event_loop.zig` (kqueue), `test_tls.zig` (handshake), and `test_async_request.zig` (HTTP state machine) in isolation before wiring them together.
*   **Search > Guess**: Zig 0.16 is bleeding edge.
    *   **Do**: Search `lib/std` source code (e.g., `lib/std/http/Client.zig`) for usage patterns.
    *   **Do**: Search web/Discord for specific 0.16 migration guides.
    *   **Don't**: Guess method signatures based on 0.13 docs.

## 🏗️ High-Performance I/O Stack (Completed Components)

### 1. `AsyncS3Source` (`src/zpq/s3/async_s3_source.zig`)
*   **Role**: Orchestrator. Manages `ConnectionPool`, `EventLoop`, and parallel fetches.
*   **Capabilities**:
    *   **Range Coalescing**: Merges adjacent ranges (Polars-style) to minimize requests.
    *   **Request Splitting**: Splits huge ranges into 64MB chunks.
    *   **TLS Support**: Automatically upgrades to TLS for `https` schemes using `TlsAdapter`.
    *   **Reliable DNS**: Uses `std.c.getaddrinfo` for robust resolution (bypassing `std.net` instability in Zig master).

### 2. `TlsAdapter` (`src/zpq/s3/tls_adapter.zig`)
*   **Status**: Verified against Cloudflare (1.1.1.1).
*   **Architecture**:
    *   Wraps raw `fd` via `std.Io.net.Stream`.
    *   Uses `std.crypto.tls` (Pure Zig).
    *   Manages own buffers to avoid hidden allocations.
    *   Propagates `WouldBlock` for event loop integration.

### 3. `AsyncRequest` (`src/zpq/s3/async_request.zig`)
*   **Status**: Working.
*   **Features**:
    *   **Scatter/Gather**: Reads directly into multiple user buffers (`addSegment`).
    *   **Zero-Allocation Gaps**: Skips bytes on the socket (reads into scratch buffer) to handle gaps without allocating heap memory.
    *   **State Machine**: `Idle` -> `Sending` -> `Headers` -> `Body` -> `Finished`.

## ⏭️ Next Session: Optimization & Hardening

### Completed: Phase G (Parquet Integration)
*   **Integrated**: `AsyncS3Source` is now wired into `ParquetFile` and `main.zig`.
*   **Verification**:
    *   **Local Mock**: `debug-s3` works correctly against local HTTP mock server (127.0.0.1:9000).
    *   **Leak Fix**: Fixed `EventLoop` map leak in `AsyncS3Source.init` error path.

### Current Focus: Real S3 (HTTPS) Hardening
*   **Issue**: `debug-s3` against real S3 fails with `HeadRequestFailed` / `EndOfStream`.
*   **Hypothesis**: TLS connection closure or HTTP response handling issue with real S3 (possibly Keep-Alive or Header parsing nuance).
*   **Plan**:
    1.  Debug TLS/HTTP interaction with real S3.
    2.  Verify leak fix in failure scenarios.
    3.  Run full S3 benchmark.
