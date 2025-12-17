# ZPQ Technical Context & Deep Dive

**Last Updated**: Dec 16, 2025
**Current State**: **Breakthrough**: `libxev` now runs on macOS and Linux (Zig 0.16.0-dev). Moving to TLS integration.

## 🏆 Milestone: Libxev Cross-Platform Stability
We successfully patched `libxev` to work with the bleeding-edge Zig 0.16 compiler, bypassing significant standard library regressions.
*   **Problem**: `std.net` removed, `@Type` removed, `std.posix` wrappers missing errors (`SocketNotListening`, `AddressInUse`).
*   **Solution**:
    *   Created `shim_net.zig` to replace `std.net.Address`.
    *   Patched `dynamic.zig` to use `@Enum`/`@Union` instead of `@Type`.
    *   Injected raw syscall shims (`accept`, `connect`, `getsockopt`) into `kqueue.zig` to bypass `std`.
*   **Verification**: `test_xev_tcp.zig` passes on macOS (native) and Linux (Docker `debian:bookworm-slim`).

## ⚡ Performance Verification
*   **Benchmark**: TCP Echo (Sequential, Single Connection, 500k iterations)
*   **Zig (libxev)**: **~70,121 RPS** (ReleaseFast)
*   **Node.js (v24)**: **~54,140 RPS**
*   **Result**: Zig `libxev` is **~1.3x faster** than Node.js.

## 🛠 Infrastructure
*   **Backup/Remote**: `oci-josh-arm-vm` (ARM64 Linux) configured as git remote `backup`.
*   **Use Case**: Native ARM64 builds and `io_uring` verification for AWS Lambda targets.

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

## 🚀 New I/O Stack Implementation Plan

**Goal**: Replace flaky legacy stack with robust `libxev` (event loop) + `boring_tls` (OpenSSL) implementation.

### Architecture
*   **`Client` Struct**: Orchestrates `xev.Loop` and connection pool.
*   **`Connection` Struct**: Wraps `xev.TCP` + `boring_tls.TlsClient`.
*   **`readRanges`**: Zero-allocation pipeline directly from socket to user buffers.

### Development Roadmap (Micro-Test Driven)

#### Phase 1: Micro-Tests (Current)
1.  **[DONE] TCP Connectivity (`test_xev_tcp`)**: 
    *   Proved `libxev` works on macOS and Linux.
2.  **[NEXT] TLS Handshake (`test_boring_connect`)**:
    *   **Goal**: Verify "BIO Pair" pattern for `boring_tls` + `libxev`.
    *   **Action**: Create `tests/io/test_boring_connect.zig`. Connect to `google.com:443`.
3.  **S3 Protocol (`test_s3_head`)**:
    *   **Goal**: Verify S3 specifics (Host header, signature).
    *   **Action**: Connect to S3 bucket, send HEAD.

#### Phase 2: Implementation & Integration
1.  **Core Client**: Move successful micro-test code into `src/zpq/io/http/Client.zig`.
2.  **Parquet Wiring**: Implement `readRanges` and hook into `ParquetFile`.

## 🏗️ Legacy Stack (Reference/Backup)
Located in `src/zpq/s3_legacy/`.
*   `AsyncS3Source`: Pure Zig `std.Io` + `std.crypto.tls` implementation.
*   **Status**: Works with local Mock S3 (HTTP) but flaky/broken with real S3 (HTTPS/Keep-Alive issues).
