# ZPQ Technical Context & Deep Dive

**Last Updated**: Dec 17, 2025
**Current State**: **Breakthrough**: `libxev` + `boring_tls` stack verified. Core `http.Client` and `TlsConnection` implemented and verified against S3.

## 🏆 Milestone: Libxev + BoringTLS Integration
We have successfully established a secure TLS 1.3 connection to `google.com` AND `s3.amazonaws.com` using `libxev` (async I/O) and `boring_tls` (OpenSSL) on **both macOS M1 and Linux ARM64**.

*   **Verification**: 
    *   **Local (macOS M1)**: `zig build --build-file micro_build.zig test-http-client` passes.
    *   **Remote (Linux ARM64)**: `test-boring-connect` and `test-s3-head` pass on `oci-josh-arm-vm`.
*   **Key Fixes**:
    *   **"Unknown Target CPU"**: Patched `boring_tls/build.zig` to define `_M_ARM64` for `aarch64` targets, resolving BoringSSL header compilation errors on M1 and Linux ARM.
    *   **Build Isolation**: Created `micro_build.zig` to run experimental tests without polluting the main `build.zig`.
    *   **BIO Pattern**: Verified that `boring_tls` interacts correctly with non-blocking `libxev` sockets via the standard BIO interface.

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

## 🔒 TLS Integration Roadmap (BoringTLS)
**Goal**: Unified, high-performance TLS across all platforms (macOS/Linux) using `boring_tls`.

*   **Strategy**: "Filter Pattern" (BIO).
    *   Treat TLS as a pure data transformation layer, decoupled from the underlying socket.
    *   Use `libxev` for transport (already verified).
    *   Use `boring_tls` for crypto (statically linked, identical behavior everywhere).
*   **Phases**:
    1.  **The Dumb Adapter**: Implement a buffer-based BIO shim that `boring_tls` can read/write to.
    2.  **The Pump (Microtest)**: Manually drive the handshake loop in a single file (`test_boring_connect.zig`).
    3.  **The Component**: Encapsulate the pump into a reusable `TlsClient` struct adhering to `std.Io`.

## 🚀 New I/O Stack Implementation Plan

**Goal**: Replace flaky legacy stack with robust `libxev` (event loop) + `boring_tls` (OpenSSL) implementation.

### Architecture
*   **`Client` Struct**: Orchestrates `xev.Loop` and connection pool.
*   **`Connection` Struct**: Wraps `xev.TCP` + `boring_tls.TlsClient`.
*   **`readRanges`**: Zero-allocation pipeline directly from socket to user buffers.

### Development Roadmap (Micro-Test Driven)

#### Phase 1: Micro-Tests (Completed)
1.  **[DONE] TCP Connectivity (`test_xev_tcp`)**: 
    *   Proved `libxev` works on macOS and Linux.
2.  **[DONE] TLS Handshake (`test_boring_connect`)**:
    *   **Goal**: Verify "BIO Pair" pattern for `boring_tls` + `libxev`.
    *   **Action**: `tests/io/test_boring_connect.zig` successfully connects to `google.com:443` on macOS and Linux ARM64.
3.  **[DONE] S3 Protocol (`test_s3_head`)**:
    *   **Goal**: Verify S3 protocol over TLS.
    *   **Action**: Successfully sent HEAD request to `s3.amazonaws.com` and received HTTP 405 (Method Not Allowed), confirming transport and protocol functionality on both platforms.

#### Phase 2: Implementation & Integration (In Progress)
1.  **Core Client**:
    *   **[DONE] `TlsConnection`**: Implemented in `src/zpq/io/tls/connection.zig`. Supports async connect, read, write, close, and EOF handling.
    *   **[DONE] `http.Client`**: Implemented in `src/zpq/io/http/client.zig`.
    *   **Verification**: `zig build --build-file micro_build.zig test-http-client` passes (fetches HEAD from S3).
    *   **[DONE] MinIO Verification**: Verified `zpq` against local MinIO with self-signed TLS.
2.  **Parquet Wiring**: Implement `readRanges` and hook into `ParquetFile`.

## 🏗️ Legacy Stack (Reference/Backup)
Located in `src/zpq/s3_legacy/`.
*   `AsyncS3Source`: Pure Zig `std.Io` + `std.crypto.tls` implementation.
*   **Status**: Works with local Mock S3 (HTTP) but flaky/broken with real S3 (HTTPS/Keep-Alive issues).
