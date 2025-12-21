# ZPQ Technical Context & Deep Dive

**Last Updated**: Dec 18, 2025
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
    *   **[DONE] MinIO Range GET (Byte-Exact) Over TLS**:
        *   **What we proved**:
            *   TLS handshake + encrypted reads/writes using `libxev` + `boring_tls`.
            *   HTTP/1.1 request/response over that TLS connection.
            *   `Range: bytes=a-b` returns **exact expected bytes** for a known binary fixture.
        *   **What we did NOT test**: **No Parquet parsing yet** (this is transport correctness only).
        *   **Command**:
            *   `python3 tools/no_output_timeout.py --idle-seconds 60 tools/minio_tls/setup_fixture.sh`
            *   `python3 tools/no_output_timeout.py --idle-seconds 10 zig build -Dexperimental test-minio-range-get`
        *   **Key implementation details**:
            *   Fixture embedding is via `ci/fixtures/minio/fixtures.zig` (module) to satisfy Zig’s `@embedFile` package-path restriction.
            *   `build.zig` wires that in as `minio_fixtures` for `test_minio_range_get`.
    *   **[DONE] Build Hygiene**: Added `just cross-check-experimental` to verify Linux compilation locally. Fixed Linux CI by isolating macOS-only `EventLoop`.
2.  **Parquet Wiring**: Implement `readRanges` and hook into `ParquetFile`.

## 🧠 Next-Session Insights (Keep Us Sane)
*   **Docker + idle timeouts**: `docker-compose up/down` can be “quiet” for >10s while doing real work. Use a longer idle timeout for these steps (or ensure scripts print progress).
*   **Always verify HTTPS health**: When MinIO certs are missing, it silently falls back to HTTP. We now generate certs automatically and poll `https://localhost:9000/minio/health/live` before running tests.
*   **Self-signed TLS warning is expected**: `Certificate verification failed: 18` is fine for local MinIO (we run `--insecure` intentionally). Don’t “fix” it unless we’re testing trust stores.
*   **Transport-first milestone is real value**: Range GET correctness is the core primitive for Parquet-on-S3 (footer + column chunk reads). Next work should focus on formalizing an S3 transport API and then wiring it into `RandomAccessSource` for Parquet.

### Next: S3 Transport Skeleton (Execution Checklist)
*   **API shape**: `Transport.head(host, ip, port, path, headers) -> ResponseMeta`
*   **API shape**: `Transport.getRange(host, ip, port, path, range) -> []u8` (or caller-provided buffer)
*   **Connection lifecycle**: explicit close; keep-alive later (start with `Connection: close` correctness).
*   **HTTP parsing**: use `ResponseParser` for status/headers/body; ensure 206 path is solid.
*   **MinIO as harness**:
    *   Continue using `tools/minio_tls/setup_fixture.sh` + `test-minio-range-get` as the “golden transport test”.
    *   Add 1 failing-case test next: missing object → 404, and ensure error path is deterministic.

### S3 Implementation Status
- **Sync Stack (`zpq.s3.S3Source`)**: Fully functional with SigV4 support. Uses `std.http.Client`.
- **Async Stack (`zpq.s3.AsyncS3Source`)**: 
    - Event loop based on `libxev`.
    - TLS supported via `boring_tls`.
    - **NEW**: SigV4 signing integrated into `AsyncRequest`.
    - **NEW**: Leaner unmanaged container architecture (Zig 0.16.dev compliant).
    - **NEW**: Consolidated into `src/zpq/s3/`.
- **Next Step**: Implement Async DNS resolution to remove hardcoded IPs.

## 🏗️ Legacy Stack (Reference/Backup)
Removed. All core logic migrated to `src/zpq/s3/`.
