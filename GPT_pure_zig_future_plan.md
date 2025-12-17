# Plan: The "Right" Pure-Zig Implementation

**Goal**: Design the ultimate, high-performance, pure-Zig architecture for `zpq` that removes C dependencies (`libxev`, `boring_tls`) and leverages Zig 0.16's async/native capabilities to the fullest.

## Context & Assets
*   **Zig 0.16 `std.Io`**: Evolving, async-native (IoUring/Kqueue).
*   **References**:
    *   `tls13-zig`: Pure Zig TLS 1.3 implementation (MIT).
    *   `zig-s3`: Existing AWS Sign v4 logic.
    *   `bun`: High-perf HTTP reference.
*   **Ambition**: "Soup-to-Nuts" - minimal deps, maximum control/perf.

## Architecture Design

### 1. The Runtime (Native `io_uring`/`kqueue` Wrapper)
*   **Decision**: Bypass `std.Io` abstraction layer overhead and build a thin, S3-optimized wrapper around `std.os.linux.io_uring` (Linux) and `std.os.kqueue` (macOS).
*   **Why**:
    *   `libxev` proves this approach works (70k RPS).
    *   We can "vendor" the best parts of `libxev` into a `src/zpq/core/loop.zig` tailored for our needs (e.g., specialized for large buffer reads, no generic overhead).
    *   Allows "Zero-Copy" read pipeline: Socket -> Ring Buffer -> Parquet Decoder (mapped memory).

### 2. TLS Strategy (Async `tls13-zig`)
*   **The Problem**: `tls13-zig` is designed for synchronous, blocking `Reader`/`Writer`.
*   **The Solution**: **Refactor to State Machine**.
    *   **Goal**: Make `tls13-zig` resumable.
    *   **Mechanism**:
        *   Modify `connect()` and `handshake()` to return `enum { Done, WantRead, WantWrite }`.
        *   Preserve state (cursor, buffer indices) in the `Client` struct between calls.
    *   **Benefit**: Seamless integration with the Async Loop defined in (1).
    *   **Zero-Copy Potential**: Decrypt directly into the target buffer if possible (AEAD in-place).

### 3. HTTP/2 & S3
*   **S3 Specifics**:
    *   Use **HTTP/1.1 Keep-Alive**. It's simpler and sufficient for high-throughput streaming of large objects.
    *   **Connection Pooling**: Maintain a pool of warm connections to S3 endpoints.
*   **Authentication**:
    *   Port `zig-s3` logic.
    *   Ensure efficient canonicalization (minimize allocation).

### Execution Steps for Agent (Stubbing Phase)
**Note: Do not start this until `libxev` + `boring_tls` (Plan 2) is stable.**

1.  **Scaffold Directory**: `src/zpq/future/`.
2.  **Define Interfaces**:
    *   `src/zpq/future/loop.zig`: The abstract loop interface.
    *   `src/zpq/future/tls.zig`: The state machine interface.
3.  **Draft the TLS Shim**:
    *   Create a `MockTLS` struct that mimics `tls13-zig` but with the proposed "Resumable" API.
    *   Demonstrate how it would plug into an `io_uring` loop.
4.  **Draft the S3 Client**:
    *   `S3Client` struct that uses the `MockTLS`.

## Verification
*   **Deliverable**: A compilable prototype in `src/zpq/future/` that mocks the actual crypto/IO but validates the *structure* and *state machine flow*.
*   **Review**: Validate that the architecture allows for zero-copy data paths from socket to application.

