# Pure Zig ZPQ Implementation Plan

**Goal**: A high-performance, "pure Zig" (no libc dependency where possible, static binary), AWS Lambda-optimized Parquet S3 reader.

**Status**: Planning Phase (Post-Verification of Transport Layer)

## 1. Core Philosophy: "Sans-I/O" & Explicit Control
To avoid the flakiness of the previous implementation and ensure maximum throughput:
*   **Sans-I/O Protocol**: The HTTP/S3 logic should be a pure state machine that accepts bytes and produces bytes. It should NOT own the socket.
*   **Event Loop as Driver**: `libxev` will be the engine that pumps data into the state machines.
*   **Static Linking**: Use `boring_tls` (statically linked OpenSSL via Zig) to avoid system library dependency hell on Lambda.

## 2. Architecture

### A. The Transport Layer (`zpq.io`)
*   **`EventLoop`**: Wrapper around `xev.Loop`. Handles signal safety and tick logic.
*   **`Transport`**: A unified interface for TCP and TLS connections.
    *   `read(buffer) -> number_of_bytes | WouldBlock | EOF`
    *   `write(buffer) -> number_of_bytes | WouldBlock`
*   **`TlsTransport`**: Implements `Transport`. Wraps a `TcpTransport` and a `boring_tls` state machine (BIO pattern).

### B. The Protocol Layer (`zpq.http` / `zpq.s3`)
*   **`HttpRequest`**: Struct representing a request.
*   **`HttpResponseParser`**: A state machine that parses HTTP/1.1 headers and chunked encoding.
    *   `feed(bytes) -> State (HeaderComplete, BodyChunk, Done, Error)`
*   **`S3Signer`**: Existing logic (from legacy) to sign requests.

### C. The Application Layer (`zpq.parquet`)
*   **`AsyncS3Source` (New)**:
    *   Maintains a pool of `Transport` connections.
    *   Implements `readRanges` (Parallel Fetch).
    *   **Logic**:
        1.  Take `ranges` (list of offset/length).
        2.  Acquire N connections from pool.
        3.  Pipeline HTTP GET requests (Range header).
        4.  Stream bytes directly into destination buffers via the Event Loop callbacks.
        5.  Release connections back to pool.

## 3. Implementation Steps

### Step 1: The "Transport" Component
Move the successful logic from `test_s3_head.zig` into a reusable struct.
*   **File**: `src/zpq/io/transport.zig`
*   **Struct**: `TlsConnection`
*   **Methods**:
    *   `connect(host, port) !void`
    *   `read(buf) !usize`
    *   `write(buf) !usize`
    *   `handshake() !void` (Drives the pump)

### Step 2: The HTTP State Machine
Port or write a minimal HTTP parser.
*   **Requirements**: Fast, zero-allocation (views into buffer), supports Content-Length and Chunked.
*   **Note**: Zig's `std.http` might be too coupled to `std.Io`. We might need a "sans-io" version or a thin wrapper if `std.http` allows external buffer feeding.

### Step 3: Connection Pooling
*   **Goal**: Reuse TLS connections to save handshake time (critical for S3 latency).
*   **Logic**:
    *   `pool.acquire()`: Returns ready connection or creates new.
    *   `pool.release(conn)`: Checks if connection is still alive (keep-alive) and stores it.
    *   *Challenge*: Handling "dead" idle connections. `libxev` timer can sweep them.

### Step 4: Parquet Integration
*   Replace the `RawS3Source` in `ParquetFile` with the new `AsyncS3Source`.
*   Ensure `readRanges` API matches exactly what `zpq` needs.

## 4. Open Questions / Stubbing
*   **DNS Resolution**: `libxev` doesn't do DNS. We need `std.net.getAddressList` (blocking) or a c-ares wrapper.
    *   *Lambda*: Blocking DNS is usually fine as it's fast (internal AWS DNS) and cached.
    *   *Solution*: Run `getaddrinfo` in a thread pool (Zig `std.Thread.Pool`) to avoid blocking the Event Loop.
*   **Certificates**: `boring_tls` needs a CA bundle.
    *   *Lambda*: `/etc/pki/tls/certs/ca-bundle.crt` usually exists.
    *   *Embed*: We could embed `cacert.pem` in the binary for true portability (adds ~200KB).

## 5. Verification Plan
1.  **Unit Test**: `TlsConnection` reading/writing dummy data.
2.  **Integration Test**: `AsyncS3Source` fetching 10 ranges in parallel from S3.
3.  **Benchmark**: `remote_bench.sh` running full Parquet decode on Lambda/ARM64.

