# Plan: TLS Integration (BoringTLS + Libxev)

**Goal**: Implement a production-ready HTTPS client for `zpq` using `libxev` for async I/O and `boring_tls` for encryption.

## Context & Assets
*   **`libxev`**: Patched and verified (70k RPS).
*   **`boring_tls`**: Vendored in `vendor/boring_tls`.
*   **Architecture**: "BIO Pair" pattern.
    *   `libxev` handles TCP.
    *   `boring_tls` processes memory buffers (encrypted <-> plaintext).
    *   Glue code moves bytes between them.

## Implementation Plan

### Phase 1: The BIO Adapter (`src/zpq/io/tls/bio.zig`)
1.  **Design**: Create a struct that interfaces with `boring_tls` (OpenSSL `BIO`).
    *   **Goal**: Decouple `boring_tls` from `std.Io` blocking readers.
2.  **Logic**:
    *   `BIO` in BoringSSL usually abstracts the socket. We need a "Memory BIO" or a "Custom BIO".
    *   *Approach*: Use `boring_tls.Client` which likely wraps a `Reader`/`Writer`.
    *   **The Adapter**: Create a `Context` struct that holds input/output buffers. Pass this `Context`'s reader/writer to `boring_tls.Client`.
    *   **Flow**:
        1.  `libxev` reads TCP -> fills `Context.input_buf`.
        2.  Call `boring_client.read()` -> reads from `Context.input_buf`, decrypts.
        3.  Call `boring_client.write()` -> encrypts, writes to `Context.output_buf`.
        4.  `libxev` writes `Context.output_buf` -> TCP.

### Phase 2: The Microtest (`tests/io/test_boring_connect.zig`)
1.  **Setup**: Copy `tests/io/test_xev_tcp.zig`.
2.  **Integration**:
    *   Init `xev.TCP`.
    *   Init `boring.Client` with the custom `Context` adapter.
    *   **Handshake Loop**:
        *   The handshake is the trickiest part. `boring_tls.connect()` might block.
        *   We need to handle `error.WouldBlock`.
        *   **State Machine**:
            ```zig
            while (true) {
                boring_client.handshake() catch |err| {
                    if (err == error.WouldBlock) {
                        // Check if we need to read from TCP or write to TCP?
                        // If boring_client output buffer has data -> Write to TCP.
                        // If boring_client needs input -> Read from TCP.
                        continue;
                    }
                    return err;
                };
                break; // Handshake done
            }
            ```
3.  **Target**: `google.com:443`.
4.  **Success Criteria**: Handshake completes, HTTP GET returns 200/301.

### Phase 3: The Client Struct (`src/zpq/io/http/client.zig`)
1.  **Encapsulation**: Move the loop/state-machine logic into a clean `Client` struct.
2.  **API**: `request(method, url, headers, body) -> Future/Promise`.

## Execution Steps for Agent
1.  **Analyze `vendor/boring_tls`**: Read the source to understand the specific Zig wrapper API.
2.  **Create Microtest**: `tests/io/test_boring_connect.zig`.
3.  **Implement State Machine**: Write the `WantRead`/`WantWrite` loop logic.
4.  **Verify**: Run `zig build test-io`.

## Verification
*   `zig build test-io` passes.
*   Log output shows decrypted HTTP headers from a real HTTPS server.

