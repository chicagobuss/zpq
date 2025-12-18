# Execution Plan: TLS Integration (BoringTLS + Libxev)
**Date**: Dec 17, 2025
**Goal**: Establish a working HTTPS connection to `google.com` using `libxev` and `boring_tls`.

## 1. Analysis & Preparation
*   [ ] **Inspect `boring_tls` API**:
    *   Read `vendor/boring_tls/src/client.zig`.
    *   Identify the exact interface it expects (likely `std.io.Reader`/`Writer` or a custom Context).
    *   Determine how to handle `WantRead`/`WantWrite` errors (non-blocking mode).

## 2. The Adapter (`src/zpq/io/tls/bio.zig`)
*   [ ] **Create `BioBuffer` struct**:
    *   `input: std.ArrayList(u8)` (Bytes received from TCP, waiting for TLS to read).
    *   `output: std.ArrayList(u8)` (Bytes encrypted by TLS, waiting to be sent to TCP).
*   [ ] **Implement `Reader`/`Writer` interfaces**:
    *   `read()`: Pulls from `input`. Returns `error.WouldBlock` if empty.
    *   `write()`: Appends to `output`.

## 3. The Microtest (`tests/io/test_boring_connect.zig`)
*   [ ] **Setup**:
    *   Initialize `xev.Loop`.
    *   Initialize `xev.TCP` connected to `142.250.xxx.xxx:443` (Google).
    *   Initialize `boring.Client` wrapping our `BioBuffer`.
*   [ ] **The Pump Loop**:
    *   **State Machine**:
        ```zig
        enum State { Handshake, Sending, Receiving, Done }
        ```
    *   **Logic**:
        *   Try `client.handshake()`.
        *   If `WouldBlock`:
            *   Check `BioBuffer.output`. If data exists -> `xev.TCP.write`.
            *   If no output data -> `xev.TCP.read` (append to `BioBuffer.input`).
        *   Repeat until Handshake complete.
*   [ ] **Verification**:
    *   Send: `GET / HTTP/1.1\r\nHost: google.com\r\n\r\n`
    *   Receive: `HTTP/1.1 200 OK` (encrypted -> decrypted).

## 4. Graduation
*   [ ] **Refactor**: Once the loop works, move the "Pump" logic into `src/zpq/io/tls/client.zig`.
*   [ ] **Interface**: Ensure it exposes `std.Io.Reader`/`Writer` so it can be injected into `S3Source`.

