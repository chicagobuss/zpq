# ZPQ Project Status

## 🚀 High-Level Goals
*   **Performance**: Outperform PyArrow and Rust `parquet` crate (Standard Arrow Reader) in scan throughput.
*   **Safety**: Zero-copy where possible, safe memory management, robust error handling.
*   **Completeness**: Support all standard Parquet encodings and compression codecs.
*   **Portability**: Run as a standalone CLI, AWS Lambda (Zig's cross-compilation), or library.

## 🧠 Philosophy & Architecture
*   **Lambda-First**: The ultimate goal is blazing fast Lambdas with minimal cold starts.
    *   **Zero-Dependency**: No generic "kitchen sink" SDKs (e.g. `aws-sdk-for-zig`). We only implement the exact S3 `GET`/`HEAD` logic we need.
    *   **Tiny Footprint**: Minimize binary size and memory usage.
*   **Performance > Compliance**:
    *   We favor Zig-native performance optimizations (zero-allocation, comptime) over matching "standard" SDK implementation patterns.
    *   Auth support is "pragmatic": we support environment variables and basic profiles (what Lambda needs), ignoring complex flows (SSO, MFA) unless critical.
*   **Dependency Boundary**:
    *   Core Parquet logic (`src/zpq/decoder.zig`, etc.) must remain pure logic with NO I/O or transport knowledge.
    *   I/O is abstracted via `RandomAccessSource`.
    *   Transport implementation (`src/zpq/io/s3/`) is allowed to be "dirty" with HTTP/Auth logic but must remain dependency-free.

---

## 🏆 Project Milestones

### ✅ Milestone 1: Core Parquet Engine & Encodings
*   [x] **Core Reading**: Thrift metadata parsing, Page iteration, Column chunk handling.
*   [x] **Encodings**:
    *   [x] `PLAIN`
    *   [x] `RLE` (Run-Length Encoding)
    *   [x] `BIT_PACKED` (Deprecated but present)
    *   [x] `RLE_DICTIONARY` / `PLAIN_DICTIONARY`
*   [x] **Decompression**:
    *   [x] **Snappy**: Native Zig implementation (verified & benchmarked).
*   [x] **Complex Features**:
    *   [x] **Definition Levels**: Handling NULL values via RLE decoding.
    *   [x] **Dictionary Resolution**: Reconstructing values from dictionary pages.

### ✅ Milestone 2: CLI Tools & Initial Benchmarking
*   [x] **CLI Tools**: `schema`, `meta`, `pages`, `cat`, `scan`, `debug-s3`.
*   [x] **Throughput**: ~911 MB/s (ZPQ) vs ~370 MB/s (PyArrow) vs ~265 MB/s (Rust Arrow) on M1 Max.
*   [x] **Validation**: Verified against `parquet-read` and `pyarrow`.

### ✅ Milestone 3: Async Foundation (`libxev` + `boring_tls`)
*   [x] **Evented I/O**:
    *   [x] **Micro-test**: `test_event_loop.zig` (prove kqueue/epoll works).
    *   [x] **Integration**: Integrated `EventLoop` into `AsyncS3Source` to drive parallel range fetches.
    *   [x] **Scaling**: Verified ~1.3x faster TCP echo than Node.js.
*   [x] **TLS Integration**:
    *   [x] Implemented `TlsAdapter` using `boring_tls` (OpenSSL).
    *   [x] Verified secure handshakes on macOS M1 and Linux ARM64.
    *   [x] Patched `boring_tls` build for ARM architecture compatibility.

### ✅ Milestone 4: Transport Verification (MinIO TLS & Coalescing)
*   [x] **MinIO TLS Proof**: Verified byte-exact Range GET against local MinIO.
    *   [x] Proved TLS handshake + encrypted reads/writes work with `libxev`.
    *   [x] HTTP/1.1 request/response framing (manual implementation).
*   [x] **Zig Superpowers**:
    *   [x] **Zero-Allocation Gap**: Skipping bytes on the socket without allocation.
    *   [x] **No-HEAD Open**: Suffix range parsing from mock server.
*   [x] **Coalescing**: Polars-style range merging/splitting implemented in `scheduler.zig`.

### ✅ Milestone 5: S3 Architecture Consolidation (Zig 0.16 Alignment)
*   [x] **Architecture**: Consolidated all S3 code (Sync & Async) into `src/zpq/io/s3/`.
*   [x] **I/O Abstraction**: 
    *   [x] Refactor `ParquetFile` to use `RandomAccessSource` interface.
    *   [x] Implement `LocalFileSource` and `AsyncS3Source`.
*   [x] **AWS SigV4**:
    *   [x] Implemented "Clean Room" zero-dependency signer in `sigv4.zig`.
    *   [x] **Optimized Hot Path**: 
        *   [x] Zero-heap hot path using `stackFallback` allocator for requests.
        *   [x] Pre-parsed `std.Uri` to avoid redundant parsing per chunk.
        *   [x] Constant-time hash for empty payloads.
        *   [x] **Speculative Read**: Optimized `readFooter` to fetch last 64KB in one request (round-trips 3 -> 2).
*   [x] **Zig 0.16.dev Ready**:
    *   [x] Updated for `std.Io.Writer`, `std.time`, and `std.ArrayList` breaking changes.
    *   [x] **Unmanaged pattern**: `AsyncRequest`, `ColumnReader`, and `Page` no longer store allocators.
*   [x] **CLI**: Added `--async` flag for engine switching.

### 🚀 Milestone 6: High-Performance Async DNS (CURRENT)
*   [ ] **Speculative resolution**: Race IPv4 vs IPv6 to launch connections earlier.
*   [ ] **Threshold Trigger (N-Lane)**: Uncork the pipeline as soon as target IP count is met.
*   [ ] **Single-Flight Broadcast**: Prevent redundant lookups for the same bucket across 100+ parallel requests.
*   See `@STATUS_CURRENT_DETAIL.md` for the technical blueprint.

### 🎯 Milestone 7+: Future Roadmap
*   [ ] **Persistent Connection Pool**: Keyed by `(scheme, host, port)` with idle timeout and stale detection.
*   [ ] **Nested Types**: Support for Lists and Maps (Repetition Levels).
*   [ ] **Modern Encodings**: `DELTA_BINARY_PACKED` and `BYTE_STREAM_SPLIT`.
*   [ ] **Arena Decompression**: Benchmark arena vs generic allocator for heavy columnar throughput.

---

## 🧭 High-Performance HTTP Plan
ZPQ’s current S3 support is functional, but `std.http.Client` has known limitations with aggressive keep-alive reuse in Zig 0.16 dev.
We are moving to a purpose-built “Bare Metal” HTTP/1.1 client specialized for S3 (`HEAD` + `GET Range`) that:
- Owns sockets explicitly (no hidden state machine).
- Reuses connections deterministically (keep-alive + pooling).
- Evolves from blocking correctness → evented kqueue/epoll for concurrency.

### 📚 Reference Map (Bun)
We keep Bun as a “how the pros do it” reference for the async stack:
- **Event loop (kqueue/epoll abstraction)**: `references/bun/src/deps/uws/Loop.zig`
- **HTTP client thread ownership + lifecycle**: `references/bun/src/http/HTTPThread.zig`
- **Keep-alive pooling + release semantics**: `references/bun/src/http/HTTPContext.zig`
- **Request execution plumbing**: `references/bun/src/http/AsyncHTTP.zig`
- **S3 usage path**: `references/bun/src/s3/client.zig`

---

## 📉 Benchmarks (Local File - M3 Max)
| Implementation | Time (s) | Throughput (File) | Throughput (Values) |
| :--- | :--- | :--- | :--- |
| **ZPQ (Zig)** | **0.16s** | **~842 MB/s** | **~857 MVal/s** |
| PyArrow (Python) | 0.29s | ~372 MB/s | N/A |
| Rust (Arrow) | 0.65s | ~160 MB/s | N/A |
| Rust (CLI) | 20.14s | ~5 MB/s | N/A |

## 📉 Benchmarks (S3 Metadata - Cloud Latency)
| Implementation | Time (s) | Notes |
| :--- | :--- | :--- |
| **PyArrow (C++)** | ~0.15s | Native C++ S3FS with persistent connection pool. |
| **Polars (Rust)** | ~0.40s | Rust `object_store` via `reqwest`. |
| **ZPQ (Zig)** | ~0.58s | 2 Round-trips (Init HEAD + Speculative Footer). Functional but limited by connection reuse. |

## ⚡️ Lambda Benchmark (Internal Loop)
To verify the scalability of our async engine (`libxev` + `epoll`) on AWS Lambda (ARM64):
| Memory | RPS (Approx) | Scaling Factor | Notes |
| :--- | :--- | :--- | :--- |
| 128 MB | ~1,573 | 1.0x | CPU limited / noisy neighbor prone. |
| 1024 MB | ~15,355 | 9.7x | Strong baseline. ~10x speedup from 128MB. |
| **2048 MB** | **~30,547** | **19.4x** | **Ideal linear scaling relative to 128MB.** |
| 4096 MB | ~45,065 | 28.6x | High performance, diminishing returns show. |
