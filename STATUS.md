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
    *   Transport implementation (`src/zpq/s3/`) is allowed to be "dirty" with HTTP/Auth logic but must remain dependency-free.

## ✅ Current Progress (Phase 3: Optimization & Benchmarking)
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
*   [x] **CLI Tools**:
    *   [x] `schema`: View file structure.
    *   [x] `meta`: View row group/compression stats.
    *   [x] `pages`: Deep inspection of page headers/stats.
    *   [x] `cat`: Dump values (partial CSV-like).
    *   [x] `scan`: High-performance throughput benchmark.
    *   [x] `debug-s3`: Micro-benchmark for S3 latency and connection reuse.
    *   [ ] **Optimization**: See `docs/high_performance_io_plan.md` for the roadmap to "Bare Metal" S3 performance (Connection Reuse, Custom Client).
*   [x] **Benchmarking**:
    *   [x] **Throughput**: ~911 MB/s (ZPQ) vs ~370 MB/s (PyArrow) vs ~265 MB/s (Rust Arrow) on M1 Max.
    *   [x] **Validation**: Verified against `parquet-read` and `pyarrow`.

## 🧭 High-Performance HTTP Plan (Connection Reuse + Evented I/O)
ZPQ’s current S3 support is functional, but `std.http.Client` has known limitations with aggressive keep-alive reuse in Zig 0.16 dev.
We are moving to a purpose-built “Bare Metal” HTTP/1.1 client specialized for S3 (`HEAD` + `GET Range`) that:
- Owns sockets explicitly (no hidden state machine).
- Reuses connections deterministically (keep-alive + pooling).
- Evolves from blocking correctness → evented kqueue/epoll for concurrency.

### 📚 Reference Map (Bun)
We keep Bun as a “how the pros do it” reference, but only need a small subset:
- **Event loop (kqueue/epoll abstraction)**: `references/bun/src/deps/uws/Loop.zig`
- **HTTP client thread ownership + lifecycle**: `references/bun/src/http/HTTPThread.zig`
- **Keep-alive pooling + release semantics**: `references/bun/src/http/HTTPContext.zig` (see `pending_sockets` + `releaseSocket(...)`)
- **Request execution plumbing**: `references/bun/src/http/AsyncHTTP.zig`
- **S3 usage path**: `references/bun/src/s3/client.zig` (S3 ops → `bun.http.AsyncHTTP.init(...)`)

### 🎯 Next Milestones (ZPQ)
- [x] **(A) Deterministic correctness (local mock)**: keep `tools/mock_s3_server.zig` as the harness; keep tests killable via `tools/no_output_timeout.py`.
- [x] **(B) Raw keep-alive reuse**: make `RawS3Source` reuse a single socket for multiple range requests (no reconnect per read).
- [x] **(C) Connection pool**: keyed by `(scheme, host, port, tls-config)` with idle timeout + stale detection.
- [x] **(D) Evented I/O**:
    - [x] **Micro-test**: `test_event_loop.zig` (prove kqueue works).
    - [x] **Integration**: Integrate `EventLoop` into `RawS3Source` (or `AsyncS3Source`) to drive parallel range fetches.
    - [x] **Coalescing**: Implement Polars-style range merging/splitting.
- [x] **(E) Zig Superpowers (Micro-Tests)**:
    - [x] **Zero-Allocation Gap**: Test skipping bytes on the socket without allocation.
    - [x] **No-HEAD Open**: Test suffix range parsing from mock server.
    - [ ] **Arena Decompression**: (Phase 3) Benchmark arena vs generic allocator for heavy columnar allocs.
- [x] **(F) TLS**: add HTTPS + session reuse for real S3 (Implemented TlsAdapter with std.crypto.tls, Verified against Cloudflare 1.1.1.1).
- [x] **(G) Parquet Integration**: Update `ParquetFile` to use `AsyncS3Source` for parallel column fetching.

## 🚧 Upcoming (Phase 4: Cloud & Modernization)
*   [x] **I/O Abstraction**:
    *   [x] Refactor `ParquetFile` to use `RandomAccessSource` interface.
    *   [x] Implement `LocalFileSource`.
    *   [x] Implement `S3Source` (HTTP Range Requests). *Currently supports public/anonymous buckets via `std.http`.*
    *   [x] **Dependency boundary & Architecture**:
        *   [x] **ZPQ Parquet core stays “pure”**: No external libraries for core logic.
        *   [x] **Transport/Auth**: Implemented `S3Source` using `std.http` (Pure Zig) to avoid broken external dependencies.
        *   [x] **Standardization**: S3 support is enabled by default (zero extra deps).
*   [x] **Cloud Auth & Features**:
    *   [x] **AWS SigV4 Authentication**:
        *   [x] Implemented "Clean Room" SigV4 signer in `src/zpq/s3/sigv4.zig`.
        *   [x] Zero external dependencies (uses `std.crypto`, `std.http`).
        *   [x] **Verified against real S3**: Successfully reading Parquet schemas from private S3 buckets.
        *   [x] Solved "Duplicate Host Header" issue (400 Bad Request) and "Heap Allocation" segfaults.
        *   [x] **Refactored**: Cleaned up `S3Source` with `S3Config` struct and centralized request logic.
        *   [x] **Optimized**: 
            *   [x] Zero-heap hot path using `stackFallback` allocator for requests.
            *   [x] Pre-parsed `std.Uri` to avoid redundant parsing per chunk.
            *   [x] Constant-time hash for empty payloads.
            *   [x] **Speculative Read**: Optimized `readFooter` to fetch last 64KB in one request (reduced S3 round-trips from 3 to 2).
        *   [x] Updated for **Zig 0.16.0-dev** (std.Io.Writer, std.time, std.ArrayList breaking changes).
    *   [ ] **Connection Reuse**: Replace `std.http.Client` with `RawS3Source` + explicit keep-alive pooling (see plan above).
    *   [ ] Async/Parallel Range Fetching (Read-ahead).
*   [ ] **Nested Types**:
    *   [ ] Repetition Levels (Lists/Maps).
*   [ ] **Modern Encodings**:
    *   [ ] `DELTA_BINARY_PACKED`.
    *   [ ] `BYTE_STREAM_SPLIT`.

## 📉 Benchmarks (Local File - M3 Max, before optimizations)
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

*Note: ZPQ is currently ~1.8x faster than PyArrow and ~4x faster than Rust (safe Arrow reader) for raw scanning on local files. S3 throughput optimization is in progress.*
