# ZPQ Project Status

## High-Level Goals
*   **Performance**: Outperform PyArrow and Rust `parquet` crate (Standard Arrow Reader) in scan throughput.
*   **Safety**: Zero-copy where possible, safe memory management, robust error handling.
*   **Completeness**: Support all standard Parquet encodings and compression codecs.
*   **Portability**: Run as a standalone CLI, AWS Lambda (Zig's cross-compilation), or library.

## Philosophy & Architecture
*   **Lambda-First**: The ultimate goal is high-performance AWS Lambdas with minimal cold starts.
    *   **No Runtime SDKs**: We re-write S3, SigV4, and HTTP/1.1 natively to eliminate SDK bloat and maintain a zero-copy, zero-allocation path.
    *   **Minimal Footprint**: Reduction of binary size and memory utilization.
*   **The Spirit of Zig 0.16**: We align with upcoming 0.16 patterns today:
    *   **Unmanaged-Only**: No internal allocators in core structs (`AsyncRequest`, `ColumnReader`).
    *   **Interface-Driven**: Business logic sees only `std.Io` interfaces, never `libxev` directly.
    *   **Reproducible Build**: The project pins to a specific Zig nightly commit (see `.zig-version`).
*   **Pragmatic Library Usage**:
    *   **`libxev` & `boring_tls`**: These are viewed as high-performance "bridging" technologies. They provide the necessary OS-level primitives (io_uring/kqueue) and secure transport while being isolated for a future transition to a purely native `std` stack.
    *   **Exit Strategy**: We will migrate to `std.Io` once it reaches feature parity with `libxev` regarding completion-based I/O on Linux and macOS.
*   **Dependency Boundaries**:
    *   Core Parquet logic remains pure logic with no I/O or transport knowledge.
    *   I/O is abstracted via the `RandomAccessSource` interface.

---

## Project Milestones

### Milestone 0: Hardening & Verification [IN PROGRESS]
*   [x] **Philosophy**: Documented "Golden Rules" and "Sans-I/O" patterns.
*   [x] **Zig 0.16.dev**: Compiler pinned (`.zig-version`), `std.meta.Int` fixes applied.
*   [x] **Safety**: TLS verification enabled by default.
*   [x] **Flow Control**: Implemented backpressure (write queue) in `Connection`.
*   [x] **Verification**: Zero-allocation gap skipping formally proved (`tests/io/test_gap_skipping.zig`).
*   [x] **Property-Based Testing (Minish)**:
    *   [x] **Adoption**: Vendored and patched `minish` for Zig 0.16.
    *   [x] **Context Injection**: Enabled allocator-aware property testing.
    *   [x] **Thrift Fuzzer**: Verified metadata parser robustness (Round-Trip).
    *   [x] **HTTP Fuzzer**: Fixed zero-body hang bug discovered by Minish.
*   [x] **DNS Stack**: Verified deduplication property and non-blocking integration.
*   [ ] **Lambda E2E**: Preparing end-to-end validation suite.

### Milestone 1: Core Parquet Engine & Encodings [COMPLETE]
*   [x] **Core Reading**: Thrift metadata parsing, Page iteration, Column chunk handling.
*   [x] **Encodings**:
    *   [x] `PLAIN`
    *   [x] `RLE` (Run-Length Encoding)
    *   [x] `BIT_PACKED` (Deprecated but supported)
    *   [x] `RLE_DICTIONARY` / `PLAIN_DICTIONARY`
*   [x] **Decompression**:
    *   [x] **Snappy**: Native Zig implementation.
*   [x] **Complex Features**:
    *   [x] **Definition Levels**: NULL value handling via RLE decoding.
    *   [x] **Dictionary Resolution**: Reconstructing values from dictionary pages.

### Milestone 2: CLI Tools & Initial Benchmarking [COMPLETE]
*   [x] **CLI Tools**: `schema`, `meta`, `pages`, `cat`, `scan`, `debug-s3`.
*   [x] **Throughput**: Verified ~911 MB/s (ZPQ) vs ~370 MB/s (PyArrow) vs ~265 MB/s (Rust Arrow) on M1 Max.
*   [x] **Validation**: Verified against `parquet-read` and `pyarrow`.

### Milestone 3: Async Foundation (libxev + boring_tls) [COMPLETE]
*   [x] **Evented I/O**:
    *   [x] **Micro-test**: `test_event_loop.zig` (proving kqueue/epoll functionality).
    *   [x] **Integration**: Integrated `EventLoop` into `AsyncS3Source` for parallel range fetches.
    *   [x] **Scaling**: Verified ~1.3x faster TCP echo than Node.js.
*   [x] **TLS Integration**:
    *   [x] Implemented `TlsAdapter` using `boring_tls` (OpenSSL).
    *   [x] Verified secure handshakes on macOS M1 and Linux ARM64.
    *   [x] Patched `boring_tls` build for ARM architecture compatibility.

### Milestone 4: Transport Verification (MinIO TLS & Coalescing) [COMPLETE]
*   [x] **MinIO TLS Proof**: Verified byte-exact Range GET against local MinIO.
    *   [x] TLS handshake and encrypted reads/writes verified with `libxev`.
    *   [x] Manual HTTP/1.1 request/response framing implementation.
*   [x] **Zig Native Optimizations**:
    *   [x] **Zero-Allocation Gap**: Skipping bytes at the socket level without allocation.
    *   [x] **No-HEAD Open**: Suffix range parsing from mock server.
*   [x] **Coalescing**: Polars-style range merging and splitting in `scheduler.zig`.

### Milestone 5: S3 Architecture Consolidation (Zig 0.16 Alignment) [COMPLETE]
*   [x] **Architecture**: Consolidated S3 implementations (Sync & Async) into `src/zpq/io/s3/`.
*   [x] **I/O Abstraction**: 
    *   [x] Refactored `ParquetFile` to utilize the `RandomAccessSource` interface.
    *   [x] Implemented `LocalFileSource` and `AsyncS3Source`.
*   [x] **AWS SigV4**:
    *   [x] Implemented zero-dependency signer in `sigv4.zig`.
    *   [x] **Hot Path Optimization**: 
        *   [x] Zero-heap hot path using `stackFallback` allocator.
        *   [x] Pre-parsed `std.Uri` to minimize redundant parsing.
        *   [x] Constant-time hash for empty payloads.
        *   [x] **Speculative Read**: Optimized `readFooter` to fetch trailing 64KB in a single request.
*   [x] **Zig 0.16.dev Migration**:
    *   [x] Updated for `std.Io.Writer`, `std.time`, and `std.ArrayList` breaking changes.
    *   [x] **Unmanaged Pattern**: Adopted unmanaged containers for `AsyncRequest`, `ColumnReader`, and `Page`.
*   [x] **CLI**: Introduced `--async` flag for engine selection.

### Milestone 6: High-Performance Async DNS [COMPLETE]
*   [x] **Interface Design**: Defined `Resolver` interface with `Completion` and `VTable`.
*   [x] **Tier 1: Stable**: Implemented `ThreadPoolResolver` using `libxev.ThreadPool`.
*   [x] **Middleware: Single-Flight**: Implemented `SingleFlightResolver` for lookup deduplication.
*   [x] **Tier 2: Speculative**: Implemented `SpeculativeResolver` for IPv4/IPv6 racing.
*   [x] **Verification**: `tests/io/test_dns.zig` successfully verified deduplication and racing.
*   [x] **Justification**: Verified deduplication property (50 parallel requests satisfied by 1 OS call) via `tests/io/bench_dns.zig`.

### Milestone 7: Pure Async Lifecycle [COMPLETE]
*   [x] **Transport Abstraction**: Implemented `Connection` (TCP/TLS) using a non-blocking "Pump" pattern.
*   [x] **Pure Async I/O**: Refactored `AsyncRequest` to use `libxev` completions and callbacks.
*   [x] **DNS Integration**: Wired `dns.Resolver` into `AsyncS3Source` with round-robin IP distribution.
*   [x] **Reliability**: Resolved `io_uring` issues by pinning the `EventLoop` in memory.

### Milestone 8: Persistent Pool & Repetition Levels [IN PROGRESS]
*   [ ] **Enhanced Connection Pool**: Implementation of keep-alive timeouts and stale detection.
*   [ ] **Repetition Levels**: Support for Lists and Maps (Nested structures).
*   [ ] **Advanced Range Coalescing**: Dynamic merging based on latency/throughput profiling.

---

## High-Performance HTTP Plan
ZPQ's S3 support utilizes a purpose-built “Bare Metal” HTTP/1.1 client specialized for S3 (`HEAD` + `GET Range`). Key characteristics include:
- Explicit socket ownership.
- Deterministic connection reuse.
- Event-driven concurrency via `libxev`.

### Reference Material
We utilize the following references for architectural validation:
- **Event loop**: `references/bun/src/deps/uws/Loop.zig`
- **HTTP thread ownership**: `references/bun/src/http/HTTPThread.zig`
- **Keep-alive semantics**: `references/bun/src/http/HTTPContext.zig`
- **Request execution**: `references/bun/src/http/AsyncHTTP.zig`
- **S3 implementation**: `references/bun/src/s3/client.zig`

---

## Benchmarks (Local File - M3 Max)
| Implementation | Time (s) | Throughput (File) | Throughput (Values) |
| :--- | :--- | :--- | :--- |
| **ZPQ (Zig)** | **0.16s** | **~842 MB/s** | **~857 MVal/s** |
| PyArrow (Python) | 0.29s | ~372 MB/s | N/A |
| Rust (Arrow) | 0.65s | ~160 MB/s | N/A |
| Rust (CLI) | 20.14s | ~5 MB/s | N/A |

## Benchmarks (S3 Metadata - Cloud Latency)
| Implementation | Time (s) | Notes |
| :--- | :--- | :--- |
| **PyArrow (C++)** | ~0.15s | Native C++ S3FS with persistent connection pool. |
| **Polars (Rust)** | ~0.40s | Rust `object_store` via `reqwest`. |
| **ZPQ (Zig)** | ~0.58s | Initial implementation; connection reuse optimizations pending in Milestone 8. |

## Lambda Benchmark (Internal Loop Proof-of-Concept)
*Note: These metrics represent an internal ping-pong loop of the `libxev` event loop on Lambda hardware to verify scaling. They do not yet reflect a real-world S3 scan (see Milestone 0 / Plan 09).*
Verified scalability of the async engine (`libxev` + `epoll`) on AWS Lambda (ARM64):
| Memory | RPS (Approx) | Scaling Factor | Notes |
| :--- | :--- | :--- | :--- |
| 128 MB | ~1,573 | 1.0x | CPU limited. |
| 1024 MB | ~15,355 | 9.7x | Strong performance baseline. |
| **2048 MB** | **~30,547** | **19.4x** | **Optimal linear scaling.** |
| 4096 MB | ~45,065 | 28.6x | Diminishing returns observed. |
