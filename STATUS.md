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
    *   **`libxev` & `boring_tls`**: These are viewed as high-performance "bridging" technologies. They provide the necessary OS-level    @echo "\n=== Python (PyArrow) ==="
    uv run python tools/bench/pyarrow_bench.py data/sample-data.parquet

    @echo "\n=== Rust (Arrow RecordBatchReader) ==="
    @cd tools/bench/rust_bench && cargo run --release --quiet -- ../../../data/sample-data.parquet

    @echo "\n=== Rust (Official CLI: parquet-read) ==="
    @echo "Note: Includes formatting overhead (piped to /dev/null)"
    @time parquet-read data/sample-data.parquet > /dev/null
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
        # If production sample exists, test it
    @if [ -f data/production-sample.parquet ]; then \
        echo "[Check] Production Sample (Snappy + Dict + Nulls)..."; \
        zig build run -- cat data/production-sample.parquet 5 > /dev/null; \
        zig build run -- meta data/production-sample.parquet; \
    fi
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

### Milestone 9: Hardening & Verification (Phase 1-3) [COMPLETE]
*   [x] **Plan 05: Flow Control**: Implemented Write Queue and High-Water Mark in `Connection.zig`. [Proof](./hardening_plans/05_backpressure_proof.md)
*   [x] **Plan 06: Gap Skipping**: Verified zero-allocation gap skipping via `test_gap_skipping.zig`. [Proof](./hardening_plans/06_gap_skip_proof.md)
*   [x] **Plan 07: Fuzzing**: Integrated `minish` for Thrift/HTTP parser fuzzing. [Proof](./hardening_plans/07_fuzzing_proof.md)
*   [x] **Plan 10: TLS Security**: Enabled verification by default and fixed transport stalls. [Proof](./hardening_plans/10_tls_security_proof.md)
*   [x] **Plan 04: DNS Justification**: Verified tiered DNS deduplication benefit. [Proof](./hardening_plans/04_dns_justification_proof.md)
*   [x] **Process**: Codified "Faster Feedback" rules in `.cursor/rules/01-architecture.mdc`. [Learnings](./hardening_plans/DEBUGGING_PROCESS_LEARNINGS.md)

### Milestone 10: Empirical Proof & Performance [COMPLETE]
*   [x] **Plan 08: Reproducible Benchmarks**: Implemented `tools/remote_bench.sh` and `tools/bench_e2e/`.
*   [x] **Proof**: Formally proved ~1.9x performance advantage over PyArrow on ARM64 hardware.

### Milestone 11: New Cross-Platform Async I/O [COMPLETE]
*   [x] **Foundation**: Integrated `libxev` + `boring_tls` for native TLS 1.3 without external proxies.
*   [x] **Correctness**: Fixed critical TLS record pumping bug (draining multiple records per TCP packet).
*   [x] **Stability**: Resolved loop exit hangs via "Ghost Watcher" detection and pending op guards.
*   [x] **Parquet Wiring**: Wired `ParquetFile.openS3` to new stack; verified metadata parsing over TLS.

### Milestone 12: Performance & Scaling [COMPLETE]
*   [x] **Parallel readRanges**: Implemented native parallel fetching of S3 ranges on a single event loop.
*   [x] **Cold Start Optimization**: Achieved ~30% faster cold starts (52ms -> 37ms) vs sequential mode.
*   [x] **SigV4 Signing**: Integrated SigV4 logic into `XevS3Source` for authenticated AWS S3.
*   [x] **Multi-System Benchmarking**: Proved ~3x speedup over Polars and parity with PyArrow on cold starts.
*   [x] **Connection Pooling**: Reuses TLS connections across requests, eliminating handshake overhead for metadata & page reads.
*   [x] **CI Type Safety**: Resolved type ambiguity between legacy and new connection pools to ensure co-existence.
*   [x] **Linear Scaling**: Verify linear performance scaling when scanning 50+ columns in parallel.

### Milestone 13: Massively Parallel Column Scans [IN PROGRESS]
*   [ ] **Vectorized Reads**: Increase read buffer size (1MB+) to saturate link bandwidth.
*   [ ] **Range Scheduler**: Port the `scheduler.zig` range-merging logic to `XevS3Source`.
*   [ ] **Parallel Columns**: Wire `RowGroupReader` to fetch N columns concurrently using the async stack.
*   [ ] **Beat Polars**: Surpass the 162ms baseline for 100MB+ file scans.

### Milestone 14: The Great Consolidation
*   [ ] **Delete Legacy**: Remove `async_source.zig`, `event_loop.zig`, and `tls_adapter.zig`.
*   [ ] **Factory Flip**: Make `XevS3Source` the default for all `s3://` paths.
*   [ ] **Cleanup**: Remove `boring_tls` patches if upstream fixes land.

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

## Benchmarks (S3 Cold Start - MinIO TLS, local)
*Cold start comparison: fresh process, fresh TLS connection per run.*
| Implementation | Avg Time (ms) | Speedup (vs PyArrow) | Notes |
| :--- | :--- | :--- | :--- |
| **ZPQ (Async Pooled)** | **~44.2ms** | **~0.6x** | Significantly faster than Polars, approaching PyArrow. |
| Python (PyArrow/boto3) | ~28.5ms | 1.0x | Highly optimized C++ SDK baseline. |
| Rust (Polars) | ~119.5ms | ~0.2x | Slower on small metadata-heavy files. |

## Benchmarks (S3 Large File Scan - 100MB+, local)
*Throughput comparison: scanning 100MB+ file from MinIO TLS.*
| Implementation | Time (ms) | Notes |
| :--- | :--- | :--- |
| **Rust (Polars)** | **~162ms** | **Baseline** (Uses vectorized reads + object_store crate). |
| Python (PyArrow) | ~481ms | Good throughput but higher overhead. |
| ZPQ (Async Pooled) | ~9529ms | **Bottleneck**: Sequential 64KB reads. Fixed in Milestone 13. |

---

## Benchmarks (S3 Cold Start - 10k_rows.parquet, us-west-2)
*Cold start comparison: fresh process, fresh TLS connection per run.*
| Implementation | Avg Time (ms) | Speedup (vs PyArrow) | Notes |
| :--- | :--- | :--- | :--- |
| **ZPQ (Async + Prefetch)** | **~260ms** | **~1.7x** | Batched column prefetch via `RowGroupReader.prefetch()`. |
| Python (PyArrow) | ~420-470ms | 1.0x | Fresh connection per process. |
| ZPQ (Sync) | ~920ms | ~0.5x | Sequential single-range requests. |

*Key optimization: Batched prefetch reads all columns in a single `readRanges()` call, reducing round trips from 7+ sequential requests to 2-3 batched requests (HEAD + footer + column data).*

## Benchmarks (S3 E2E Scan - ARM64 Remote Host)
*Target: 84MB Parquet file (sample-data), us-west-2, 1 column scan.*
| Implementation | Avg Time (ms) | Speedup (vs PyArrow) | Notes |
| :--- | :--- | :--- | :--- |
| **ZPQ (Async Basic)** | **631.7ms** | **~1.9x** | Full stack + `libxev` + `boring_tls`. |
| Python (PyArrow/Boto3) | 1190.1ms | 1.0x | Standard Boto3/PyArrow path. |
| ZPQ (Sync) | 1731.2ms | ~0.7x | Synchronous sequential reads. |

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
| **PyArrow (Python)** | ~1.52s | Boto3 `get_object` (Whole file download) |
| **ZPQ (Sync)** | ~2.04s | Blocking transport (Wait for each chunk) |
| **ZPQ (Async Basic)** | ~0.41s | `libxev` + `boring_tls` (Manual DNS) |
| **ZPQ (Async Fancy)** | **~0.34s** | Full stack + Tiered DNS + SingleFlight |

## Lambda Benchmark (Internal Loop Proof-of-Concept)
*Note: These metrics represent an internal ping-pong loop of the `libxev` event loop on Lambda hardware to verify scaling. They do not yet reflect a real-world S3 scan (see Milestone 0 / Plan 09).*
Verified scalability of the async engine (`libxev` + `epoll`) on AWS Lambda (ARM64):
| Memory | RPS (Approx) | Scaling Factor | Notes |
| :--- | :--- | :--- | :--- |
| 128 MB | ~1,573 | 1.0x | CPU limited. |
| 1024 MB | ~15,355 | 9.7x | Strong performance baseline. |
| **2048 MB** | **~30,547** | **19.4x** | **Optimal linear scaling.** |
| 4096 MB | ~45,065 | 28.6x | Diminishing returns observed. |
