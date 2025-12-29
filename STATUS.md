# ZPQ Project Status

## High-Level Goals
*   **Performance**: Outperform PyArrow and Rust `parquet` crate (Standard Arrow Reader) in scan throughput.
*   **Safety**: Zero-copy where possible, safe memory management, robust error handling.
*   **Completeness**: Support all standard Parquet encodings and compression codecs.
*   **Portability**: Run as a standalone CLI, AWS Lambda (Zig's cross-compilation), or library.

## Philosophy & Architecture

### The Laziness Principle (Foundational)
> **ZPQ preserves data in its most compact/encoded form as long as possible. Decoding, decompression, and re-encoding happen only at the boundaries where transformation is required.**

This principle drives every architectural decision:
*   **Late Materialization**: Data stays in Parquet's native encoding (RLE, dictionary, delta) until a consumer needs decoded values.
*   **Metadata-First**: Leverage row group stats and column indexes to skip work before touching data bytes.
*   **Copy-Through Optimization**: Unchanged columns pass through as compressed bytes - only the footer is rewritten.
*   **Selective Decoding**: For filtered reads, decode only predicate columns to build selection vectors.

### Lambda-First Design
*   **No Runtime SDKs**: We re-write S3, SigV4, and HTTP/1.1 natively to eliminate SDK bloat and maintain a zero-copy, zero-allocation path.
*   **Minimal Footprint**: ~2MB binary size for the full S3+TLS+Parquet stack.
*   **Cold Start Optimized**: Binary size and initialization overhead are first-class concerns.

### The Spirit of Zig 0.16
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

### Milestone 0: Hardening & Verification [COMPLETE]
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
*   [x] **Lambda E2E**: Prepared end-to-end validation suite.

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
*   [x] **Open Source Testing**: Integrated **RustFS** (Apache 2.0) as the primary S3-compatible testing backend.
*   [x] **Protocol Hardening**: Fixed foundational bugs in short-read handling and case-insensitive header parsing.
*   [x] **Connection Pooling**: Reuses TLS connections across requests, eliminating handshake overhead for metadata & page reads.
*   [x] **CI Type Safety**: Resolved type ambiguity between legacy and new connection pools to ensure co-existence.
*   [x] **Linear Scaling**: Verify linear performance scaling when scanning 50+ columns in parallel.

### Milestone 13: Massively Parallel Column Scans [COMPLETE]
*   [x] **Vectorized Reads**: Increased effective read buffer size by coalescing adjacent column ranges into large HTTP blobs.
*   [x] **Range Scheduler**: Ported the `scheduler.zig` logic to the `XevS3Source` stack for smart request merging.
*   [x] **Parallel Columns**: Wired `RowGroupReader` to fetch all columns concurrently via `prefetch(null)`.
*   [x] **Correctness**: Verified against **RustFS** over HTTPS with zero memory leaks.
*   [x] **Performance**: Achieved **28.3ms** cold starts—matching/surpassing established baselines.

### Milestone 14: Global Connection Pool [COMPLETE]
**Goal**: Eliminate TLS handshake overhead for Lambda warm starts and batch operations.

*   [x] **Global Pool Architecture**: Process-wide singleton pool keyed by (host, port, use_tls).
*   [x] **Thread Safety**: Mutex-protected acquire/release with stats tracking.
*   [x] **Idle Timeout**: 30s expiration to prevent stale connections.
*   [x] **XevS3Source Integration**: Uses global pool by default; connections survive across file opens.
*   [x] **Probe Verification**: `probe_batch_reuse` confirms 2.3x speedup (125ms cold → 54ms warm).
*   [x] **Unit Tests**: 5 tests for pool lifecycle, key hashing, and stats tracking.
*   [x] **Isolated Probe Build**: `probes/build.zig` for fast iteration without full rebuild.

**Performance Impact**:
- Cold batch (new TLS): ~125ms
- Warm batches (pooled): ~54ms average
- Lambda benefit: Connections persist across invocations in warm containers

### Milestone 15: Cleanup & Consolidation [COMPLETE]
*   [x] **Delete Legacy**: Removed `async_source.zig`, `event_loop.zig`, `tls_adapter.zig`, `connection.zig`, `connection_pool.zig`, `request.zig`, `raw_s3_source.zig`, and related test files (~2,180 lines deleted).
*   [x] **I/O Consolidation**: Moved `LocalFileSource` and `MemorySource` into `src/zpq/io/local/` and introduced `io.local` namespace.
*   [x] **Factory Flip**: Made `XevS3Source` the definitive default for all `s3://` paths via `factory.openFile`.
*   [x] **CI Optimization**: Implemented a "Lean CI" strategy using `zig build check` and parallel multi-arch runners (ARM64 + x86_64), reducing feedback loops while maintaining 100% platform coverage.

### Milestone 16: Real-World Benchmarking & Performance Baseline [COMPLETE]
**Goal**: Establish baseline performance against competitors on real S3/R2 with production-sized files.

*   [x] **Large file testing**: Benchmarked 114MB AWS CUR Parquet file locally (3-11 columns).
*   [x] **Competitor comparison**: PyArrow, DuckDB, Polars on identical files/queries.
*   [x] **Document baseline**: Recorded local decode numbers - ZPQ is 2.6-3.1x faster than PyArrow.
*   [x] **R2 testing**: Benchmarked against Cloudflare R2 (10MB, 100MB files). ZPQ warm runs fastest.
*   [x] **CI Integration**: Added R2 benchmarks to CI via `bench.sh` (1MB, 10MB, 100MB smoke tests).
*   [x] **Factory robustness**: `S3_ENDPOINT` now accepts bare hostnames or full URLs.
*   [x] **Verification**: Confirmed stable performance after massive I/O refactor.

### Milestone 17: Loop-Agnostic Refactor & Lambda Foundation [COMPLETE]
**Goal**: Decouple I/O stack from specific loops and enable high-performance DNS in Lambda.

*   [x] **Factory Polish**: Consolidated S3 path parsing, environment discovery, and endpoint discovery.
*   [x] **Resolver Injection**: Correctly pass `dns.ResolverGen(XevApi)` down to the I/O stack.
*   [x] **Lambda Verification**: Proved ~16ms cold starts for minimal loop and ~260ms for full scan on Lambda.
*   [x] **CI Confirmation**: Verified multi-arch builds and benchmarks pass on `main`.

## ⏭️ Roadmap & Future Technical Objectives

### Technical Debt & Standards (Zig 0.16 Alignment)
*   **`shim_net` Compliance**: Ensure all custom networking types exactly match the signatures of `std.Io.net` to allow zero-cost swapping when the stdlib stabilizes.
*   **`std.Io` Injection**: Fully refactor all I/O components to accept `std.Io` interfaces at the top level.
*   **TLS Abstraction**: Wrap `boring_tls` in a `std.Io.Reader/Writer` interface to prepare for future native TLS support or offloading.
*   **Upstream Contributions**: Port `std.http.Client` to the new `std.Io` interface to allow native async HTTP without custom "Bare Metal" clients.

### Milestone 18: SIMD Decoders [IN PROGRESS]
**Goal**: Maximize decode throughput for cases where we must decode.

*   [x] **SIMD Bit-Unpacking**: Achieved **3.1 GVal/s** (up to 15x speedup) using comptime-generated vector kernels for bit-widths 1-32.
*   [x] **Vectorized nextBatch**: Implemented batch reading in `RleDecoder` to realize SIMD gains.
*   [ ] **Vectorized RLE runs**: Fully vectorize repetition runs (repeats) using SIMD splat/memset.
*   [ ] **SIMD null bitmap expansion**: Fast expansion of compact values into nullable buffers.
*   [ ] **E2E Integration**: Refactor core reader to use `nextBatch` for all column types.
*   [ ] **Benchmark suite**: Verified **~2.5 GVal/s** micro-benchmark throughput on ARM64/x86_64.
*   **Target**: 2x+ decode throughput vs current implementation (Scalar: ~0.2-0.5 GVal/s, SIMD: ~3.1 GVal/s).

### Milestone 19: Predicate Pushdown (Read-Side)
**Goal**: Skip work on reads - Laziness Principle for filtering.

*   [ ] **Row group statistics evaluation**: Skip row groups via min/max metadata.
*   [ ] **Page index support**: Finer-grained skipping (if present in file).
*   [ ] **Selection vector generation**: Decode filter column, build bitmap.
*   [ ] **Selective column decode**: Only decode rows matching selection vector.
*   [ ] **Benchmark**: "1% selectivity" should be ~100x faster than full scan.

### Milestone 20: Parquet Writer Foundation
**Goal**: Basic write capability for "slice and dice" operations.

*   [ ] **PLAIN encoder** for primitives (INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY).
*   [ ] **Definition level writer** (RLE encoded) for nullable columns.
*   [ ] **Page/row group assembly**: Data pages with headers, column chunks.
*   [ ] **Footer generation**: FileMetaData Thrift serialization.
*   [ ] **CLI**: `zpq slice input.parquet -o output.parquet --columns a,b,c`
*   [ ] **Round-trip verification**: Read → write → read, compare results.

### Milestone 21: Copy-Through Optimization
**Goal**: The "cheat path" - copy unchanged columns without decode/encode.

*   [ ] **Passthrough detection**: Identify columns not touched by filter/projection.
*   [ ] **Compressed chunk copying**: Byte-copy column chunks directly.
*   [ ] **Metadata-only rewrite**: Update offsets in footer without touching data.
*   [ ] **Partial row group handling**: Fall back to decode when only some rows match.
*   [ ] **Benchmark**: Prove 10x+ speedup for "1% filter, 25 columns" vs naive.

### Milestone 22: Compression & Dictionary Encoding
**Goal**: Production-quality output files.

*   [ ] **Snappy compressor**: Reuse decompressor knowledge, add compression.
*   [ ] **Dictionary encoder**: Build dictionary for string columns, encode indices.
*   [ ] **Dictionary passthrough**: Preserve input dictionary when possible.
*   [ ] **Compression selection**: Match input compression or specify via CLI.

### Milestone 23: Streaming S3 Output
**Goal**: Write directly to S3 without local temp files.

*   [ ] **S3 multipart upload**: Initiate, upload parts, complete.
*   [ ] **Streaming writer interface**: Write pages as they're ready.
*   [ ] **Full pipeline**: S3 input → filter → S3 output (no local disk).
*   [ ] **Memory-bounded**: Limit buffering, flush parts incrementally.
*   [ ] **CLI**: `zpq slice s3://in/file.parquet -o s3://out/file.parquet`

### Milestone 24: Lambda-Ready Package
**Goal**: Production deployment for serverless lakehouse.

*   [ ] **Lambda handler**: Event-driven entry point.
*   [ ] **Configuration via environment**: Input/output paths, filter expressions.
*   [ ] **Error handling**: Graceful failures, structured logging.
*   [ ] **Metrics**: Execution time, bytes read/written, rows processed.
*   [ ] **Binary size audit**: Target <3MB for fast cold starts.
*   [ ] **Benchmark vs DuckDB Lambda**: Prove the cost/performance advantage.

### Milestone 25: Zero-Copy TLS (Research)
**Goal**: Eliminate final memcpy in TLS decryption path.

*   [ ] **Direct decryption into column buffers**: Decrypt S3 data directly into destination.
*   [ ] **Vendor boring_tls modifications**: Requires changes to SSL_read path.
*   [ ] **Benchmark**: Measure throughput improvement on large file scans.

### Milestone 26: Query Engine Foundation (Future)
**Goal**: Lay groundwork for operators beyond scan.

*   [ ] **Batch/morsel abstraction**: Fixed-size column chunks (1024/2048 rows).
*   [ ] **Operator interface**: `open()`, `next() -> Batch`, `close()`.
*   [ ] **Filter operator**: Evaluate predicates on batches.
*   [ ] **Project operator**: Compute expressions, select columns.
*   [ ] **Pipeline builder**: Compose operators into execution plan.

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

## Benchmarks (Cloudflare R2 - 100MB file, 3 columns)
*Remote S3-compatible storage benchmark from local Mac.*

| Implementation | Min Time | Avg Time | Notes |
| :--- | :--- | :--- | :--- |
| **ZPQ** | **310ms** | 538ms | Warm runs fastest; TLS cert warning |
| Polars | 455ms | 507ms | Consistent performance |
| PyArrow | 505ms | 532ms | C++ S3 SDK |

*ZPQ shows higher variance (cold TLS handshake) but best warm-run performance.*

## Benchmarks (S3 Cold Start - MinIO/RustFS TLS, local)
*Cold start comparison: fresh process, fresh TLS connection per run.*
| Implementation | Avg Time (ms) | Speedup (vs PyArrow) | Notes |
| :--- | :--- | :--- | :--- |
| **ZPQ Async (Massively Parallel)** | **~28.3ms** | **1.0x** | **Record parity with PyArrow.** |
| Python (PyArrow/boto3) | ~28.5ms | 1.0x | Highly optimized C++ SDK baseline. |
| Rust (Polars) | ~119.5ms | ~0.2x | Slower on small metadata-heavy files. |

## Benchmarks (S3 Column Projection - 10MB file, us-west-2)
*Column projection benchmark: 3 columns from 10MB Parquet file over real S3.*
| Implementation | Avg Time (ms) | Notes |
| :--- | :--- | :--- |
| **ZPQ (Async + Global Pool)** | **~289ms** | Parallel prefetch, ~35 MB/s effective throughput. |

*Breakdown (avg): Open=98ms, Footer=136ms, Prefetch=54ms, Decode=0.6ms*

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

## Benchmarks (Local File Decode - M3 Max, 114MB AWS CUR file)
*Apples-to-apples decode benchmark: warm runs, column projection.*

### 3 Columns (bill_bill_type, bill_billing_entity, bill_billing_period_end_date)
| Implementation | Avg Time | Speedup vs PyArrow |
| :--- | :--- | :--- |
| **ZPQ** | **3.21ms** | **2.6x** |
| Polars | 4.27ms | 1.9x |
| PyArrow | 8.32ms | 1.0x |
| DuckDB | 258.56ms | 0.03x |

### 10 Columns
| Implementation | Avg Time | Speedup vs PyArrow |
| :--- | :--- | :--- |
| **ZPQ** | **10.94ms** | **3.1x** |
| PyArrow | 33.96ms | 1.0x |
| Polars | 84.23ms | 0.4x |

*Note: Column 12+ contains nested REPEATED types not yet supported by ZPQ.*

## Benchmarks (Local File - M3 Max, older)
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
