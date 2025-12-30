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

---

## 🚀 Roadmap: The Path to Lambda Parquet Dominance

ZPQ's objective is to become the fastest, leanest Parquet engine for serverless environments by leveraging the **Laziness Principle**.

### Phase 1: Read Dominance (Skip Everything)
**Goal**: Minimize I/O and maximize decode throughput.

#### Milestone 18: SIMD Decoders [IN PROGRESS]
*   [x] **SIMD Bit-Unpacking**: Achieved **3.1 GVal/s** using comptime kernels.
*   [x] **BatchReader Integration**: Unified 1024-wide vectorized paths for all types.
*   [x] **Deranged Data Proof**: Verified correctness against industrial edge cases.
*   [ ] **Vectorized RLE runs**: SIMD splat/memset for repetition runs.
*   [ ] **SIMD null bitmap expansion**: Branchless expansion of nullable batches.

#### Milestone 19: Predicate Pushdown (Read-Side)
*   [ ] **Metadata Pruning**: Skip row groups using min/max statistics.
*   [ ] **Selection Vector Generation**: Decode filter columns first to build bitmaps.
*   [ ] **Lazy Materialization**: Only decode rows matching the selection vector.

### Phase 2: Transformation Dominance (The Cheat Path)
**Goal**: High-speed filtering and projection for data lake ETL.

#### Milestone 20: Parquet Writer Foundation
*   [ ] **Encoders**: PLAIN and RLE encoders for all physical types.
*   [ ] **Metadata Synthesis**: Generate valid Thrift FileMetaData from scratch.
*   [ ] **Round-trip CLI**: `zpq slice input.parquet -o output.parquet --columns a,b`.

#### Milestone 21: Copy-Through Optimization
*   [ ] **Zero-Decode Passthrough**: Byte-copy compressed column chunks for unchanged columns.
*   [ ] **Metadata-Only Rewrite**: Update offsets/statistics without touching data bytes.

### Phase 3: Infrastructure Dominance (Zero-Disk)
**Goal**: Run 100GB transformations in 128MB Lambda containers.

#### Milestone 22: Streaming S3 Output
*   [ ] **S3 Multipart Writer**: Direct streaming to S3 without local `/tmp` usage.
*   [ ] **Memory-Bounded Buffering**: Flush parts incrementally to maintain <50MB overhead.

### Phase 4: Ecosystem Dominance (Arrow & SQL)
**Goal**: Seamless integration with the modern data stack.

#### Milestone 23: Arrow C Data Interface
*   [ ] **ABI Stability**: Implement `ArrowSchema` and `ArrowArray` C structs.
*   [ ] **Zero-Copy Export**: Expose ZPQ internal batches to DuckDB/Polars without copying.

#### Milestone 24: Headless OLAP (Research)
*   [ ] **SQL Frontend**: Integrate `libpg_query` or SQLite VTable.
*   [ ] **Vectorized Operator Pipeline**: morsel-driven execution for aggregations.

---

## Project Milestones (Historical)

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
    *   [x] `PLAIN`, `RLE`, `BIT_PACKED`, `RLE_DICTIONARY` / `PLAIN_DICTIONARY`
*   [x] **Decompression**:
    *   [x] **Snappy**: Native Zig implementation.
*   [x] **Complex Features**:
    *   [x] **Definition Levels**: NULL value handling via RLE decoding.
    *   [x] **Dictionary Resolution**: Reconstructing values from dictionary pages.

### Milestone 2: CLI Tools & Initial Benchmarking [COMPLETE]
*   [x] **CLI Tools**: `schema`, `meta`, `pages`, `cat`, `scan`, `debug-s3`.
*   [x] **Throughput**: Verified ~911 MB/s (ZPQ) vs ~370 MB/s (PyArrow) vs ~265 MB/s (Rust Arrow) on M1 Max.

### Milestone 3: Async Foundation (libxev + boring_tls) [COMPLETE]
*   [x] **Evented I/O**: Integrated `EventLoop` into `AsyncS3Source` for parallel range fetches.
*   [x] **TLS Integration**: Implemented `TlsAdapter` using `boring_tls` (OpenSSL). Verified macOS/Linux ARM64.

### Milestone 10: Empirical Proof & Performance [COMPLETE]
*   [x] **Proof**: Formally proved ~1.9x performance advantage over PyArrow on ARM64 hardware.

### Milestone 14: Global Connection Pool [COMPLETE]
*   [x] **Performance**: ~2.3x speedup for warm containers (125ms cold → 54ms warm).

### Milestone 15: Cleanup & Consolidation [COMPLETE]
*   [x] **Delete Legacy**: Removed ~2,180 lines of legacy async code.
*   [x] **CI Optimization**: Parallel multi-arch runners (ARM64 + x86_64).

### Milestone 17: Loop-Agnostic Refactor & Lambda Foundation [COMPLETE]
*   [x] **Resolver Injection**: Correctly pass `dns.ResolverGen(XevApi)` down to the I/O stack.
*   [x] **Lambda Verification**: Proved ~16ms cold starts for minimal loop.

---

## Technical Debt & Standards (Zig 0.16 Alignment)
*   **`shim_net` Compliance**: Ensure all custom networking types match `std.Io.net`.
*   **`std.Io` Injection**: Fully refactor all I/O components to accept `std.Io` interfaces.
*   **TLS Abstraction**: Wrap `boring_tls` in a `std.Io.Reader/Writer` interface.

---

## Benchmarks Summary

### Cloudflare R2 (100MB file, 3 columns)
| Implementation | Min Time | Avg Time | Notes |
| :--- | :--- | :--- | :--- |
| **ZPQ** | **310ms** | 538ms | Warm runs fastest |
| Polars | 455ms | 507ms | |
| PyArrow | 505ms | 532ms | |

### S3 Cold Start (local MinIO/RustFS TLS)
| Implementation | Avg Time (ms) | Speedup (vs PyArrow) |
| :--- | :--- | :--- |
| **ZPQ Async** | **~28.3ms** | **1.0x** |
| PyArrow | ~28.5ms | 1.0x |
| Polars | ~119.5ms | ~0.2x |

### Local File Decode (M3 Max, 114MB AWS CUR file)
| Columns | ZPQ Time | PyArrow Time | Speedup |
| :--- | :--- | :--- | :--- |
| 3 Columns | **3.21ms** | 8.32ms | 2.6x |
| 10 Columns | **10.94ms** | 33.96ms | 3.1x |
