# ZPQ Technical Context and Detail

**Last Updated**: Dec 23, 2025
**Current State**: Milestone 0 (CI & Build Hardening) Completed. Project is stable on Zig master with optimized pre-built dependency workflows. Milestone 10 (Empirical Proof) verified on ARM64 hardware. Batched column prefetch optimization delivers 1.7x speedup over PyArrow on cold starts.

## Milestone 9: Hardening & Transport Stability (Completed)
Extensive debugging and refactoring to ensure production-grade stability and memory safety.

### Key Fixes and Improvements:
*   **Memory Safety**: Fixed multiple memory leaks in `ParquetFile.readFooter`, `AsyncS3Source.init`, and `ColumnReader.next` (specifically Snappy decompression buffers). Verified zero leaks in the "Basic" DNS lane.
*   **Transport Stability**: 
    *   **TLS 1.3**: Fixed handshake completion logic in `Connection.zig` to correctly handle cases where the final handshake leg returns no application data.
    *   **Backpressure**: Implemented a write queue and high-water mark system in `Connection.zig` to prevent buffer bloat during high-throughput writes.
    *   **ARM Compatibility**: Patched `boring_tls` and `Connection.zig` for consistent behavior on ARM64 (OCI instances).
*   **Correctness**: 
    *   **Thrift Booleans**: Fixed a critical decoding bug in `DictionaryPageHeader` where booleans were incorrectly read as ZigZag ints.
    *   **Aggressive Draining**: Modified `internalOnTcpRead` to aggressively drain the TLS BIO, preventing truncated requests.

## Milestone 0: Build System Hardening (Completed)
Successfully stabilized the project on Zig `master` and implemented a high-performance CI/CD pipeline for dependencies.

### Key Achievements:
*   **Zig 0.16.x Core Migration**: Updated all build logic and standard library calls to comply with the latest Breaking Changes in Zig nightly.
*   **Pre-built BoringSSL Pipeline**: Automated the cross-compilation and release of static libraries for Linux/Mac (x86 and ARM), reducing local/CI build times by ~90% for clean builds.
*   **Just Workflow Integration**: Optimized CI and local development using `just` recipes, including `fetch-deps` for instant bootstrapping.
*   **Unique Artifact Triplets**: Implemented a naming convention for release assets (e.g., `libcrypto-aarch64-macos.a`) to facilitate automated fetching across environments.

## Milestone 10: Empirical Proof & Performance (Phase 1 Complete)
Formal benchmarking against PyArrow/Boto3 on remote ARM64 hardware to validate the asynchronous architecture.

### Benchmark Results (S3 Cold Start - 10k_rows.parquet, us-west-2):
*Cold start comparison: fresh process, fresh TLS connection per run.*
| Implementation | Avg Time (ms) | Speedup (vs PyArrow) | Notes |
| :--- | :--- | :--- | :--- |
| **ZPQ (Async + Prefetch)** | **~260ms** | **~1.7x** | Batched column prefetch via `RowGroupReader.prefetch()`. |
| Python (PyArrow) | ~420-470ms | 1.0x | Fresh connection per process. |
| ZPQ (Sync) | ~920ms | ~0.5x | Sequential single-range requests. |

### Benchmark Results (ARM64 Remote Host - 84MB file):
*Target: 84MB Parquet file, us-west-2, 1 column scan.*
| Implementation | Avg Time (ms) | Speedup (vs PyArrow) | Notes |
| :--- | :--- | :--- | :--- |
| **ZPQ (Async Basic)** | **631.7ms** | **~1.9x** | Full native stack + `libxev`. |
| Python (PyArrow/Boto3) | 1190.1ms | 1.0x | Standard production baseline. |
| ZPQ (Sync) | 1731.2ms | ~0.7x | Sequential blocking transport. |

### Technical Achievements:
*   **Batched Column Prefetch**: `RowGroupReader.prefetch()` reads all columns in a single `readRanges()` call, reducing round trips from 7+ sequential requests to 2-3 batched requests (HEAD + footer + column data). This is the key optimization for cold start performance.
*   **Remote Benchmark Harness**: Automated sync, build, and execution on remote ARM hosts via `tools/remote_bench.sh`.
*   **Timeout Robustness**: Integrated `tools/no_output_timeout.py` to handle stalled cleanups in the async engine during benchmarks.
*   **Columnar Scaling**: Beefed up `bench-e2e` to support multi-column scans and iterative testing with per-run `ArenaAllocator` isolation.

## Lessons Learned: Zig 0.16.x and Development Workflow

### 1. "Faster Feedback" Debugging
*   **Isolation First**: Prototyping fixes in small `probes/` before integration saved hours of build time and variables.
*   **Linter as Compiler**: Using `just lint` (which runs `zig build --summary none`) proved faster for verification than full test suites.

### 2. Ownership and Deinitialization
*   **Cleanup Contexts**: Using `cleanup_fn` and `cleanup_context` in `ParquetFile` allows the core library to own the cleanup of complex asynchronous sources without knowing their internal structure.
*   **Arena Per-Iteration**: In high-throughput scans, using a per-iteration `ArenaAllocator` for data pages is critical for preventing fragmented heap growth.

---

## Known Issues / TODO

### Debug Output Leaking to Stdout
**Status**: Needs fix  
**Observed**: `[LIB_DEBUG] FREEING page data at ...` messages appear in `cat` and `scan` output.  
**Root Cause**: Debug print statements in the page deallocation path are unconditionally enabled.  
**Solution**: Add a `--debug` or `-v` CLI flag to enable verbose/debug output. Debug logging should be gated behind this flag and disabled by default.

### Test Data Available
S3 test corpus uploaded to `s3://zpq-staging-data/test_data/`:
```
valid/
├── compression/{snappy,gzip,zstd,uncompressed}/basic_1k.parquet
├── sizes/{tiny/50_rows.parquet, small/10k_rows.parquet}
└── schemas/{primitives/all_types.parquet, nested/structs_lists_maps.parquet, nulls/sparse_nulls.parquet}
invalid/
├── corrupted/{truncated.parquet, random_garbage.parquet}
└── malformed/{empty.parquet, bad_magic.parquet, bad_footer_magic.parquet}
```
Local copies persist at `/mnt/d/work/zpq-scratch/test_data/` (WSL2).

---

## Milestone 11: New cross-platform I/O Stack Integration (Completed)
Integration of the new `libxev` + `boring_tls` stack with `ParquetFile` for robust, cross-platform S3 access.

### Key Achievements:
*   **XevS3Source Implementation**: Created a new `io.RandomAccessSource` implementation that uses `libxev` for asynchronous transport and `boring_tls` for TLS 1.3 encryption.
*   **TLS Record Pumping**: Fixed a critical bug where multiple TLS records in a single TCP packet were being ignored. The stack now correctly pumps the TLS engine in a loop.
*   **Isolated Loop Lifecycle**: Implemented a "fresh loop per request" pattern in `XevS3Source` to prevent resource leaks and completion corruption across serial requests.
*   **Parquet Wiring**: Verified that `ParquetFile.openS3` correctly fetches file size via HEAD and parses footer metadata via ranged GETs against MinIO over TLS.
*   **CI Robustness**: Integrated `tools/no_output_timeout.py` and watchdog timers into integration tests to ensure deterministic failure modes.

## Milestone 12: Performance & Scaling (Parallelization & Auth) (Completed)
Implementation of high-concurrency fetching and optimization of the new I/O stack.

### Key Achievements:
*   **Parallel readRanges**: Refactored `XevS3Source.readRanges` to fire multiple `TlsConnection` requests simultaneously on a single shared `xev.Loop`.
*   **SigV4 Integration**: Successfully ported the AWS SigV4 signing logic to the new `XevS3Source`. Authenticated requests are now supported, with automatic credential loading from environment variables.
*   **"Ghost Watcher" Bug Fix**: Diagnosed and resolved a critical hang in the loop exit caused by double-arming `libxev` completions. Implemented `pending_read` and `pending_write` guards in `TlsConnection` to maintain perfect loop balance.
*   **Cold Start Speedup**: Verified a ~30% reduction in cold start latency (from 52ms down to 37ms for a full metadata scan) on local MinIO TLS hardware.
*   **Cleanup Stability**: Fixed a double-free bug in the `ParquetFile` cleanup path where the S3 source was being destroyed multiple times.
*   **Connection Pooling**: Implemented a robust LIFO connection pool (`XevConnectionPool`) that reuses TLS connections for subsequent requests (e.g., HEAD followed by GETs), eliminating handshake overhead.
*   **Multi-System Benchmark Results (S3 Cold Start - MinIO TLS):**
    *   **ZPQ Async (Pooled)**: **~44ms** (Approaching PyArrow)
    *   Python (PyArrow/boto3): ~28.5ms
    *   Rust (Polars): ~119.5ms
    *   *Note: ZPQ is achieving near-parity with PyArrow and is ~3x faster than Polars on cold starts.*

## Future Technical Objectives (Milestone 13):
1.  **Massive Parallel Scans**: Implement large-buffer (1MB+) parallel column fetching to saturate network bandwidth.
2.  **Scheduler Port**: Migrate the `scheduler.zig` range-coalescing logic to the new `XevS3Source` stack.
3.  **Linear Scaling**: Verify linear performance scaling when scanning 50+ columns in parallel.
4.  **Beat Polars**: Optimize throughput to surpass the Polars baseline (~162ms) for large file scans.
5.  **CI Co-existence**: Resolved a critical type ambiguity where the legacy `AsyncS3Source` and the new `XevS3Source` both tried to use a `Connection` type; fixed by creating a dedicated `XevConnectionPool`.

## Lessons Learned: I/O Evolution & Cleanup Roadmap

### 1. The "Two-Stack" Problem
*   **Conflict**: Having two asynchronous stacks (`s3_legacy` and the new `Xev`) in one codebase is dangerous for CI (naming collisions) and maintenance.
*   **Cleanup Strategy**: Once Milestone 13 (Parallel Scans + Range Scheduler) is complete, the legacy stack will be deleted. It currently serves only as a regression baseline.

### 2. Connection Pooling Nuances
*   **LIFO is Key**: Using a Last-In-First-Out (LIFO) stack for idle connections ensures we pick the "warmest" connection, reducing the chance of server-side timeouts.
*   **Timestamp Precision**: On POSIX systems, `std.time.Instant.now().timestamp` is a `posix.timespec`. Correctly extracting milliseconds requires `(sec * 1000) + (nsec / 1_000_000)`.

### 3. Loop Reusability
*   **Ghost Watchers**: The most common source of event loop hangs in `libxev` is an unbalanced `active` count. Never `loop.stop()` a shared loop inside a connection's `onClose` callback if other requests are still inflight.

## Milestone 15: Cleanup & Consolidation (Completed)
Successfully transitioned to the `Xev` stack as the definitive engine and cleaned up the codebase.

### Key Achievements:
*   **Massive Deletion**: Removed ~2,180 lines of legacy asynchronous code (the old `AsyncS3Source`, `Connection`, `TlsAdapter`, and `ConnectionPool`). The codebase is now 100% focused on the modern `libxev` + `boring_tls` stack.
*   **I/O Re-homing**: Moved `LocalFileSource` and `MemorySource` from the generic `interface.zig` into a dedicated `src/zpq/io/local/` directory. This improves modularity and organization.
*   **Namespace Refactor**: Introduced `zpq.io.local` and `zpq.io.s3` namespaces to clearly separate local vs remote storage implementations.
*   **CI Build Optimization**: Refactored the CI workflow into a parallel matrix (ARM64 + x86_64). Differentiates between critical binaries (fully built with `ReleaseFast`) and auxiliary tools/probes (verified via `zig build check` in Debug mode). This significantly reduced build times by avoiding redundant heavy optimization passes.
*   **Factory Consolidation**: Simplified `src/zpq/io/s3/factory.zig` to treat `XevS3Source` as the first-class, default implementation for all S3 access.

## Milestone 16: Real-World Benchmarking & Baseline (Completed)
Established the performance baseline for ZPQ on production-sized workloads.

### Results & Findings:
*   **Throughput**: ZPQ consistently outperforms PyArrow and Polars in warm-container scenarios on S3-compatible storage (Cloudflare R2).
*   **Latency**: Achieved **28.3ms** cold starts (process creation to full scan complete) on local S3-over-TLS, matching or beating the best available C++/Rust baselines.
*   **Scale**: Verified that the parallel scheduler correctly handles 100MB+ files with dozens of columns, efficiently coalescing requests to maximize bandwidth.
*   **Reliability**: The stack successfully handles high-latency remote storage with robust connection pooling and DNS racing.

## Milestone 17: AWS Lambda Integration & Minimal Runtime (In Progress)
First-class support for serverless execution via a custom Zig Lambda runtime.

### Key Achievements:
*   **Minimal Runtime Loop**: Implemented a native Lambda Runtime API loop in `examples/lambda/main.zig` that avoids heavyweight dependencies.
*   **Architecture Parity**: Verified that the Lambda bootstrap builds and links correctly on both ARM64 (native) and macOS (cross-compilation verified for logic).
*   **Libxev 0.16 Alignment**: Patched the `io_uring` and `epoll` backends in vendored `libxev` to align with Zig's strict `std.posix` vs `std.os.linux` type decoupling.
*   **CI Production Artifacts**: Optimized CI to produce optimized `ReleaseFast` binaries for the Lambda runtime (`bootstrap`) and the E2E benchmark suite.

### The "SQL Layer" Roadmap (Research Synthesis):
Exhaustive research into "Headless OLAP" frontends has yielded a two-track roadmap for ZPQ's query intelligence:

1.  **Track A: The Pragmatic SQL (SQLite VTable)**:
    *   **Frontend**: SQLite (~1MB).
    *   **Integration**: Custom Virtual Table in Zig (using the `Stanchion` project as a pattern).
    *   **Optimization**: Use `xBestIndex` for row group pruning and "Pointer-as-BLOB" for vectorized SIMD aggregations (bypassing SQLite's row-at-a-time bottleneck).
    *   **Status**: Ready for prototyping.

2.  **Track B: The Holy Grail (Frankenstein Engine)**:
    *   **Parser**: `libpg_query` (Postgres parser as a standalone C lib, ~3MB).
    *   **Planner**: Lightweight rule-based optimizer in Zig.
    *   **Executor**: ZPQ's native SIMD kernels operating directly on Arrow buffers.
    *   **Outcome**: Bare-metal performance with 100% Postgres-compatible SQL syntax, fitting comfortably under the 5MB Lambda budget.

### Common Requirement: Arrow C Data Interface
Both tracks require ZPQ to export data via the **Apache Arrow C Data Interface** (two ABI-stable C structs). This is our next high-level technical objective.

## Future Technical Objectives:
1.  **Arrow C Data Interface**: Implement zero-copy column exposure using stable C ABI structs.
2.  **SIMD Decoders**: Vectorized bit-unpacking for PLAIN and RLE encodings.
3.  **Speculative Footer Fetch**: Merge HEAD and first GET into a single 128KB speculative read of the file tail.
4.  **Connection Warming**: Persist TCP/TLS connections across Lambda invocations using the `GlobalConnectionPool`.
