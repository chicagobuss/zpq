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
S3 test corpus uploaded to `s3://skyway-diat-staging-data/test_data/`:
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
*   **Multi-System Benchmark Results (S3 Cold Start - MinIO TLS):**
    *   **ZPQ Async (Xev)**: **~40ms** (Min: 33ms)
    *   Python (PyArrow/boto3): ~35ms (Min: 8ms)
    *   Rust (Polars): ~120ms
    *   *Note: ZPQ is achieving near-parity with PyArrow even without connection pooling. We are currently 3x faster than Polars.*

## Future Technical Objectives (Milestone 13):
1.  **Connection Pooling**: Investigate the "_amazin_" Zig-native connection pool implementation (or adapt `std.http.Client.ConnectionPool`) to reuse TLS connections and eliminate handshake overhead.
2.  **Milestone 14: Repetition Levels**: Implementing Dremel-style shredding for nested Parquet structures (Lists and Maps).
3.  **Linear Scaling**: Verify linear performance scaling when scanning 50+ columns in parallel.
