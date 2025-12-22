# ZPQ Technical Context and Detail

**Last Updated**: Dec 22, 2025
**Current State**: Milestone 0 (CI & Build Hardening) Completed. Project is stable on Zig master with optimized pre-built dependency workflows. Milestone 10 (Empirical Proof) verified on ARM64 hardware.

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

### Benchmark Results (ARM64 OCI `josh-oci-work-box-0`):
*Target: 84MB Parquet file, us-west-2, 1 column scan.*
| Implementation | Avg Time (ms) | Speedup (vs PyArrow) | Notes |
| :--- | :--- | :--- | :--- |
| **ZPQ (Async Basic)** | **631.7ms** | **~1.9x** | Full native stack + `libxev`. |
| Python (PyArrow/Boto3) | 1190.1ms | 1.0x | Standard production baseline. |
| ZPQ (Sync) | 1731.2ms | ~0.7x | Sequential blocking transport. |

### Technical Achievements:
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

## Future Technical Objectives (Milestone 11):
1.  **Plan 09: Lambda End-to-End**: Packaging the verified ARM64 engine into a deployable Lambda function with optimized cold starts.
2.  **Milestone 8: Persistent Pool & Timeouts**: Addressing the cleanup hang in `AsyncFancy` by implementing formal keep-alive timeouts and idle connection harvesting.
3.  **Milestone 12: Repetition Levels**: Implementing Dremel-style shredding for nested Parquet structures (Lists and Maps).
4.  **Plan 11: Massively Parallel Column Scans**: Stress-testing the `AsyncS3Source` with 50+ concurrent column readers to find the next bottleneck in the `libxev` completion queue.
