# ZPQ Architectural Reference

This document clarifies the roles and interactions of the core components in ZPQ.

## 1. Pipeline (The Orchestrator)
The `Pipeline` is the high-level coordinator. It does not perform raw I/O. Its jobs are:
- **Planning**: Accepting input/output paths, filters (SQL-like predicates), and projections.
- **Resource Management**: Coordinating the `xev` event loop and thread pool.
- **Strategy Selection**: Deciding which execution engine to use (Slot vs. Morsel vs. Surgical) based on the target storage and query selectivity.
- **Lifecycle**: Handling the transitions from `open()` to `execute()`.

## 2. I/O Abstraction (Zig 0.16.x)
We use a unified `RandomAccessSource` interface to hide storage differences.
- **Factory**: The `factory.zig` is the "smart" entry point. It detects `s3://` vs. local paths and returns a `ParquetFile`. It now handles the initial `readFooter()` so the Pipeline doesn't have to deal with file structure details.
- **ParquetFile**: A light wrapper around the source that manages the Parquet metadata (footer) and provides access to row groups.

## 3. libxev (The Engine)
Everything I/O-related in ZPQ is powered by `libxev`:
- **Networking**: Handles the S3 HTTP/TLS stack asynchronously.
- **Concurrency**: Provides the `ThreadPool` used to scan multiple row groups in parallel.
- **Portability**: Automatically uses `io_uring` on Linux and `kqueue` on macOS for peak performance.

## 4. Morsel Architecture (S3 Output)
S3 is an object store, not a filesystem; it doesn't support random-access writes (`pwrite`).
- **Morsels**: We split output into "morsels" (parts).
- **Multipart Upload**: These morsels are uploaded in parallel to S3.
- **Coordination**: The `MorselCoordinator` ensures parts are uploaded correctly and finalized into a single object.

## 5. Slot Writer (Local Output)
Local files support random-access parallel writes.
- **Slots**: We pre-calculate exactly where each row group's data will sit in the output file.
- **pwrite**: Threads write their results directly into these "slots" in parallel, avoiding a centralized bottleneck.

## 6. Surgical Mode (Selective Reading)
Surgical is a **reading** strategy used for both Local and S3.
- **Page Pruning**: Instead of reading whole row groups, it uses the Parquet `ColumnIndex` and `OffsetIndex` to identify exactly which **pages** match a filter.
- **I/O Minimization**: It only fetches the specific byte ranges for those pages, which can reduce I/O by 99% for needle-in-a-haystack queries.
