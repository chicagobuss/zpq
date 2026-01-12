# Tier 3: The Current Strategy

**"Write Dominance" & The Fast Path**

## Goal
Implement a high-performance S3 Sink that can saturate network bandwidth using parallel multipart uploads and zero-copy streaming.

## Architecture
1.  **Parallel S3 Sink**:
    *   **Morsel-Driven**: Processing units ("Morsels") are processed in parallel.
    *   **Bounded Channel**: A channel limits memory usage by blocking producers when the sink is full.
    *   **Multipart Uploads**: 8 concurrent active uploads using HTTP Range requests / discrete Part creation.
    *   **Async Task Pool**: `SinkTask` decouples serialization overhead from network I/O.

2.  **Zero-Copy "Fast Path"**:
    *   **The Principle**: If `SELECT *` (no filter, no projection) is requested, we should look like `cp`.
    *   **Implementation**: Identify "Identity Transform" in the `Planner`. Call `runFastPath` which bypasses Parquet decoding entirely, streaming raw bytes from Source to Sink.

## Next Tacticals
1.  **Fast Path Logic**: Wire up `main.zig` to detect the identity case logic.
2.  **Planner Separation**: Refactor `runQuery` into strict `Planner` (Decision) and `Executor` (Action) phases.
