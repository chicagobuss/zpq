# ZPQ Lambda Ladder: Performance Tracking

This document tracks the "Performance Tax" of each layer added to the ZPQ Lambda stack. By building in sequential steps, we ensure that we never lose sight of our "Speed Floor."

## The Baseline: "Speed Floor"
**Target Architecture**: `arm64` (Graviton3/4)
**Runtime**: `provided.al2023` (Custom Runtime)

| Level | Name | Init (Cold) | Exec (Hot) | Memory | Binary Size | Added Complexity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **01** | **[minimal](./examples/lambda/01-minimal/main.zig)** | **6.22 ms** | **1.45 ms** | **11 MB** | **432 KB** | Baseline (Pure Zig Stdlib) |
| **02** | **dns-warming** | **16.8 ms** | **0.65 ms** | **11 MB** | **440 KB** | `libxev.Epoll` loop + Generic DNS |
| **03** | **warm-s3** | *Pending* | *Pending* | *Pending* | *Pending* | Connection Pool + S3 HEAD |
| **04** | **scan-bench** | **16.8 ms** | **268.4 ms** | **12 MB** | **480 KB** | Full Parquet Logic + TLS (Generic) |

---

## Detailed Analysis

### Level 01: minimal
*   **Strategy**: No external dependencies. Uses `std.http.Client` for the Lambda Runtime API.
*   **Takeaway**: We are at the theoretical floor of AWS Lambda. Our Init duration (6.22ms) is significantly lower than the ~15-20ms typically reported for Rust/Go. This is due to the extreme lack of static initializers and a tiny binary footprint (432KB).

---

## Industry Comparison (2024/2025 Benchmarks)
*Data sourced from community benchmarks (e.g., lambda-perf).*

| Runtime | Avg. Cold Start | Avg. Memory |
| :--- | :--- | :--- |
| **ZPQ (Zig)** | **~6ms** | **11MB** |
| **Rust** | ~15-30ms | ~12-15MB |
| **Go** | ~40-80ms | ~25-30MB |
| **Python** | ~100-200ms | ~35MB |
| **Java** | ~500ms - 2s | ~150MB+ |


### Level 04: scan-bench (SUV Lambda)
*   **Strategy**: Full ZPQ stack using loop-agnostic refactor. Explicitly initializes `xev.Epoll` to bypass Lambda's `io_uring` block.
*   **Result**: Cold starts stay under 20ms even with the full I/O stack (TLS + DNS + libxev). The execution time (268ms) represents a full Parquet scan over S3 (1MB file, includes handshake latency).
*   **Takeaway**: The generic refactor works. We've proven we can run the "powerhouse" ZPQ logic inside the leanest possible custom runtime without significant overhead.
