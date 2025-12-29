# ZPQ Lambda Ladder: Performance Tracking

This document tracks the "Performance Tax" of each layer added to the ZPQ Lambda stack. By building in sequential steps, we ensure that we never lose sight of our "Speed Floor."

## The Baseline: "Speed Floor"
**Target Architecture**: `arm64` (Graviton3/4)
**Runtime**: `provided.al2023` (Custom Runtime)

| Level | Name | Init (Cold) | Exec (Hot) | Memory | Binary Size | Added Complexity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **01** | **[minimal](./examples/lambda/01-minimal/main.zig)** | **6.22 ms** | **1.45 ms** | **11 MB** | **432 KB** | Baseline (Pure Zig Stdlib) |
| **02** | **dns-warming** | *Pending* | *Pending* | *Pending* | *Pending* | `libxev` loop + Custom DNS |
| **03** | **warm-s3** | *Pending* | *Pending* | *Pending* | *Pending* | Connection Pool + S3 HEAD |
| **04** | **scan-bench** | *Pending* | *Pending* | *Pending* | *Pending* | Full Parquet Logic + TLS |

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

