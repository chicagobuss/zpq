# ZPQ: The High-Performance Zig Parquet Tool

ZPQ is a specialized Parquet tool built for extreme performance, specifically targeting cloud-native environments like AWS S3 and Lambda.

## 🚀 The Ferrari Architecture (Milestone 7 Complete)

ZPQ features a custom-built, "Pure Async" I/O engine designed to outperform generic Parquet tools by leveraging Zig's low-level control and modern Linux/macOS primitives.

### Key Features:
- **Pure Event-Driven Lifecycle**: Built on top of `libxev` (Static backend), ZPQ uses a non-blocking completion state machine. No busy-waiting, no `WouldBlock` polling loops.
- **Aggressive Parallelism**: Interleaves hundreds of concurrent range requests to S3 on a single thread.
- **Zero-Allocation Gaps**: Our custom HTTP/1.1 engine can skip "gaps" between Parquet columns directly at the socket level without allocating memory for discarded bytes.
- **Tiered Async DNS**: A high-performance DNS stack that races IPv4/IPv6 (Speculative) and deduplicates lookups (Single-Flight) to minimize connection latency.
- **Cross-Platform**: Supports `io_uring` on Linux (WSL2) and `kqueue` on macOS with a unified completion-based API.

## 📈 Benchmarks

ZPQ is designed to be the fastest Parquet reader in the world for cloud-latency scenarios.

| Implementation | Local Throughput (Values) | S3 Metadata Latency |
| :--- | :--- | :--- |
| **ZPQ (Zig)** | **~857 MVal/s** | **~0.58s (WIP)** |
| PyArrow (Python) | N/A | ~0.15s |
| Polars (Rust) | N/A | ~0.40s |

*Note: S3 metadata latency is currently bottlenecked by persistent connection reuse, which is the focus of Milestone 8.*

## 🛠 Project Status

ZPQ is currently in active development.

- **Milestone 6**: High-Performance Async DNS [COMPLETE]
- **Milestone 7**: Pure Async Lifecycle & Transport Abstraction [COMPLETE]
- **Milestone 8**: Enhanced Connection Pooling & Repetition Levels [IN PROGRESS]

## 💻 Building & Testing

ZPQ requires the latest Zig master (0.16.dev).

```bash
# Build the project
just build

# Run all IO tests (Async stack)
zig build test-io -Dexperimental=true

# Run the AsyncS3Source integration test
zig build test-async-source -Dexperimental=true
```

## 📜 Documentation
- `STATUS.md`: High-level roadmap and progress tracking.
- `STATUS_CURRENT_DETAIL.md`: Granular technical details and lessons learned.
- `docs/high_performance_io_plan.md`: The architectural blueprint for the async engine.

