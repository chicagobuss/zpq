# ZPQ: High-Performance Zig Parquet Tool

ZPQ is a specialized Parquet utility designed for high-throughput and low-latency data access, specifically optimized for cloud-native environments such as AWS S3 and Lambda.

## Architecture: Milestone 7 Overview

ZPQ utilizes a custom-built asynchronous I/O engine designed to maximize throughput by leveraging Zig's low-level control and modern system primitives.

### Key Technical Features:
- **Event-Driven Lifecycle**: Built on `libxev` (Static backend), ZPQ employs a non-blocking completion state machine. This eliminates busy-waiting and polling loops.
- **Interleaved Concurrency**: Supports hundreds of concurrent range requests to S3 on a single thread.
- **Zero-Allocation Gaps**: The HTTP/1.1 engine can skip "gaps" between Parquet columns at the socket level without allocating memory for discarded bytes.
- **Tiered Asynchronous DNS**: Employs a speculative DNS stack that races IPv4/IPv6 and utilizes single-flight deduplication to minimize connection latency.
- **Cross-Platform Compatibility**: Supports `io_uring` on Linux and `kqueue` on macOS with a unified completion-based API.

## Benchmarks

ZPQ aims to be the leading Parquet reader for cloud-latency scenarios.

| Implementation | Local Throughput (Values) | S3 Metadata Latency |
| :--- | :--- | :--- |
| **ZPQ (Zig)** | **~857 MVal/s** | **~0.58s (WIP)** |
| PyArrow (Python) | N/A | ~0.15s |
| Polars (Rust) | N/A | ~0.40s |

*S3 metadata latency is currently under optimization in Milestone 8 (Persistent Connection Pooling).*

## Project Status

ZPQ is currently in active development.

- **Milestone 6**: High-Performance Asynchronous DNS [COMPLETE]
- **Milestone 7**: Pure Asynchronous Lifecycle & Transport Abstraction [COMPLETE]
- **Milestone 8**: Enhanced Connection Pooling & Repetition Levels [IN PROGRESS]

## Usage

ZPQ requires the Zig compiler (0.16.dev).

```bash
# Build the project
just build

# Run asynchronous I/O integration tests
zig build test-io -Dexperimental=true

# Run the AsyncS3Source full-stack test
zig build test-async-source -Dexperimental=true
```

## Documentation
- `STATUS.md`: High-level roadmap and progress tracking.
- `STATUS_CURRENT_DETAIL.md`: Granular technical details and internal benchmarks.
- `docs/high_performance_io_plan.md`: Architectural blueprint for the asynchronous engine.
