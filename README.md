# ZPQ: A Zig Parquet Tool

ZPQ is a Parquet utility designed for data access in cloud environments, with a specific focus on AWS S3 and Lambda.

## Design Philosophy

ZPQ is built for scenarios where resource utilization and execution time are primary constraints.

### Characteristics:
- **Asynchronous I/O**: Utilizes a completion-based state machine for interleaved request execution on a single thread.
- **Native Implementation**: S3, SigV4, and HTTP/1.1 protocols are implemented directly in Zig to minimize external dependencies.
- **Resource Constraints**: Optimized for low memory overhead and minimal binary size to improve AWS Lambda cold-start times.

### Scope:
- **Analytical Workloads**: Focuses on performance for common analytical query patterns rather than exhaustive Parquet feature parity.
- **Read-Oriented**: Designed for scanning and extracting data; it is not a query engine or storage manager.
- **Dependency Isolation**: Does not wrap existing C/C++ SDKs; all protocol logic is implemented natively.

## Architectural Implementation

The codebase adheres to the following internal standards:

- **Protocol/Transport Separation**: Protocol logic (e.g., S3 request formatting) is decoupled from socket operations. This allows protocol verification against memory buffers without requiring network connectivity.
- **Abstract I/O**: Business logic interacts with a `RandomAccessSource` interface. Platform-specific primitives (such as `io_uring` or `kqueue`) are isolated within transport implementations.
- **Explicit Memory Management**: Performance-critical paths utilize unmanaged containers. All allocations are explicit, requiring the caller to manage resource lifecycles.
- **Memory Pinning**: Resources with strict identity requirements, such as event loops and network buffers, are heap-allocated to ensure validity across asynchronous transitions.

## Testing and Verification

ZPQ is tested for stability across several environments:

- **Platform Support**: Verified on Linux (`io_uring`), macOS (`kqueue`), and WSL2.
- **Architecture Support**: Native testing for x86_64 and ARM64.
- **Component Isolation**: Critical components (DNS stack, TLS integration, HTTP state machine) are verified through independent micro-tests prior to integration.
- **Memory Safety**: Validated with the Zig `GeneralPurposeAllocator` to confirm the absence of leaks and correct alignment during concurrent operations.

## Performance

Initial measurements for scan throughput and metadata retrieval are documented below.

| Implementation | Local Throughput (Values) | S3 Metadata Latency |
| :--- | :--- | :--- |
| **ZPQ (Zig)** | **~857 MVal/s** | **~0.58s** |
| PyArrow (Python) | N/A | ~0.15s |
| Polars (Rust) | N/A | ~0.40s |

*S3 metadata retrieval time is currently subject to optimization via persistent connection management.*

## Documentation
Additional technical detail is available in the following files:
- `STATUS.md`: Project roadmap and milestone tracking.
- `STATUS_CURRENT_DETAIL.md`: Technical implementation notes and lessons learned.
- `docs/high_performance_io_plan.md`: Architectural design for the asynchronous I/O engine.
