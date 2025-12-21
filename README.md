# ZPQ: High-Performance Zig Parquet Tool

ZPQ is a specialized Parquet utility optimized for high-throughput and low-latency data access in cloud-native environments, specifically targeting AWS S3 and Lambda.

## Design Philosophy

ZPQ is engineered for specific use cases where performance and footprint are critical.

### What ZPQ Is:
- **Cloud-Latency Optimized**: Specialized for S3 `GET Range` and `HEAD` operations with aggressive connection management.
- **Async-First**: Built on a non-blocking completion state machine for massive concurrency on a single thread.
- **Zero-Dependency Core**: Features a "bare-metal" implementation of S3, SigV4, and HTTP/1.1 to eliminate the overhead of generic SDKs.
- **Embedded-Ready**: Designed with a minimal binary footprint and low memory overhead for AWS Lambda cold-start optimization.

### What ZPQ Is Not:
- **General-Purpose Parquet SDK**: Does not aim to support every possible Parquet feature; prioritizes performance for common analytical workloads.
- **Database Engine**: ZPQ is a tool for reading and scanning data, not for long-term storage management or complex query planning.
- **Standard SDK Wrapper**: It does not wrap existing libraries (like `aws-sdk-cpp`); it implements the wire protocols directly in Zig.

## Architectural Guardrails

The development of ZPQ is guided by strict architectural principles to ensure maintainability and performance:

- **Protocol/Transport Decoupling (Sans-I/O)**: Protocol logic (S3 request formatting, SigV4 signing) is strictly separated from socket operations. Logic is tested against abstract buffers, ensuring correctness without network mocks.
- **Interface-Driven I/O**: Business logic (`ParquetFile`, `Decoder`) interacts exclusively with the `RandomAccessSource` interface. Low-level primitives like `libxev` or `epoll` are isolated within transport implementations.
- **Unmanaged Container Pattern**: High-performance paths utilize unmanaged containers (`std.ArrayListUnmanaged`). All allocations are explicit, providing the caller with total control over memory lifecycles.
- **Pinned Resource Lifecycle**: System-level resources (like event loops and network buffers) are heap-allocated and pinned to ensure validity across asynchronous state transitions.

## Testing and Robustness

ZPQ undergoes rigorous verification across multiple dimensions:

- **Cross-Platform Portability**: Verified on Linux (utilizing `io_uring`), macOS (utilizing `kqueue`), and WSL2.
- **Architecture Support**: Native support and testing for both x86_64 and ARM64 (Graviton) architectures.
- **Isolation Testing**: The tiered DNS stack, TLS pump, and HTTP state machine are verified via standalone micro-tests before integration.
- **Memory Integrity**: All core operations are validated using the Zig `GeneralPurposeAllocator` to ensure zero memory leaks and correct alignment during high-concurrency interleaved I/O.

## Benchmarks

Initial performance data indicates ZPQ significantly outperforms generic implementations in scan throughput.

| Implementation | Local Throughput (Values) | S3 Metadata Latency |
| :--- | :--- | :--- |
| **ZPQ (Zig)** | **~857 MVal/s** | **~0.58s** |
| PyArrow (Python) | N/A | ~0.15s |
| Polars (Rust) | N/A | ~0.40s |

*Current optimizations are focused on narrowing the S3 metadata latency gap through persistent connection pooling.*

## Documentation
Refer to the following files for detailed information:
- `STATUS.md`: High-level roadmap and progress tracking.
- `STATUS_CURRENT_DETAIL.md`: Granular technical details and lessons learned.
- `docs/high_performance_io_plan.md`: The architectural blueprint for the asynchronous engine.
