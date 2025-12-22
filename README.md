# ZPQ: A Zig Parquet Tool

ZPQ is a Parquet utility designed for fast data access across both local filesystems and cloud object storage, with a specific focus on AWS S3 and Lambda. It is built to support multiple storage providers through an abstract I/O interface.

## Design Philosophy

ZPQ is built for scenarios where resource utilization and execution time are primary constraints.

### Characteristics:
- **Asynchronous I/O**: Utilizes a completion-based state machine for interleaved request execution on a single thread.
- **Minimal Dependencies**: The S3, SigV4, and HTTP/1.1 protocols are implemented directly in Zig to eliminate dependencies on large, general-purpose SDKs.
- **Resource Constraints**: Optimized for low memory overhead and minimal binary size to improve AWS Lambda cold-start times.

### Scope:
- **Analytical Workloads**: Focuses on performance for common analytical query patterns rather than exhaustive Parquet feature parity.
- **Read-Oriented**: Designed for scanning and extracting data; it is not a query engine or storage manager.
- **Direct Implementation**: Does not wrap the AWS C++ or Rust SDKs; all protocol logic is implemented natively to ensure zero-copy performance.

## Architectural Implementation

The codebase does its best to adhere to the following internal standards:

- **Protocol/Transport Separation**: Protocol logic (e.g., S3 request formatting) is decoupled from socket operations. This allows protocol verification against memory buffers without requiring network connectivity.
- **Abstract I/O**: Business logic interacts with a `RandomAccessSource` interface. Platform-specific primitives (such as `io_uring` or `kqueue`) are isolated within transport implementations.
- **Explicit Memory Management**: Performance-critical paths utilize unmanaged containers. All allocations are explicit, requiring the caller to manage resource lifecycles.
- **Memory Pinning**: Resources with strict identity requirements, such as event loops and network buffers, are heap-allocated to ensure validity across asynchronous transitions.

## Technical Philosophy

ZPQ tries to balance emerging Zig 0.16.dev features with stable development practices to ensure performance without incurring excessive technical debt.

### Supporting the Spirit of Zig 0.16
While targeting the latest master branch, ZPQ prioritizes the architectural "spirit" of the upcoming standard library:
- **Interface-First I/O**: Business logic is restricted to `std.Io` style interfaces. This ensures that the core engine remains decoupled from the specific event loop implementation.
- **Unmanaged Containers**: We adopt the 0.16 pattern of unmanaged containers (`ArrayListUnmanaged`, etc.) to make memory allocation explicit and visible throughout the hot path.
- **Composition over Inheritance**: Transport implementations are composed of independent parts (DNS, TCP, TLS) rather than hidden behind a monolithic client object.

### Library Selection and the "Re-write" Rationale
ZPQ is selective about external dependencies, favoring native re-writes for the core protocol stack to maintain performance and small binary sizes.

**Curated Dependencies:**
- **`libxev`**: A high-performance, cross-platform event loop abstraction. We use it to bridge the gap while Zig's `std.Io` event loop matures. It is isolated behind our internal abstractions for a future transition to native Zig async.
- **`boring_tls`**: Provides secure transport via statically-linked BoringSSL. This is required for secure communication with S3.
- **`libc`**: Required for system-level networking primitives and DNS resolution (via `getaddrinfo`).

**Protocol Re-writes (S3, HTTP/1.1, SigV4):**
These are implemented natively in ZPQ to avoid the significant overhead of general-purpose SDKs. This allows for specialized optimizations like zero-allocation gap skipping.

## Testing and Verification

ZPQ is tested for stability across several environments:

- **Platform Support**: Verified on Linux (`io_uring`), macOS (`kqueue`).
- **Architecture Support**: Native testing for x86_64 and ARM64.
- **Component Isolation**: Critical components (DNS stack, TLS integration, HTTP state machine) are verified through independent micro-tests prior to integration.
- **Memory Safety**: Validated with the Zig `GeneralPurposeAllocator` to confirm the absence of leaks and correct alignment during concurrent operations.
- **Property-Based Verification**: We utilize `minish` for fuzzing and property-based testing. This allows us to verify invariants (e.g., "re-serializing a struct matches the original") and automatically shrink complex crash cases into minimal reproductions. This is particularly critical for the Thrift metadata parser and HTTP header handling.

## Performance

// TODO

## Documentation
Additional technical detail is available in the following files:
- `STATUS.md`: Project roadmap and milestone tracking.
- `STATUS_CURRENT_DETAIL.md`: Technical implementation notes and lessons learned.
- `docs/high_performance_io_plan.md`: Architectural design for the asynchronous I/O engine.
