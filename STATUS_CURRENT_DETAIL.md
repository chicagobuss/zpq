# ZPQ Technical Context and Detail

**Last Updated**: Dec 21, 2025
**Current State**: Pure Asynchronous Lifecycle Reached. The S3 engine is fully event-driven, utilizing non-blocking callbacks for all I/O operations.

## Milestone 6: High-Performance Asynchronous DNS (Completed)
Implementation of a tiered, asynchronous DNS resolver stack that decouples lookups from the main event loop and enables parallel connection launching.

### Verification Results:
*   **Integration Testing**: `tests/io/test_dns.zig` verified the complete stack functionality.
*   **Deduplication**: `SingleFlightResolver` successfully merged concurrent requests for identical hosts.
*   **Racing**: `SpeculativeResolver` correctly executed parallel IPv4/IPv6 lookups.
*   **Memory Management**: Verified zero memory leaks using the `GeneralPurposeAllocator`.

### Core Components:
*   **ThreadPoolResolver**: Offloads `getaddrinfo` syscalls to `libxev.ThreadPool`.
*   **SingleFlightResolver**: Deduplicates lookups at the endpoint level.
*   **SpeculativeResolver**: Provides the foundation for "Happy Eyeballs" and accelerated connection initialization.

## Milestone 7: Pure Asynchronous Lifecycle (Completed)
Successful migration to a pure event-driven transport lifecycle for S3 operations.

### Implementation and Verification:
*   **DNS Integration**: `AsyncS3Source` incorporates the `dns.Resolver` interface.
*   **Dynamic Resolution**: Removed hardcoded IP addresses; bucket hosts are resolved at source initialization.
*   **IP Round-Robin**: `connectNew()` cycles through resolved IP addresses to maximize parallel throughput.
*   **EventLoop Refactor**: `EventLoop` now utilizes heap-allocated `xev.Loop` to ensure memory pinning and cross-platform reliability.
*   **Transport Abstraction**: `Connection` (supporting both Plain TCP and TLS) implemented with a non-blocking "Pump" pattern.
*   **Asynchronous Lifecycle**: `AsyncRequest` refactored to utilize callbacks, eliminating polling.
*   **Verification**: All integration tests (`test_async_source`, `test_async_request`, `test_event_loop`) passed on Linux/WSL2.

### Future Technical Objectives:
1.  **Enhanced Connection Pool**:
    *   Integrate keep-alive timeouts and stale connection detection (zero-byte read probes).
    *   Objective: Maximize connection reuse for high-frequency columnar scans.
2.  **Repetition Levels (Milestone 8)**:
    *   Implementation of logic for nested Parquet structures (Lists and Maps).
    *   Constraint: Efficiently skipping large nested structures using the asynchronous gapped reader.

## Lessons Learned: Zig 0.16.x and Development Workflow

### 1. Zig 0.16.x Technical Patterns
*   **Pinned Loops**: `xev.Loop` must remain pinned in memory. Relocating the structure (e.g., returning by value) results in memory corruption in backends like `io_uring` due to invalid internal pointers.
*   **Unmanaged Containers**: `std.ArrayListUnmanaged` is mandatory for high-performance, zero-heap execution paths.
*   **Memory Alignment**: `xev.shim_net.Address` is used to ensure correct `sockaddr` alignment across platforms.

### 2. Testing and Validation Strategy
*   **Micro-Test Driven Development (MTDD)**: Prototyping the `boring_tls` pump in isolation was essential prior to integration into the core `Connection` structure.
*   **Asynchronous Observability**: The introduction of `on_done` callbacks in `AsyncRequest` enabled deterministic batch operation tracking without manual polling.

---

## Legacy Documentation
The legacy I/O stack has been decommissioned. All active logic is located in `src/zpq/io/s3/`.
