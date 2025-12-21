# ZPQ Technical Context & Deep Dive

**Last Updated**: Dec 21, 2025
**Current State**: **DNS Integration Reached**: The verified high-performance DNS stack is now wired into the async S3 engine. Connections are spreading across multiple resolved IPs.

## 🏆 Milestone 6: High-Performance Async DNS (Completed)
We have successfully implemented a tiered, asynchronous DNS resolver stack that decouples DNS lookups from the main event loop and enables aggressive parallel connection launching.

*   **Verification**: 
    *   **Integration Test**: `tests/io/test_dns.zig` successfully verified the entire stack.
    *   **Deduplication**: Proved that `SingleFlightResolver` merges concurrent requests for the same host.
    *   **Racing**: `SpeculativeResolver` correctly launches IPv4/IPv6 races.
    *   **Memory Hygiene**: Verified zero leaks using `GeneralPurposeAllocator`.
*   **Key Components**:
    *   **`ThreadPoolResolver`**: Offloads `getaddrinfo` to `libxev.ThreadPool`.
    *   **`SingleFlightResolver`**: Deduplicates lookups at the bucket/endpoint level (6 attempts -> 1 query).
    *   **`SpeculativeResolver`**: Foundations for "Happy Eyeballs" and fast connection startup.

## 🚀 Milestone 7: DNS Integration & Advanced I/O (In Progress)
**Goal**: Wire the DNS stack into `AsyncS3Source` and move to a pure event-driven transport lifecycle.

### Progress & Verification:
*   [x] **DNS Integration**: `AsyncS3Source` now holds a `dns.Resolver`.
*   [x] **Dynamic Resolution**: Hardcoded IPs removed. Bucket hosts are resolved at source initialization.
*   [x] **IP Round-Robin**: Every call to `connectNew()` now cycles through the list of resolved IPs. This maximizes parallel throughput into the AWS frontend fleet.
*   [x] **EventLoop Refactor**: `src/zpq/io/s3/event_loop.zig` now wraps `libxev.Loop`. This ensures the DNS stack works on Linux/WSL2 and macOS with zero code changes.
*   [x] **Verification**: `tests/io/test_async_source.zig` confirms that we resolve "127.0.0.1" (or real hosts) and initiate connections to the correct network address.

### Technical Blueprint (Pure Async Next Steps):
1.  **Refactor `AsyncRequest`**:
    *   Current: `WouldBlock` polling loop in `main.zig`.
    *   Goal: Pure completion-based lifecycle using `libxev` watchers.
2.  **State Machine Evolution**:
    *   `AsyncRequest` will register completions for `Connect`, `Send`, and `Recv`.
    *   The loop will drive state transitions, eliminating "busy waiting" or serial ticks.
3.  **Connection Pool**:
    *   Implement persistent socket reuse keyed by `(host, port, tls)`.
    *   Handle server-side keep-alive timeouts.

---

## 🧠 Lessons Learned: Working with Zig 0.16.x & ZPQ Workflow

### 1. Zig 0.16.x Breaking Changes & Patterns
*   **Unmanaged Containers**: `std.ArrayListUnmanaged` and `std.StringHashMap` are required for high-performance zero-heap paths. Always pass the allocator to every operation.
*   **Alignment Safety**: `xev.shim_net.Address` is critical for handling `sockaddr` alignment correctly. `@ptrCast(@alignCast(&addr))` is your friend when bridging between raw memory and specialized address types.
*   **Static vs Dynamic xev**: We chose the Static path (`xev.Loop`) for raw performance, avoiding vtable jumps in the hot I/O loop.

### 2. Workflow & Testing Strategy
*   **Micro-Test Driven Development (MTDD)**: Proving the DNS middleware in isolation (`test_dns.zig`) was the only reason the integration into `AsyncS3Source` was smooth.
*   **Probing Mandatory**: When Zig 0.16 behavior is unclear, a 20-line `probe_*.zig` file is 10x faster than trying to interpret compiler errors in a large project.

---

## 🏗️ Legacy Stack (Reference/Backup)
Removed. All core logic migrated to `src/zpq/io/s3/`.
