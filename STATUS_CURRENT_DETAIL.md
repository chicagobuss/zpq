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
We are transitioning from a **Polling State Machine** (busy-waiting on `WouldBlock`) to a **Pure Completion State Machine**.

1.  **Completion Ownership**:
    *   `AsyncRequest` will embed `xev.Completion` and `xev.TCP` structs.
    *   **Probing Strategy**: Create `probe_xev_tcp_lifecycle.zig` to verify the latest `xev.TCP.connect` and `read/write` signatures. The Zig 0.16 `std.posix` transition changed how FDs are passed to event loops.
    *   **Microtest**: `tests/io/test_async_state_machine.zig` will verify the "Connect -> Send -> Recv" chain without S3 logic.

2.  **Callback-Driven State Machine**:
    *   Each state will have a dedicated `libxev` callback (e.g., `onConnect`, `onWrite`, `onRead`).
    *   **Concurrency**: This allows 100+ requests to be truly interleaved on a single thread.
    *   **Zero-Alloc Transition**: No memory should be allocated when moving between "Headers Received" and "Reading Body Segments".

3.  **TLS "Pump" Integration**:
    *   **Web Search Task**: Profusely search for "libxev TLS adapter patterns" and "BoringSSL async BIO pump" to ensure our `TlsAdapter` doesn't deadblock when the loop is driving multiple completions.
    *   **Refactor**: Integrate `TlsAdapter` directly into the `xev` callback chain so TLS handshakes happen "asynchronously" without blocking other requests.

4.  **Persistent Connection Pool**:
    *   Implement LIFO reuse with keyed host/port/tls.
    *   Implement "Stale Check": Before handing a connection back, perform a zero-byte `read` completion to see if the server closed it.

### Next Session Focus:
*   [ ] Create `probe_xev_tcp_lifecycle.zig` to lock down the `libxev` API for 0.16.dev.
*   [ ] Refactor `AsyncRequest` header serialization to be fully unmanaged.
*   [ ] Implement the completion-based "Connect" flow in `AsyncS3Source`.

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
