# ZPQ Technical Context & Deep Dive

**Last Updated**: Dec 21, 2025
**Current State**: **Pure Async Lifecycle Reached**: The entire S3 stack is now event-driven. Polling loops have been replaced with non-blocking callbacks.

## 🏆 Milestone 6: High-Performance Async DNS (Completed)
We have successfully implemented a tiered, asynchronous DNS resolver stack that decouples DNS lookups from the main event loop and enables aggressive parallel connection launching.

## 🚀 Milestone 7: Pure Async Lifecycle (Completed)
**Goal**: Move to a pure event-driven transport lifecycle for S3.

### Progress & Verification:
*   [x] **DNS Integration**: `AsyncS3Source` now holds a `dns.Resolver`.
*   [x] **Dynamic Resolution**: Hardcoded IPs removed. Bucket hosts are resolved at source initialization.
*   [x] **IP Round-Robin**: Every call to `connectNew()` now cycles through the list of resolved IPs.
*   [x] **EventLoop Refactor**: `EventLoop` now heap-allocates `xev.Loop` to ensure pinning and cross-platform safety.
*   [x] **Transport Abstraction**: `Connection` (Plain/TLS) implemented with a non-blocking "Pump" pattern.
*   [x] **Pure Async Lifecycle**: `AsyncRequest` refactored to use callbacks. `WouldBlock` polling removed.
*   [x] **Verification**: All integration tests (`test_async_source`, `test_async_request`, `test_event_loop`) passed on Linux/WSL2.

### Technical Blueprint (Next Steps):
1.  **Persistent Connection Pool (Enhanced)**:
    *   Currently, the pool is simple. We need to add **Keep-Alive Timeouts** and **Stale Detection** (zero-byte read probe).
    *   **Goal**: Maximize reuse for heavy columnar scans (1000+ small ranges).

2.  **Repetition Levels (Milestone 8)**:
    *   Implement logic for nested Lists and Maps.
    *   **Challenge**: Efficiently skip large nested structures using the async gapped reader.

---

## 🧠 Lessons Learned: Working with Zig 0.16.x & ZPQ Workflow

### 1. Zig 0.16.x Breaking Changes & Patterns
*   **Pinned Loops**: `xev.Loop` MUST be pinned in memory. Moving the struct (e.g. returning by value from `init()`) will cause random segfaults in `io_uring` as internal pointers become invalid.
*   **Unmanaged Containers**: `std.ArrayListUnmanaged` is required for high-performance zero-heap paths.
*   **Alignment Safety**: `xev.shim_net.Address` handles `sockaddr` alignment correctly.

### 2. Workflow & Testing Strategy
*   **MTDD (Micro-Test Driven Development)**: Proving the `boring_tls` pump in `probe_tls_pump.zig` was critical before integrating it into the production `Connection` struct.
*   **State Machine Observability**: Adding `on_done` callbacks to `AsyncRequest` made it possible to drive batch operations without manual polling.

---

## 🏗️ Legacy Stack (Reference/Backup)
Removed. All core logic migrated to `src/zpq/io/s3/`.
