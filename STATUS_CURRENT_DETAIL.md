# ZPQ Technical Context & Deep Dive

**Last Updated**: Dec 21, 2025
**Current State**: **DNS Milestone Reached**: High-performance, zero-blocking DNS stack implemented and verified. Ready for integration into the async S3 engine.

## 🏆 Milestone 6: High-Performance Async DNS (Completed)
We have successfully implemented a tiered, asynchronous DNS resolver stack that decouples DNS lookups from the main event loop and enables aggressive parallel connection launching.

*   **Verification**: 
    *   **Integration Test**: `tests/io/test_dns.zig` successfully verified the entire stack.
    *   **Deduplication**: Proved that `SingleFlightResolver` merges concurrent requests for the same host.
    *   **Racing**: `SpeculativeResolver` correctly launches IPv4/IPv6 races.
    *   **Memory Hygiene**: Verified zero leaks using `GeneralPurposeAllocator`.
*   **Key Components**:
    *   **`ThreadPoolResolver`**: Offloads `getaddrinfo` to `libxev.ThreadPool`.
    *   **`SingleFlightResolver`**: Deduplicates lookups at the bucket/endpoint level.
    *   **`SpeculativeResolver`**: Foundations for "Happy Eyeballs" and fast connection startup.

## ⚡ Performance Verification
*   **Benchmark**: DNS Deduplication (3 parallel requests)
*   **Result**: 6 logical attempts (3 requests × 2 races) -> **exactly 1 DNS query**.
*   **Impact**: Massive reduction in DNS round-trip overhead for high-concurrency S3 scanning.

## 🚀 Milestone 7: DNS Integration & Advanced Pooling (CURRENT)
**Goal**: Wire the new DNS stack into `AsyncS3Source` and implement a production-grade connection pool.

### Technical Blueprint:
1.  **Wiring**:
    *   Modify `AsyncS3Source.init` to accept a `dns.Resolver`.
    *   Replace hardcoded IP logic in `scheduler.zig` with dynamic resolution.
    *   Implement "Round-Robin" selection from the `dns.Address` list.
2.  **Connection Pool**:
    *   Implement persistent socket reuse keyed by `(host, port, tls)`.
    *   Handle idle timeouts and server-side disconnects.
3.  **N-Lane Trigger**:
    *   Enable the "Early Start" optimization: launch connections as soon as the first `N` IPs are resolved.

---

## 🧠 Lessons Learned: Working with Zig 0.16.x & ZPQ Workflow

### 1. Zig 0.16.x Breaking Changes & Patterns
*   **Unmanaged Containers**: `std.ArrayListUnmanaged` and `std.StringHashMap` are now the standard for performance.
    *   *Correction*: Always pass `allocator` to `append`, `put`, and `deinit`.
*   **Alignment Safety**: `xev.shim_net.Address` is critical for handling `sockaddr` alignment correctly.
    *   *Gotcha*: `@ptrCast(@alignCast(&addr))` is required when moving from raw bytes to specialized `sockaddr` types.
*   **C-ABI Boundaries**: `std.c.getaddrinfo` requires null-terminated strings. Use `allocator.dupeZ` for safety.

### 2. Workflow & Testing Strategy
*   **Micro-Test Driven Development (MTDD)**: 
    *   We built `tests/io/test_dns.zig` to verify the logic before touching the main `AsyncS3Source`. 
    *   This saved hours of debugging complex state machine interactions in the larger system.
*   **Probing is Mandatory**: When in doubt about a new Zig API, a 20-line `probe_*.zig` file is faster than reading the (often outdated) docs.

---

## 🏗️ Legacy Stack (Reference/Backup)
Removed. All core logic migrated to `src/zpq/io/s3/`.
