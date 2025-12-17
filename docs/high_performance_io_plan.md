# High Performance I/O Plan

## 🧭 High-Performance HTTP Plan (Connection Reuse + Evented I/O)

ZPQ’s current S3 support is functional, but `std.http.Client` has known limitations with aggressive keep-alive reuse in Zig 0.16 dev.
We are moving to a purpose-built “Bare Metal” HTTP/1.1 client specialized for S3 (`HEAD` + `GET Range`) that:
- Owns sockets explicitly (no hidden state machine).
- Reuses connections deterministically (keep-alive + pooling).
- Evolves from blocking correctness → evented kqueue/epoll for concurrency.

### 📚 Reference Map (Bun)
We keep Bun as a “how the pros do it” reference, but only need a small subset:
- **Event loop (kqueue/epoll abstraction)**: `references/bun/src/deps/uws/Loop.zig`
- **HTTP client thread ownership + lifecycle**: `references/bun/src/http/HTTPThread.zig`
- **Keep-alive pooling + release semantics**: `references/bun/src/http/HTTPContext.zig` (see `pending_sockets` + `releaseSocket(...)`)
- **Request execution plumbing**: `references/bun/src/http/AsyncHTTP.zig`
- **S3 usage path**: `references/bun/src/s3/client.zig` (S3 ops → `bun.http.AsyncHTTP.init(...)`)

### 🔬 Architecture Insights (Polars & Arrow)
After analyzing `references/polars` and `references/arrow`, we adopt the following patterns:

#### 1. Range Coalescing (Polars Strategy)
Source: `polars-io/src/cloud/polars_object_store.rs`
- **Goal**: Minimize request count without wasting bandwidth on huge gaps.
- **Logic**: Merge adjacent ranges if the gap is small.
- **Heuristic**: Merge if `gap < 12.5%` of the total request size, clamped to `[1MB, 8MB]`.
- **Implementation**: `merge_ranges(ranges: &[Range])` -> `[(merged_range, original_indices...)]`.

#### 2. Request Splitting (Polars Strategy)
Source: `polars-io/src/cloud/polars_object_store.rs`
- **Goal**: Maximize throughput by parallelizing large reads.
- **Logic**: Split requests larger than `chunk_size` (64MB) into multiple parallel fetches.
- **Implementation**: `split_range(range)`.

#### 3. Concurrency Model (Bun + Polars)
- **Event Loop**: Drive all socket I/O on a single thread (Bun style) using `kqueue`/`epoll`.
- **Throttling**: Use a semaphore/budget (Polars style) to limit active S3 requests (e.g., max 50 concurrent).

### ⚡ Zig Superpowers (ZPQ Unique Optimizations)
We leverage Zig's low-level control to exceed reference implementations:

#### 1. The "Zero-Allocation Gap" (Socket-Level Discard)
- **Problem**: Polars/Arrow allocate memory for the "gap" when merging ranges, limiting how aggressive merging can be (RAM waste).
- **ZPQ Solution**: Since we control the socket `read()` loop, we coalesce aggressively but **skip the allocation** for gap bytes.
    - `read(socket, dest_buf_A)`
    - `read(socket, small_stack_buf)` (loop to discard gap)
    - `read(socket, dest_buf_B)`
- **Benefit**: Zero memory cost for gaps, allowing larger gap tolerance (reducing RTTs further).

#### 2. The "No-HEAD" Open (Speculative Suffix)
- **Problem**: Opening a file typically requires `HEAD` (size) + `GET` (footer) = 2 RTTs.
- **ZPQ Solution**: Use S3 Suffix Range request (`Range: bytes=-N`).
    - Request last 64KB directly.
    - Parse `Content-Range: bytes START-END/TOTAL` to get the file size.
- **Benefit**: 1 RTT to get Footer + File Size. 50% startup latency reduction.

#### 3. Arena-Scoped Decompression
- **Problem**: Parquet involves thousands of tiny allocations.
- **ZPQ Solution**: Use `std.heap.ArenaAllocator` per RowGroup.
- **Benefit**: Freeing memory becomes a single pointer bump (no-op).

### 🎯 Next Milestones (ZPQ)
- [x] **(A) Deterministic correctness (local mock)**: keep `tools/mock_s3_server.zig` as the harness; keep tests killable via `tools/no_output_timeout.py`.
- [x] **(B) Raw keep-alive reuse**: make `RawS3Source` reuse a single socket for multiple range requests (no reconnect per read).
- [x] **(C) Connection pool**: keyed by `(scheme, host, port, tls-config)` with idle timeout + stale detection.
- [x] **(D) Evented I/O**:
    - [x] **Micro-test**: `test_event_loop.zig` (prove kqueue works).
    - [x] **Integration**: Integrate `EventLoop` into `RawS3Source` (or `AsyncS3Source`) to drive parallel range fetches.
    - [x] **Coalescing**: Implement Polars-style range merging/splitting.
- [x] **(E) Zig Superpowers (Micro-Tests)**:
    - [x] **Zero-Allocation Gap**: Test skipping bytes on the socket without allocation.
    - [x] **No-HEAD Open**: Test suffix range parsing from mock server.
    - [ ] **Arena Decompression**: (Phase 3) Benchmark arena vs generic allocator for heavy columnar allocs.
- [ ] **(F) TLS**: add HTTPS + session reuse for real S3 (then validate against MinIO + AWS S3).
