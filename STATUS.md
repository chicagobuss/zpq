# ZPQ Project Status

**The Fastest Serverless Parquet Engine.**

## 🔴 BLOCKING ISSUE: S3 Event Loop Conflict (2025-01-12)

### Problem
S3→S3 path crashes with `error.NestedRunsNotAllowed` when using parallel threads.

**Root Cause**: The S3 source (`AsyncS3Source`) shares the executor's xev event loop. When worker threads call `loop.run(.once)` for blocking S3 reads, it conflicts with the main thread (or other workers) also trying to drive the same loop.

```
Executor (main loop.run)
  └─ Worker Thread 1 → S3Source.readAt → loop.run(.once) ❌ CONFLICT
  └─ Worker Thread 2 → S3Source.readAt → loop.run(.once) ❌ CONFLICT
```

**Observation**: S3Sink already has a private loop (line 39 of s3_sink.zig) and works correctly.

### Current Behavior
| Configuration | Result |
|---------------|--------|
| `--threads 4` (default) | ❌ `NestedRunsNotAllowed` → segfault |
| `--threads 1` | ⚠️ Avoids loop conflict, but hits `EncodingError` (separate issue) |
| Local file (any threads) | ✅ Works (no event loop for local I/O) |

### Fix Options

#### Option A: Private Loop for S3Source (~30min)
Give S3Source its own private event loop like S3Sink.
- **Pros**: Minimal change, proven pattern
- **Cons**: Each S3 source spins its own loop, may have thread-affinity issues
- **Files**: `src/io/s3.zig` lines 38-43

#### Option B: Completion-Based Async S3 (~2-3hr)
Refactor S3Source to never call `loop.run()`. Instead, use xev completions and callbacks.
- **Pros**: True async, no blocking, proper concurrency
- **Cons**: Significant refactor, needs careful state management
- **Files**: `src/io/s3.zig` entire file

#### Option C: Serialize S3 Reads Through Main Loop (~1hr)
Worker threads don't call S3 directly. They post read requests to a queue; main loop services them.
- **Pros**: Single loop driver, no conflicts
- **Cons**: Loses parallel S3 reads (performance regression)
- **Files**: `src/core/executor.zig`, `src/io/s3.zig`

#### Option D: Skip Parallel S3 For Now (Immediate)
Force `--threads 1` for S3 sources while we fix the EncodingError bug first.
- **Pros**: Unblocks progress on other issues
- **Cons**: No parallel S3 performance

### Recommendation
Start with **Option A** (private loop) since it's proven to work for S3Sink. If that causes issues, escalate to Option B.

---

## 🟡 Secondary Issue: EncodingError on S3 Data

When running with `--threads 1`, we hit `error.EncodingError` on row group 0. This is a separate data decoding bug, likely in RLE/dictionary handling for certain column types.

---

## 🟢 Current Focus: Columnar Re-architecture
We are rewriting the core engine to use columnar batches and vectorized decoding, targeting Polars-competitive performance.

## 📊 Benchmarks (Latest - 100MB Parquet)
*   **Local -> S3**:
    *   **AWS CLI**: 6.84s (Raw Copy)
    *   **Polars**: 7.64s (Parallel Arrow)
    *   **ZPQ**: 13.49s (Serial Row-based)
*   **Bottleneck**: 35% Parquet encoding, 20% memcpy.

## 🗺️ Roadmap
*   **[COMPLETE] Phase 1: Read Dominance**
    *   Zero-Copy Page Reading, Async S3 Source.
*   **[COMPLETE] Phase 2: Write Dominance**
    *   Multipart S3 Sink, Parallel Row Group buffering.
*   **[BLOCKED] Phase 3: High Performance Architecture**
    *   ✅ Columnar Batch Container & Vectorized Reader
    *   ✅ Filter Evaluation (all types + AND/OR)
    *   ✅ Parallel Row Group Pipeline (local files)
    *   ❌ **S3 Parallel Reads** (blocked by event loop issue above)
    *   ⏳ Zero-Copy Fast Path
*   **[PLANNED] Phase 4: SQL Layer**
    *   Arrow C Data Interface
    *   SQLite VTable Integration

## 🛠️ Known Issues
*   **[CRITICAL]** S3 event loop conflict blocks parallel S3 reads (see above)
*   **[HIGH]** EncodingError on S3 data with single-threaded mode
*   Memory usage during massive parallel writes needs tuning (backpressure is working but aggressive).
*   "Fast Path" logic is designed but not yet wired into `main.zig`.

## 🧠 Knowledge Base
See `.agent/rules/` for the Tier 1-3 operating manuals.

**Key Architecture Docs:**
- [S3→S3 Pipeline Strategy](docs/tier_3_s3_pipeline_strategy.md) - The North Star for optimal S3 workloads

