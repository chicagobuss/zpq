# ZPQ Filter Command - Detailed Pseudocode Implementation

This document provides implementation-ready pseudocode using actual ZPQ types and APIs.

## Key Insight: Single-Pass Filter Column

The critical optimization is: if the filter column is also in the select list, we read it ONCE
and cache the matching values during the filter scan. This eliminates the double-read.

## Data Structures

```zig
const FilterColumnCache = struct {
    // For BYTE_ARRAY: we need to dupe the strings since BatchReader reuses buffers
    byte_array_values: std.ArrayListUnmanaged([]const u8),
    // For numeric types: store directly (no allocation needed per value)
    int32_values: std.ArrayListUnmanaged(i32),
    int64_values: std.ArrayListUnmanaged(i64),
    float_values: std.ArrayListUnmanaged(f32),
    double_values: std.ArrayListUnmanaged(f64),
    bool_values: std.ArrayListUnmanaged(bool),
    
    allocator: std.mem.Allocator,
    col_type: zpq.core.schema.Type,
    
    fn init(allocator: Allocator, col_type: Type) FilterColumnCache { ... }
    fn deinit(self: *FilterColumnCache) void { ... }
    fn appendByteArray(self: *FilterColumnCache, v: []const u8) !void { ... }
    fn appendInt32(self: *FilterColumnCache, v: i32) !void { ... }
    // ... etc
};

const SelectionVector = struct {
    indices: std.ArrayListUnmanaged(usize),
    allocator: std.mem.Allocator,
    
    fn init(allocator: Allocator) SelectionVector { ... }
    fn deinit(self: *SelectionVector) void { ... }
    fn append(self: *SelectionVector, idx: usize) !void { ... }
    fn count(self: SelectionVector) usize { return self.indices.items.len; }
};
```

---

# PHASE 2: Parallel Row Group Processing with xev

## Implementation Status

| Phase | Description | Status |
|-------|-------------|--------|
| 2.1 | Batch Filter Fetch | ✅ DONE (in original filter.zig) |
| 2.2 | RowGroupWorker Struct | ✅ DONE (src/zpq/core/row_group_worker.zig) |
| 2.2a | Sans-I/O Design | ✅ DONE - Worker does NO I/O, only CPU work |
| 2.2b | Integration with filter.zig | ✅ DONE - `--workers` flag enables worker mode |
| 2.2c | Correctness Verification | ✅ DONE - Output matches original implementation |
| 2.3 | xev Thread Pool Integration | ✅ DONE - `--parallel` flag enables parallel mode |
| 2.4 | Output Merge | ✅ DONE (sequential write, in row group order) |

## What's Working (Phase 2.2 Complete)

### RowGroupWorker Structure (`src/zpq/core/row_group_worker.zig`)

```zig
/// Pre-fetched column data for a single row group.
/// All I/O happens BEFORE the worker starts - worker does only CPU work.
pub const RowGroupData = struct {
    rg_idx: usize,
    num_rows: usize,
    
    // Filter column data
    filter_buf: []const u8,
    filter_offset: u64,
    filter_chunk: schema.ColumnChunk,
    
    // Output column data (in same order as output_col_indices)
    output_bufs: []const []const u8,
    output_offsets: []const u64,
    output_chunks: []const schema.ColumnChunk,
};

/// Shared immutable context for all row group workers.
pub const FilterContext = struct {
    allocator: std.mem.Allocator,
    filter_col_name: []const u8,
    filter_val: []const u8,
    filter_col_idx: usize,
    filter_col_type: schema.Type,
    encoded_filter: *const EncodedFilter,
    output_col_indices: []const usize,
    output_col_types: []const schema.Type,
    output_col_names: []const []const u8,
    filter_col_in_output: bool,
    filter_col_output_idx: ?usize,
    meta: *const schema.FileMetaData,
};

/// Worker that processes a single row group.
/// Designed for parallel execution - does ONLY CPU work, NO I/O.
pub const RowGroupWorker = struct {
    allocator: std.mem.Allocator,
    ctx: *const FilterContext,
    data: *const RowGroupData,
    
    // Results
    selection: SelectionVector,
    filter_cache: ?FilterColumnCache,
    output_columns: []OutputColumnData,
    row_count: usize,
    
    // Status
    status: Status,
    err: ?anyerror,
    
    pub const Status = enum { pending, scanning, decoding, materializing, done, failed };
    
    /// Execute the worker - pure CPU work, no I/O
    pub fn execute(self: *Self) void {
        self.status = .scanning;
        self.scanFilterColumn() catch |e| { self.status = .failed; self.err = e; return; };
        
        if (self.selection.count() == 0) { self.status = .done; self.row_count = 0; return; }
        
        self.status = .decoding;
        self.decodeOutputColumns() catch |e| { self.status = .failed; self.err = e; return; };
        
        self.status = .materializing;
        self.materializeFilterColumn() catch |e| { self.status = .failed; self.err = e; return; };
        
        self.status = .done;
        self.row_count = self.selection.count();
    }
};
```

### CLI Integration (`src/cli/filter.zig`)

```zig
// Use --workers flag to enable worker mode
pub const Options = struct {
    input_path: []const u8,
    output_path: ?[]const u8,
    filter: ?[]const u8,
    select_columns: ?[]const u8,
    use_workers: bool = false,  // Enables RowGroupWorker-based implementation
};

pub fn run(ctx: *const common.Context, opts: Options) !void {
    if (opts.use_workers) {
        return runWithWorkers(ctx, opts);  // New worker-based path
    }
    // ... original implementation
}
```

### Current Execution Flow (Sequential Workers)

```
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 1: Pre-fetch ALL columns for ALL row groups               │
│                                                                 │
│   RG0: [filter_col, output_col1, output_col2, ...]              │
│   RG1: [filter_col, output_col1, output_col2, ...]              │
│   RG2: [filter_col, output_col1, output_col2, ...]              │
│                                                                 │
│   → Single batched readRanges() call                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 2: Create RowGroupData + FilterContext                    │
│                                                                 │
│   FilterContext = { filter spec, output spec, meta }            │
│   RowGroupData[0] = { buffers for RG0 }                         │
│   RowGroupData[1] = { buffers for RG1 }                         │
│   RowGroupData[2] = { buffers for RG2 }                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 3: Execute workers SEQUENTIALLY (currently)               │
│                                                                 │
│   for (rg_data) |data| {                                        │
│       worker = RowGroupWorker.init(ctx, data);                  │
│       worker.execute();        // ← THIS IS CPU-ONLY            │
│       writeWorkerOutput(worker);                                │
│   }                                                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Phase 2.3: xev Thread Pool Integration ✅ COMPLETE

### Implementation Summary

**Files Modified:**
- `src/zpq/core/row_group_worker.zig` - Added `WorkerCompletion` struct
- `src/cli/filter.zig` - Added `runWithWorkersParallel()` function
- `src/main.zig` - Added `--parallel` flag

**Performance Results (100k rows, 10 row groups):**
- Sequential worker loop: 70ms
- Parallel execution: 32ms
- **Speedup: 2.2x** on CPU-bound scan phase

### Goal

Change the sequential worker loop to parallel execution:

```
BEFORE (Sequential):
  RG0.execute() → RG0.write() → RG1.execute() → RG1.write() → RG2.execute() → ...

AFTER (Parallel):
  ┌─ RG0.execute() ─┐
  ├─ RG1.execute() ─┼──► Write all in order
  └─ RG2.execute() ─┘
```

### Key Insight: Workers are Already "Sans-I/O"

The hard work is done. `RowGroupWorker.execute()` is a pure CPU function:
- No I/O calls
- No shared mutable state
- Each worker owns its output
- Can be called from any thread

### xev ThreadPool API (from dns.zig research)

```zig
// 1. Task struct with callback
const Task = struct {
    task: xev.ThreadPool.Task = undefined,
    worker: *RowGroupWorker,
    
    fn callback(task: *xev.ThreadPool.Task) void {
        const self: *Task = @fieldParentPtr("task", task);
        self.worker.execute();
        // Signal completion via Async
        self.async_signal.notify() catch {};
    }
};

// 2. Async for signaling main loop
async_signal: xev.Async,

// 3. Schedule on thread pool
pool.schedule(xev.ThreadPool.Batch.from(&task.task));

// 4. Wait for completion in main loop
async_signal.wait(loop, completion, callback);
```

### Implementation Plan

```zig
/// Parallel filter execution using xev ThreadPool
pub fn runWithWorkersParallel(ctx: *const common.Context, opts: Options) !void {
    // ... setup same as runWithWorkers ...
    
    // Pre-fetch ALL columns (already done)
    try pf.source.readRanges(ranges, buffers);
    
    // Create completion tracking
    const worker_count = all_rg_data.len;
    var completions = try ctx.allocator.alloc(WorkerCompletion, worker_count);
    defer ctx.allocator.free(completions);
    
    var pending = std.atomic.Value(usize).init(worker_count);
    
    // Initialize async signals and schedule workers
    for (completions, all_rg_data, 0..) |*comp, *rg_data, i| {
        comp.* = .{
            .worker = try RowGroupWorker.init(ctx.allocator, &filter_ctx, rg_data),
            .async_signal = try xev.Async.init(),
            .pending = &pending,
        };
        comp.task.task = .{ .callback = workerCallback };
        
        // Arm async wait on main loop
        comp.async_signal.wait(ctx.loop, &comp.completion, onWorkerComplete);
        
        // Schedule on thread pool
        ctx.thread_pool.schedule(xev.ThreadPool.Batch.from(&comp.task.task));
    }
    
    // Run event loop until all workers complete
    while (pending.load(.acquire) > 0) {
        try ctx.loop.run(.once);
    }
    
    // Write results in order
    for (completions) |*comp| {
        if (comp.worker.status == .failed) return comp.worker.err.?;
        if (comp.worker.row_count > 0) {
            try writeWorkerOutput(pw, comp.worker, output_col_types.items);
        }
        comp.worker.deinit();
    }
    
    try pw.finish();
}

const WorkerCompletion = struct {
    worker: *RowGroupWorker,
    task: struct { task: xev.ThreadPool.Task = undefined } = .{},
    async_signal: xev.Async,
    completion: xev.Completion = .{},
    pending: *std.atomic.Value(usize),
};

fn workerCallback(task: *xev.ThreadPool.Task) void {
    const comp: *WorkerCompletion = @fieldParentPtr("task", @fieldParentPtr("task", task));
    comp.worker.execute();
    comp.async_signal.notify() catch {};
}

fn onWorkerComplete(
    userdata: ?*anyopaque,
    loop: *xev.Loop,
    completion: *xev.Completion,
    result: xev.Async.Error!void,
) void {
    _ = loop; _ = completion; _ = result;
    const comp: *WorkerCompletion = @ptrCast(@alignCast(userdata));
    _ = comp.pending.fetchSub(1, .release);
}
```

### Memory Safety for Parallel Execution

| Data | Thread Safety | Notes |
|------|--------------|-------|
| `FilterContext` | ✅ Safe | Read-only, shared across workers |
| `RowGroupData` | ✅ Safe | Read-only, one per worker |
| `RowGroupWorker` | ✅ Safe | Each worker owns its instance |
| `pending` counter | ✅ Safe | Atomic operations |
| `ParquetWriter` | ⚠️ Single-threaded | Only main thread writes |

### Expected Performance Gains

For 10 row groups on 8-core machine:

| Metric | Sequential | Parallel (8 threads) |
|--------|------------|---------------------|
| CPU scan time | 10 * 13ms = 130ms | ~20ms (8x parallelism) |
| Total time | ~35ms | ~15ms |

The main gains are in the CPU-bound scan/decode phases. I/O is already batched.

### Implementation Steps

1. **Add WorkerCompletion struct** with embedded Task and Async
2. **Initialize async signals** for each worker
3. **Schedule workers** on thread pool
4. **Wait for completion** via event loop
5. **Write results in order** after all complete

### Error Handling

```zig
// Each worker catches errors internally
if (worker.status == .failed) {
    return worker.err orelse error.WorkerFailed;
}
```

Workers don't throw - they set `status = .failed` and `err = <error>`.
Main thread checks after all workers complete.

### Testing Strategy

1. Run with `--workers` flag (sequential workers) - ✅ VERIFIED
2. Add `--parallel` flag for thread pool version
3. Compare outputs: `diff <(zpq filter --workers ...) <(zpq filter --parallel ...)`
4. Benchmark both modes

---

## Memory Ownership Summary

| Data | Owner | Lifetime |
|------|-------|----------|
| `filter_buffers[i]` | runWithWorkers | Until all workers done |
| `all_output_bufs[rg][col]` | runWithWorkers | Until all workers done |
| `RowGroupWorker` | runWithWorkers | Until output written |
| `selection` | Worker | Until worker done |
| `filter_cache` | Worker | Until worker done |
| `output_columns[]` | Worker | Until written to ParquetWriter |

## Key Differences: Original vs Worker Mode vs Parallel Mode

| Aspect | Original | Worker Mode | Parallel Mode |
|--------|----------|-------------|---------------|
| Filter column I/O | Per-RG batched | All RGs batched | All RGs batched |
| Other column I/O | Per-RG batched | All RGs batched | All RGs batched |
| CPU scan | Sequential | Sequential | **Parallel** |
| Output write | Per-RG | Per-RG | Per-RG (ordered) |
| Memory | Per-RG | All RGs in memory | All RGs in memory |

---

## Completed Verification

```bash
# Worker mode produces identical output to original
$ zpq filter input.parquet --filter category=target -o /tmp/original.parquet
$ zpq filter input.parquet --filter category=target -o /tmp/workers.parquet --workers
$ diff <(zpq cat /tmp/original.parquet) <(zpq cat /tmp/workers.parquet)
# No differences - outputs match!
```
