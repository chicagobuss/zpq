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

## Main Function Pseudocode

```zig
fn cmdFilterOptimized(
    allocator: Allocator,
    input_path: []const u8,
    output_path: []const u8,
    filter_str: []const u8,       // "col=val"
    select_columns_opt: ?[]const u8, // "col1,col2,col3" or null for all
    pf: *ParquetFile,
) !void {
    // =========================================================================
    // PHASE 1: PARSE AND PLAN
    // =========================================================================
    
    // Parse filter expression
    const eq_idx = std.mem.indexOfScalar(u8, filter_str, '=') orelse return error.InvalidFilter;
    const filter_col_name = filter_str[0..eq_idx];
    const filter_val = filter_str[eq_idx + 1 ..];
    
    // Parse select columns
    var select_col_names = std.ArrayListUnmanaged([]const u8){};
    defer select_col_names.deinit(allocator);
    if (select_columns_opt) |cols| {
        var iter = std.mem.splitScalar(u8, cols, ',');
        while (iter.next()) |col| {
            const trimmed = std.mem.trim(u8, col, " ");
            if (trimmed.len > 0) try select_col_names.append(allocator, trimmed);
        }
    }
    
    // Read footer (already done by caller, but for clarity)
    const meta = pf.metadata orelse return error.NoMetadata;
    
    // Find filter column
    var filter_col_idx: ?usize = null;
    var filter_col_type: ?Type = null;
    for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
        if (col.meta_data) |md| {
            const name = md.path_in_schema.items[md.path_in_schema.items.len - 1];
            if (std.mem.eql(u8, name, filter_col_name)) {
                filter_col_idx = idx;
                filter_col_type = md.type;
                break;
            }
        }
    }
    if (filter_col_idx == null) return error.FilterColumnNotFound;
    
    // Build output column list
    var output_cols = std.ArrayListUnmanaged(OutputColumn){};
    defer output_cols.deinit(allocator);
    
    // CRITICAL: Track if filter column is in output
    var filter_col_in_output: bool = false;
    var filter_col_output_idx: ?usize = null;
    
    if (select_col_names.items.len > 0) {
        for (select_col_names.items, 0..) |name, out_idx| {
            const col_idx = findColumnIndex(meta, name) orelse return error.ColumnNotFound;
            const col_type = getColumnType(meta, col_idx);
            try output_cols.append(allocator, .{ .idx = col_idx, .type = col_type, .name = name });
            
            if (col_idx == filter_col_idx.?) {
                filter_col_in_output = true;
                filter_col_output_idx = out_idx;
            }
        }
    } else {
        // All columns
        for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
            if (col.meta_data) |md| {
                const name = md.path_in_schema.items[md.path_in_schema.items.len - 1];
                try output_cols.append(allocator, .{ .idx = idx, .type = md.type, .name = name });
                if (idx == filter_col_idx.?) {
                    filter_col_in_output = true;
                    filter_col_output_idx = output_cols.items.len - 1;
                }
            }
        }
    }
    
    // Create EncodedFilter
    var encoded_filter = try EncodedFilter.parse(allocator, filter_val, filter_col_type.?);
    defer encoded_filter.deinit();
    
    // Build row group skip mask using statistics
    var rg_skip_mask = try allocator.alloc(bool, meta.row_groups.items.len);
    defer allocator.free(rg_skip_mask);
    @memset(rg_skip_mask, false);
    
    var active_rg_count: usize = 0;
    for (meta.row_groups.items, 0..) |_, rg_idx| {
        if (pf.shouldSkipRowGroup(rg_idx, filter_col_name, &encoded_filter)) {
            rg_skip_mask[rg_idx] = true;
        } else {
            active_rg_count += 1;
        }
    }
    
    // =========================================================================
    // PHASE 2: BATCH FETCH FILTER COLUMNS
    // =========================================================================
    
    // Allocate buffers for filter column data (one per active row group)
    var filter_buffers = try allocator.alloc([]u8, active_rg_count);
    var filter_offsets = try allocator.alloc(u64, active_rg_count);
    defer {
        for (filter_buffers) |buf| allocator.free(buf);
        allocator.free(filter_buffers);
        allocator.free(filter_offsets);
    }
    
    // Collect ranges
    var ranges = try allocator.alloc(Range, active_rg_count);
    defer allocator.free(ranges);
    
    var buf_idx: usize = 0;
    for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
        if (rg_skip_mask[rg_idx]) continue;
        
        const chunk = rg_meta.columns.items[filter_col_idx.?];
        const md = chunk.meta_data.?;
        
        // Calculate range (include dictionary page if present)
        var start: u64 = @intCast(md.data_page_offset);
        if (md.dictionary_page_offset) |dpo| {
            if (dpo < start) start = @intCast(dpo);
        }
        const len: u64 = @intCast(md.total_compressed_size);
        
        ranges[buf_idx] = .{ .start = start, .end = start + len };
        filter_buffers[buf_idx] = try allocator.alloc(u8, @intCast(len));
        filter_offsets[buf_idx] = start;
        buf_idx += 1;
    }
    
    // SINGLE batched I/O for ALL filter columns
    try pf.source.readRanges(ranges, filter_buffers);
    
    // =========================================================================
    // PHASE 3: INITIALIZE OUTPUT WRITER
    // =========================================================================
    
    var pw = try ParquetWriter.initWithOptions(allocator, output_path, .{ .compression = .SNAPPY });
    defer pw.deinit();
    
    // Build schema
    var col_defs = try allocator.alloc(ColumnDef, output_cols.items.len);
    defer allocator.free(col_defs);
    for (output_cols.items, 0..) |oc, i| {
        col_defs[i] = .{ .name = oc.name, .type = oc.type };
    }
    try pw.setColumns(col_defs);
    
    // =========================================================================
    // PHASE 4: PROCESS EACH ROW GROUP
    // =========================================================================
    
    var filter_buf_idx: usize = 0;
    var total_output_rows: u64 = 0;
    
    for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
        if (rg_skip_mask[rg_idx]) continue;
        
        const num_rows: usize = @intCast(rg_meta.num_rows);
        
        // ----- PHASE 4a: Create MemorySource from pre-fetched buffer -----
        const prefetched_buf = filter_buffers[filter_buf_idx];
        const prefetched_offset = filter_offsets[filter_buf_idx];
        filter_buf_idx += 1;
        
        var mem_source = MemorySource.initWithOffset(prefetched_buf, prefetched_offset);
        
        // Get filter column metadata
        const filter_chunk = rg_meta.columns.items[filter_col_idx.?];
        const filter_md = filter_chunk.meta_data.?;
        const filter_levels = meta.getColumnLevels(filter_md.path_in_schema.items);
        const filter_schema_elem = meta.getColumnSchema(filter_md.path_in_schema.items);
        const filter_type_len = if (filter_schema_elem) |se| se.type_length else null;
        
        // Create column reader from memory source
        const filter_col_reader = try ColumnReader.init(mem_source.source(), filter_chunk);
        
        // ----- PHASE 4b: Scan filter column, build selection vector -----
        // AND cache values if filter column is in output
        
        var selection = SelectionVector.init(allocator);
        defer selection.deinit();
        
        var filter_cache: ?FilterColumnCache = null;
        defer if (filter_cache) |*fc| fc.deinit();
        if (filter_col_in_output) {
            filter_cache = FilterColumnCache.init(allocator, filter_col_type.?);
        }
        
        // Type-dispatch for filter scan
        switch (filter_col_type.?) {
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                var reader = BatchReader([]const u8).init(
                    allocator, filter_col_reader, filter_md.type,
                    @intCast(filter_levels.max_def), @intCast(filter_levels.max_rep),
                    filter_type_len,
                );
                defer reader.deinit();
                
                var row_idx: usize = 0;
                var buf: [1024]?[]const u8 = undefined;
                
                while (row_idx < num_rows) {
                    const batch_size = @min(1024, num_rows - row_idx);
                    const n_read = try reader.nextBatch(buf[0..batch_size]);
                    if (n_read == 0) break;
                    
                    for (buf[0..n_read], 0..) |maybe_val, i| {
                        if (maybe_val) |v| {
                            if (encoded_filter.matchesBytes(v)) {
                                try selection.append(row_idx + i);
                                // Cache value if needed for output
                                if (filter_cache) |*fc| {
                                    try fc.appendByteArray(try allocator.dupe(u8, v));
                                }
                            }
                        }
                    }
                    row_idx += n_read;
                }
            },
            .INT32 => {
                try scanFilterColumnTyped(i32, allocator, filter_col_reader, ...);
            },
            // ... other types
        }
        
        // If no matches, skip this row group entirely
        if (selection.count() == 0) continue;
        
        total_output_rows += selection.count();
        
        // ----- PHASE 4c: Fetch other output columns -----
        // Calculate ranges for non-filter output columns
        
        var other_col_count: usize = 0;
        for (output_cols.items) |oc| {
            if (oc.idx != filter_col_idx.?) other_col_count += 1;
        }
        
        var other_ranges = try allocator.alloc(Range, other_col_count);
        var other_buffers = try allocator.alloc([]u8, other_col_count);
        var other_offsets = try allocator.alloc(u64, other_col_count);
        defer {
            for (other_buffers) |buf| allocator.free(buf);
            allocator.free(other_ranges);
            allocator.free(other_buffers);
            allocator.free(other_offsets);
        }
        
        var other_idx: usize = 0;
        for (output_cols.items) |oc| {
            if (oc.idx == filter_col_idx.?) continue;
            
            const chunk = rg_meta.columns.items[oc.idx];
            const md = chunk.meta_data.?;
            
            var start: u64 = @intCast(md.data_page_offset);
            if (md.dictionary_page_offset) |dpo| {
                if (dpo < start) start = @intCast(dpo);
            }
            const len: u64 = @intCast(md.total_compressed_size);
            
            other_ranges[other_idx] = .{ .start = start, .end = start + len };
            other_buffers[other_idx] = try allocator.alloc(u8, @intCast(len));
            other_offsets[other_idx] = start;
            other_idx += 1;
        }
        
        // Batch fetch other columns
        if (other_col_count > 0) {
            try pf.source.readRanges(other_ranges, other_buffers);
        }
        
        // ----- PHASE 4d: Materialize output columns -----
        
        var out_rg = try pw.beginRowGroup();
        
        other_idx = 0;
        for (output_cols.items, 0..) |oc, out_col_idx| {
            if (oc.idx == filter_col_idx.?) {
                // USE CACHED VALUES - no re-read!
                switch (oc.type) {
                    .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                        try out_rg.writeByteArrayColumn(filter_cache.?.byte_array_values.items);
                    },
                    .INT32 => try out_rg.writeInt32Column(filter_cache.?.int32_values.items),
                    .INT64 => try out_rg.writeInt64Column(filter_cache.?.int64_values.items),
                    // ... etc
                }
            } else {
                // Read from pre-fetched buffer using selection vector
                var col_mem_source = MemorySource.initWithOffset(
                    other_buffers[other_idx], 
                    other_offsets[other_idx]
                );
                other_idx += 1;
                
                const chunk = rg_meta.columns.items[oc.idx];
                const col_reader = try ColumnReader.init(col_mem_source.source(), chunk);
                
                // Selective read using skip()
                try readSelectedRows(allocator, col_reader, oc, meta, selection.indices.items, out_rg);
            }
        }
        
        try pw.finishRowGroup(out_rg, @intCast(selection.count()));
    }
    
    try pw.finish();
}
```

## Selective Row Reading

```zig
/// Read only the rows specified in selection_indices using skip()
fn readSelectedRows(
    allocator: Allocator,
    col_reader: ColumnReader,
    oc: OutputColumn,
    meta: *FileMetaData,
    selection_indices: []const usize,
    out_rg: *RowGroupWriter,
) !void {
    const chunk = ...; // get from rg_meta
    const md = chunk.meta_data.?;
    const levels = meta.getColumnLevels(md.path_in_schema.items);
    const schema_elem = meta.getColumnSchema(md.path_in_schema.items);
    const type_len = if (schema_elem) |se| se.type_length else null;
    
    switch (oc.type) {
        .INT32 => {
            var values = std.ArrayListUnmanaged(i32){};
            defer values.deinit(allocator);
            try values.ensureTotalCapacity(allocator, selection_indices.len);
            
            var reader = BatchReader(i32).init(
                allocator, col_reader, md.type,
                @intCast(levels.max_def), @intCast(levels.max_rep),
                type_len,
            );
            defer reader.deinit();
            
            var current_row: usize = 0;
            for (selection_indices) |target_row| {
                // Skip to target row
                if (target_row > current_row) {
                    try reader.skip(target_row - current_row);
                    current_row = target_row;
                }
                
                // Read one value
                var buf: [1]?i32 = undefined;
                const n = try reader.nextBatch(&buf);
                if (n == 1) {
                    try values.append(allocator, buf[0] orelse 0);
                }
                current_row += 1;
            }
            
            try out_rg.writeInt32Column(values.items);
        },
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
            var values = std.ArrayListUnmanaged([]const u8){};
            defer {
                for (values.items) |v| allocator.free(v);
                values.deinit(allocator);
            }
            try values.ensureTotalCapacity(allocator, selection_indices.len);
            
            var reader = BatchReader([]const u8).init(...);
            defer reader.deinit();
            
            var current_row: usize = 0;
            for (selection_indices) |target_row| {
                if (target_row > current_row) {
                    try reader.skip(target_row - current_row);
                    current_row = target_row;
                }
                
                var buf: [1]?[]const u8 = undefined;
                const n = try reader.nextBatch(&buf);
                if (n == 1) {
                    if (buf[0]) |v| {
                        try values.append(allocator, try allocator.dupe(u8, v));
                    } else {
                        try values.append(allocator, try allocator.dupe(u8, ""));
                    }
                }
                current_row += 1;
            }
            
            try out_rg.writeByteArrayColumn(values.items);
        },
        // ... other types
    }
}
```

## Memory Ownership Summary

| Data | Owner | Lifetime |
|------|-------|----------|
| `filter_buffers[i]` | cmdFilter | Until row group loop complete |
| `filter_cache.byte_array_values` | FilterColumnCache | Until row group complete |
| `other_buffers[i]` | cmdFilter per-RG | Until row group write complete |
| `selection.indices` | SelectionVector | Until row group complete |
| `values` in readSelectedRows | Local ArrayList | Written to ParquetWriter, then freed |

## Key Differences from Current Implementation

| Aspect | Current | Optimized |
|--------|---------|-----------|
| Filter column I/O | Per row-group, blocking | All RGs batched, single call |
| Filter column read | 2x (filter, then output) | 1x (cached if in output) |
| Output column I/O | Per column, blocking | Per RG, batched |
| Row materialization | Read ALL rows, filter after | Skip to selected rows |
| Memory for filter | Temporary per batch | Cached for output if needed |

## API Verification

### Verified APIs
1. **BatchReader.skip()** - VERIFIED: O(n) calls to O(1) skipByteArray(). No allocations.
   - For PLAIN BYTE_ARRAY: calls `decoder.skipByteArray()` which just advances pos
   - For dictionary: calls `rle_decoder.skip()` which is efficient
   - For FIXED_LEN: single pointer advance

2. **MemorySource.initWithOffset()** - VERIFIED: Works with pre-fetched buffers.
   Used in cmdSchema for batched filter column reads.

3. **RandomAccessSource.readRanges()** - VERIFIED: Accepts Range[] and buffer[].
   Falls back to sequential readAt if implementation doesn't override.

4. **ColumnReader.init(source, chunk)** - VERIFIED: Can take any RandomAccessSource,
   including MemorySource.

### API Gaps / TODO
1. **readRanges fallback** - For local files, the default fallback is sequential. 
   Acceptable for now; async optimization can come later.
2. **Null handling** - Current pseudocode uses `0` or `""` for nulls. 
   ParquetWriter may need explicit null support for proper round-tripping.
3. **INT96/DOUBLE types** - Need to add to FilterColumnCache (straightforward)

## Expected Performance Improvement

- **Phase 2 (batched I/O)**: Eliminates per-column latency. For S3, huge win.
- **Phase 4b (single-pass filter)**: Eliminates 50% of filter column I/O.
- **Phase 4d (selective read with skip)**: For low selectivity (e.g., 20%), 
  skips 80% of decode work.

For the benchmark file (602K rows, 129K matches = 21% selectivity):
- Current: Read filter col 2x, read all 602K rows for output
- Optimized: Read filter col 1x, skip 79% of non-filter column decodes

---

# PHASE 2: Parallel Row Group Processing with xev

## Current State (Sequential)

```
RG0: fetch → scan → fetch_other → write ─┐
RG1: ─────────────────────────────────────┼─ fetch → scan → fetch_other → write ─┐
RG2: ────────────────────────────────────────────────────────────────────────────┼─ fetch → scan → ...
                                                                                  │
Total time = sum of all RG times
```

## Target State (Parallel with xev)

```
                    ┌─ RG0: scan → write ─┐
Batch Fetch All ───►├─ RG1: scan → write ─├───► Merge/Finalize
Filter Cols         └─ RG2: scan → write ─┘

Total time ≈ max(RG times) + overhead
```

## Architecture: Row Group Workers

```zig
const RowGroupWorker = struct {
    allocator: std.mem.Allocator,
    rg_idx: usize,
    rg_meta: *const RowGroup,
    
    // Pre-fetched filter column data (owned by parent)
    filter_buf: []const u8,
    filter_offset: u64,
    
    // Shared immutable context
    filter_col_idx: usize,
    filter_col_type: Type,
    encoded_filter: *const EncodedFilter,
    output_cols: []const OutputColumn,
    filter_col_in_output: bool,
    
    // Results (owned by worker)
    selection: SelectionVector,
    filter_cache: ?FilterColumnCache,
    output_buffers: [][]u8,  // Serialized column data ready for writing
    row_count: usize,
    
    // State
    status: enum { pending, scanning, fetching, writing, done, failed },
    err: ?anyerror,
    
    pub fn init(allocator: Allocator, ctx: *const FilterContext, rg_idx: usize) !*RowGroupWorker { ... }
    pub fn deinit(self: *RowGroupWorker) void { ... }
};
```

## Parallel Filter with xev Event Loop

```zig
const ParallelFilter = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    pf: *ParquetFile,
    
    // Shared context
    ctx: FilterContext,
    
    // Workers
    workers: []*RowGroupWorker,
    pending_count: std.atomic.Value(usize),
    
    // Results
    output_writer: *ParquetWriter,
    
    pub fn init(
        allocator: Allocator,
        loop: *xev.Loop,
        pf: *ParquetFile,
        filter_str: []const u8,
        select_columns: ?[]const u8,
        output_path: []const u8,
    ) !*ParallelFilter { ... }
    
    pub fn run(self: *ParallelFilter) !void {
        // =====================================================================
        // PHASE 1: Batch fetch ALL filter columns (single async I/O)
        // =====================================================================
        
        var ranges = try self.buildFilterColumnRanges();
        defer self.allocator.free(ranges);
        
        // Async batch read - xev will parallelize across connections for S3
        var completion = xev.Completion{};
        self.pf.source.readRangesAsync(ranges, self.filter_buffers, &completion);
        
        // Wait for batch fetch to complete
        try self.loop.run(.until_done);
        
        // =====================================================================
        // PHASE 2: Spawn parallel workers for each row group
        // =====================================================================
        
        for (self.workers, 0..) |worker, i| {
            // Each worker gets its pre-fetched filter buffer
            worker.filter_buf = self.filter_buffers[i];
            worker.filter_offset = self.filter_offsets[i];
            
            // Schedule worker on event loop
            self.loop.spawn(workerTask, worker, .{ .priority = .normal });
        }
        
        // =====================================================================
        // PHASE 3: Run event loop until all workers complete
        // =====================================================================
        
        try self.loop.run(.until_done);
        
        // =====================================================================
        // PHASE 4: Merge results and finalize output
        // =====================================================================
        
        // Workers have produced serialized column buffers
        // Write them in order to output
        for (self.workers) |worker| {
            if (worker.status == .failed) {
                return worker.err.?;
            }
            if (worker.row_count > 0) {
                try self.writeWorkerOutput(worker);
            }
        }
        
        try self.output_writer.finish();
    }
};

fn workerTask(worker: *RowGroupWorker) void {
    worker.status = .scanning;
    
    // PHASE 2a: Scan filter column from pre-fetched buffer
    var mem_source = MemorySource.initWithOffset(worker.filter_buf, worker.filter_offset);
    const filter_reader = ColumnReader.init(mem_source.source(), worker.filter_chunk) catch |e| {
        worker.status = .failed;
        worker.err = e;
        return;
    };
    
    // Build selection vector (CPU-bound, can run in parallel)
    worker.scanFilterColumn(filter_reader) catch |e| {
        worker.status = .failed;
        worker.err = e;
        return;
    };
    
    if (worker.selection.count() == 0) {
        worker.status = .done;
        worker.row_count = 0;
        return;
    }
    
    // PHASE 2b: Fetch other columns (async I/O)
    worker.status = .fetching;
    worker.fetchOtherColumnsAsync() catch |e| {
        worker.status = .failed;
        worker.err = e;
        return;
    };
    
    // PHASE 2c: Materialize to output buffers
    worker.status = .writing;
    worker.materializeOutput() catch |e| {
        worker.status = .failed;
        worker.err = e;
        return;
    };
    
    worker.status = .done;
    worker.row_count = worker.selection.count();
}
```

## Key Design Decisions

### 1. Pre-fetch Filter Columns Together
```
Before: RG0 fetch → RG1 fetch → RG2 fetch (sequential)
After:  [RG0, RG1, RG2] fetch (batched, single round-trip for S3)
```

For S3, this is critical: one HTTP request with Range header vs N requests.

### 2. Workers Own Their Output Buffers

Each worker produces serialized column data in memory. The main thread
writes these buffers in order. This avoids:
- Lock contention on the output file
- Out-of-order writes requiring seeking
- Complex coordination for interleaved writes

### 3. CPU-bound Scan Phase is Naturally Parallel

The filter scan (comparing values, building selection vector) is CPU-bound.
xev's thread pool can execute these in parallel across cores.

### 4. Async I/O for Other Columns

Each worker can issue async reads for its non-filter columns:
```zig
fn fetchOtherColumnsAsync(self: *RowGroupWorker) !void {
    var ranges = try self.buildOtherColumnRanges();
    
    // This returns immediately, completion happens via event loop
    self.pf.source.readRangesAsync(ranges, self.other_buffers, &self.fetch_completion);
    
    // The event loop will call our callback when done
    self.fetch_completion.callback = onOtherColumnsFetched;
    self.fetch_completion.userdata = self;
}

fn onOtherColumnsFetched(completion: *xev.Completion) void {
    const self = @fieldParentPtr(RowGroupWorker, "fetch_completion", completion);
    
    // Now materialize output
    self.materializeOutput() catch |e| {
        self.status = .failed;
        self.err = e;
        return;
    };
    
    self.status = .done;
}
```

## Memory Layout for Parallel Execution

```
ParallelFilter
├── filter_buffers[]     ─── Owned, freed after all workers done
├── filter_offsets[]     ─── Owned
├── workers[]
│   ├── Worker[0]
│   │   ├── selection        ─── Owned by worker
│   │   ├── filter_cache     ─── Owned by worker (if filter in output)
│   │   ├── other_buffers[]  ─── Owned by worker
│   │   └── output_buffers[] ─── Owned by worker, moved to writer
│   ├── Worker[1]
│   │   └── ...
│   └── Worker[N]
└── output_writer        ─── Receives buffers from workers
```

## Synchronization Points

1. **After batch filter fetch**: All workers can start scanning
2. **After all workers complete**: Main thread writes output in order
3. **No locks during worker execution**: Each worker operates on its own data

## Expected Performance

For 5M rows (50 row groups) on 8-core machine:

| Phase | Sequential | Parallel (8 workers) |
|-------|------------|---------------------|
| Filter fetch | 50 * 2ms = 100ms | 1 batch = 10ms |
| Scan (CPU) | 50 * 0.4ms = 20ms | 50/8 * 0.4ms = 2.5ms |
| Other fetch | 50 * 1ms = 50ms | Overlapped with scan |
| Write | 50 * 0.5ms = 25ms | 50 * 0.5ms = 25ms |
| **Total** | **~195ms** | **~40ms** |

This would make ZPQ competitive with DuckDB even at 5M+ rows.

## Implementation Phases

### Phase 2.1: Batch Filter Fetch (Low Risk)
- Already have `readRanges()` 
- Just need to collect all filter column ranges upfront
- **Estimated: Done in current implementation**

### Phase 2.2: Worker Struct (Medium Risk)
- Factor out per-RG state into `RowGroupWorker`
- Keep sequential execution initially
- Verify correctness with existing tests

### Phase 2.3: xev Thread Pool Integration (Medium Risk)
- Use `loop.spawn()` for CPU-bound scan phase
- Each worker scans independently
- Main thread waits for all completions

### Phase 2.4: Async Other-Column Fetch (Higher Risk)
- Each worker issues async reads
- Requires careful completion tracking
- Test with local files first, then S3

### Phase 2.5: Output Merge (Low Risk)
- Workers produce serialized buffers
- Main thread writes in RG order
- Simple concatenation

## API Requirements from xev

| API | Purpose | Status |
|-----|---------|--------|
| `loop.spawn(fn, ctx, opts)` | Schedule CPU work | Available |
| `ThreadPool.schedule()` | Parallel task execution | Available |
| `Completion` callbacks | Async I/O notification | Available |
| `source.readRangesAsync()` | Non-blocking batch read | **Needs wrapper** |

The main gap is `readRangesAsync()` - need to wrap the sync `readRanges()` to:
1. Submit to thread pool for local files
2. Use xev async I/O for S3 (already async internally)
