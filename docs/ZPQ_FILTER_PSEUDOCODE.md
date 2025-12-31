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
