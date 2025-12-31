# ZPQ Filter Command State Machine Design

## Design Goals

Based on DuckDB analysis, ZPQ's filter command needs:
1. **Single-pass filter column read** - never read the same column twice
2. **Selection vector architecture** - track matching row indices, don't copy
3. **Batched async I/O** - coalesce ranges, use event loop
4. **Row group parallelism** - process row groups concurrently

## State Machine Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         ZPQ FILTER STATE MACHINE                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  State 0: INIT                                                                │
│  ─────────────────                                                            │
│  Input:  input_path, filter_expr, select_cols, output_path                   │
│  Action: Parse arguments, validate                                            │
│  Output: FilterContext initialized                                            │
│  Next:   → READ_FOOTER                                                        │
│                                                                               │
│  State 1: READ_FOOTER                                                         │
│  ─────────────────────                                                        │
│  Action: Prefetch last 64KB, parse footer + metadata                         │
│  Output: FileMetadata, schema, row_group_count                               │
│  Next:   → PLAN_EXECUTION                                                    │
│                                                                               │
│  State 2: PLAN_EXECUTION                                                      │
│  ───────────────────────                                                      │
│  Action:                                                                      │
│    - Find filter column index + type                                         │
│    - Build output column list (indices, types)                               │
│    - Create EncodedFilter for predicate                                      │
│    - Check row group stats → build skip_mask[]                               │
│    - Calculate I/O ranges for filter columns                                 │
│  Output: ExecutionPlan { skip_mask, filter_col_ranges, output_col_indices }  │
│  Next:   → FETCH_FILTER_COLUMNS                                              │
│                                                                               │
│  State 3: FETCH_FILTER_COLUMNS (async, batched)                              │
│  ──────────────────────────────────────────────                              │
│  Action:                                                                      │
│    - Collect ranges for filter column across ALL non-skipped row groups      │
│    - Single readRanges() call via event loop                                 │
│    - Store in filter_buffers[rg_idx]                                         │
│  Output: Pre-fetched filter column data for all row groups                   │
│  Next:   → SCAN_AND_SELECT (for each row group, potentially parallel)        │
│                                                                               │
│  State 4: SCAN_AND_SELECT (per row group)                                    │
│  ─────────────────────────────────────────                                    │
│  Action:                                                                      │
│    - Create MemorySource from pre-fetched filter buffer                      │
│    - Scan filter column in batches (1024 values)                             │
│    - Build selection_vector[] of matching row indices                        │
│    - Store filter column values for rows that match (if in select list)     │
│  Output: selection_vector, filter_values (if needed)                         │
│  Next:   → FETCH_OUTPUT_COLUMNS (if matches > 0)                             │
│          → next row group (if matches == 0)                                  │
│                                                                               │
│  State 5: FETCH_OUTPUT_COLUMNS (async, batched)                              │
│  ──────────────────────────────────────────────                              │
│  Action:                                                                      │
│    - For columns NOT the filter column:                                      │
│      - Calculate byte ranges needed                                          │
│      - Batch fetch via readRanges()                                          │
│    - For filter column: reuse values from State 4                            │
│  Output: Column data buffers                                                 │
│  Next:   → MATERIALIZE_OUTPUT                                                │
│                                                                               │
│  State 6: MATERIALIZE_OUTPUT (per row group)                                 │
│  ─────────────────────────────────────────────                                │
│  Action:                                                                      │
│    - For each output column:                                                 │
│      - If filter column: use cached values                                   │
│      - Else: decode only rows in selection_vector                            │
│    - Write to ParquetWriter row group                                        │
│  Output: Row group written to output                                         │
│  Next:   → SCAN_AND_SELECT (next row group)                                  │
│          → FINALIZE (if last row group)                                      │
│                                                                               │
│  State 7: FINALIZE                                                            │
│  ─────────────                                                                │
│  Action:                                                                      │
│    - pw.finish() - write footer                                              │
│    - Print statistics                                                        │
│  Output: Complete parquet file                                               │
│  Next:   → DONE                                                              │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Key Data Structures

### FilterContext
```zig
const FilterContext = struct {
    allocator: Allocator,
    
    // Input
    input_path: []const u8,
    output_path: []const u8,
    filter_col_name: []const u8,
    filter_value: []const u8,
    select_col_names: []const []const u8,
    
    // Parsed from metadata
    pf: *ParquetFile,
    metadata: *FileMetaData,
    filter_col_idx: usize,
    filter_col_type: Type,
    output_col_indices: []usize,
    output_col_types: []Type,
    
    // Filter
    encoded_filter: EncodedFilter,
    
    // Row group state
    rg_skip_mask: []bool,
    
    // Pre-fetched filter column data (one buffer per non-skipped row group)
    filter_buffers: [][]u8,
    filter_offsets: []u64,
    
    // Output
    writer: *ParquetWriter,
    
    // Stats
    total_input_rows: u64,
    total_output_rows: u64,
    row_groups_skipped: usize,
};
```

### SelectionVector
```zig
const SelectionVector = struct {
    // Indices of rows that passed the filter
    indices: []usize,
    count: usize,
    
    // For filter column reuse: cached decoded values
    // Only populated if filter column is in select list
    filter_values: ?[]Value,
};
```

### RowGroupWork
```zig
const RowGroupWork = struct {
    rg_idx: usize,
    selection: SelectionVector,
    
    // Pre-fetched column buffers for this row group
    col_buffers: [][]u8,
    col_offsets: []u64,
    
    // Output values ready for writing
    output_columns: []ColumnData,
};
```

## Implementation Phases

### Phase 1: Single-Pass Filter (10x improvement)
- Read filter column once
- If filter column is in select list, cache values during scan
- Use cached values for output instead of re-reading

### Phase 2: Batched I/O (2-5x improvement)
- Batch fetch filter columns for ALL row groups in one readRanges()
- Batch fetch output columns per row group

### Phase 3: Selection Vector (2-3x improvement)
- Don't copy/collect values during filter scan
- Just track indices
- Decode output columns only for matching indices

### Phase 4: Parallelism (Nx improvement, N = core count)
- Process row groups in parallel
- Each row group: scan → select → write prepared
- Serialize only the final write

## Comparison with Current Implementation

| Current | Optimized |
|---------|-----------|
| Read filter col (602K rows) | Read filter col ONCE |
| Build matching_rows[] | Build selection_vector[] |
| Read filter col AGAIN for output | Reuse cached values |
| Sequential I/O per batch | Batched readRanges() |
| Single-threaded | Per-row-group parallel |

## Critical Path Optimizations

### 1. Filter Column Caching
```zig
// During filter scan, if filter_col in select_cols:
if (encoded_filter.matchesBytes(v)) {
    selection.indices[match_count] = row_idx;
    if (cache_filter_values) {
        selection.filter_values[match_count] = allocator.dupe(u8, v);
    }
    match_count += 1;
}
```

### 2. Batched Range Fetch
```zig
// Collect ALL filter column ranges upfront
var ranges: []Range = allocator.alloc(Range, active_rg_count);
for (rg_meta, rg_idx) in metadata.row_groups {
    if (skip_mask[rg_idx]) continue;
    ranges[buf_idx] = .{
        .start = filter_col_offset,
        .end = filter_col_offset + filter_col_size,
    };
}
// Single async fetch
try source.readRanges(ranges, filter_buffers);
```

### 3. Selective Column Decode
```zig
// Only decode rows in selection vector
fn decodeSelected(
    comptime T: type,
    reader: *BatchReader(T),
    selection: []const usize,
    out: *[]T,
) !void {
    var sel_idx: usize = 0;
    var row_idx: usize = 0;
    
    while (sel_idx < selection.len) {
        // Skip to next selected row
        const skip_count = selection[sel_idx] - row_idx;
        if (skip_count > 0) {
            try reader.skip(skip_count);
            row_idx += skip_count;
        }
        
        // Read the selected row
        out[sel_idx] = try reader.readOne();
        row_idx += 1;
        sel_idx += 1;
    }
}
```

## Event Loop Integration

```zig
// State machine driven by event loop completions
const FilterStateMachine = struct {
    state: State,
    ctx: *FilterContext,
    
    const State = enum {
        init,
        read_footer,
        plan_execution,
        fetch_filter_columns,
        scan_and_select,
        fetch_output_columns,
        materialize_output,
        finalize,
        done,
    };
    
    fn onCompletion(self: *@This(), result: anytype) void {
        switch (self.state) {
            .fetch_filter_columns => {
                // All filter columns fetched
                self.state = .scan_and_select;
                self.startScanAndSelect();
            },
            .fetch_output_columns => {
                // Output columns fetched
                self.state = .materialize_output;
                self.materializeOutput();
            },
            // ...
        }
    }
};
```

## Expected Performance

Based on DuckDB patterns:
- **Phase 1** (no double-read): 15s → ~8s (2x)
- **Phase 2** (batched I/O): 8s → ~2s (4x)
- **Phase 3** (selection vectors): 2s → ~1s (2x)
- **Phase 4** (parallelism): 1s → ~0.2s (5x on 8 cores)

**Target**: 30ms-200ms for 100MB file (matching DuckDB)
