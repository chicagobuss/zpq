# Two-Phase Column Fetching: The Critical I/O Optimization

## Executive Summary

**Problem**: ZPQ's filtered scan takes the same time as a full scan (17.4s vs 17.5s on a 100MB file over R2) because we prefetch ALL columns before evaluating filters.

**Solution**: Implement two-phase column fetching - fetch filter columns first, evaluate predicates, then fetch remaining columns only for matching data.

**Expected Impact**: For a 1% selectivity filter over network storage, this could reduce I/O by ~99% and query time by 10-50x.

**Evidence**: DuckDB, Polars, and Arrow all implement this pattern. See competitor analysis below.

---

## The Problem in Detail

### Current Behavior

```zig
// main.zig:210
try rg.prefetch(null);  // Fetches ALL columns immediately
```

For a 100MB Parquet file with 50 columns:
- Full scan: Fetch 100MB, decode 100MB → 17.5s
- Filtered scan (1%): Fetch 100MB, decode 2MB, discard 98MB → 17.4s

**We're paying full I/O cost regardless of selectivity.**

### Benchmark Evidence

```
R2 Full Scan:     88M values in 17.5s = 5.06 MVal/s
R2 Filtered (1%): 602K values in 17.4s = 0.03 MVal/s  ← Same time!
```

The filtered scan is **168x slower per-value** because we're fetching data we don't need.

---

## Competitor Analysis

### DuckDB Implementation

From `extension/parquet/parquet_reader.cpp:1316-1338`:

```cpp
// lazy fetching is when all tuples in a column can be skipped. 
// With lazy fetching the buffer is only fetched on the first read.
bool lazy_fetch = filters != nullptr;

for (idx_t i = 0; i < column_ids.size(); i++) {
    bool has_filter = filters->filters.find(col_idx) != filters->filters.end();
    
    // Key: Only eagerly prefetch filter columns
    root_reader.GetChildReader(file_col_idx)
        .RegisterPrefetch(trans, !(lazy_fetch && !has_filter));
}

if (!lazy_fetch) {
    trans.PrefetchRegistered();  // Only fetch immediately if no filters
}
```

**DuckDB explicitly delays fetching non-filter columns when filters are present.**

### Polars Implementation

Polars' lazy API performs at the query planning level:

| Optimization | Description |
|--------------|-------------|
| Predicate pushdown | Applies filters at scan level |
| Projection pushdown | Select only needed columns at scan level |

### Arrow Implementation

Arrow implements `predicate push-down for parquet tables` (ARROW-11074) and `column projection pushdown` (ARROW-13797).

---

## Proposed Architecture

### Phase 1: Filter Column Fetch

```
┌─────────────────────────────────────────────────────────────────┐
│                        R2/S3 Storage                            │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │ Col A   │ │ Col B   │ │ Col C   │ │ Col D   │ │ Col E   │   │
│  │ (filter)│ │         │ │         │ │         │ │         │   │
│  └────┬────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘   │
└───────┼─────────────────────────────────────────────────────────┘
        │
        ▼ Fetch only filter column
┌───────────────────┐
│ Decode & Evaluate │
│ Build SelectionVec│
└────────┬──────────┘
         │
         ▼ Selection: rows [12, 47, 891, 1024, ...]
```

### Phase 2: Selective Column Fetch

```
┌─────────────────────────────────────────────────────────────────┐
│                        R2/S3 Storage                            │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │ Col A   │ │ Col B   │ │ Col C   │ │ Col D   │ │ Col E   │   │
│  │ (done)  │ │ FETCH   │ │ FETCH   │ │ FETCH   │ │ FETCH   │   │
│  └─────────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘   │
└───────────────────┼───────────┼───────────┼───────────┼─────────┘
                    │           │           │           │
                    ▼           ▼           ▼           ▼
              ┌─────────────────────────────────────────────┐
              │ Decode only selected rows using SelectionVec│
              └─────────────────────────────────────────────┘
```

### Future: Page-Level Skipping (Phase 3)

With ColumnIndex support, we can skip entire pages:

```
Row Group 0:
  Page 0: min=A, max=F  → SKIP (filter=Lambda, Lambda > F)
  Page 1: min=G, max=M  → FETCH (Lambda in range)
  Page 2: min=N, max=Z  → SKIP (Lambda < N)
```

---

## Implementation Plan

### Stage 1: Basic Two-Phase Prefetch (High Impact, Medium Effort)

**Files to modify:**
- `src/main.zig` - `cmdScan` function
- `src/zpq/core/file.zig` - `RowGroupReader.prefetch`

**Changes:**

1. **Add selective prefetch to RowGroupReader**

```zig
// file.zig
pub fn prefetchColumns(self: *RowGroupReader, column_indices: []const usize) !void {
    // Only fetch specified columns
}

pub fn prefetchExcluding(self: *RowGroupReader, exclude_indices: []const usize) !void {
    // Fetch all columns except the excluded ones
}
```

2. **Modify cmdScan for two-phase approach**

```zig
// main.zig - cmdScan with filter
if (filter_col_idx) |f_idx| {
    // Phase 1: Fetch only filter column
    try rg.prefetchColumns(&[_]usize{f_idx});
    
    // Decode filter column and build selection
    const filter_reader = try rg.columnReader(f_idx);
    var selection = try buildSelectionVector(allocator, filter_reader, filter_val);
    
    // Early exit if no matches in this row group
    if (selection.count() == 0) {
        tracer.recordRowGroup(false);
        continue;
    }
    
    // Phase 2: Fetch remaining columns
    try rg.prefetchExcluding(&[_]usize{f_idx});
    
    // Decode with selection vector
    // ...
} else {
    // No filter - prefetch everything
    try rg.prefetch(null);
}
```

**Estimated effort**: 4-6 hours
**Expected speedup**: 10-50x for low-selectivity queries over network storage

### Stage 2: Selection-Aware Column Reading (Medium Impact, Medium Effort)

After Phase 1 filtering, we know exactly which rows we need. We can:

1. **Skip entire batches** that have no selected rows (current implementation)
2. **Use `nextBatchSelected`** to only materialize selected rows (current implementation)

This stage is mostly implemented but needs integration with the two-phase prefetch.

**Estimated effort**: 2-3 hours

### Stage 3: Page-Level ColumnIndex Support (High Impact, High Effort)

Parse Parquet ColumnIndex to skip pages within row groups.

**Files to modify:**
- `src/zpq/core/schema.zig` - Add ColumnIndex/OffsetIndex types
- `src/zpq/core/thrift.zig` - Parse ColumnIndex from footer
- `src/zpq/core/file.zig` - Add `shouldSkipPage` logic
- `src/zpq/core/column.zig` - Page-level skip support

**Changes:**

1. **Parse ColumnIndex from footer**

```zig
pub const ColumnIndex = struct {
    null_pages: []bool,
    min_values: [][]const u8,
    max_values: [][]const u8,
    null_counts: ?[]i64,
};
```

2. **Add page-level filtering**

```zig
pub fn shouldSkipPage(
    column_index: ColumnIndex,
    page_idx: usize,
    filter: Filter,
) bool {
    const min = column_index.min_values[page_idx];
    const max = column_index.max_values[page_idx];
    return !filter.mightMatch(min, max);
}
```

**Estimated effort**: 8-12 hours
**Expected additional speedup**: 2-10x for queries where data is clustered

### Stage 4: Speculative Prefetch (Future)

Based on statistics and historical patterns:
- If row group stats suggest high selectivity → prefetch all columns eagerly
- If row group stats suggest low selectivity → use two-phase
- Use ColumnIndex to prefetch only matching pages

---

## API Design

### New RowGroupReader Methods

```zig
pub const RowGroupReader = struct {
    // Existing
    pub fn prefetch(self: *RowGroupReader, indices: ?[]const usize) !void;
    
    // New
    pub fn prefetchColumns(self: *RowGroupReader, indices: []const usize) !void;
    pub fn prefetchExcluding(self: *RowGroupReader, exclude: []const usize) !void;
    pub fn isPrefetched(self: *RowGroupReader, col_idx: usize) bool;
    
    // Future: Page-level
    pub fn prefetchPages(self: *RowGroupReader, col_idx: usize, page_indices: []const usize) !void;
};
```

### Filter Interface

```zig
pub const Filter = struct {
    column_name: []const u8,
    op: enum { eq, ne, lt, le, gt, ge, in, between },
    value: Value,
    
    pub fn evaluate(self: Filter, data: []const ?T) SelectionVector;
    pub fn mightMatch(self: Filter, min: []const u8, max: []const u8) bool;
};
```

---

## Testing Strategy

### Benchmarks

1. **Local file baseline**: Measure decode-only overhead
2. **R2/S3 with varying selectivity**: 0.1%, 1%, 10%, 50%, 100%
3. **Compare with DuckDB**: Verify we match their performance characteristics

### Correctness

1. **Round-trip verification**: Compare filtered results with full scan + filter
2. **Edge cases**: Empty results, all rows match, single row match
3. **Multi-column filters**: AND/OR combinations (future)

### Probes

```
probes/
├── bench_two_phase_prefetch.zig    # Measure I/O savings
├── bench_page_skip.zig             # Measure page-level skip savings
└── verify_filter_correctness.zig   # Correctness verification
```

---

## Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| 1% selectivity query time (100MB, R2) | 17.4s | <2s |
| I/O bytes fetched (1% selectivity) | 100% | <5% |
| Throughput (1% selectivity) | 0.03 MVal/s | >10 MVal/s |

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Two round-trips to S3 adds latency | Parallel prefetch of filter column while parsing metadata |
| Filter column is large (e.g., strings) | Consider dictionary-only evaluation when possible |
| Multiple filter columns | Fetch all filter columns in Phase 1 |
| Complex predicates (OR, functions) | Fall back to full prefetch |

---

## Timeline

| Stage | Effort | Priority |
|-------|--------|----------|
| Stage 1: Basic two-phase | 4-6 hours | P0 - Critical |
| Stage 2: Selection integration | 2-3 hours | P0 - Critical |
| Stage 3: ColumnIndex | 8-12 hours | P1 - High |
| Stage 4: Speculative prefetch | 4-6 hours | P2 - Medium |

**Recommended approach**: Complete Stages 1-2 first, benchmark against R2, then evaluate Stage 3 ROI.
