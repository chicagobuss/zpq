# DuckDB Parquet Filter + Write Pipeline Architecture

## Overview

This document analyzes how DuckDB achieves 30ms filter + write operations on 100MB Parquet files - approximately 500x faster than a naive sequential implementation. Understanding this architecture is critical for designing ZPQ's optimized filter pipeline.

## Key Performance Principles

### 1. Vectorized Execution (STANDARD_VECTOR_SIZE = 2048)

DuckDB processes data in **vectors** of 2048 values, not row-by-row. This:
- Maximizes CPU cache utilization
- Enables SIMD operations
- Amortizes function call overhead
- Allows batch I/O operations

```cpp
// From parquet_reader.cpp:1343
auto scan_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, GetGroup(state).num_rows - state.offset_in_group);
```

### 2. Late Materialization with Selection Vectors

Instead of copying filtered data, DuckDB uses **selection vectors** - arrays of indices pointing to qualifying rows:

```cpp
// From parquet_reader.cpp:1365-1395
if (filters || deletion_filter) {
    idx_t filter_count = result.size();
    vector<bool> need_to_read(column_ids.size(), true);
    state.sel.Initialize(nullptr);
    
    // First load columns used in filters
    for (auto &scan_filter : state.scan_filters) {
        child_reader.Filter(scan_count, define_ptr, repeat_ptr, result_vector, 
                           scan_filter.filter, *scan_filter.filter_state, 
                           state.sel, filter_count, is_first_filter);
        need_to_read[local_idx] = false;  // Don't re-read filter column
    }
    
    // Only read remaining columns for surviving rows
    for (idx_t i = 0; i < column_ids.size(); i++) {
        if (!need_to_read[col_idx]) continue;
        if (filter_count == 0) {
            child_reader.Skip(result.size());  // Skip entirely!
            continue;
        }
        child_reader.Select(result.size(), define_ptr, repeat_ptr, 
                           result_vector, state.sel, filter_count);
    }
}
```

**Key insight**: Non-filter columns are read with `Select()` using the selection vector, or `Skip()` entirely if filter_count == 0.

### 3. Two-Phase Prefetching Strategy

DuckDB's I/O is optimized via a **two-phase prefetch** system:

#### Phase 1: Register Ranges
```cpp
// From thrift_tools.hpp:170
void RegisterPrefetch(idx_t pos, uint64_t len, bool can_merge = true) {
    ra_buffer.AddReadHead(pos, len, can_merge);
}
```

#### Phase 2: Execute Prefetch
```cpp
// From thrift_tools.hpp:180
void PrefetchRegistered() {
    ra_buffer.Prefetch();
}
```

#### Smart Merging (16KB gap tolerance)
```cpp
// From thrift_tools.hpp:39
struct ReadHeadComparator {
    static constexpr uint64_t ALLOW_GAP = 1 << 14; // 16 KiB
    // Merges ranges that are adjacent or within 16KB
};
```

This means if you need bytes 0-1000 and 1500-2500, DuckDB fetches 0-2500 in ONE I/O operation.

### 4. Adaptive Filter Ordering

DuckDB dynamically reorders filters based on selectivity:

```cpp
// From parquet_reader.cpp:1378
auto filter_state = state.adaptive_filter->BeginFilter();
for (idx_t i = 0; i < state.scan_filters.size(); i++) {
    auto &scan_filter = state.scan_filters[state.adaptive_filter->permutation[i]];
    // Most selective filters run first
}
state.adaptive_filter->EndFilter(filter_state);
```

### 5. Dictionary-Based Filter Pushdown

For dictionary-encoded columns, filters are pushed into the dictionary decoder itself:

```cpp
// From column_reader.cpp:738-746
if (encoding == ColumnEncoding::DICTIONARY && read_now == to_read && dictionary_decoder.HasFilter()) {
    if (page_is_filtered_out) {
        approved_tuple_count = 0;  // Skip entire page!
    } else {
        // Push filter into dictionary directly
        dictionary_decoder.Filter(define_ptr, read_now, result, sel, approved_tuple_count);
    }
}
```

This is HUGE for string columns - instead of comparing strings, compare dictionary indices.

### 6. Row Group & Page-Level Skip

Before reading any data, DuckDB checks statistics:

```cpp
// Row group level (metadata statistics)
// Page level (column index / page statistics)
if (page_is_filtered_out) {
    page_rows_available -= skip_now;
    to_skip -= skip_now;
    continue;
}
```

## Threading Model

### Per-Row-Group Parallelism

```cpp
// From parquet_multi_file_info.cpp:461-470
optional_idx ParquetMultiFileInfo::MaxThreads(...) {
    if (expand_result == FileExpandResult::MULTIPLE_FILES) {
        return optional_idx();  // Unlimited threads for multiple files
    }
    return MaxValue(bind_data.initial_file_row_groups, static_cast<idx_t>(1));
}
```

DuckDB launches up to **one thread per row group**. Each thread:
1. Gets assigned a row group
2. Prefetches its columns
3. Scans and filters independently
4. Outputs to a shared result buffer

### Pipeline Execution

DuckDB uses a **push-based pipeline** model:
1. Source operators (ParquetScan) push chunks downstream
2. Filter/Project operators transform in-place
3. Sink operators (ParquetWrite) collect results

## Write Pipeline

### Prepared Row Groups

```cpp
// From parquet_writer.cpp:684-701
void ParquetWriter::Flush(ColumnDataCollection &buffer, ...) {
    PreparedRowGroup prepared_row_group;
    PrepareRowGroup(buffer, prepared_row_group, transform_data);  // CPU work
    buffer.Reset();
    FlushRowGroup(prepared_row_group);  // I/O work
}
```

### Lock-Based Serialization for Output

```cpp
// From parquet_writer.cpp:648
void ParquetWriter::FlushRowGroup(PreparedRowGroup &prepared) {
    lock_guard<mutex> glock(lock);  // Serialize row group writes
    // ... write to file
}
```

Multiple threads can **prepare** row groups in parallel, but **flush** is serialized.

## Data Flow Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DUCKDB PARQUET SCAN                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. METADATA PHASE                                                           │
│     ├── Prefetch footer (last 256KB or estimated)                           │
│     ├── Parse FileMetaData                                                   │
│     ├── Build column_ids[] for projection                                   │
│     └── Initialize filter state (adaptive ordering)                         │
│                                                                              │
│  2. ROW GROUP ASSIGNMENT (parallel)                                          │
│     ├── Thread 0 → Row Group 0                                              │
│     ├── Thread 1 → Row Group 1                                              │
│     └── Thread N → Row Group N                                              │
│                                                                              │
│  3. PER-ROW-GROUP SCAN (per thread)                                         │
│     │                                                                        │
│     ├── 3a. PREFETCH DECISION                                               │
│     │   ├── If scan% > 80%: prefetch entire row group                       │
│     │   └── Else: register column ranges, prefetch selectively              │
│     │                                                                        │
│     ├── 3b. VECTORIZED SCAN LOOP (2048 rows per iteration)                  │
│     │   │                                                                    │
│     │   ├── For each filter column (ordered by selectivity):                │
│     │   │   ├── Read/decode column values                                   │
│     │   │   ├── Apply filter → update selection vector                      │
│     │   │   └── If filter_count == 0: break (skip remaining)               │
│     │   │                                                                    │
│     │   └── For each non-filter column:                                     │
│     │       ├── If filter_count == 0: Skip()                                │
│     │       └── Else: Select() using selection vector                       │
│     │                                                                        │
│     └── 3c. OUTPUT                                                          │
│         └── Push DataChunk to downstream operator                           │
│                                                                              │
│  4. WRITE PHASE (if COPY TO)                                                │
│     ├── Collect chunks into ColumnDataCollection                            │
│     ├── PrepareRowGroup() - parallel compression/encoding                   │
│     └── FlushRowGroup() - serialized file I/O                              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Why DuckDB is 500x Faster Than Naive ZPQ

| Aspect | Naive ZPQ | DuckDB |
|--------|-----------|--------|
| **I/O Pattern** | Sequential, row-by-row | Batched prefetch, 16KB coalescing |
| **Filter Application** | Read all → filter → copy | Selection vectors, no copy |
| **Column Reading** | Read filter col, then read SAME col again | Read once, reuse for output |
| **Non-filter Columns** | Always read all rows | Skip() if filter_count == 0 |
| **Dictionary Columns** | Decode strings, compare | Compare indices directly |
| **Parallelism** | None | Thread per row group |
| **Vectorization** | 1 row at a time | 2048 rows per batch |

## Key Optimizations for ZPQ

### Must-Have (10-100x impact)
1. **Single-pass filter column**: Read filter column ONCE, keep values for output
2. **Selection vector**: Don't copy data, track indices
3. **Batched I/O**: Coalesce nearby ranges, single readRanges() call
4. **Skip non-matching columns**: If row group has 0 matches, skip all columns

### Should-Have (2-10x impact)
5. **Dictionary filter pushdown**: For BYTE_ARRAY, check dictionary first
6. **Adaptive filter ordering**: Run most selective filter first
7. **Page-level statistics**: Skip pages that can't match

### Nice-to-Have (parallelism)
8. **Per-row-group threading**: Each row group on separate thread
9. **Parallel compression**: Compress output row groups in parallel
10. **Async I/O**: Use event loop for non-blocking reads

## References

- `references/duckdb/extension/parquet/parquet_reader.cpp` - Core scan logic
- `references/duckdb/extension/parquet/column_reader.cpp` - Filter application
- `references/duckdb/extension/parquet/include/thrift_tools.hpp` - Prefetch mechanism
- `references/duckdb/extension/parquet/parquet_writer.cpp` - Write pipeline
- `references/duckdb/extension/parquet/parquet_multi_file_info.cpp` - Threading
