# ZPQ Technical Context and Detail

**Last Updated**: Dec 30, 2024
**Current State**: Phase 1 (Read Dominance) - Milestone 19 complete! Predicate pushdown with unified `EncodedFilter` abstraction supporting all types. Page-level skip achieving 230x speedup on low-selectivity queries.

---

## Phase 1: Read Dominance (Skip Everything)

### Milestone 18: SIMD Decoders [COMPLETE]
ZPQ's decode performance leverages Zig 0.16's native `@Vector` support for extreme throughput.

*   [x] **Vector-Vector Shifts**: Parallel bit-unpacking for widths 1-32.
*   [x] **Large-Integer Load**: Use `u128/u256` containers for single-read 8-value loads.
*   [x] **BatchReader Integration**: Unified 1024-wide vectorized paths for all types.
*   [x] **SIMD Null Expansion**: Achieved branchless expansion using shuffle tables and vectorized mask generation from definition levels.
*   [x] **Vectorized RLE Runs**: Leveraged `@memset` for high-speed repetition decoding.

### Milestone 18.5: The Deranged Data Verification Gauntlet [COMPLETE]
**Goal**: Stress-test correctness and SIMD alignment using non-standard data.

*   [x] **Sparsity**: INT32/INT64 with 99% nulls. Fixed RLE overflow bug.
*   [x] **Bloat**: BYTE_ARRAY with strings > 64KB. Fixed `BatchReader` page-lifecycle memory safety bug.
*   [x] **Alignment**: FIXED_LEN_BYTE_ARRAY with non-standard lengths (7 bytes). Correct.
*   [x] **Transitions**: Dictionary -> PLAIN encoding transitions mid-column. Correct.

### Milestone 19: Predicate Pushdown & Lazy I/O [COMPLETE]

**Critical Discovery (Dec 2024)**: Benchmarking revealed that filtered scans take the **same time** as full scans over network storage:

| Scan Type | Values | Time | Throughput |
|-----------|--------|------|------------|
| Full scan (R2) | 88M | 17.5s | 5.06 MVal/s |
| Filtered 1% (R2) | 602K | 17.4s | 0.03 MVal/s |

**Root Cause**: `rg.prefetch(null)` fetches ALL columns before filter evaluation.

**Solution**: Two-phase column fetching (like DuckDB). See `plans/two_phase_column_fetching.md`.

#### All Items Complete
*   [x] **Metadata Pruning**: Row group skipping using min/max stats.
*   [x] **Selection Primitives**: `SelectionVector`, `BatchReader.skip(n)`, `BatchReader.nextBatchSelected(...)`.
*   [x] **Observability Infrastructure**: `src/zpq/trace.zig`, `bench/` directory, `probes/bench_skip_breakdown.zig`.
*   [x] **Competitor Analysis**: Confirmed DuckDB, Polars, Arrow all implement lazy column fetching.
*   [x] **Two-Phase Column Fetching** (Dec 30, 2024): Implemented!
    *   Added `RowGroupReader.prefetchColumns(indices)` - fetch only specified columns
    *   Added `RowGroupReader.prefetchExcluding(indices)` - fetch all except specified
    *   Added `RowGroupReader.isPrefetched(col_idx)` - check if column already fetched
    *   Refactored `cmdScan` to use two-phase approach:
        - Phase 1: Fetch only filter column, build selection vectors
        - Phase 2: If matches exist, fetch remaining columns and process with selections
        - If no matches in row group, skip fetching all other columns entirely
*   [x] **Page-Level ColumnIndex** (P1 - Dec 29, 2024): Parse ColumnIndex for per-page min/max, skip entire pages.
    *   Removed 64-bit bitmap limitation - checks ColumnIndex directly per page
    *   Achieved **230/231 pages skipped** on sorted test data
*   [x] **BatchReader.skip() Optimization** (P2 - Dec 29, 2024): `RleDecoder.skipAndCountMatching()` for O(runs) def level skipping.
    *   New method counts matching values in O(runs) instead of O(values)
    *   Counts how many values have `def_level == max_def_level` (present values) during skip

#### R2 Benchmark Results (Dec 30, 2024) - THE PROOF
| Scenario | Time | Speedup | Notes |
|----------|------|---------|-------|
| Full scan (88M values) | **59.8s** | 1.0x | Baseline - fetches all columns |
| Filtered 100% (602K selected) | 15.4s | 3.9x | All rows match, but only filter col fetched in Phase 1 |
| Filtered 0.001% (4 selected) | **14.0s** | 4.3x | Only 4 rows match - skips most non-filter I/O |
| Filtered 0% (0 matches) | **0.26s** | **230x** | Row group stats skip 1 RG, no Phase 2 needed |

#### Local Benchmark Results (Dec 30, 2024)
| Scenario | Time | Notes |
|----------|------|-------|
| Full scan (88M values) | 10.3s | Baseline |
| Filtered scan (602K selected) | 9.9s | High selectivity, similar decode time |
| Filtered scan (0 matches) | 41ms | Row group stats skip all 5 RGs |
| Filtered scan (4 matches) | 5.2s | Scans filter column only, skips non-filter I/O |

#### Page-Level Skip Results (Dec 29-30, 2024) - Sorted Data
| Scenario | Pages Skipped | Notes |
|----------|---------------|-------|
| BYTE_ARRAY filter on sorted customer_id | 230/231 | ColumnIndex min/max enables tight page bounds |
| INT64 filter on sorted int64_sorted | 11/12 | Same mechanism works for numeric types |

#### EncodedFilter Abstraction (Dec 30, 2024)
Unified predicate handling across all types using byte-level comparison:
*   `EncodedFilter.parse(allocator, filter_str, parquet_type)` - Parse filter value to encoded bytes
*   `EncodedFilter.matchesBytes(value_bytes)` - O(1) byte comparison for value matching
*   `EncodedFilter.mightContainInPage(column_index, page_idx)` - Type-aware range check for page skip
*   `EncodedFilter.mightContainInRowGroup(stats)` - Type-aware range check for RG skip

**Key insight**: For equality predicates, byte comparison works for ALL types. Range checks (min/max) need type-aware comparison due to signed integer byte ordering.

**Supported types**: INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, BOOLEAN

**Key Implementation Files**:
*   `src/zpq/core/filter.zig` - New `EncodedFilter` abstraction
*   `src/zpq/core/rle.zig` - Added `RleDecoder.skipAndCountMatching()`
*   `src/zpq/core/batch_reader.zig` - Updated `skip()` to use O(runs) algorithm
*   `src/zpq/core/file.zig` - Updated `shouldSkipRowGroup()` to use `EncodedFilter`
*   `src/main.zig` - Unified page-level skip for all types via `EncodedFilter`

**Test data**: `data/test_all_types_sorted.parquet` - 100K rows, 8 columns (all types), sorted, with page index

**Result**: 4-230x speedup achieved for low-selectivity queries. Interleaved loop optimization deferred to future milestone.

---

## 🛠️ Key Technical Achievements (Recent)

### 1. The `active_pages` Lifecycle
The `BatchReader` now manages an `active_pages` list. A page is only freed when `nextBatch` is called and the current read position has moved entirely past that page. This allows for zero-copy string slices to remain valid throughout the user's processing loop.

### 2. Comptime SIMD Kernels
Bit-unpacking for any width (1-32) is handled by specialized kernels generated at compile time. This eliminates branch mispredictions in the hot loop and allows the CPU to saturate memory bandwidth.

### 3. Full Type Parity
Support for "Legacy" types like `INT96` (timestamps) and `FIXED_LEN_BYTE_ARRAY` (Decimals/UUIDs) has been integrated into the vectorized path, ensuring ZPQ can handle any modern Parquet file.

---

## Lessons Learned: Zig 0.16.x and Development Workflow

### 1. The Shadowing Hazard
Zig 0.16 is strict about variable shadowing. Refactoring loops into `nextBatch` often triggers shadowing errors when reusing `i` or `count` from outer scopes. Prefer descriptive names like `page_idx` or `out_pos`.

### 2. Memory Safety in Vectorized Reads
Zero-copy is a double-edged sword. When batch reading across page boundaries, the previous page *must* stay alive until the batch is consumed. The `active_pages` pattern is the standard for ZPQ to prevent UAF (Use-After-Free) on string columns.

---

---

## Milestone 19.5: Zero-Copy Page Reading [COMPLETE - Dec 29, 2024]

**Discovery (Dec 29, 2024)**: Arrow C Data Interface benchmarking revealed that ZPQ is ~3x slower than PyArrow on local file reads, even for non-nullable PLAIN uncompressed data.

**Root Cause Analysis** (via Arrow C++ source inspection):

1. **PyArrow uses mmap**: `stream_->Read()` returns a slice/view of mapped memory - zero allocation, zero copy.
2. **PyArrow reuses buffers**: `SerializedPageReader` reuses a `decompression_buffer_` across pages.
3. **ZPQ allocates per-page**: `ColumnReader.next()` calls `allocator.alloc()` for every page (~8 pages × 1MB = 8 allocations + 8MB copies).

**Solution Implemented**:

#### Changes Made

1. **Added `getSlice` to RandomAccessSource vtable** (`src/zpq/io/interface.zig`)
   - Optional method returning `?[]const u8` for zero-copy access
   - Default implementation returns `null` (no zero-copy available)

2. **Implemented in MemorySource** (`src/zpq/io/local/memory_source.zig`)
   - Returns direct slice into pre-fetched buffer
   - Zero allocation, zero copy for uncompressed pages

3. **Zero-copy path in ColumnReader.next()** (`src/zpq/core/column.zig`)
   - If source supports `getSlice` AND codec is UNCOMPRESSED:
     - Returns borrowed Page with slice (no alloc, no copy)
   - Else: falls back to allocation path

4. **Reusable decompression buffer** (`src/zpq/core/column.zig`)
   - Added `decompression_buffer` field to ColumnReader
   - Grows as needed, reuses across pages (25% headroom)
   - Added `ColumnReader.deinit()` method to free buffer

5. **Updated Page struct** (`src/zpq/core/column.zig`)
   - Added `borrowed: bool` field (default false)
   - `Page.deinit()` only frees if `!borrowed`

6. **Updated Arrow bridge** (`examples/python_arrow/zpq_arrow.zig`)
   - Added `rg.prefetchColumns()` call before reading each column
   - Enables zero-copy path for prefetched columns

#### Verification Results (probes/test_zerocopy.zig)
```
--- Without prefetch ---
Pages: 9, Borrowed (zero-copy): 0

--- With prefetch ---
Pages: 9, Borrowed (zero-copy): 9
```

#### Benchmark Results (Dec 29, 2024)
| Scenario | PyArrow | ZPQ | Ratio |
|----------|---------|-----|-------|
| Compressed i64 (Snappy) | 4.9ms | 11.7ms | 0.42x |
| Uncompressed i64 | 1.1ms | 3.9ms | 0.28x |
| Uncompressed str_col | 6.5ms | 8.1ms | 0.80x |

**Note**: PyArrow remains faster. We tested mmap separately and it provided no benefit - the bottleneck is not syscall overhead. The remaining gap is likely in the decode path itself (BatchReader, RLE decoder, dictionary lookups). Profiling needed to identify specific hotspots.

---

## Known Issues / TODO

### Debug Output Leaking to Stdout
**Status**: Needs fix. Debug print statements in the page deallocation path are unconditionally enabled.

### Streaming Writer
**Status**: Planned for Phase 2. Currently, ZPQ is optimized for Read/Scan.
