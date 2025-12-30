# ZPQ Technical Context and Detail

**Last Updated**: Dec 30, 2024
**Current State**: Phase 1 (Read Dominance) in progress. **Critical discovery**: I/O dominates filtered scan performance. Two-phase column fetching is the top priority.

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

### Milestone 19: Predicate Pushdown & Lazy I/O [IN PROGRESS - CRITICAL]

**Critical Discovery (Dec 2024)**: Benchmarking revealed that filtered scans take the **same time** as full scans over network storage:

| Scan Type | Values | Time | Throughput |
|-----------|--------|------|------------|
| Full scan (R2) | 88M | 17.5s | 5.06 MVal/s |
| Filtered 1% (R2) | 602K | 17.4s | 0.03 MVal/s |

**Root Cause**: `rg.prefetch(null)` fetches ALL columns before filter evaluation.

**Solution**: Two-phase column fetching (like DuckDB). See `plans/two_phase_column_fetching.md`.

#### Completed
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

#### R2 Benchmark Results (Dec 30, 2024) - THE PROOF
| Scenario | Time | Speedup | Notes |
|----------|------|---------|-------|
| Full scan (88M values) | **59.8s** | 1.0x | Baseline - fetches all columns |
| Filtered 100% (602K selected) | 15.4s | 3.9x | All rows match, but only filter col fetched in Phase 1 |
| Filtered 0.001% (4 selected) | **14.0s** | 4.3x | Only 4 rows match - skips most non-filter I/O |
| Filtered 0% (0 matches) | **0.26s** | **230x** | Row group stats skip 1 RG, no Phase 2 needed |

**Key Insight**: Even with 100% selectivity, we see 4x improvement because Phase 1 only fetches the filter column. The full 10-50x improvement requires ColumnIndex (P1) to skip pages within row groups.

#### Local Benchmark Results (Dec 30, 2024)
| Scenario | Time | Notes |
|----------|------|-------|
| Full scan (88M values) | 10.3s | Baseline |
| Filtered scan (602K selected) | 9.9s | High selectivity, similar decode time |
| Filtered scan (0 matches) | 41ms | Row group stats skip all 5 RGs |
| Filtered scan (4 matches) | 5.2s | Scans filter column only, skips non-filter I/O |

#### Planned (P1 - High)
*   [ ] **Page-Level ColumnIndex**: Parse ColumnIndex from footer for per-page min/max.
*   [ ] **Page-Level Skip**: Skip entire pages within row groups based on filter.

#### Planned (P2 - Medium)  
*   [ ] **BatchReader.skip() Optimization**: Use `RleDecoder.skip()` for def levels.
*   [ ] **Interleaved Loop Optimization**: Reduce tagged union overhead.

**Target**: 10-50x speedup for low-selectivity queries over network storage.

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

## Known Issues / TODO

### Debug Output Leaking to Stdout
**Status**: Needs fix. Debug print statements in the page deallocation path are unconditionally enabled.

### Streaming Writer
**Status**: Planned for Phase 2. Currently, ZPQ is optimized for Read/Scan.
