# ZPQ Technical Context and Detail

**Last Updated**: Jan 2, 2025
**Current State**: Phase 1 (Read Dominance) - Milestone 20 complete. Unibin Lambda deployment with runtime io_uring → epoll fallback complete. S3 output working with epoll backend.

---

## Current Work: Lambda Benchmarking Matrix

### Context
Benchmarking zpq performance across different environments to establish baselines before Phase 2 (Write Path).

### Benchmark Matrix Status

| Environment | File Size | Storage | Status | Notes |
|-------------|-----------|---------|--------|-------|
| RIE + Local | 10mb × 3 | Local | **COMPLETE** | Baseline established |
| RIE + Local | 100mb × 1 | Local | **COMPLETE** | |
| RIE + S3 | 10mb × 3 | S3 | **COMPLETE** | |
| RIE + S3 | 100mb × 1 | S3 | **COMPLETE** | |
| Lambda + S3 | 10mb × 3 | S3 | **COMPLETE** | 64.4ms median |
| Lambda + S3 | 100mb × 1 | S3 | **COMPLETE** | 245.7ms |
| RIE + S3 Output | 10mb | S3→S3 | **COMPLETE** | 13.4s (epoll) |

### Milestone 20: Unibin Lambda Deployment [COMPLETE - Jan 1, 2025]

**Problem**: Lambda deployment failed with `SystemOutdated` error because libxev defaults to io_uring on Linux, but Lambda's older kernel doesn't support io_uring.

**Solution**: Use libxev's built-in `xev.Dynamic` API for runtime backend detection.

#### Key Changes

1. **Runtime Backend Detection** (`src/main.zig`, `src/lambda.zig`)
   - Added `xev.Dynamic.detect()` call at startup (when available)
   - On Linux: probes for io_uring, falls back to epoll if unavailable
   - On macOS: single kqueue backend, no detection needed
   - Uses `@hasDecl(xev.Dynamic, "detect")` for cross-platform compatibility

2. **Unified Loop Type** (all execution paths)
   - Changed `xev.Loop` → `xev.Dynamic.Loop` throughout codebase
   - `src/query.zig`: `executeQuery` accepts `*xev.Dynamic.Loop`
   - `src/zpq/core/pipeline.zig`: `setRuntime` accepts `*xev.Dynamic.Loop`
   - `src/zpq/io/s3/factory.zig`: `OpenOptions.loop` is `*xev.Dynamic.Loop`

3. **Generic Completions Use Dynamic** (`src/zpq/core/row_group_worker.zig`)
   - `WorkerCompletion = WorkerCompletionGen(xev.Dynamic)`
   - `SlotWriteCompletion = SlotWriteCompletionGen(xev.Dynamic)`

4. **S3 Sources Use Dynamic** (`src/zpq/io/s3/factory.zig`)
   - `XevS3SourceGen(xev.Dynamic)` for internal S3 stack
   - `ResolverGen(xev.Dynamic)` for DNS resolution

#### How It Works

```
Single Binary → Startup → xev.Dynamic.detect()
                              ↓
              ┌───────────────┴───────────────┐
              ↓                               ↓
        io_uring available?             Only one backend?
              ↓                               ↓
         Use io_uring                   Use that backend
              ↓                          (kqueue/epoll)
         Lambda Kernel?
              ↓
         Use epoll (fallback)
```

**Key insight**: `xev.Dynamic` is a valid XevApi type that works with all the generic types (`XevS3SourceGen`, `S3WriterGen`, `SlotWriteCompletionGen`). On single-backend systems (macOS), `xev.Dynamic` resolves to the static API with zero overhead.

#### Files Modified
- `src/main.zig` - Dynamic detect + loop type
- `src/lambda.zig` - Dynamic detect + loop type + backend name handling
- `src/query.zig` - Accept `*xev.Dynamic.Loop`
- `src/zpq/core/pipeline.zig` - Loop field and setRuntime type
- `src/zpq/core/row_group_worker.zig` - Default completion types
- `src/zpq/io/s3/factory.zig` - OpenOptions and internal S3 source

### Milestone 20.5: S3 Writer with xev.Dynamic [COMPLETE - Jan 2, 2025]

**Problem**: S3 multipart uploads failed with `MissingContentLength` error when using epoll backend.

**Root Cause**: 
1. S3 multipart upload responses use `Transfer-Encoding: chunked`, but our HTTP response parser only supported `Content-Length`
2. Bucket host was being incorrectly overwritten with path-style endpoint for standard AWS S3

**Solution**:

1. **Chunked Transfer-Encoding Support** (`src/zpq/io/http/response_parser.zig`)
   - Added `is_chunked` field and chunked state machine
   - New states: `reading_chunk_size`, `reading_chunk_data`, `reading_chunk_trailer`
   - Added `consumeChunked()` function to parse chunked response bodies
   - Updated `parseHeaders()` to detect `Transfer-Encoding: chunked`

2. **Fixed S3 Bucket Host Generation** (`src/zpq/core/pipeline.zig`)
   - Only override host/port/tls for custom endpoints (MinIO, R2, LocalStack)
   - For standard AWS S3, use virtual-hosted style: `{bucket}.s3.{region}.amazonaws.com`

#### Working Example Commands

```bash
# Source AWS credentials
source .env

# Start Lambda RIE with epoll (io_uring blocked via seccomp)
cd benchmarks && docker-compose -f docker-compose.no-io-uring.yml up -d

# S3 read + local output (2.5s)
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"file": "s3://{BUCKET}/benchmark/benchmark_10mb.parquet", "output": "/tmp/zpq_bench_output.parquet", "filter": "string_dict_low=category_0001", "select": "int32_sorted,string_dict_low,float64"}'

# S3 read + S3 output (13.4s with multipart upload)
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"file": "s3://{BUCKET}/benchmark/benchmark_10mb.parquet", "output": "s3://{BUCKET}/output/zpq_bench_output.parquet", "filter": "string_dict_low=category_0001", "select": "int32_sorted,string_dict_low,float64"}'

# Verify output with DuckDB
duckdb -c "SELECT COUNT(*), COUNT(DISTINCT string_dict_low) FROM 's3://{BUCKET}/output/zpq_bench_output.parquet'"
# Result: 5142 rows, 1 distinct value (category_0001)
```

#### Files Modified
- `src/zpq/io/http/response_parser.zig` - Chunked encoding support
- `src/zpq/core/pipeline.zig` - Fixed S3 bucket host for standard AWS

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

## Key Technical Achievements (Recent)

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

### 3. libxev Dynamic Backend Selection (Jan 2025)
When using libxev for cross-platform async I/O:
- `xev.Dynamic` provides runtime backend detection (io_uring vs epoll)
- On single-backend systems (macOS/kqueue), `xev.Dynamic` equals the static API
- Use `@hasDecl(xev.Dynamic, "detect")` to check if detection is needed
- All generic types (`XevS3SourceGen`, etc.) work with `xev.Dynamic` as the XevApi parameter

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

---

## Bugs Fixed (Recent)

### BOOLEAN Bit-Packed + RLE Double-Decrement (Dec 31, 2024)
- **Symptom**: Boolean columns with RLE encoding produced incorrect values
- **Root Cause**: Double-decrement of `remaining_values` in RLE decoder
- **Fix**: Removed duplicate decrement in `RleDecoder.readBitPacked()`

### S3 Range Request Sorting (Dec 31, 2024)
- **Symptom**: S3 reads returned corrupted data when ranges were coalesced
- **Root Cause**: Range coalescing sorted by start offset but didn't preserve original buffer mapping
- **Fix**: Track original indices through sort and map back correctly
