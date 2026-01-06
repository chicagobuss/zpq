# ZPQ Filter Implementation Status

**Last Updated**: January 5, 2026
**Status**: Filter Implementation In Progress

---

## Current Work: Filter System Implementation

We are implementing a comprehensive filter system for ZPQ's surgical read engine. The goal is to support SQL-like filter predicates with page-level pruning using Parquet ColumnIndex statistics.

### Completed Filters ✅

#### 1. Range Filters (`>`, `>=`, `<`, `<=`)
- **File**: `src/zpq/core/filters/range.zig`
- **Status**: Fully working in both regular and surgical modes
- **Features**:
  - Page-level pruning using ColumnIndex min/max
  - Row-group statistics pruning
  - Support for INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY
- **Test Results**:
  - `>` operator: 124287 rows for values > 400000
  - `>=` operator: 124288 rows (one more including boundary)
  - Surgical mode: 99.5% I/O reduction achieved

#### 2. IS NULL / IS NOT NULL Filters
- **File**: `src/zpq/core/filters/null.zig`
- **Status**: Working with one limitation
- **Features**:
  - Page pruning using null_counts from ColumnIndex
  - Proper null handling in row-level filtering
- **Limitation**: IS NULL filter column cannot be in output (ZPQ doesn't write definition levels)
- **Test Results**: Both operators work correctly

#### 3. BETWEEN Filter
- **File**: `src/zpq/core/filters/between.zig`
- **Status**: Fully working in both regular and surgical modes
- **Features**:
  - Page-level pruning: skip if page_max < low OR page_min > high
  - Inclusive range matching (BETWEEN 100 AND 200 includes both endpoints)
- **Test Results**:
  ```
  Filter: int32_sorted BETWEEN 100 AND 200
  Output: 101 rows (values 100-200 inclusive)
  Surgical mode: Working correctly
  ```

### In Progress 🔄

#### 4. Dictionary Pre-Filter
- **Files Modified**: `src/zpq/core/row_group_worker.zig`
**Dictionary Pre-Filter Bug Fixed**:
- Issue: Integer equality filtering returned 0 rows due to raw string vs encoded byte mismatch.
- Fix: usages `EncodedFilter.bytes` for dictionary lookup instead of raw CLI string.
- Status: **FIXED**
Integer equality filtering returns 0 rows even when values exist. Example:
```bash
./zpq data/benchmark/benchmark_100mb.parquet /tmp/out.parquet --filter "int32_sorted = 1000"
# Returns 0 rows, but value 1000 exists at index 1000 in row group 0
```

**Bug Investigation Status**:
- `Filter.matchesInt32()` uses `std.mem.asBytes(&value)` to compare
- `EncodedFilter.parse()` correctly encodes i32 as little-endian bytes
- The encoding should match, but something is broken
- Need to add debug output or write unit tests to isolate

**Next Steps to Fix**:
1. [x] Fix test compilation errors in `between.zig` and `null.zig` (ColumnIndex struct missing `boundary_order` field) - **DONE**
2. Add unit test that verifies INT32 equality matching works
3. Debug why `matchesInt32` fails in practice

### Pending ⏳

#### 5. Bloom Filter Support
- **Status**: Not started
- **Description**: Use Parquet Bloom filters for faster filtering when available
- **Files to create**: `src/zpq/core/filters/bloom.zig`

#### 6. AND Conjunctions
- **Status**: Not started
- **Description**: Support multiple filter predicates with AND logic
- **Example**: `--filter "col1 > 100 AND col2 = 'foo'"`
- **Required changes**:
  - Parse AND in predicate string
  - Multiple filters in FilterContext
  - Combine page pruning (intersection)

---

## Architecture Overview

### Filter Module Structure
```
src/zpq/core/filters/
├── mod.zig          # Unified Filter interface
├── range.zig        # RangeFilter for >, >=, <, <=
├── null.zig         # NullFilter for IS NULL / IS NOT NULL
└── between.zig      # BetweenFilter for BETWEEN x AND y
```

### Unified Filter Interface (`mod.zig`)
```zig
pub const Filter = union(enum) {
    equality: EncodedFilter,    // From filter.zig
    range: RangeFilter,
    null_check: NullFilter,
    between: BetweenFilter,
    
    // Page-level pruning
    pub fn mightMatchPage(...) bool;
    pub fn mightMatchRowGroup(...) bool;
    
    // Row-level matching
    pub fn matchesInt32(value: i32) bool;
    pub fn matchesInt64(value: i64) bool;
    pub fn matchesFloat(value: f32) bool;
    pub fn matchesDouble(value: f64) bool;
    pub fn matchesBytes(value: []const u8) bool;
    pub fn matchesNull() bool;
    
    // Type checks
    pub fn isEquality() bool;
    pub fn isNullCheck() bool;
    pub fn isBetween() bool;
};
```

### Key Files Modified
- `src/zpq/core/pipeline.zig` - Predicate parsing, filter creation, surgical engine calls
- `src/zpq/core/row_group_worker.zig` - Filter context, scanning, dictionary pre-filter
- `src/zpq/core/surgical/engine.zig` - SurgicalEngine with BETWEEN support

---

## Surgical Read Engine Status

The surgical read engine is **working** with all implemented filters:

```bash
# All these work:
./zpq input.parquet output.parquet --filter "col > 100" --surgical
./zpq input.parquet output.parquet --filter "col >= 100" --surgical
./zpq input.parquet output.parquet --filter "col < 100" --surgical
./zpq input.parquet output.parquet --filter "col <= 100" --surgical
./zpq input.parquet output.parquet --filter "col IS NOT NULL" --surgical
./zpq input.parquet output.parquet --filter "col BETWEEN 100 AND 200" --surgical

# String equality works (uses dictionary fast path):
./zpq input.parquet output.parquet --filter "string_col = 'value'" --surgical
```

### Surgical I/O Savings
Typical results on sorted columns:
- Without surgical: ~25MB fetched per row group
- With surgical: ~50KB-800KB fetched (99%+ reduction)

---

## Test Commands

```bash
# Build
cd /Users/joshua/code/zpq && zig build

# Test BETWEEN (working)
./zig-out/bin/zpq data/benchmark/benchmark_100mb.parquet /tmp/test.parquet \
  --filter "int32_sorted BETWEEN 100 AND 200" --select "int32_sorted" --surgical

# Test string equality (working)
./zig-out/bin/zpq data/benchmark/benchmark_100mb.parquet /tmp/test.parquet \
  --filter "string_dict_low = category_0005" --select "string_dict_low"

# Test dictionary pre-filter (working for strings)
./zig-out/bin/zpq data/benchmark/benchmark_100mb.parquet /tmp/test.parquet \
  --filter "string_dict_low = nonexistent" --select "string_dict_low"
# ^ Should return 0 rows in ~0.5ms (early exit)

# Test integer equality (BUG - returns 0 when should return 1)
./zig-out/bin/zpq data/benchmark/benchmark_100mb.parquet /tmp/test.parquet \
  --filter "int32_sorted = 1000" --select "int32_sorted"
```

---

## Known Issues

### 1. Integer Equality Filter Bug
- **Symptom**: `int32_sorted = 1000` returns 0 rows but value exists
- **Location**: Likely in `Filter.matchesInt32()` or related code
- **Impact**: Integer equality filters broken

### 2. Test Compilation Errors
- **Files**: `between.zig` line 259, `null.zig` line 201
- **Issue**: ColumnIndex struct tests use `null` for required fields
- **Fix needed**: Add `boundary_order` field and use empty slices instead of null

---

## Handoff Notes

To continue this work:

1. **Immediate Priority**: Fix integer equality bug
   - The filter system changed from `encoded_filter` to unified `Filter`
   - Check that `matchesInt32()` byte comparison works correctly
   - Add unit test in `mod.zig` for INT32 equality

2. **Fix Test Compilation**:
   - Update test ColumnIndex structs in `between.zig` and `null.zig`
   - Add `boundary_order: .UNORDERED` field
   - Change `null_pages = null` to `null_pages = &[_]bool{false}`

3. **Then Implement**:
   - Bloom filter support (optional, lower priority)
   - AND conjunctions (higher priority for usability)

4. **Related Context**:
   - Morsel architecture design in this file (below) is separate work
   - S3 output support was recently added (PR #8)
   - Surgical mode is the main optimization path for selective queries

---

# Historical Context: Morsel Architecture Design

(Previous content about parallel S3 I/O morsel architecture preserved below for reference)

**Note**: The morsel architecture is separate from the current filter implementation work. It deals with parallel S3 writes using multipart upload.

---

## Vision: Fully Pipelined Parallel S3 I/O

The goal is to overlap all pipeline stages so that row groups flow through independently:

```
Time →
────────────────────────────────────────────────────────────────────────────
S3 Read:    [RG1 bytes][RG2 bytes][RG3 bytes][RG4 bytes]
Decode:          [RG1]     [RG2]     [RG3]     [RG4]
Filter:            [RG1]     [RG2]     [RG3]     [RG4]
Encode:              [RG1]     [RG2]     [RG3]     [RG4]
S3 Write:              [part1]   [part2]   [part3]  [part4][footer]
```

Each row group is a **morsel** - an independent unit that flows through the pipeline without blocking other morsels.

---

## S3 Multipart Upload Integration

This morsel architecture enables parallel S3 writes via multipart upload:
- Parts can upload in any order
- Footer is uploaded last after calculating byte offsets
- Currently implemented and working (see `src/zpq/io/s3/writer.zig`)
