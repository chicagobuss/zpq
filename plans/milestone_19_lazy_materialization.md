# Milestone 19: Full Lazy Materialization Plan

## Critical Finding: I/O Dominates Everything

**December 2024 Discovery**: Benchmarking revealed that ZPQ's filtered scans take the same time as full scans over network storage (17.4s vs 17.5s for 100MB file on R2). The root cause: we prefetch ALL columns before evaluating filters.

```zig
// main.zig:210 - THE BOTTLENECK
try rg.prefetch(null);  // Fetches ALL columns before any filtering
```

**This means**: Optimizing `BatchReader.skip()` or the interleaved loop provides marginal gains. The real win is **not fetching bytes we don't need**.

See `plans/two_phase_column_fetching.md` for the detailed implementation plan.

---

## Revised Objective

Achieve "Lambda Parquet Dominance" by minimizing **I/O bytes fetched**, not just CPU cycles. For a 1% selectivity filter:

| Metric | Current | Target |
|--------|---------|--------|
| Bytes fetched | 100% | <5% |
| Query time (100MB, R2) | 17.4s | <2s |
| Throughput | 0.03 MVal/s | >10 MVal/s |

---

## Revised Implementation Strategy

### Phase 1: Two-Phase Column Fetching (P0 - Critical)

**The key insight from DuckDB** (`parquet_reader.cpp:1316-1338`):
```cpp
// lazy fetching: buffer is only fetched on first read
bool lazy_fetch = filters != nullptr;

// Only eagerly prefetch filter columns; others are lazy
root_reader.GetChildReader(file_col_idx)
    .RegisterPrefetch(trans, !(lazy_fetch && !has_filter));
```

**ZPQ implementation:**

```zig
if (filter_col_idx) |f_idx| {
    // Phase 1: Fetch ONLY filter column
    try rg.prefetchColumns(&[_]usize{f_idx});
    
    // Decode filter column, build selection vector
    const selection = try buildSelectionVector(rg, f_idx, filter_val);
    
    // Early exit if no matches
    if (selection.count() == 0) {
        continue;  // Skip entire row group - NO I/O for other columns!
    }
    
    // Phase 2: Fetch remaining columns (only if needed)
    try rg.prefetchExcluding(&[_]usize{f_idx});
    
    // Decode with selection vector...
} else {
    try rg.prefetch(null);  // No filter - fetch all
}
```

**Files to modify:**
- `src/zpq/core/file.zig` - Add `prefetchColumns()`, `prefetchExcluding()`
- `src/main.zig` - Refactor `cmdScan` for two-phase approach

**Estimated effort**: 4-6 hours
**Expected impact**: 10-50x speedup for low-selectivity queries

### Phase 2: Interleaved Batch Processing (P1 - High)

Once we have selection vectors from Phase 1:

```zig
while (row_idx < rg.num_rows) {
    const batch_size = @min(1024, rg.num_rows - row_idx);
    
    // Filter already evaluated in Phase 1
    // Use pre-built selection vector for this batch range
    
    for (other_readers) |r| {
        if (batch_selection.anySet()) {
            _ = try r.nextBatchSelected(buf, batch_selection, batch_size);
        } else {
            try r.skip(batch_size);  // Skip is now I/O-free (data already fetched)
        }
    }
    row_idx += batch_size;
}
```

**Status**: Partially implemented. Needs integration with two-phase prefetch.

### Phase 3: Page-Level ColumnIndex Support (P1 - High)

Parse Parquet ColumnIndex to skip pages within row groups:

```
Row Group 0, Filter: product_code = "AWSLambda"
  Page 0: min="A", max="F"      → SKIP (Lambda > F)
  Page 1: min="G", max="M"      → FETCH (Lambda in range)  
  Page 2: min="N", max="Z"      → SKIP (Lambda < N)
```

**Files to modify:**
- `src/zpq/core/schema.zig` - Add ColumnIndex types
- `src/zpq/core/thrift.zig` - Parse ColumnIndex from footer
- `src/zpq/core/file.zig` - Page-level skip logic

**Estimated effort**: 8-12 hours

### Phase 4: BatchReader.skip() Optimization (P2 - Medium)

The original focus of this milestone. Still valuable but lower priority than I/O reduction.

Current skip implementation decodes def levels to count non-nulls:
```zig
// batch_reader.zig:78-89 - Decodes to count
if (self.def_levels_decoder) |*d| {
    const n = try d.nextBatch(def_levels[0..batch_rem]);  // DECODING
    for (def_levels[0..n]) |dl| {
        if (dl == self.max_def_level) values_to_skip_in_data += 1;
    }
}
```

**Optimization**: Use `RleDecoder.skip()` directly (already O(1) for RLE runs).

**Estimated effort**: 2-3 hours

---

## Competitor Evidence

### DuckDB
- Implements explicit lazy column fetching when filters present
- `RegisterPrefetch(trans, !(lazy_fetch && !has_filter))`
- See `references/duckdb/extension/parquet/parquet_reader.cpp:1316-1338`

### Polars
- Predicate pushdown: "Applies filters as early as possible/at scan level"
- Projection pushdown: "Select only needed columns at scan level"

### Arrow
- ARROW-11074: Predicate push-down for parquet tables
- ARROW-13797: Column projection pushdown

---

## Probes & Benchmarks

### Existing Probes
- `probes/test_lazy_materialization.zig` - Correctness verification
- `probes/test_skip_performance.zig` - Skip vs decode micro-benchmark
- `probes/bench_skip_breakdown.zig` - Time breakdown analysis

### New Probes Needed
- `probes/bench_two_phase_prefetch.zig` - Measure I/O savings
- `probes/bench_page_skip.zig` - Measure ColumnIndex page skip savings

### Key Benchmark
```bash
# Current baseline
source .env && zpq scan s3://$R2_BUCKET/benchmark_100mb.parquet \
  --filter line_item_product_code=AWSLambda

# Target: 10x+ faster than current 17.4s
```

---

## Implementation Roadmap

| Priority | Task | Effort | Status |
|----------|------|--------|--------|
| P0 | Two-phase column prefetch | 4-6h | Not started |
| P0 | Integrate with cmdScan | 2-3h | Not started |
| P1 | Page-level ColumnIndex parsing | 4-6h | Not started |
| P1 | Page-level skip in RowGroupReader | 4-6h | Not started |
| P2 | BatchReader.skip() def-level optimization | 2-3h | Not started |
| P2 | Interleaved loop overhead reduction | 2-3h | Partial |

---

## Success Criteria

1. **I/O Reduction**: 1% selectivity query fetches <5% of file bytes
2. **Latency**: 1% selectivity query on 100MB R2 file completes in <2s
3. **Correctness**: Filtered results match full-scan-then-filter baseline
4. **No Regression**: Full scan performance unchanged

---

## References

- `plans/two_phase_column_fetching.md` - Detailed implementation plan
- `plans/critical_skip_optimization.md` - Original skip analysis (superseded)
- `references/duckdb/extension/parquet/parquet_reader.cpp` - DuckDB implementation
- `bench/traces/` - Benchmark results
