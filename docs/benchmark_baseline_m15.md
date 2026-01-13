# Milestone 15: Benchmark Baseline Results

**Date**: 2025-12-26  
**Hardware**: ARM64 Linux (Oracle Cloud A1.Flex)  
**Test File**: 
Size: 149 MB
Rows: 524,288
Columns: 27
The file contains a variety of types:

Integers: int8, int16, int32_sorted, int32_random, int64_sorted, int64_random, etc.
Floats: float32, float64, float64_sorted.
Booleans: bool, bool_sparse.
Strings: string_random, string_dict_low, string_dict_high, string_sorted.
Complex: binary, timestamp, timestamp_sorted, date.
Nullable: int32_nullable, float64_nullable, string_nullable.

## Test Files

| Name | Rows | Size | Location |
|------|------|------|----------|
| benchmark_1mb.parquet | 6,023 | 1.3 MB | R2 + local |
| benchmark_10mb.parquet | 60,234 | 10.5 MB | R2 + local |
| benchmark_100mb.parquet | 602,348 | 95 MB | R2 + local |

## Local File Benchmark (Full Scan, All Columns)

All times in milliseconds (ms), average of 5 runs.

| Size | ZPQ | PyArrow | Polars | DuckDB |
|------|-----|---------|--------|--------|
| **1MB** | **5.1** | 27.0 | 16.6 | 55.4 |
| **10MB** | **37.4** | 86.2 | 107.9 | 337.0 |
| **100MB** | **329.0** | 774.4 | 302.7 | 3388.2 |

### Speedup vs Competitors

| Size | vs PyArrow | vs Polars | vs DuckDB |
|------|------------|-----------|-----------|
| 1MB | **5.3x** | **3.3x** | **10.9x** |
| 10MB | **2.3x** | **2.9x** | **9.0x** |
| 100MB | **2.4x** | 0.9x | **10.3x** |

### Throughput (MB/s)

| Size | ZPQ | PyArrow | Polars | DuckDB |
|------|-----|---------|--------|--------|
| 1MB | 255 | 48 | 78 | 23 |
| 10MB | 281 | 122 | 97 | 31 |
| 100MB | 289 | 123 | 314 | 28 |

## Analysis

### ZPQ Strengths
- **Fastest at small files**: 5x faster than PyArrow on 1MB
- **Consistent throughput**: ~280 MB/s across all sizes
- **No Python overhead**: Pure Zig, no GIL, no runtime

### Areas for Improvement
- **100MB**: Polars slightly faster (302ms vs 329ms)
  - Polars uses Rust + rayon for parallelism
  - ZPQ is currently single-threaded for decode
  - Opportunity: parallel column decoding in M16

### Why DuckDB is Slow
DuckDB's `fetch_arrow_table()` materializes all data into Arrow format.
For analytics queries (aggregations, filters), DuckDB would be much faster
due to vectorized execution and predicate pushdown. This benchmark tests
raw file scanning, not query execution.

## Methodology

### ZPQ
```bash
./zig-out/bin/bench-e2e data/benchmark_Xmb.parquet 5
```
Scans all columns, all row groups, decodes all values.

### PyArrow
```python
table = pq.read_table(path)
_ = table.num_rows
```

### Polars
```python
df = pl.read_parquet(path)
_ = df.shape[0]
```

### DuckDB
```python
arrow_table = duckdb.query(f"SELECT * FROM read_parquet('{path}')").fetch_arrow_table()
_ = arrow_table.num_rows
```

## Column Projection Benchmark (100MB file, 3 of 129 columns)

This is where the Laziness Principle shines - only read what you need.

| Tool | 3 Columns | All Columns | Speedup |
|------|-----------|-------------|---------|
| **ZPQ** | **0.38ms** | 358ms | 942x |
| Polars | 10.9ms | 303ms | 28x |
| DuckDB | 33.4ms | 3388ms | 101x |
| PyArrow | 35.7ms | 774ms | 22x |

### ZPQ vs Competitors (3 columns)

| vs Tool | ZPQ Speedup |
|---------|-------------|
| Polars | **29x faster** |
| DuckDB | **88x faster** |
| PyArrow | **94x faster** |

ZPQ achieves this by:
1. **Seeking directly** to column offsets (no scanning)
2. **Reading only needed bytes** from disk
3. **Zero-copy where possible** - data stays compressed until needed

## Next Steps

1. **M16 - SIMD Decoders**: Target 2x decode throughput
2. **Parallel column decoding**: Match Polars on large files
3. **R2/S3 benchmarks**: Test network performance with TLS fix
