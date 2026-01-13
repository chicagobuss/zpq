# Benchmark Methodology

## Processing Stages

When comparing Parquet readers, we must be clear about what "read" means:

| Stage | Description | ZPQ | PyArrow | Polars | DuckDB |
|-------|-------------|-----|---------|--------|--------|
| 1. Metadata | Parse footer, schema, row groups | ✓ | ✓ | ✓ | ✓ |
| 2. I/O | Read compressed bytes from disk | ✓ | ✓ | ✓ | ✓ |
| 3. Decompress | Snappy/Zstd/etc decode | ✓ | ✓ | ✓ | ✓ |
| 4. Decode | Parse PLAIN/RLE/Dict into values | ✓ | ✓ | ✓ | ✓ |
| 5. Materialize | Create Arrow/DataFrame in memory | - | ✓ | ✓ | ✓ |

ZPQ decodes values but doesn't create language-specific structures (DataFrames).

## Benchmark Levels

### Level 1: Metadata Only
- Parse footer, count rows/columns
- ZPQ: `file.readFooter()`
- PyArrow: `pq.ParquetFile(path).metadata`

### Level 2: I/O + Decompress
- Read and decompress all pages
- Count values from page headers
- Does NOT decode actual values
- **This is what `bench-projection` measures**

### Level 3: Full Decode (Apples-to-apples)
- Decompress AND decode all values (RLE indices, dictionary lookups)
- ZPQ: `bench-decode-full` - iterates through decoded values
- PyArrow/Polars: Read to Arrow table (without Python conversion)
- **This is the fair comparison**

### Level 4: Materialize to Language
- Create language-native structures (Python lists, DataFrames)
- Not applicable to ZPQ - it's a streaming reader
- Adds significant overhead (PyArrow `to_pylist()` is 10x slower)

## Fair Comparison Results (Level 3)

Column projection benchmark: reading first 3 columns, 5 iterations, warm cache.

### Minimum Times

| File Size | ZPQ | PyArrow | Polars | DuckDB |
|-----------|-----|---------|--------|--------|
| 1MB | 3.92ms | 5.75ms | **2.20ms** | 2.17ms |
| 10MB | 7.62ms | 7.46ms | **4.04ms** | 4.55ms |
| 100MB | 43.64ms | 22.70ms | **10.58ms** | 30.98ms |

### Average Times

| File Size | ZPQ | PyArrow | Polars | DuckDB |
|-----------|-----|---------|--------|--------|
| 1MB | 4.11ms | 16.22ms | 15.63ms | 10.68ms |
| 10MB | 7.94ms | 7.73ms | 4.49ms | 4.89ms |
| 100MB | 43.89ms | 24.81ms | 16.35ms | 32.55ms |

### Analysis

1. **Small files (1MB)**: ZPQ is competitive. First-run variance is high for all tools.

2. **Medium files (10MB)**: All tools perform similarly. Polars has slight edge.

3. **Large files (100MB)**: Polars is 4x faster than ZPQ. PyArrow is 2x faster.

**Why is ZPQ slower on large files?**
- Polars/DuckDB use SIMD-optimized decoders
- Polars has vectorized RLE decoding
- ZPQ decodes value-by-value (not vectorized yet)
- ZPQ's strength is streaming/lazy evaluation, not raw decode speed

### Previous Misleading Claim

Earlier benchmarks showed "29x faster" for column projection. This was **incorrect** because:
- ZPQ was measuring Level 2 (decompress only, no value decode)
- Competitors were measuring Level 3-4 (full decode + materialize)

The fair comparison shows ZPQ is competitive but not faster for CPU-bound decoding.

## ZPQ's Actual Advantages

1. **Streaming**: Process data without loading entire file
2. **Memory efficiency**: Arena allocator, zero-copy where possible
3. **Lazy evaluation**: Only decode what's needed
4. **Network-first**: Designed for S3/R2, not local files
5. **Predictable latency**: No GC pauses

## Benchmark Files

Created from `data/166mb_120columns.snappy.parquet`:
- `benchmark_1mb.parquet` - 6K rows, 1.3MB
- `benchmark_10mb.parquet` - 60K rows, 10.5MB
- `benchmark_100mb.parquet` - 602K rows, 95MB

All stored in Cloudflare R2: `s3://zpq-artifacts/benchmarks/`

## Cache Considerations

| Scenario | What it tests |
|----------|---------------|
| Cold cache | Real-world first read, I/O bound |
| Warm cache | CPU/decode bound, repeated queries |

Current benchmarks use warm cache (files already in OS page cache).
For true I/O benchmarks, use: `sync && echo 3 > /proc/sys/vm/drop_caches`

## S3/R2 Benchmark Results (Network)

Tested against Cloudflare R2 from local machine (2025-12-28).

### Full Scan (All Columns)

| File Size | ZPQ (avg) | DuckDB (avg) | ZPQ Speedup |
|-----------|-----------|--------------|-------------|
| 10MB | 846ms | 1835ms | **2.2x faster** |
| 100MB | 7084ms | 16396ms | **2.3x faster** |

### ZPQ Time Breakdown (100MB file)

| Stage | Time | % of Total |
|-------|------|------------|
| Open (HEAD) | 113ms | 1.6% |
| Footer (GET) | 94ms | 1.3% |
| Prefetch (GETs) | 6484ms | 91.5% |
| Decode | 392ms | 5.5% |

### Analysis

With large files over network, **download dominates** (~92% of time). ZPQ's advantages:

1. **Efficient parallel range GETs** with xev async I/O
2. **Fast decode** - 392ms for 62M values = 159M values/sec
3. **No Python/JIT overhead** - pure compiled code

The passthrough optimization (for 100% row selection) would eliminate the 392ms decode entirely.

### Methodology

```bash
# ZPQ
S3_HOST="${R2_ACCOUNT_ID}.r2.cloudflarestorage.com" \
S3_BUCKET="zpq" S3_KEY="benchmark_100mb.parquet" \
S3_ACCESS_KEY="..." S3_SECRET_KEY="..." \
NUM_COLS=0 ITERATIONS=5 \
zig build run-bench-s3

# DuckDB
con.execute("SET s3_endpoint='...'; SET s3_access_key_id='...'")
result = con.execute("SELECT * FROM 's3://zpq/benchmark_100mb.parquet'").fetchall()
```

DuckDB caching was disabled with `PRAGMA disable_object_cache`.

## TODO

- [ ] Add cold cache benchmarks (local)
- [ ] Add stddev/percentile statistics
- [x] ~~Benchmark S3/R2 (network latency)~~ Done - ZPQ 2.3x faster
- [ ] Profile ZPQ decoder for optimization opportunities
- [ ] Add SIMD decoding to ZPQ
- [ ] Test passthrough optimization on R2 (skip decode for 100% selection)

## Unified Benchmark Tooling (v2026)

We have consolidated benchmarking logic into `tools/bench.sh` and `tools/bench_engines.py`.

### Comparing Engines (S3 -> S3)

To run a fair "apples-to-apples" comparison between ZPQ, Polars, DuckDB, and PyArrow:

```bash
# Syntax: tools/bench.sh engine <engine> <input_type> [size] [runs] [scenario]
# Scenarios: pass-through | filter | select-1 | select-3

# Example: Compare Zero-Copy Performance (100MB)
./tools/bench.sh engine polars s3 100mb 1 pass-through
./tools/bench.sh engine duckdb s3 100mb 1 pass-through
```

The tool handles:
- Dependency management via `uv` (isolated environments)
- S3 credential propagation
- Standardized timing and reporting
