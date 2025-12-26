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

## TODO

- [ ] Add cold cache benchmarks
- [ ] Add stddev/percentile statistics
- [ ] Benchmark S3/R2 (network latency)
- [ ] Profile ZPQ decoder for optimization opportunities
- [ ] Add SIMD decoding to ZPQ
