# ZPQ Benchmark Results

## Benchmark Methodology

### Build Configuration
All ZPQ benchmarks are built with `-Doptimize=ReleaseFast` (hardcoded in build_tests.zig).
This ensures consistent, reproducible results regardless of how the benchmark is invoked.

### Available Benchmark Commands

```bash
# Local file benchmarks
zig build run-bench-projection    # Column projection (local files)
zig build run-bench-decode        # Full decode (apples-to-apples with competitors)

# S3/R2 benchmarks (requires credentials)
zig build run-bench-s3            # S3/R2 column projection
zig build run-bench-e2e           # End-to-end S3 benchmark

# DNS benchmark
zig build run-bench-dns           # DNS resolution benchmark
```

### Environment Variables for S3 Benchmarks
```bash
export R2_ACCESS_KEY_ID="your_access_key"
export R2_SECRET_ACCESS_KEY="your_secret_key"
export R2_ENDPOINT="your-account.r2.cloudflarestorage.com"
```

### Competitor Benchmarks
Python competitors are in `benchmarks/competitors/` and use:
- PyArrow 18.x
- Polars 1.x  
- DuckDB 1.x

Run with:
```bash
cd benchmarks/competitors
python pyarrow_s3.py   # PyArrow S3 benchmark
python polars_s3.py    # Polars S3 benchmark
python duckdb_s3.py    # DuckDB S3 benchmark
```

---

## Phase 3: Local File Benchmarks (2024-12-15)

### Workload
- **Data**: `data/sample-data.parquet` (Multiple row groups, Snappy compression, Dictionary encoding)
- **Operation**: Scan (Read all pages, decompress, decode values, count totals)
- **Hardware**: Apple M1 Max

### Results

| Implementation | Tool / Method | Time (s) | Throughput (MB/s) | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **ZPQ (Zig)** | `zig build run ... scan` | **0.1601s** | **842.92** | Zero-copy page iteration, manual Snappy |
| **PyArrow** | `pyarrow.parquet.read_table` | 0.2919s | 372.54 | C++ backend, highly optimized |
| **Rust** | `ParquetRecordBatchReader` | 0.6500s | ~167.00 | Safe Arrow reader |

### Analysis
- ZPQ is **~1.8x faster** than PyArrow
- ZPQ is **~4x faster** than the safe Rust Arrow reader
- Performance advantage from zero-copy slicing and manual memory management

---

## M15: S3/R2 Column Projection Benchmarks (2024-12-26)

### Workload
- **Data**: Parquet files on Cloudflare R2 (1MB, 10MB, 100MB)
- **Operation**: Read 3 columns (`VendorID`, `passenger_count`, `trip_distance`)
- **Location**: Oracle Cloud ARM instance (same region as R2)
- **Network**: ~100ms RTT to R2

### Results (Before Optimizations - 64KB footer prefetch)

| File Size | ZPQ | PyArrow | Polars | DuckDB |
|-----------|-----|---------|--------|--------|
| 1MB | 180ms | 454ms | 261ms | **115ms** |
| 10MB | 216ms | 439ms | 288ms | **122ms** |
| 100MB | 203ms | 460ms | 263ms | **150ms** |

### Key Observations
- DuckDB is fastest due to:
  - Footer prefetching (256KB speculative read at end of file)
  - Read coalescing (16KB gap threshold)
  - Large connection pool (AWS SDK)
- ZPQ beats PyArrow and Polars but trails DuckDB

### Optimizations Applied (M15)
1. **Adaptive footer prefetch**: 16KB-256KB based on file size (`file_size / 256`, clamped)
   - Small files (1MB): 16KB prefetch
   - Medium files (10MB): ~40KB prefetch  
   - Large files (100MB+): 256KB prefetch (max)
2. **Small-gap coalescing**: Always merge reads within 16KB gaps (reduces HTTP request count)

### Provider Comparison: R2 vs AWS S3 (2024-12-26)

Testing from Oracle Cloud ARM instance (us-ashburn-1):

| File Size | R2 (avg) | S3 us-west-2 (avg) | Winner |
|-----------|----------|-------------------|--------|
| 1MB | 288ms | 242ms | **S3** (~16% faster) |
| 10MB | 371ms | 303ms | **S3** (~18% faster) |
| 100MB | 340ms | 280ms | **S3** (~18% faster) |

**Observations:**
- AWS S3 shows lower latency and more consistent timing from this test location
- R2 has higher variance (122ms spread vs 24ms for S3 on 1MB file)
- Both providers work correctly with ZPQ's adaptive prefetch and coalescing

### Competitor Comparison: S3 us-west-2 (2024-12-26)

Column projection benchmark (3 columns) from Oracle Cloud ARM instance:

| File Size | ZPQ | PyArrow | Polars | DuckDB |
|-----------|-----|---------|--------|--------|
| 1MB | **242ms** | 261ms | 245ms | 172ms* |
| 10MB | **303ms** | 280ms | 246ms | 267ms* |
| 100MB | **280ms** | 306ms | **255ms** | 905ms |

*DuckDB first run includes cache warmup (~370ms), subsequent runs faster for small files.

**Analysis:**
- ZPQ competitive with PyArrow/Polars across all file sizes
- DuckDB fastest for small files (connection pooling + caching)
- DuckDB slowest for 100MB (full table scan behavior?)
- Polars most consistent across file sizes

### Implementation Details

**Adaptive Footer Prefetch** (`src/zpq/core/file.zig:256-266`):
```zig
const MIN_PREFETCH = 16 * 1024;   // 16KB minimum
const MAX_PREFETCH = 256 * 1024;  // 256KB maximum
const adaptive_size = self.file_size / 256;
const clamped_size = @max(MIN_PREFETCH, @min(adaptive_size, MAX_PREFETCH));
```

**Small-Gap Coalescing** (`src/zpq/io/s3/scheduler.zig`):
```zig
pub const MIN_GAP_THRESHOLD = 16 * 1024; // 16KB
// In mergeRanges(): Always merge if gap <= MIN_GAP_THRESHOLD
```

---

## Reference Codebase Analysis

### DuckDB S3 Optimizations
- `references/duckdb/extension/parquet/parquet_reader.cpp:105-130`: Footer prefetching
- `references/duckdb/extension/parquet/include/thrift_tools.hpp`: 16KB read coalescing

### Arrow Optimizations  
- `ReadRangeCache` with `hole_size_limit` for coalescing
- Network-aware caching based on TTFB and bandwidth

### Polars Optimizations
- L2 prefetch instructions (`_mm_prefetch` with `_MM_HINT_T1`)
- Configurable madvise strategies
