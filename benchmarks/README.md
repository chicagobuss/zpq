# ZPQ Lambda Benchmarks

Benchmark zpq filter performance across deployment environments.

## Quick Start

```bash
# 1. Source AWS credentials (from project root)
cd /path/to/zpq
source .env

# 2. Build for Lambda (aarch64-linux)
zig build -Doptimize=ReleaseFast -Dtarget=aarch64-linux
cp zig-out/bin/zpq benchmarks/zpq

# 3. Start Lambda RIE
cd benchmarks
docker-compose up -d

# 4. Run a benchmark
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"file": "/data/benchmark_10mb.parquet", "output": "/tmp/out.parquet", "filter": "string_dict_low=category_0001", "select": "int32_sorted,string_dict_low,float64"}'
```

## Example Commands

### Local File Read
```bash
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"file": "/data/benchmark_10mb.parquet", "output": "/tmp/zpq_bench_output.parquet", "filter": "string_dict_low=category_0001", "select": "int32_sorted,string_dict_low,float64"}'
```

### S3 Read + Local Output
```bash
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"file": "s3://{BUCKET}/benchmark/benchmark_10mb.parquet", "output": "/tmp/zpq_bench_output.parquet", "filter": "string_dict_low=category_0001", "select": "int32_sorted,string_dict_low,float64"}'
```

### S3 Read + S3 Output
```bash
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"file": "s3://{BUCKET}/benchmark/benchmark_10mb.parquet", "output": "s3://{BUCKET}/output/zpq_bench_output.parquet", "filter": "string_dict_low=category_0001", "select": "int32_sorted,string_dict_low,float64"}'
```

### Force epoll Backend (Simulate Lambda)
Lambda's older kernel doesn't support io_uring. To test epoll locally:

```bash
docker-compose -f docker-compose.no-io-uring.yml up -d
```

This uses a seccomp profile (`no-io-uring.json`) to block io_uring syscalls.

## Test Data

**Local**: `/tmp/zpq_r2_bucket/benchmark/` (mounted as `/data` in container)
**S3**: `s3://{BUCKET}/benchmark/` (us-west-2)

### Files

| File | Size | Rows |
|------|------|------|
| benchmark_10mb.parquet | 16MB | ~52K |
| benchmark_100mb.parquet | 156MB | ~524K |

Compression variants: `_snappy` (default), `_gzip`, `_zstd`, `_none`

### Schema (27 columns)

Key columns for filtering:
- `string_dict_low` - 10 categories (~10% each): `category_0000` through `category_0009`
- `bool` - ~51% true
- `bool_sparse` - ~1% true
- `int32_sorted` - sequential integers

## Filter Scenarios

| Scenario | Filter | Selectivity | Use Case |
|----------|--------|-------------|----------|
| **High selectivity** | `bool_sparse=true` | ~1% | Needle in haystack |
| **Medium selectivity** | `string_dict_low=category_0001` | ~10% | Category filter |
| **Low selectivity** | `bool=true` | ~50% | Broad filter |

## Deployment Environments

| Environment | Description |
|-------------|-------------|
| **RIE + local** | Lambda RIE, local parquet file |
| **RIE + S3** | Lambda RIE, real S3 (us-west-2) |
| **Lambda + S3** | Real Lambda (arm64/1769MB, us-west-2) |

---

## Results

### Phase 1: zpq Baseline

**Filter**: `string_dict_low=category_0001` (~10% selectivity)

#### 10mb (3 runs, times in ms)

| Environment | Run 1 | Run 2 | Run 3 | Median | Rows Out |
|-------------|-------|-------|-------|--------|----------|
| RIE + local | 80.0 | 65.6 | 64.1 | 65.6 | 5,142 |
| RIE + S3 | 1779.3 | 2615.6 | 1637.4 | 1779.3 | 5,142 |
| Lambda + S3 | 68.0 | 64.4 | 64.4 | **64.4** | 5,142 |

#### 100mb (1 run)

| Environment | Time (ms) | Rows Out |
|-------------|-----------|----------|
| RIE + local | 553.8 | 52,526 |
| RIE + S3 | 1860.2 | 52,526 |
| Lambda + S3 | **245.7** | 52,526 |

#### S3 Output (RIE + epoll)

| Scenario | Time | Notes |
|----------|------|-------|
| S3 read + S3 write (10mb) | 13.4s | Includes multipart upload |

### Verification

```bash
# Verify output with DuckDB
duckdb -c "SELECT COUNT(*), COUNT(DISTINCT string_dict_low) FROM 's3://{BUCKET}/output/zpq_bench_output.parquet'"
# Result: 5142 rows, 1 distinct value (category_0001)
```

---

## Benchmark Methodology

### Iterations
- 10mb files: **3 runs** each
- 100mb files: **1 run** each

### Phases
1. **Phase 1**: zpq baseline across all environments [COMPLETE]
2. **Phase 2**: Add DuckDB, verify apples-to-apples
3. **Phase 3**: Add PyArrow, Polars
4. **Phase 4**: Full matrix (all filters x all sizes)

---

## Status

- [x] Phase 1: zpq baseline (RIE local -> RIE S3 -> Lambda S3)
- [ ] Phase 2: DuckDB comparison
- [ ] Phase 3: PyArrow/Polars comparison
- [ ] Phase 4: Full matrix
