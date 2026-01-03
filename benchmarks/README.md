# ZPQ Lambda Benchmarks

Benchmark zpq filter performance across deployment environments.

## Setup

```bash
# 1. Ensure .env has AWS_S3_BUCKET set (and AWS credentials)
cat .env | grep AWS_S3_BUCKET
# AWS_S3_BUCKET=skyway-staging-perf-test

# 2. Build for Lambda (aarch64-linux)
zig build -Doptimize=ReleaseFast -Dtarget=aarch64-linux
cp zig-out/bin/zpq benchmarks/zpq

# 3. Start Lambda RIE (uses seccomp to force epoll, simulating Lambda)
cd benchmarks
docker-compose up -d
```

## Test Commands

All commands below assume you've run `source .env` first. The bucket path is:
- Input: `s3://$AWS_S3_BUCKET/zpq_test_data/benchmark/`
- Output: `s3://$AWS_S3_BUCKET/zpq_test_data/output/`

### Local File I/O (baseline)

```bash
# 10MB
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"file": "/data/benchmark_10mb.parquet", "output": "/tmp/out.parquet", "filter": "string_dict_low=category_0001", "select": "int32_sorted,string_dict_low,float64"}'

# 100MB
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"file": "/data/benchmark_100mb.parquet", "output": "/tmp/out.parquet", "filter": "string_dict_low=category_0001", "select": "int32_sorted,string_dict_low,float64"}'
```

### Real AWS S3

```bash
source .env

# 10MB: S3 read + S3 write
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d "{\"file\": \"s3://$AWS_S3_BUCKET/zpq_test_data/benchmark/benchmark_10mb.parquet\", \"output\": \"s3://$AWS_S3_BUCKET/zpq_test_data/output/out_10mb.parquet\", \"filter\": \"string_dict_low=category_0001\", \"select\": \"int32_sorted,string_dict_low,float64\"}"

# 100MB: S3 read + S3 write
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d "{\"file\": \"s3://$AWS_S3_BUCKET/zpq_test_data/benchmark/benchmark_100mb.parquet\", \"output\": \"s3://$AWS_S3_BUCKET/zpq_test_data/output/out_100mb.parquet\", \"filter\": \"string_dict_low=category_0001\", \"select\": \"int32_sorted,string_dict_low,float64\"}"
```

### RustFS (Local S3 Emulation)

Start rustfs in a separate terminal:
```bash
rustfs --mount-point /tmp/zpq_r2_bucket
```

Update docker-compose.yml environment to point to rustfs:
```yaml
environment:
  - S3_ENDPOINT=http://host.docker.internal:3000
  - AWS_ACCESS_KEY_ID=test
  - AWS_SECRET_ACCESS_KEY=test
```

Then:
```bash
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"file": "s3://zpq-r2-bucket/benchmark/benchmark_10mb.parquet", "output": "s3://zpq-r2-bucket/output/out.parquet", "filter": "string_dict_low=category_0001", "select": "int32_sorted,string_dict_low,float64"}'
```

## Verify Output

```bash
source .env

# Check row count with DuckDB
duckdb -c "SELECT COUNT(*), COUNT(DISTINCT string_dict_low) FROM 's3://$AWS_S3_BUCKET/zpq_test_data/output/out_10mb.parquet'"
# Expected: 5,142 rows, 1 distinct value (category_0001)

duckdb -c "SELECT COUNT(*) FROM 's3://$AWS_S3_BUCKET/zpq_test_data/output/out_100mb.parquet'"
# Expected: 52,526 rows
```

## Test Data

**S3 path**: `s3://$AWS_S3_BUCKET/zpq_test_data/benchmark/`
**Local mount**: `/tmp/zpq_r2_bucket/benchmark/` -> `/data` in container

| File | Size | Rows |
|------|------|------|
| benchmark_10mb.parquet | ~16MB | ~52K |
| benchmark_100mb.parquet | ~156MB | ~524K |

### Filter Columns

- `string_dict_low` - 10 categories (~10% each): `category_0000` through `category_0009`
- `bool` - ~51% true
- `bool_sparse` - ~1% true

## Architecture Notes

- **slot_parallel**: pwrite-based parallel writes, for local filesystem only
- **morsel_parallel**: S3 multipart streaming, auto-selected for `s3://` output
- Pipeline auto-switches to morsel_parallel when output is `s3://`

## Docker Setup

The default `docker-compose.yml` uses a seccomp profile to disable io_uring, simulating Lambda's kernel constraints:

```yaml
security_opt:
  - seccomp=./no-io-uring.json
```
