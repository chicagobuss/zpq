# ZPQ Test Data

## Overview

This directory contains tools for generating and managing test Parquet files.

## R2 Bucket Structure

All test data is stored in Cloudflare R2 bucket `zpq`:

```
s3://zpq/
├── boring_tls/                    # Pre-built BoringSSL artifacts
├── testdata/
│   ├── benchmark/                 # Benchmark files (generated)
│   │   ├── benchmark_1mb.parquet
│   │   ├── benchmark_1mb_snappy.parquet
│   │   ├── benchmark_1mb_zstd.parquet
│   │   ├── benchmark_1mb_gzip.parquet
│   │   ├── benchmark_1mb_none.parquet
│   │   ├── benchmark_10mb*.parquet
│   │   └── benchmark_100mb*.parquet
│   └── core/                      # Core test files
│       ├── simple.parquet
│       ├── required.parquet
│       ├── types.parquet
│       ├── test_all_types_sorted.parquet
│       └── ...
```

## Benchmark File Schema (27 columns)

Generated files include comprehensive type coverage:

| Column | Type | Notes |
|--------|------|-------|
| int8, int16 | int8, int16 | Signed small integers |
| int32_sorted, int32_random | int32 | Sorted (predicate pushdown) + random |
| int64_sorted, int64_random | int64 | Sorted + random |
| uint8, uint16, uint32, uint64 | unsigned | Unsigned integers |
| float32, float64, float64_sorted | float/double | Floats, one sorted |
| bool, bool_sparse | bool | Regular + 1% true (RLE test) |
| string_random | string | Random 10-100 char strings |
| string_dict_low | string | 10 unique values (dictionary) |
| string_dict_high | string | 1000 unique values (dictionary) |
| string_sorted | string | Sorted strings |
| binary | binary | Random binary data |
| timestamp, timestamp_sorted | timestamp[us] | Timestamps |
| date | date32 | Dates |
| int32_nullable, float64_nullable, string_nullable | various | 10% null |
| int32_sparse | int32 | 99% null (RLE efficiency test) |

## Scripts

### generate_benchmarks.py

Generate benchmark Parquet files with all compression variants:

```bash
# Generate 1MB, 10MB, 100MB files
python tools/testdata/generate_benchmarks.py --sizes 1,10,100 --output-dir data/benchmark

# Generate custom sizes
python tools/testdata/generate_benchmarks.py --sizes 5,50 --output-dir /tmp/custom
```

### sync_to_r2.sh

Upload test files to R2:

```bash
./tools/testdata/sync_to_r2.sh
```

## Querying with DuckDB

```bash
source .env

duckdb -c "
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='${R2_ACCOUNT_ID}.r2.cloudflarestorage.com';
SET s3_access_key_id='${R2_ACCESS_KEY_ID}';
SET s3_secret_access_key='${R2_SECRET_ACCESS_KEY}';
SET s3_region='auto';
SET s3_url_style='path';

SELECT * FROM 's3://zpq/testdata/benchmark/benchmark_1mb.parquet' LIMIT 5;
"
```

## Local Files

Local test files are in:
- `data/benchmark/` - Generated benchmark files
- `data/` - Ad-hoc test files
- `ci/fixtures/parquet/` - CI test fixtures (subset)
