# Filter Backends

This directory contains the implementations for filter evaluation. usage is dispatched from `../filter.zig`.

## Structure

- `mod.zig` / `../filter.zig`: Key types (`Filter`, `Operator`) and the main `evaluate` dispatcher.
- `operator.zig`: Shared `Operator` enum definition.
- `scalar.zig`: Reference scalar implementation (generic over primitive types).
- `avx2.zig` (Planned): AVX2 SIMD implementation.
- `neon.zig` (Planned): ARM NEON SIMD implementation.

## Design

The filter system uses a "backend" pattern where the main `evaluate` method chooses the best implementation available at compile time (or runtime).
Currently, it defaults to `scalar` for all platforms.

## Deployment Guide: AWS Lambda

ZPQ is designed as a high-performance, zero-dependency Lambda runtime. The same binary used for CLI work functions as the Lambda `bootstrap` handler.

### 1. Packaging & Deployment

The simplest way to ship ZPQ to Lambda is using the `just` recipes:

```bash
# 1. Build and deploy (defaults to ARM64, 1769MB)
# Requires LAMBDA_ROLE env var set to your IAM Role ARN
export LAMBDA_ROLE="arn:aws:iam::..."
just lambda-ship "zpq-etl"

# 2. Manual build if you want to inspect the Zip
just lambda-build arm64
# Result: zip-out/lambda/zpq-lambda-arm64.zip
```

### 2. S3-to-S3 ETL Examples

ZPQ accepts standard JSON payloads via `aws lambda invoke`.

#### Example A: Zero-Copy Passthrough
High-speed data copy without row decoding. Ideal for moving files between buckets/prefixes.
```bash
just lambda-invoke "zpq-etl" '{
  "input_path": "s3://source-bucket/data.parquet",
  "output_path": "s3://dest-bucket/copy.parquet"
}'
```

#### Example B: Selective Filtering & Projection
Filter rows based on column values and select only necessary columns.
```bash
just lambda-invoke "zpq-etl" '{
  "input_path": "s3://source-bucket/logs.parquet",
  "output_path": "s3://dest-bucket/errors.parquet",
  "filter": "status=error",
  "select": "timestamp,request_id,error_msg"
}'
```

### 3. Monitoring

```bash
# View last 50 logs and performance report
just lambda-logs "zpq-etl"

# View execution stats (Cold start vs Execution time)
just lambda-metrics "zpq-etl"
```
