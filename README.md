# ZPQ: Fast Parquet for Cloud & Lambda

ZPQ is a high-performance Parquet CLI and Lambda runtime. Filter, project, and transform Parquet files locally or in S3 with minimal latency and memory.

## Quick Start

### Download Binary

```bash
# Linux x86_64
curl -fsSL https://github.com/chicagobuss/zpq/releases/latest/download/zpq-linux-x86_64.tar.gz | tar -xz

# Linux ARM64 (Graviton)
curl -fsSL https://github.com/chicagobuss/zpq/releases/latest/download/zpq-linux-arm64.tar.gz | tar -xz

# macOS ARM64 (Apple Silicon)
curl -fsSL https://github.com/chicagobuss/zpq/releases/latest/download/zpq-macos-arm64.tar.gz | tar -xz

# macOS x86_64
curl -fsSL https://github.com/chicagobuss/zpq/releases/latest/download/zpq-macos-x86_64.tar.gz | tar -xz
```

### CLI Usage

```bash
# View schema
./zpq data.parquet --schema

# View metadata (row groups, compression, etc.)
./zpq data.parquet --meta

# Filter rows and output to new file
./zpq input.parquet output.parquet --filter "status=active"

# Filter with column projection
./zpq input.parquet output.parquet --filter "country=US" --select "id,name,email"

# Works with S3 (requires AWS credentials in env)
./zpq s3://bucket/input.parquet output.parquet --filter "year=2024"
```

### Environment Variables

For S3 access:
```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_REGION="us-west-2"

# For non-AWS S3 (R2, MinIO, etc.)
export S3_ENDPOINT="https://your-endpoint.com"
```

## AWS Lambda

Deploy ZPQ as a Lambda function for serverless Parquet filtering.

### Quick Deploy

```bash
# Download Lambda zip (ARM64 recommended for cost/performance)
curl -fsSLO https://github.com/chicagobuss/zpq/releases/latest/download/zpq-lambda-arm64.zip

# Create function
aws lambda create-function \
  --function-name zpq-filter \
  --runtime provided.al2023 \
  --handler bootstrap \
  --architectures arm64 \
  --memory-size 512 \
  --timeout 120 \
  --zip-file fileb://zpq-lambda-arm64.zip \
  --role arn:aws:iam::YOUR_ACCOUNT:role/YOUR_ROLE
```

### Invoke

```bash
aws lambda invoke \
  --function-name zpq-filter \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "input_path": "s3://source-bucket/data.parquet",
    "output_path": "s3://dest-bucket/filtered.parquet",
    "filter": "status=active",
    "select": "id,name,created_at"
  }' \
  response.json
```

### Lambda Payload Schema

```json
{
  "input_path": "s3://bucket/input.parquet",
  "output_path": "s3://bucket/output.parquet",
  "filter": "column=value",
  "select": "col1,col2,col3"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `input_path` | Yes | S3 path to source Parquet file |
| `output_path` | Yes | S3 path for filtered output |
| `filter` | Yes | Filter expression (e.g., `status=active`, `year>=2020`) |
| `select` | No | Comma-separated columns to include (default: all) |

## Performance

ZPQ is built for serverless - minimal cold start, low memory, fast execution.

| Metric | ZPQ | PyArrow | Polars |
|--------|-----|---------|--------|
| S3 Cold Start | ~42ms | ~35ms | ~121ms |
| Local Scan | 842 MB/s | 372 MB/s | 160 MB/s |
| Lambda Binary | ~4 MB | ~50 MB | ~30 MB |

## Build from Source

Requires [Zig 0.16.x](https://ziglang.org/download/) (master branch).

```bash
git clone https://github.com/chicagobuss/zpq
cd zpq

# Fetch pre-built BoringSSL
just fetch-deps

# Build
just build

# Test
just test

# Binary at zig-out/bin/zpq
```

## Design Philosophy

### The Laziness Principle

> ZPQ preserves data in its most compact/encoded form as long as possible.

| Operation | Conventional | ZPQ |
|-----------|--------------|-----|
| Column projection | Decode all, select some | Never read unselected columns |
| Row filtering | Decode all, discard | Skip row groups via stats |
| Pass-through columns | Decode → re-encode | Copy compressed bytes verbatim |

This makes ZPQ dramatically faster for selective operations - the exact workloads that dominate serverless data processing.

### Architecture Highlights

- **Native S3/SigV4**: No SDK dependencies, zero-copy where possible
- **Async I/O**: Single-threaded completion-based state machine
- **Static Binary**: ~4MB with TLS, no runtime dependencies
- **Cross-Platform**: Linux (io_uring), macOS (kqueue), x86_64 & ARM64

## License

MIT
