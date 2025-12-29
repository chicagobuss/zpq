# ZPQ Benchmarks

This directory contains benchmarks for comparing ZPQ against other Parquet implementations (PyArrow, Polars, DuckDB).

## Quick Start

```bash
# 1. Build ZPQ in release mode
zig build -Doptimize=ReleaseFast

# 2. Run a benchmark
./benchmarks/bench.sh e2e data/many_rows.parquet

# 3. Compare against competitors
./benchmarks/bench.sh compare e2e s3://your-bucket/file.parquet
```

## Prerequisites

### Build
```bash
zig build -Doptimize=ReleaseFast
```

### Python Dependencies (for competitor benchmarks)
The benchmark scripts use `uv` to manage Python dependencies automatically:
```bash
# Install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# Dependencies are installed on-demand by bench.sh
```

## Environment Setup

Copy `.env.example` to `.env` in the project root and configure:

```bash
cp .env.example .env
```

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | AWS access key for S3 | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `...` |
| `AWS_REGION` | AWS region | `us-west-2` |

### Optional Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `S3_ENDPOINT` | Custom S3 endpoint (MinIO, R2) | `http://localhost:9000` |
| `ZPQ_TEST_S3_PATH` | Default S3 test file | `s3://bucket/file.parquet` |
| `ZPQ_BENCH_FILE` | Default local test file | `data/large.parquet` |

### Cloudflare R2 Setup

For R2 benchmarks, add these to your `.env`:
```bash
R2_ACCESS_KEY_ID=your_r2_key
R2_SECRET_ACCESS_KEY=your_r2_secret
R2_ACCOUNT_ID=your_account_id
R2_ENDPOINT=https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com
R2_BUCKET=zpq

# Pre-configured benchmark files (create these yourself)
R2_BENCH_FILE_1MB=benchmark_1mb.parquet
R2_BENCH_FILE_10MB=benchmark_10mb.parquet
R2_BENCH_FILE_100MB=benchmark_100mb.parquet
```

## Benchmark Files

### Local Files

The `data/` directory is gitignored. You need to provide your own test files:

| File | Purpose | How to Create |
|------|---------|---------------|
| `data/many_rows.parquet` | Row scan benchmark | Included (88KB, ~10K rows) |
| `data/simple.parquet` | Basic functionality | Included (small) |
| `data/large.parquet` | Large file benchmark | Download or generate |

To generate test files:
```bash
# Using DuckDB
duckdb -c "COPY (SELECT * FROM range(1000000)) TO 'data/million_rows.parquet'"

# Using Python
python3 -c "
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

n = 1_000_000
table = pa.table({
    'id': np.arange(n),
    'value': np.random.randn(n),
    'category': np.random.choice(['A', 'B', 'C'], n)
})
pq.write_table(table, 'data/million_rows.parquet')
"
```

### S3 Files

Upload your benchmark files to S3:
```bash
aws s3 cp data/million_rows.parquet s3://your-bucket/benchmarks/
```

## Available Benchmarks

### List All Benchmarks
```bash
./benchmarks/bench.sh list
```

### Individual Benchmarks

| Command | Description |
|---------|-------------|
| `bench.sh dns` | DNS resolver performance |
| `bench.sh ping` | TCP ping-pong throughput |
| `bench.sh e2e <path>` | Full parquet scan (local or S3) |
| `bench.sh scan <path>` | Quick scan via zpq CLI |
| `bench.sh pyarrow <path>` | PyArrow baseline |

### Comparison Mode

Compare ZPQ against PyArrow and Polars:

```bash
# Local file
./benchmarks/bench.sh compare e2e data/large.parquet

# S3 file (tests network + parsing)
./benchmarks/bench.sh compare e2e s3://bucket/file.parquet -n 5
```

Output includes:
- ZPQ Sync timing
- ZPQ Async timing  
- PyArrow timing
- Polars timing
- Speedup calculations

### E2E Options

```bash
./benchmarks/bench.sh e2e <path> [options]

Options:
  --sync          Synchronous I/O (blocking)
  --async         Async I/O with speculative DNS (default)
  --dns=basic     Async I/O with basic thread-pool DNS
  -n, --iterations N   Number of iterations (default: 1)
```

## Benchmark Files (Zig)

| File | Description |
|------|-------------|
| `e2e.zig` | End-to-end benchmark (local or S3) |
| `decode_full.zig` | Full decode benchmark |
| `projection.zig` | Local column projection benchmark |
| `s3_cold_start.zig` | S3 cold start latency |
| `dns.zig` | DNS resolver benchmark |
| `ping_pongs.zig` | TCP throughput benchmark |

## Competitor Implementations

| File | Description |
|------|-------------|
| `competitor.py` | PyArrow S3 benchmark |
| `pyarrow_bench.py` | PyArrow local file benchmark |
| `competitors/s3_bench.py` | Python S3 benchmark |
| `competitors/rust_bench/` | Rust arrow-rs benchmark |
| `competitors/node_ping_pong.js` | Node.js baseline |

## Example Benchmark Session

```bash
# Build
zig build -Doptimize=ReleaseFast

# Local file benchmarks
./benchmarks/bench.sh e2e data/many_rows.parquet -n 10
./benchmarks/bench.sh compare e2e data/many_rows.parquet

# S3 benchmarks (requires .env setup)
./benchmarks/bench.sh e2e s3://my-bucket/large.parquet --async -n 5
./benchmarks/bench.sh compare e2e s3://my-bucket/large.parquet -n 3

# DNS benchmark
./benchmarks/bench.sh dns

# TCP throughput
./benchmarks/bench.sh ping
```

## Interpreting Results

### E2E Output
```
=== E2E Benchmark Results ===
Platform:   aarch64-darwin
Target:     s3://bucket/file.parquet
Iterations: 5

  Run 1: 1000000 rows in 45.23ms
  Run 2: 1000000 rows in 42.18ms
  ...

Min: 42.18ms
Max: 48.92ms
Avg: 44.56ms
```

### Comparison Output
```
=== Summary ===

  ZPQ Async:          44.56 ms
  PyArrow:            98.23 ms
  Polars:             67.45 ms

  ZPQ Async vs PyArrow: 2.20x
  ZPQ Async vs Polars:  1.51x
```

## Troubleshooting

### "Binary not found"
Build first: `zig build -Doptimize=ReleaseFast`

### S3 authentication errors
Check your `.env` file has correct AWS credentials.

### TLS errors with custom endpoints
For self-signed certs (MinIO), the benchmark scripts handle this automatically.

### Missing Python dependencies
Install `uv`: `curl -LsSf https://astral.sh/uv/install.sh | sh`
