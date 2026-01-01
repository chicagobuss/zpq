# Running ZPQ in Serverless Environments

ZPQ works as a serverless function by using the same binary you'd run locally. No special Lambda-specific code is needed.

## AWS Lambda

### Quick Start (from release)

```bash
# Download latest Lambda zip
just lambda-deploy-release zpq-filter arm64 512

# Or manually:
curl -fsSLO https://github.com/chicagobuss/zpq/releases/latest/download/zpq-lambda-arm64.zip
aws lambda create-function \
  --function-name zpq-filter \
  --runtime provided.al2023 \
  --handler bootstrap \
  --architectures arm64 \
  --memory-size 512 \
  --timeout 120 \
  --zip-file fileb://zpq-lambda-arm64.zip \
  --role arn:aws:iam::YOUR_ACCOUNT:role/YOUR_LAMBDA_ROLE
```

### Build from source

```bash
# Build for ARM64 (Graviton - recommended for cost/performance)
just lambda-build arm64

# Build for x86_64
just lambda-build x86_64

# Build and deploy in one step
just lambda-ship zpq-filter arm64 1769
```

### Invoke

```bash
# Via just
just lambda-invoke zpq-filter '{"file": "s3://bucket/path.parquet"}'

# Via AWS CLI
aws lambda invoke \
  --function-name zpq-filter \
  --payload '{"file": "s3://bucket/path.parquet"}' \
  response.json
```

### Logs & Metrics

```bash
just lambda-logs zpq-filter
just lambda-metrics zpq-filter
just lambda-list
```

### Benchmarking

Run across all 4 arch/memory combinations (arm64/x86_64 x 512MB/1769MB):

```bash
just lambda-bench-matrix '{"file": "s3://bucket/test.parquet"}'
```

## Local Testing with RIE

Test Lambda behavior locally using AWS Lambda Runtime Interface Emulator:

```bash
# Start container with R2 backend
just lambda-local

# Or with local rustfs
./tools/serverless/test-local.sh --local

# Invoke
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"file": "s3://bucket/path.parquet"}'

# Stop
cd tools/serverless && docker-compose down
```

## Architecture Notes

- **ARM64 (Graviton)**: 20% cheaper, often faster for ZPQ workloads
- **1769 MB memory**: Sweet spot - gives 1 full vCPU on Lambda
- **provided.al2023**: Amazon Linux 2023 runtime for custom binaries

The Lambda zip is simply the `zpq` binary renamed to `bootstrap`. No wrapper code needed.
