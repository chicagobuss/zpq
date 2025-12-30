#!/bin/bash
# Query Parquet files in Cloudflare R2 with zpq
#
# Usage:
#   ./examples/r2-query.sh schema s3://zpq/testdata/benchmark/benchmark_1mb.parquet
#   ./examples/r2-query.sh cat s3://zpq/testdata/benchmark/benchmark_1mb.parquet
#   ./examples/r2-query.sh inspect s3://zpq/testdata/benchmark/benchmark_1mb.parquet
#
# Environment:
#   Requires .env file with R2 credentials:
#     R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
#
# R2 Configuration Notes:
#   - S3_ENDPOINT must include https:// prefix
#   - AWS_REGION must be "auto" for R2 (not us-east-1 or other AWS regions)
#   - Credentials use standard AWS env vars (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

set -e

cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
    echo "Error: .env file not found. Copy .env.example and configure R2 credentials."
    exit 1
fi

source .env

: "${R2_ACCOUNT_ID:?R2_ACCOUNT_ID not set in .env}"
: "${R2_ACCESS_KEY_ID:?R2_ACCESS_KEY_ID not set in .env}"
: "${R2_SECRET_ACCESS_KEY:?R2_SECRET_ACCESS_KEY not set in .env}"

# R2 requires:
# 1. Custom endpoint (not AWS S3)
# 2. Region set to "auto" for SigV4 signing
export S3_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
export AWS_REGION="auto"
export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"

# Default command and path
CMD="${1:-schema}"
PATH_ARG="${2:-s3://zpq/testdata/benchmark/benchmark_1mb.parquet}"

# Find zpq binary
ZPQ="zig-out/bin/zpq"
if [ ! -x "$ZPQ" ]; then
    echo "Building zpq..."
    zig build -Doptimize=ReleaseFast
fi

echo "Querying: $PATH_ARG"
echo "Endpoint: $S3_ENDPOINT"
echo ""

exec "$ZPQ" "$CMD" "$PATH_ARG"
