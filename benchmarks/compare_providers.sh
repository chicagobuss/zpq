#!/bin/bash
# Compare S3 vs R2 performance for ZPQ
# Usage: ./benchmarks/compare_providers.sh
#
# Requires environment variables:
#   AWS S3:
#     AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
#     S3_BUCKET_AWS, S3_KEY_AWS (optional, defaults to benchmark_1mb.parquet)
#
#   Cloudflare R2:
#     R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT
#     S3_BUCKET_R2, S3_KEY_R2 (optional, defaults to benchmark_1mb.parquet)

set -e

ITERATIONS=${ITERATIONS:-5}
KEY=${S3_KEY:-benchmark_1mb.parquet}

echo "=============================================="
echo "ZPQ S3 Provider Comparison Benchmark"
echo "=============================================="
echo "Iterations per provider: $ITERATIONS"
echo "Test file: $KEY"
echo ""

# Build once
echo "Building benchmark (ReleaseFast)..."
zig build run-bench-s3 2>/dev/null || zig build bench-s3
echo ""

run_benchmark() {
    local provider=$1
    local host=$2
    local bucket=$3
    local access_key=$4
    local secret_key=$5
    local key=$6

    echo "--- $provider ---"
    echo "Host: $host"
    echo "Bucket: $bucket"
    echo ""

    S3_HOST="$host" \
    S3_BUCKET="$bucket" \
    S3_ACCESS_KEY="$access_key" \
    S3_SECRET_KEY="$secret_key" \
    S3_KEY="$key" \
    ITERATIONS="$ITERATIONS" \
    ./zig-out/bin/bench-s3 2>&1 | grep -E "(Run [0-9]|Summary|Total:|Open|Footer|Prefetch|Decode)"

    echo ""
}

# Test Cloudflare R2
if [ -n "$R2_ACCESS_KEY_ID" ] && [ -n "$R2_SECRET_ACCESS_KEY" ] && [ -n "$R2_ENDPOINT" ]; then
    R2_BUCKET=${S3_BUCKET_R2:-zpq}
    R2_KEY=${S3_KEY_R2:-$KEY}
    run_benchmark "Cloudflare R2" "$R2_ENDPOINT" "$R2_BUCKET" "$R2_ACCESS_KEY_ID" "$R2_SECRET_ACCESS_KEY" "$R2_KEY"
else
    echo "--- Cloudflare R2 ---"
    echo "SKIPPED: Missing R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, or R2_ENDPOINT"
    echo ""
fi

# Test AWS S3
if [ -n "$AWS_ACCESS_KEY_ID" ] && [ -n "$AWS_SECRET_ACCESS_KEY" ]; then
    AWS_REGION=${AWS_REGION:-us-east-1}
    AWS_HOST="s3.${AWS_REGION}.amazonaws.com"
    AWS_BUCKET=${S3_BUCKET_AWS:-zpq-benchmarks}
    AWS_KEY=${S3_KEY_AWS:-$KEY}
    run_benchmark "AWS S3 ($AWS_REGION)" "$AWS_HOST" "$AWS_BUCKET" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" "$AWS_KEY"
else
    echo "--- AWS S3 ---"
    echo "SKIPPED: Missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY"
    echo ""
fi

echo "=============================================="
echo "Benchmark complete"
echo "=============================================="
