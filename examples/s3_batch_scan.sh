#!/usr/bin/env bash
# Scan multiple parquet files from S3 in parallel
#
# Usage: ./examples/s3_batch_scan.sh s3://bucket/prefix/
#
# Requires: AWS CLI configured, zpq built

set -euo pipefail

PREFIX="${1:-}"
if [[ -z "$PREFIX" ]]; then
    echo "Usage: $0 s3://bucket/prefix/"
    exit 1
fi

echo "=== Scanning parquet files under: $PREFIX ==="

# List all .parquet files
FILES=$(aws s3 ls "$PREFIX" --recursive | grep '\.parquet$' | awk '{print $4}')

BUCKET=$(echo "$PREFIX" | sed 's|s3://||' | cut -d/ -f1)

for KEY in $FILES; do
    S3_PATH="s3://$BUCKET/$KEY"
    echo "--- $S3_PATH ---"
    ./zig-out/bin/zpq "$S3_PATH" --schema
    echo ""
done
