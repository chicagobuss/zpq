#!/bin/bash
# Sync benchmark files to R2 with organized structure
#
# Usage: ./tools/testdata/sync_to_r2.sh
#
# Requires: AWS CLI, .env with R2 credentials

set -e

cd "$(dirname "$0")/../.."

if [ ! -f .env ]; then
    echo "Error: .env file not found"
    exit 1
fi

source .env

: "${R2_ACCOUNT_ID:?R2_ACCOUNT_ID not set}"
: "${R2_ACCESS_KEY_ID:?R2_ACCESS_KEY_ID not set}"
: "${R2_SECRET_ACCESS_KEY:?R2_SECRET_ACCESS_KEY not set}"
: "${R2_BUCKET:?R2_BUCKET not set}"

R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"

s3cmd() {
    aws s3 "$@" --endpoint-url "$R2_ENDPOINT" --region auto
}

echo "=== Syncing testdata to R2 ==="
echo "Bucket: s3://$R2_BUCKET"
echo ""

# Upload benchmark files
echo "--- Uploading benchmark files ---"
for f in data/benchmark/benchmark_*.parquet; do
    filename=$(basename "$f")
    echo "  $filename -> testdata/benchmark/$filename"
    s3cmd cp "$f" "s3://$R2_BUCKET/testdata/benchmark/$filename" --quiet
done

# Upload core test files (from ci/fixtures)
echo ""
echo "--- Uploading core test files ---"
for f in ci/fixtures/parquet/*.parquet; do
    filename=$(basename "$f")
    echo "  $filename -> testdata/core/$filename"
    s3cmd cp "$f" "s3://$R2_BUCKET/testdata/core/$filename" --quiet
done

# Upload all-types file if it exists in data/
if [ -f data/test_all_types_sorted.parquet ]; then
    echo "  test_all_types_sorted.parquet -> testdata/core/"
    s3cmd cp data/test_all_types_sorted.parquet "s3://$R2_BUCKET/testdata/core/test_all_types_sorted.parquet" --quiet
fi

# List final structure
echo ""
echo "=== R2 testdata structure ==="
s3cmd ls "s3://$R2_BUCKET/testdata/" --recursive | grep -v "^$"

echo ""
echo "Done."
