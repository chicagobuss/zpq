#!/bin/bash
# Test Lambda locally with RIE
#
# Usage:
#   ./tools/lambda/test-local.sh              # Test with R2
#   ./tools/lambda/test-local.sh --local      # Test with local rustfs
#   ./tools/lambda/test-local.sh --invoke     # Just invoke (container already running)

set -e

cd "$(dirname "$0")/../.."

MODE="r2"
INVOKE_ONLY=false

for arg in "$@"; do
    case $arg in
        --local) MODE="local" ;;
        --invoke) INVOKE_ONLY=true ;;
    esac
done

# Build the Lambda binary
if [ "$INVOKE_ONLY" = false ]; then
    echo "=== Building Lambda binary (aarch64-linux) ==="
    zig build example-lambda-04-scan-benchmark -Dexamples -Dtarget=aarch64-linux -Doptimize=ReleaseFast

    echo ""
    echo "=== Starting container ($MODE mode) ==="

    if [ "$MODE" = "local" ]; then
        cd tools/lambda
        docker-compose --profile local up --build -d
    else
        # Source .env and export R2 vars for docker-compose
        source .env
        export S3_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        export AWS_REGION="auto"
        export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
        export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"

        cd tools/lambda
        docker-compose up --build -d
    fi

    echo "Waiting for container to start..."
    sleep 2
    cd ../..
fi

# Determine which file to test
if [ "$MODE" = "local" ]; then
    # Local rustfs bucket
    TEST_FILE="s3://zpq-ci/bench.parquet"
else
    # R2 bucket
    TEST_FILE="s3://zpq/testdata/benchmark/benchmark_1mb.parquet"
fi

echo ""
echo "=== Invoking Lambda ==="
echo "File: $TEST_FILE"
echo ""

curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
    -d "{\"file\": \"$TEST_FILE\"}" | jq .

echo ""
echo "=== Done ==="
echo ""
echo "To stop: cd tools/lambda && docker-compose down"
echo "To view logs: cd tools/lambda && docker-compose logs -f"
