#!/bin/bash
# Test Lambda locally with RIE
#
# Usage:
#   ./tools/serverless/test-local.sh              # Use latest release, test with R2
#   ./tools/serverless/test-local.sh --local      # Test with local rustfs
#   ./tools/serverless/test-local.sh --build      # Build from source instead of downloading
#   ./tools/serverless/test-local.sh --invoke     # Just invoke (container already running)

set -e

cd "$(dirname "$0")/../.."

MODE="r2"
INVOKE_ONLY=false
BUILD_FROM_SOURCE=false

for arg in "$@"; do
    case $arg in
        --local) MODE="local" ;;
        --invoke) INVOKE_ONLY=true ;;
        --build) BUILD_FROM_SOURCE=true ;;
    esac
done

# Get the Lambda binary
if [ "$INVOKE_ONLY" = false ]; then
    mkdir -p zig-out/bin

    if [ "$BUILD_FROM_SOURCE" = true ]; then
        echo "=== Building zpq for Lambda (aarch64-linux) ==="
        ./tools/serverless/aws.sh build arm64
    else
        echo "=== Downloading latest zpq release (aarch64-linux) ==="
        R2_URL="https://pub-4d2e7e2925bb43dc9d3c0323d6d61a84.r2.dev/releases/latest/zpq-linux-arm64.tar.gz"
        if curl -fsSL "$R2_URL" | tar -xz -C zig-out/bin/; then
            echo "Downloaded zpq from R2"
        else
            echo "Failed to download from R2, falling back to build from source"
            ./tools/serverless/aws.sh build arm64
        fi
    fi

    echo ""
    echo "=== Starting container ($MODE mode) ==="

    if [ "$MODE" = "local" ]; then
        cd tools/serverless
        docker-compose --profile local up --build -d
    else
        # Source .env and export R2 vars for docker-compose
        source .env
        export S3_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        export AWS_REGION="auto"
        export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
        export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"

        cd tools/serverless
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
echo "To stop: cd tools/serverless && docker-compose down"
echo "To view logs: cd tools/serverless && docker-compose logs -f"
