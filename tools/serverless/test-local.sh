#!/bin/bash
# Test Lambda locally with RIE
#
# Usage:
#   ./tools/serverless/test-local.sh              # Use latest release, test with R2
#   ./tools/serverless/test-local.sh --rustfs     # Test with local rustfs (must be running on :9999)
#   ./tools/serverless/test-local.sh --build      # Build from source instead of downloading
#   ./tools/serverless/test-local.sh --invoke     # Just invoke (container already running)
#
# Environment:
#   TEST_FILE - Override the S3 path to test (e.g., s3://test-bucket/myfile.parquet)
#   TEST_FILTER - Add a filter expression (e.g., "category=A")
#   TEST_OUTPUT - Add an output path for filter results (e.g., s3://test-bucket/output.parquet)
#
# Examples:
#   # Basic schema test against rustfs
#   ./tools/serverless/test-local.sh --rustfs --build
#
#   # Filter test with S3 output
#   TEST_FILE=s3://test-bucket/input.parquet \
#   TEST_OUTPUT=s3://test-bucket/output.parquet \
#   TEST_FILTER="cut=Premium" \
#   ./tools/serverless/test-local.sh --rustfs --invoke

set -e

cd "$(dirname "$0")/../.."

MODE="r2"
INVOKE_ONLY=false
BUILD_FROM_SOURCE=false

for arg in "$@"; do
    case $arg in
        --rustfs|--local) MODE="rustfs" ;;
        --r2) MODE="r2" ;;
        --invoke) INVOKE_ONLY=true ;;
        --build) BUILD_FROM_SOURCE=true ;;
    esac
done

# Get the Lambda binary
if [ "$INVOKE_ONLY" = false ]; then
    mkdir -p zig-out/bin

    if [ "$BUILD_FROM_SOURCE" = true ]; then
        echo "=== Building zpq for Lambda (aarch64-linux) ==="
        zig build -Dtarget=aarch64-linux
    else
        echo "=== Downloading latest zpq release (aarch64-linux) ==="
        R2_URL="https://pub-4d2e7e2925bb43dc9d3c0323d6d61a84.r2.dev/releases/latest/zpq-linux-arm64.tar.gz"
        if curl -fsSL "$R2_URL" | tar -xz -C zig-out/bin/; then
            echo "Downloaded zpq from R2"
        else
            echo "Failed to download from R2, falling back to build from source"
            zig build -Dtarget=aarch64-linux
        fi
    fi

    echo ""
    echo "=== Starting container ($MODE mode) ==="

    # Stop any existing container
    cd tools/serverless
    docker-compose down 2>/dev/null || true

    if [ "$MODE" = "rustfs" ]; then
        # Local rustfs - assumes rustfs is already running on host:9999
        export S3_ENDPOINT="https://host.docker.internal:9999"
        export AWS_REGION="us-east-1"
        export AWS_ACCESS_KEY_ID="rustfsadmin"
        export AWS_SECRET_ACCESS_KEY="rustfsadmin"
        docker-compose up --build -d lambda
    else
        # R2 mode - source .env for R2 credentials
        source ../../.env
        export S3_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        export AWS_REGION="auto"
        export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
        export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
        docker-compose up --build -d lambda
    fi

    echo "Waiting for container to start..."
    sleep 3
    cd ../..
fi

# Determine test file
if [ -n "$TEST_FILE" ]; then
    FILE="$TEST_FILE"
elif [ "$MODE" = "rustfs" ]; then
    FILE="s3://test-bucket/test_input.parquet"
else
    FILE="s3://zpq/testdata/benchmark/benchmark_1mb.parquet"
fi

# Build JSON payload
if [ -n "$TEST_FILTER" ] && [ -n "$TEST_OUTPUT" ]; then
    PAYLOAD="{\"file\": \"$FILE\", \"output\": \"$TEST_OUTPUT\", \"filter\": \"$TEST_FILTER\"}"
elif [ -n "$TEST_FILTER" ]; then
    PAYLOAD="{\"file\": \"$FILE\", \"filter\": \"$TEST_FILTER\"}"
else
    PAYLOAD="{\"file\": \"$FILE\", \"schema\": true}"
fi

echo ""
echo "=== Invoking Lambda ($MODE) ==="
echo "Payload: $PAYLOAD"
echo ""

curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
    -d "$PAYLOAD" | jq .

echo ""
echo "=== Done ==="
echo ""
echo "To stop: cd tools/serverless && docker-compose down"
echo "To view logs: docker logs serverless-lambda-1"
