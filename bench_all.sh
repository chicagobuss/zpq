#!/bin/bash
set -e

MODE=${1:-all}

# Load Env
if [ -f .env.staging.bot ]; then
    export $(grep -v '^#' .env.staging.bot | xargs)
fi
export AWS_REGION=us-west-2

BUCKET="skyway-diat-staging-data"
KEY="raw/cccis-duckbill/skyway/skyway-export/data/BILLING_PERIOD=2025-04/skyway-export-00001.snappy.parquet"
S3_PATH="s3://$BUCKET/$KEY"

echo "=== BENCHMARKING S3 PARQUET READ (Mode: $MODE) ==="
echo "Target: $S3_PATH"
echo ""

if [[ "$MODE" == "meta" || "$MODE" == "all" ]]; then
    echo "--- 1. ZPQ (Zig) ---"
    echo "[Metadata (Schema Read)]"
    start_time=$(python3 -c 'import time; print(time.time())')
    # Redirect stdout/stderr to /dev/null for timing accuracy and cleanliness
    ./zig-out/bin/zigaws schema "$S3_PATH" > /dev/null 2>&1
    end_time=$(python3 -c 'import time; print(time.time())')
    elapsed=$(python3 -c "print(f'{float(\"$end_time\") - float(\"$start_time\"):.4f}')")
    echo "Time: ${elapsed}s"
fi

if [[ "$MODE" == "scan" || "$MODE" == "all" ]]; then
    if [[ "$MODE" == "scan" ]]; then
         echo "--- 1. ZPQ (Zig) ---"
    fi
    echo "[Full Scan]"
    start_time=$(python3 -c 'import time; print(time.time())')
    # Redirect stdout to /dev/null to measure pure read/processing speed without terminal IO
    ./zig-out/bin/zigaws scan "$S3_PATH" > /dev/null
    end_time=$(python3 -c 'import time; print(time.time())')
    elapsed=$(python3 -c "print(f'{float(\"$end_time\") - float(\"$start_time\"):.4f}')")
    echo "Time: ${elapsed}s"
fi

echo ""
echo "--- 2. PyArrow & 3. Polars ---"
# Use direct python3 since we installed deps
python3 tools/bench/s3_bench.py $MODE

echo ""
echo "=== DONE ==="
