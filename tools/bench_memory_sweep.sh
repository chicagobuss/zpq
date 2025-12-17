#!/bin/bash
set -e

FUNCTION_NAME="diat-bench-zig"
# User requested 2GB (2048) and 4GB (4096). Added 128 and 1024 for context.
MEMORY_SIZES=(128 1024 2048 4096)

echo "Starting Benchmark Sweep for: $FUNCTION_NAME"

for MEM in "${MEMORY_SIZES[@]}"; do
    echo "--------------------------------------------------"
    echo "Configuring Lambda for ${MEM}MB RAM..."
    
    aws lambda update-function-configuration \
        --function-name "$FUNCTION_NAME" \
        --memory-size "$MEM" \
        > /dev/null

    echo "Waiting for update..."
    aws lambda wait function-updated --function-name "$FUNCTION_NAME"

    echo "Invoking benchmark..."
    # Invoke and capture output. 
    # Use generic payload.
    aws lambda invoke \
        --function-name "$FUNCTION_NAME" \
        --log-type Tail \
        --payload $(echo '{"event": "bench"}' | base64) \
        output.json > invoke_result.json

    # Decode logs
    cat invoke_result.json | jq -r '.LogResult' | base64 -d > bench_latest.log
    
    # Parse Result
    # Log line format: "Ping-pong result: 15881.25 roundtrips/s (10001 pongs)"
    RESULT_LINE=$(grep "Ping-pong result:" bench_latest.log || echo "No result found")
    
    if [[ "$RESULT_LINE" == *"No result found"* ]]; then
        echo "FAILURE: Could not find result in logs."
        cat bench_latest.log
    else
        echo "SUCCESS: $RESULT_LINE"
        # Extract just the number for summary
        RPS=$(echo "$RESULT_LINE" | grep -oE "[0-9]+\.[0-9]+" | head -n1)
        echo "SUMMARY_METRIC: ${MEM}MB = ${RPS} RPS"
    fi
done

echo "--------------------------------------------------"
echo "Sweep Completed."
