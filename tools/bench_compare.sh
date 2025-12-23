#!/bin/bash
#
# bench_compare.sh - Compare ZPQ vs PyArrow vs Polars performance
#
# Usage:
#   ./tools/bench_compare.sh [S3_PATH] [ITERATIONS]
#
# Environment:
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
#   Or source a .env file before running
#
set -e

# Configuration
S3_PATH="${1:-${ZPQ_TEST_S3_PATH:-s3://skyway-diat-staging-data/test_data/valid/sizes/small/10k_rows.parquet}}"
ITERATIONS="${2:-3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Results storage
declare -A RESULTS

print_header() {
    echo ""
    echo -e "${BOLD}${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${BLUE}  $1${NC}"
    echo -e "${BOLD}${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${CYAN}── $1 ──${NC}"
    echo ""
}

print_result() {
    local name="$1"
    local min="$2"
    local max="$3"
    local avg="$4"
    printf "  ${GREEN}%-20s${NC} min: %8.2fms  max: %8.2fms  avg: %8.2fms\n" "$name" "$min" "$max" "$avg"
}

print_error() {
    echo -e "  ${RED}$1${NC}"
}

# Load environment
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo -e "${YELLOW}Loading .env...${NC}"
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

cd "$PROJECT_ROOT"

print_header "ZPQ Benchmark Suite"

echo -e "Target:     ${BOLD}$S3_PATH${NC}"
echo -e "Iterations: ${BOLD}$ITERATIONS${NC}"
echo -e "Region:     ${BOLD}${AWS_REGION:-us-east-1}${NC}"

# Ensure build is up to date
print_section "Building ZPQ"
zig build bench-e2e 2>&1 | tail -5 || true
echo -e "${GREEN}Build complete${NC}"

# Parse results from output - extracts "Avg: XXX.XXms" line
parse_avg() {
    grep -oP 'Avg: \K[0-9.]+' | head -1
}

parse_min() {
    grep -oP 'Min: \K[0-9.]+' | head -1
}

parse_max() {
    grep -oP 'Max: \K[0-9.]+' | head -1
}

run_zpq_sync() {
    print_section "ZPQ (Sync I/O)"
    local output
    if output=$(./zig-out/bin/bench-e2e "$S3_PATH" "$ITERATIONS" --sync 2>&1); then
        echo "$output"
        RESULTS["zpq_sync_min"]=$(echo "$output" | parse_min)
        RESULTS["zpq_sync_max"]=$(echo "$output" | parse_max)
        RESULTS["zpq_sync_avg"]=$(echo "$output" | parse_avg)
    else
        print_error "ZPQ Sync failed"
        echo "$output"
    fi
}

run_zpq_async() {
    print_section "ZPQ (Async I/O)"
    local output
    if output=$(./zig-out/bin/bench-e2e "$S3_PATH" "$ITERATIONS" --async 2>&1); then
        echo "$output"
        RESULTS["zpq_async_min"]=$(echo "$output" | parse_min)
        RESULTS["zpq_async_max"]=$(echo "$output" | parse_max)
        RESULTS["zpq_async_avg"]=$(echo "$output" | parse_avg)
    else
        print_error "ZPQ Async failed"
        echo "$output"
    fi
}

run_pyarrow() {
    print_section "PyArrow (Python/C++)"
    local output
    if output=$(uv run --with pyarrow --with boto3 python3 "$SCRIPT_DIR/bench_e2e/competitor.py" "$S3_PATH" "$ITERATIONS" 2>&1); then
        echo "$output"
        RESULTS["pyarrow_min"]=$(echo "$output" | parse_min)
        RESULTS["pyarrow_max"]=$(echo "$output" | parse_max)
        RESULTS["pyarrow_avg"]=$(echo "$output" | parse_avg)
    else
        print_error "PyArrow failed"
        echo "$output"
    fi
}

run_polars() {
    print_section "Polars (Rust)"
    local output
    # Polars benchmark inline
    if output=$(uv run --with polars python3 -c "
import time
import sys
import os

import polars as pl

s3_path = '$S3_PATH'
iterations = $ITERATIONS

print('Benchmarking Polars E2E')
print(f'Target: {s3_path}')
print(f'Iterations: {iterations}')

storage_options = {
    'aws_region': os.environ.get('AWS_REGION', 'us-west-2'),
}
for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']:
    val = os.environ.get(key)
    if val:
        storage_options[key.lower()] = val

durations = []
for i in range(iterations):
    print(f'\\nRun {i+1}/{iterations}...')
    start = time.time()

    # Read first column only to match ZPQ behavior
    df = pl.scan_parquet(s3_path, storage_options=storage_options)
    first_col = df.columns[0]
    result = df.select(first_col).collect()
    count = result.height

    end = time.time()
    duration_ms = (end - start) * 1000
    durations.append(duration_ms)
    print(f'  Scanned {count} rows in {duration_ms:.2f}ms')

print(f'\\n--- Results ({iterations} runs) ---')
print(f'Min: {min(durations):.2f}ms')
print(f'Max: {max(durations):.2f}ms')
print(f'Avg: {sum(durations)/len(durations):.2f}ms')
" 2>&1); then
        echo "$output"
        RESULTS["polars_min"]=$(echo "$output" | parse_min)
        RESULTS["polars_max"]=$(echo "$output" | parse_max)
        RESULTS["polars_avg"]=$(echo "$output" | parse_avg)
    else
        print_error "Polars failed"
        echo "$output"
    fi
}

# Run all benchmarks
run_zpq_sync
run_zpq_async
run_pyarrow
run_polars

# Summary
print_header "Summary"

echo -e "${BOLD}Results (lower is better):${NC}"
echo ""

[ -n "${RESULTS[zpq_sync_avg]}" ] && print_result "ZPQ (Sync)" "${RESULTS[zpq_sync_min]}" "${RESULTS[zpq_sync_max]}" "${RESULTS[zpq_sync_avg]}"
[ -n "${RESULTS[zpq_async_avg]}" ] && print_result "ZPQ (Async)" "${RESULTS[zpq_async_min]}" "${RESULTS[zpq_async_max]}" "${RESULTS[zpq_async_avg]}"
[ -n "${RESULTS[pyarrow_avg]}" ] && print_result "PyArrow" "${RESULTS[pyarrow_min]}" "${RESULTS[pyarrow_max]}" "${RESULTS[pyarrow_avg]}"
[ -n "${RESULTS[polars_avg]}" ] && print_result "Polars" "${RESULTS[polars_min]}" "${RESULTS[polars_max]}" "${RESULTS[polars_avg]}"

echo ""

# Calculate speedups if we have results
if [ -n "${RESULTS[zpq_sync_avg]}" ] && [ -n "${RESULTS[pyarrow_avg]}" ]; then
    speedup=$(echo "scale=2; ${RESULTS[pyarrow_avg]} / ${RESULTS[zpq_sync_avg]}" | bc 2>/dev/null || echo "N/A")
    echo -e "${BOLD}ZPQ Sync vs PyArrow:${NC} ${GREEN}${speedup}x${NC}"
fi

if [ -n "${RESULTS[zpq_async_avg]}" ] && [ -n "${RESULTS[pyarrow_avg]}" ]; then
    speedup=$(echo "scale=2; ${RESULTS[pyarrow_avg]} / ${RESULTS[zpq_async_avg]}" | bc 2>/dev/null || echo "N/A")
    echo -e "${BOLD}ZPQ Async vs PyArrow:${NC} ${GREEN}${speedup}x${NC}"
fi

if [ -n "${RESULTS[zpq_sync_avg]}" ] && [ -n "${RESULTS[polars_avg]}" ]; then
    speedup=$(echo "scale=2; ${RESULTS[polars_avg]} / ${RESULTS[zpq_sync_avg]}" | bc 2>/dev/null || echo "N/A")
    echo -e "${BOLD}ZPQ Sync vs Polars:${NC} ${GREEN}${speedup}x${NC}"
fi

echo ""
echo -e "${YELLOW}Note: Results may vary based on network conditions, caching, and system load.${NC}"
