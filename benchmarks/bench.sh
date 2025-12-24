#!/usr/bin/env bash
#
# Unified benchmark runner for zpq
#
# Usage:
#   ./benchmarks/bench.sh <benchmark> [args...]
#   ./benchmarks/bench.sh list
#   ./benchmarks/bench.sh compare <benchmark> [args...]
#
# Examples:
#   ./benchmarks/bench.sh dns                         # DNS resolver benchmark
#   ./benchmarks/bench.sh e2e data/large.parquet      # Local file E2E
#   ./benchmarks/bench.sh e2e s3://bucket/key --async # S3 with async mode
#   ./benchmarks/bench.sh ping                        # TCP ping-pong throughput
#   ./benchmarks/bench.sh scan data/many_rows.parquet # Simple scan benchmark
#   ./benchmarks/bench.sh compare e2e s3://bucket/key # Compare ZPQ vs PyArrow
#
set -euo pipefail

# --- Configuration ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- Platform Detection ---
detect_platform() {
    local arch=$(uname -m)
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')

    # Normalize arch names
    case "$arch" in
        arm64) arch="aarch64" ;;
        amd64) arch="x86_64" ;;
    esac

    echo "${arch}-${os}"
}

PLATFORM=$(detect_platform)

# --- Colors (if terminal) ---
if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    BLUE='\033[0;34m'
    BOLD='\033[1m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' BLUE='' BOLD='' NC=''
fi

# --- Helpers ---
info() { echo -e "${BLUE}[info]${NC} $*"; }
success() { echo -e "${GREEN}[ok]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
error() { echo -e "${RED}[error]${NC} $*" >&2; }

require_built() {
    local target="$1"
    local bin_path="$PROJECT_ROOT/zig-out/bin/$target"

    if [[ ! -x "$bin_path" ]]; then
        error "Binary not found: $target"
        echo ""
        echo -e "Build it first with:"
        echo -e "  ${BOLD}zig build -Doptimize=ReleaseFast${NC}"
        echo ""
        echo -e "Or for a specific benchmark:"
        echo -e "  ${BOLD}zig build -Doptimize=ReleaseFast${NC}"
        exit 1
    fi
}

# --- Benchmark Definitions ---
# Each benchmark function handles its own argument parsing

bench_dns() {
    require_built "bench-dns"

    info "Running DNS benchmark on $PLATFORM"
    echo ""
    "$PROJECT_ROOT/zig-out/bin/bench-dns"
}

bench_ping() {
    require_built "ping-pongs"

    info "Running TCP ping-pong benchmark on $PLATFORM"
    echo ""
    "$PROJECT_ROOT/zig-out/bin/ping-pongs"
}

bench_e2e() {
    local path="${1:-}"
    shift || true

    if [[ -z "$path" ]]; then
        error "Usage: bench.sh e2e <path> [--sync|--async|--dns=basic] [--iterations N]"
        exit 1
    fi

    require_built "bench-e2e"

    # Parse remaining args
    local mode_arg=""
    local iterations=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --sync) mode_arg="--sync"; shift ;;
            --async) mode_arg="--async"; shift ;;
            --dns=basic) mode_arg="--dns=basic"; shift ;;
            --iterations) iterations="$2"; shift 2 ;;
            -n) iterations="$2"; shift 2 ;;
            *) shift ;;
        esac
    done

    info "Running E2E benchmark on $PLATFORM"
    info "Target: $path"
    [[ -n "$mode_arg" ]] && info "Mode: $mode_arg"
    echo ""

    # Load .env if present (for AWS creds)
    if [[ -f "$PROJECT_ROOT/.env" ]]; then
        set -a
        source "$PROJECT_ROOT/.env"
        set +a
    fi

    "$PROJECT_ROOT/zig-out/bin/bench-e2e" "$path" ${iterations:-1} $mode_arg
}

bench_scan() {
    local path="${1:-$PROJECT_ROOT/data/many_rows.parquet}"

    require_built "zpq"

    info "Running scan benchmark on $PLATFORM"
    info "Target: $path"
    echo ""

    "$PROJECT_ROOT/zig-out/bin/zpq" scan "$path"
}

bench_pyarrow() {
    local path="${1:-}"

    if [[ -z "$path" ]]; then
        error "Usage: bench.sh pyarrow <path>"
        exit 1
    fi

    info "Running PyArrow benchmark"
    info "Target: $path"
    echo ""

    if [[ "$path" == s3://* ]]; then
        # S3 path - use competitor.py
        if [[ -f "$PROJECT_ROOT/.env" ]]; then
            set -a
            source "$PROJECT_ROOT/.env"
            set +a
        fi
        uv run --with pyarrow --with boto3 python3 "$PROJECT_ROOT/benchmarks/competitor.py" "$path" "${2:-1}"
    else
        # Local file
        uv run --with pyarrow python3 "$PROJECT_ROOT/benchmarks/pyarrow_bench.py" "$path"
    fi
}

# --- Compare Mode ---
# Helper to extract "Avg: X.XX" from output (portable across macOS/Linux)
extract_avg() {
    grep -E 'Avg: [0-9.]+' | sed 's/.*Avg: \([0-9.]*\).*/\1/' | head -1
}

compare_e2e() {
    local path="${1:-}"
    shift || true

    if [[ -z "$path" ]]; then
        error "Usage: bench.sh compare e2e <path> [--iterations N]"
        exit 1
    fi

    local iterations="3"
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --iterations|-n) iterations="$2"; shift 2 ;;
            *) shift ;;
        esac
    done

    require_built "bench-e2e"

    echo ""
    echo -e "${BOLD}=== E2E Benchmark Comparison ===${NC}"
    echo -e "Platform:   ${BOLD}$PLATFORM${NC}"
    echo -e "Target:     ${BOLD}$path${NC}"
    echo -e "Iterations: ${BOLD}$iterations${NC}"
    echo ""

    # Load .env if present
    if [[ -f "$PROJECT_ROOT/.env" ]]; then
        set -a
        source "$PROJECT_ROOT/.env"
        set +a
    fi

    # Simple variables (bash 3 compatible)
    local zpq_sync_avg="" zpq_async_avg="" pyarrow_avg="" polars_avg=""

    # Skip sync mode for S3 (TLS initialization issues with std.http.Client)
    if [[ "$path" != s3://* ]]; then
        echo -e "${BOLD}--- ZPQ Sync ---${NC}"
        local output
        output=$("$PROJECT_ROOT/zig-out/bin/bench-e2e" "$path" "$iterations" --sync 2>&1)
        echo "$output" | tail -5
        zpq_sync_avg=$(echo "$output" | extract_avg)
        echo ""
    fi

    echo -e "${BOLD}--- ZPQ Async ---${NC}"
    output=$("$PROJECT_ROOT/zig-out/bin/bench-e2e" "$path" "$iterations" --async 2>&1)
    echo "$output" | tail -5
    zpq_async_avg=$(echo "$output" | extract_avg)
    echo ""

    if [[ "$path" == s3://* ]]; then
        echo -e "${BOLD}--- PyArrow (boto3) ---${NC}"
        output=$(uv run --with pyarrow --with boto3 python3 "$PROJECT_ROOT/benchmarks/competitor.py" "$path" "$iterations" 2>&1)
        echo "$output" | tail -5
        pyarrow_avg=$(echo "$output" | extract_avg)
        echo ""

        echo -e "${BOLD}--- Polars (Rust) ---${NC}"
        output=$(uv run --with polars python3 -c "
import time, os
import polars as pl

s3_path = '$path'
iterations = $iterations

storage_options = {'aws_region': os.environ.get('AWS_REGION', 'us-west-2')}
for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']:
    val = os.environ.get(key)
    if val:
        storage_options[key.lower()] = val

durations = []
for i in range(iterations):
    start = time.time()
    df = pl.scan_parquet(s3_path, storage_options=storage_options)
    result = df.select(df.columns[0]).collect()
    duration_ms = (time.time() - start) * 1000
    durations.append(duration_ms)
    print(f'  Run {i+1}: {result.height} rows in {duration_ms:.2f}ms')

print(f'Min: {min(durations):.2f}ms')
print(f'Max: {max(durations):.2f}ms')
print(f'Avg: {sum(durations)/len(durations):.2f}ms')
" 2>&1)
        echo "$output" | tail -5
        polars_avg=$(echo "$output" | extract_avg)
        echo ""
    else
        echo -e "${BOLD}--- PyArrow ---${NC}"
        output=$(uv run --with pyarrow python3 "$PROJECT_ROOT/benchmarks/pyarrow_bench.py" "$path" 2>&1)
        echo "$output"
        echo ""
    fi

    # Summary with speedups
    echo -e "${BOLD}=== Summary ===${NC}"
    echo ""

    [[ -n "$zpq_sync_avg" ]] && printf "  %-20s %s ms\n" "ZPQ Sync:" "$zpq_sync_avg"
    [[ -n "$zpq_async_avg" ]] && printf "  %-20s %s ms\n" "ZPQ Async:" "$zpq_async_avg"
    [[ -n "$pyarrow_avg" ]] && printf "  %-20s %s ms\n" "PyArrow:" "$pyarrow_avg"
    [[ -n "$polars_avg" ]] && printf "  %-20s %s ms\n" "Polars:" "$polars_avg"

    echo ""

    # Calculate speedups
    if [[ -n "$zpq_async_avg" ]] && [[ -n "$pyarrow_avg" ]]; then
        local speedup=$(echo "scale=2; $pyarrow_avg / $zpq_async_avg" | bc 2>/dev/null || echo "N/A")
        echo -e "  ZPQ Async vs PyArrow: ${GREEN}${speedup}x${NC}"
    fi
    if [[ -n "$zpq_async_avg" ]] && [[ -n "$polars_avg" ]]; then
        local speedup=$(echo "scale=2; $polars_avg / $zpq_async_avg" | bc 2>/dev/null || echo "N/A")
        echo -e "  ZPQ Async vs Polars:  ${GREEN}${speedup}x${NC}"
    fi

    echo ""
}

compare_dns() {
    require_built "bench-dns"

    echo ""
    echo -e "${BOLD}=== DNS Benchmark Comparison ===${NC}"
    echo -e "Platform: ${BOLD}$PLATFORM${NC}"
    echo ""

    "$PROJECT_ROOT/zig-out/bin/bench-dns"

    echo ""
    echo -e "${BOLD}=== Comparison Complete ===${NC}"
}

# --- List Available Benchmarks ---
list_benchmarks() {
    echo ""
    echo -e "${BOLD}Available Benchmarks${NC}"
    echo ""
    echo -e "  ${GREEN}dns${NC}       DNS resolver performance (serial, async, single-flight)"
    echo -e "  ${GREEN}ping${NC}      TCP ping-pong throughput (xev event loop)"
    echo -e "  ${GREEN}e2e${NC}       Full parquet scan with detailed stats (local or S3)"
    echo -e "  ${GREEN}scan${NC}      Quick scan via zpq CLI (local or S3)"
    echo -e "  ${GREEN}pyarrow${NC}   PyArrow baseline (local or S3)"
    echo ""
    echo -e "${BOLD}Comparison Mode${NC}"
    echo ""
    echo -e "  ${GREEN}compare e2e${NC} <path>  Compare ZPQ sync/async vs PyArrow vs Polars"
    echo -e "  ${GREEN}compare dns${NC}         Compare all DNS resolver strategies"
    echo ""
    echo -e "${BOLD}Options (for e2e with S3 paths)${NC}"
    echo ""
    echo -e "  --sync                Synchronous I/O (blocking)"
    echo -e "  --async               Async I/O with speculative DNS (default)"
    echo -e "  --dns=basic           Async I/O with basic thread-pool DNS"
    echo -e "  --iterations, -n N    Number of iterations (default: 1)"
    echo ""
    echo -e "${BOLD}Examples${NC}"
    echo ""
    echo -e "  ./benchmarks/bench.sh scan data/large.parquet       # local"
    echo -e "  ./benchmarks/bench.sh scan s3://bucket/key          # S3"
    echo -e "  ./benchmarks/bench.sh e2e s3://bucket/key --async -n 3"
    echo -e "  ./benchmarks/bench.sh compare e2e s3://bucket/key"
    echo ""
    echo -e "${BOLD}Build first${NC}"
    echo ""
    echo -e "  zig build -Doptimize=ReleaseFast"
    echo ""
    echo -e "Platform: $PLATFORM"
}

# --- Main ---
main() {
    cd "$PROJECT_ROOT"

    local cmd="${1:-list}"
    shift || true

    case "$cmd" in
        list|help|-h|--help)
            list_benchmarks
            ;;
        dns)
            bench_dns "$@"
            ;;
        ping|ping-pong|ping-pongs)
            bench_ping "$@"
            ;;
        e2e)
            bench_e2e "$@"
            ;;
        scan)
            bench_scan "$@"
            ;;
        pyarrow|python)
            bench_pyarrow "$@"
            ;;
        compare)
            local what="${1:-}"
            shift || true
            case "$what" in
                e2e) compare_e2e "$@" ;;
                dns) compare_dns "$@" ;;
                *)
                    error "Unknown comparison: $what"
                    error "Try: compare e2e <path> or compare dns"
                    exit 1
                    ;;
            esac
            ;;
        *)
            error "Unknown benchmark: $cmd"
            echo ""
            list_benchmarks
            exit 1
            ;;
    esac
}

main "$@"
