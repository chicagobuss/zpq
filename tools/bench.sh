#!/usr/bin/env bash
# tools/bench.sh - Unified benchmark dispatcher
#
# Commands:
#   bench <what> <input> <where> <output> [size] [runs]  - Run zpq benchmark
#   engine <engine> <input> [size] [runs]                - Run competitor engine
#   compare <input> [size] [runs]                        - Compare all engines
#
# Examples:
#   ./tools/bench.sh native local local local 10mb
#   ./tools/bench.sh serverless s3 local s3 100mb 3
#   ./tools/bench.sh engine pyarrow s3 10mb
#   ./tools/bench.sh compare s3 100mb 3

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load .env
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# --- Logging ---
info() { echo -e "\033[0;34m[bench]\033[0m $*"; }
bold() { echo -e "\033[1m$*\033[0m"; }

# --- Path builders ---
get_input_path() {
    local input="$1" size="$2" context="${3:-native}"
    case "$input" in
        local)
            if [[ "$context" == "native" ]]; then
                echo "/tmp/zpq_r2_bucket/benchmark/benchmark_$size.parquet"
            else
                echo "/data/benchmark_$size.parquet"
            fi
            ;;
        s3)
            local bucket="${AWS_S3_BUCKET:?AWS_S3_BUCKET not set}"
            echo "s3://$bucket/zpq_test_data/benchmark/benchmark_$size.parquet"
            ;;
    esac
}

get_output_path() {
    local output="$1" size="$2"
    case "$output" in
        local) echo "/tmp/bench_${size}_out.parquet" ;;
        s3)
            local bucket="${AWS_S3_BUCKET:?AWS_S3_BUCKET not set}"
            echo "s3://$bucket/zpq_test_data/output/bench_${size}_out.parquet"
            ;;
    esac
}

COLUMNS=(
    "int8" "int16" "int32_sorted" "int32_random" "int64_sorted" "int64_random"
    "uint8" "uint16" "uint32" "uint64" "float32" "float64" "float64_sorted"
    "bool" "bool_sparse" "string_random" "string_dict_low" "string_dict_high"
    "string_sorted" "binary" "timestamp" "timestamp_sorted" "date"
)

cmd_sweep() {
    local input="${1:-local}" output="${2:-null}" size="${3:-100mb}"
    local input_path=$(get_input_path "$input" "$size" "native")
    local out_target
    
    if [[ "$output" == "null" ]]; then
        out_target="/dev/null"
    elif [[ "$output" == "local" ]]; then
        out_target="/tmp/bench_sweep_out.parquet"
    else
        out_target="$output"
    fi

    info "Starting Benchmark Sweep: $input ($input_path) -> $output ($out_target)..."
    printf "%-20s | %-12s | %-12s\n" "Column" "Filtered" "Selected"
    printf "%-20s | %-12s | %-12s\n" "" "(Mrows/s)" "(Mrows/s)"
    echo "-------------------------------------------------------"

    for col in "${COLUMNS[@]}"; do
        # 1. Selected
        local select_out=$(./zig-out/bin/zpq "$input_path" "$out_target" --benchmark --select "$col" --log-level err 2>&1)
        local select_rate=$(echo "$select_out" | awk -F '(' '{print $NF}' | awk -F ' ' '{print $1}' || echo "ERR")

        # 2. Filter
        local filter_val="100"
        if [[ "$col" == *"bool"* ]]; then
            filter_val="true"
        elif [[ "$col" == *"string"* || "$col" == *"binary"* ]]; then
            case "$col" in
                string_dict_low)  filter_val="category_0001" ;;
                string_dict_high) filter_val="unique_00001" ;;
                string_sorted)    filter_val="sort_00000001" ;;
                *)                filter_val="val_0" ;;
            esac
        fi

        local filter_out=$(./zig-out/bin/zpq "$input_path" "$out_target" --benchmark --select "$col" --filter "$col=$filter_val" --log-level err 2>&1)
        local filter_rate=$(echo "$filter_out" | awk -F '(' '{print $NF}' | awk -F ' ' '{print $1}' || echo "ERR")

        printf "%-20s | %10s | %10s\n" "$col" "$filter_rate" "$select_rate"
    done
}

# --- ZPQ Runners ---
run_native() {
    local input_path="$1" output_path="$2" runs="$3" threads="${4:-4}" scenario="${5:-default}"
    local bin="$PROJECT_ROOT/zig-out/bin/zpq"

    [[ ! -x "$bin" ]] && { echo "Error: zpq not built. Run: zig build -Doptimize=ReleaseFast"; exit 1; }
    
    local args=()
    case "$scenario" in
        pass-through) ;; # No args = SELECT *
        filter)
            args+=(--filter "string_dict_low=category_0001")
            # Competitors usually select specific columns in filter benchmark, let's match default
            args+=(--select "int32_sorted,string_dict_low,float64")
            ;;
        select-1)
            args+=(--select "int64_random")
            ;;
        select-3)
            args+=(--select "int64_random,int32_sorted,float64")
            ;;
        default)
             args+=(--filter "string_dict_low=category_0001" --select "int32_sorted,string_dict_low,float64")
             ;;
        *)
            echo "Unknown scenario: $scenario"
            exit 1
            ;;
    esac

    for i in $(seq 1 "$runs"); do
        [[ "$runs" -gt 1 ]] && info "Run $i/$runs"
        "$bin" --threads "$threads" "${args[@]}" "$input_path" "$output_path"
    done
}

run_serverless_local() {
    local input_path="$1" output_path="$2" runs="$3"

    if ! docker ps --format '{{.Names}}' | grep -q 'lambda-bench'; then
        info "Starting RIE container..."
        (cd "$PROJECT_ROOT/bench" && docker-compose up -d)
        sleep 2
    fi

    local payload="{\"file\": \"$input_path\", \"output\": \"$output_path\", \"filter\": \"string_dict_low=category_0001\", \"select\": \"int32_sorted,string_dict_low,float64\"}"

    for i in $(seq 1 "$runs"); do
        [[ "$runs" -gt 1 ]] && info "Run $i/$runs"
        curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d "$payload" | jq .
    done
}

run_serverless_lambda() {
    local input_path="$1" output_path="$2" runs="$3" scenario="${4:-default}"
    local fn="${LAMBDA_FUNCTION_NAME:-zpq-perf-test}"
    local region="${AWS_REGION:-us-west-2}"
    
    local payload="{\"file\": \"$input_path\", \"output\": \"$output_path\", \"filter\": \"string_dict_low=category_0001\", \"select\": \"int32_sorted,string_dict_low,float64\"}"
    if [[ "$scenario" == "pass-through" ]]; then
        payload="{\"file\": \"$input_path\", \"output\": \"$output_path\"}"
    fi

    for i in $(seq 1 "$runs"); do
        [[ "$runs" -gt 1 ]] && info "Run $i/$runs"
        aws lambda invoke \
            --function-name "$fn" \
            --cli-binary-format raw-in-base64-out \
            --payload "$payload" \
            --region "$region" \
            /dev/stdout 2>/dev/null | jq .
    done
}

# --- Engine Runners ---
# All engines do the same operation as ZPQ:
#   - Filter: string_dict_low = 'category_0001'
#   - Select: int32_sorted, string_dict_low, float64
#   - Write: parquet output

run_engine_generic() {
    local engine="$1"
    local input_path="$2"
    local output_path="$3"
    local runs="$4"
    local scenario="${5:-default}"

    info "$engine | $input_path -> $output_path | Scenario: $scenario | $runs runs"
    
    local cmd=(uv run)
    
    # Add engine-specific dependencies
    case "$engine" in
        pyarrow) cmd+=("--with" "pyarrow" "--with" "boto3") ;;
        polars)  cmd+=("--with" "polars" "--with" "s3fs" "--with" "boto3") ;;
        duckdb)  cmd+=("--with" "duckdb" "--with" "httpfs") ;; # httpfs might be internal, duckdb pip is self-contained usually
    esac

    cmd+=("python3" "$PROJECT_ROOT/tools/bench_engines.py" \
          "--engine" "$engine" \
          "--input" "$input_path" \
          "--output" "$output_path" \
          "--runs" "$runs" \
          "--scenario" "$scenario")

    "${cmd[@]}"
}

run_pyarrow() { run_engine_generic "pyarrow" "$1" "$2" "$3" "${4:-default}"; }
run_polars()  { run_engine_generic "polars"  "$1" "$2" "$3" "${4:-default}"; }
run_duckdb()  { run_engine_generic "duckdb"  "$1" "$2" "$3" "${4:-default}"; }

# --- Commands ---
cmd_bench() {
    local type="${1:-}" input="${2:-}" output="${3:-}" size="${4:-10mb}" runs="${5:-1}" threads="${6:-4}" scenario="${7:-default}"

    [[ -z "$type" || -z "$input" || -z "$output" ]] && {
        echo "Usage: $0 bench <type> <input> <output> [size] [runs] [threads] [scenario]"
        echo "  type:   native | lambda | lambda-rie"
        echo "  input:  local | s3"
        echo "  output: local | s3"
        exit 1
    }

    local backend=""
    local where=""
    local context=""

    case "$type" in
        native)
            backend="native"
            where="local"
            context="native"
            ;;
        lambda)
            backend="serverless-lambda"
            where="lambda"
            context="serverless"
            ;;
        lambda-rie)
            backend="serverless-rie"
            where="local"
            context="serverless"
            ;;
        *)
            echo "Error: Unknown type: $type"
            exit 1
            ;;
    esac

    # Validation
    [[ "$where" == "lambda" && "$output" == "local" ]] && { echo "Error: Lambda cannot write to local"; exit 1; }
    [[ "$where" == "lambda" && "$input" == "local" ]] && { echo "Error: Lambda cannot read from local"; exit 1; }

    local input_path=$(get_input_path "$input" "$size" "$context")
    local output_path=$(get_output_path "$output" "$size")

    info "$backend | $input -> $output | $size | Scenario: $scenario | $runs runs"
    info "Input:  $input_path"
    info "Output: $output_path"
    echo ""

    case "$backend" in
        native)             run_native "$input_path" "$output_path" "$runs" "$threads" "$scenario" ;;
        serverless-rie)     run_serverless_local "$input_path" "$output_path" "$runs" ;;
        serverless-lambda)  run_serverless_lambda "$input_path" "$output_path" "$runs" "$scenario" ;;
    esac
}

cmd_engine() {
    local engine="${1:-}" input="${2:-local}" size="${3:-10mb}" runs="${4:-1}" scenario="${5:-default}"

    [[ -z "$engine" ]] && {
        echo "Usage: $0 engine <engine> [input] [size] [runs] [scenario]"
        echo "  engine:   pyarrow | polars | duckdb"
        echo "  input:    local | s3 (default: local)"
        echo "  scenario: default | pass-through | filter | select-1 | select-3"
        exit 1
    }

    local input_path=$(get_input_path "$input" "$size" "native")
    local output_path="/tmp/bench_${engine}_${size}_out.parquet"
    if [[ "$input" == "s3" ]]; then
         # If input is S3, let's make output S3 too for consistency in benchmarks
         local bucket="${AWS_S3_BUCKET:?AWS_S3_BUCKET not set}"
         output_path="s3://$bucket/zpq_test_data/output/bench_${engine}_${size}_out.parquet"
    fi

    case "$engine" in
        pyarrow) run_pyarrow "$input_path" "$output_path" "$runs" "$scenario" ;;
        polars)  run_polars "$input_path" "$output_path" "$runs" "$scenario" ;;
        duckdb)  run_duckdb "$input_path" "$output_path" "$runs" "$scenario" ;;
        *) echo "Error: Unknown engine: $engine"; exit 1 ;;
    esac
}

cmd_compare() {
    local input="${1:-local}" output="${2:-local}" size="${3:-10mb}" runs="${4:-3}"
    local input_path=$(get_input_path "$input" "$size" "native")

    # Output base path depends on output type
    local output_base
    if [[ "$output" == "s3" ]]; then
        local bucket="${AWS_S3_BUCKET:?AWS_S3_BUCKET not set}"
        output_base="s3://$bucket/zpq_test_data/benchmark/compare_${size}"
    else
        output_base="/tmp/bench_compare_${size}"
    fi

    bold "=== Benchmark Comparison ==="
    echo "Input:  $input_path"
    echo "Output: ${output_base}_<engine>.parquet"
    echo "Runs:   $runs"
    echo ""

    bold "--- ZPQ (native) ---"
    run_native "$input_path" "${output_base}_zpq.parquet" "$runs" 2>&1 || true
    echo ""

    bold "--- PyArrow ---"
    run_pyarrow "$input_path" "${output_base}_pyarrow.parquet" "$runs" 2>&1 || true
    echo ""

    bold "--- Polars ---"
    run_polars "$input_path" "${output_base}_polars.parquet" "$runs" 2>&1 || true
    echo ""

    bold "--- DuckDB ---"
    run_duckdb "$input_path" "${output_base}_duckdb.parquet" "$runs" 2>&1 || true
    echo ""

    bold "=== Done ==="
}

# --- Main ---
cmd="${1:-}"
shift || true

case "$cmd" in
    bench|native|lambda|lambda-rie)
        if [[ "$cmd" == "bench" ]]; then
            cmd_bench "$@"
        else
            # Support directly: ./tools/bench.sh native s3 s3 ...
            cmd_bench "$cmd" "$@"
        fi
        ;;
    engine)  cmd_engine "$@" ;;
    compare) cmd_compare "$@" ;;
    sweep)   cmd_sweep "$@" ;;
    help|--help|-h|"")
        cat <<EOF
Usage: $0 <command> [args...]

Commands:
  native <input> <output> [size] [runs] [threads] [scenario]
  lambda <input> <output> [size] [runs]
  lambda-rie <input> <output> [size] [runs]

  engine <engine> [input] [size] [runs]  - Run competitor engine
      engine: pyarrow | polars | duckdb

  compare [input] [size] [runs]  - Compare all engines
  sweep [input] [output] [size]  - Sweep through all types

Examples:
  $0 native local local local 10mb
  $0 serverless s3 local s3 100mb 3
  $0 engine pyarrow s3 10mb
  $0 compare s3 100mb 3
EOF
        ;;
    *)
        echo "Unknown command: $cmd"
        exit 1
        ;;
esac
