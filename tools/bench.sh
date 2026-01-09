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

# --- ZPQ Runners ---
run_native() {
    local input_path="$1" output_path="$2" runs="$3"
    local bin="$PROJECT_ROOT/zig-out/bin/zpq"

    [[ ! -x "$bin" ]] && { echo "Error: zpq not built. Run: zig build -Doptimize=ReleaseFast"; exit 1; }

    for i in $(seq 1 "$runs"); do
        [[ "$runs" -gt 1 ]] && info "Run $i/$runs"
        "$bin" --filter "string_dict_low=category_0001" --select "int32_sorted,string_dict_low,float64" "$input_path" "$output_path"
    done
}

run_serverless_local() {
    local input_path="$1" output_path="$2" runs="$3"

    if ! docker ps --format '{{.Names}}' | grep -q 'lambda-bench'; then
        info "Starting RIE container..."
        (cd "$PROJECT_ROOT/benchmarks" && docker-compose up -d)
        sleep 2
    fi

    local payload="{\"file\": \"$input_path\", \"output\": \"$output_path\", \"filter\": \"string_dict_low=category_0001\", \"select\": \"int32_sorted,string_dict_low,float64\"}"

    for i in $(seq 1 "$runs"); do
        [[ "$runs" -gt 1 ]] && info "Run $i/$runs"
        curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d "$payload" | jq .
    done
}

run_serverless_lambda() {
    local input_path="$1" output_path="$2" runs="$3"
    local fn="${LAMBDA_FUNCTION_NAME:-zpq-lambda-bench}"
    local region="${AWS_REGION:-us-west-2}"
    local payload="{\"file\": \"$input_path\", \"output\": \"$output_path\", \"filter\": \"string_dict_low=category_0001\", \"select\": \"int32_sorted,string_dict_low,float64\"}"

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

run_pyarrow() {
    local input_path="$1" output_path="$2" runs="$3"

    info "PyArrow | $input_path -> $output_path | $runs runs"
    uv run --with pyarrow --with boto3 python3 -c "
import time, os, io
import pyarrow.parquet as pq
import pyarrow.compute as pc
import boto3

input_path = '$input_path'
output_path = '$output_path'
iterations = $runs
s3 = boto3.client('s3')

durations = []
for i in range(iterations):
    start = time.time()

    # Read
    if input_path.startswith('s3://'):
        parts = input_path[5:].split('/', 1)
        obj = s3.get_object(Bucket=parts[0], Key=parts[1])
        f = io.BytesIO(obj['Body'].read())
        table = pq.read_table(f, columns=['int32_sorted', 'string_dict_low', 'float64'])
    else:
        table = pq.read_table(input_path, columns=['int32_sorted', 'string_dict_low', 'float64'])

    # Filter
    mask = pc.equal(table['string_dict_low'], 'category_0001')
    filtered = table.filter(mask)

    # Write
    if output_path.startswith('s3://'):
        parts = output_path[5:].split('/', 1)
        buf = io.BytesIO()
        pq.write_table(filtered, buf)
        buf.seek(0)
        s3.put_object(Bucket=parts[0], Key=parts[1], Body=buf.getvalue())
    else:
        pq.write_table(filtered, output_path)

    duration_ms = (time.time() - start) * 1000
    durations.append(duration_ms)
    print(f'  Run {i+1}: {table.num_rows} -> {filtered.num_rows} rows in {duration_ms:.2f}ms')

print(f'Min: {min(durations):.2f}ms')
print(f'Max: {max(durations):.2f}ms')
print(f'Avg: {sum(durations)/len(durations):.2f}ms')
"
}

run_polars() {
    local input_path="$1" output_path="$2" runs="$3"

    info "Polars | $input_path -> $output_path | $runs runs"
    uv run --with polars python3 -c "
import time, os
import polars as pl

input_path = '$input_path'
output_path = '$output_path'
iterations = $runs

durations = []
for i in range(iterations):
    start = time.time()

    # Read, filter, select, write
    df = pl.scan_parquet(input_path)
    result = (df
        .filter(pl.col('string_dict_low') == 'category_0001')
        .select(['int32_sorted', 'string_dict_low', 'float64'])
        .collect())
    result.write_parquet(output_path)

    duration_ms = (time.time() - start) * 1000
    durations.append(duration_ms)
    # Get input row count for comparison
    input_rows = pl.scan_parquet(input_path).select(pl.len()).collect().item()
    print(f'  Run {i+1}: {input_rows} -> {result.height} rows in {duration_ms:.2f}ms')

if durations:
    print(f'Min: {min(durations):.2f}ms')
    print(f'Max: {max(durations):.2f}ms')
    print(f'Avg: {sum(durations)/len(durations):.2f}ms')
"
}

run_duckdb() {
    local input_path="$1" output_path="$2" runs="$3"

    info "DuckDB | $input_path -> $output_path | $runs runs"
    uv run --with duckdb python3 -c "
import time, os
import duckdb

input_path = '$input_path'
output_path = '$output_path'
iterations = $runs

durations = []
for i in range(iterations):
    start = time.time()
    conn = duckdb.connect()

    # S3 credentials
    conn.execute(\"SET s3_region='\"+os.environ.get('AWS_REGION', 'us-west-2')+\"'\")
    conn.execute(\"SET s3_access_key_id='\"+os.environ.get('AWS_ACCESS_KEY_ID', '')+\"'\")
    conn.execute(\"SET s3_secret_access_key='\"+os.environ.get('AWS_SECRET_ACCESS_KEY', '')+\"'\")

    # Read, filter, select, write
    query = f\"\"\"
        COPY (
            SELECT int32_sorted, string_dict_low, float64
            FROM '{input_path}'
            WHERE string_dict_low = 'category_0001'
        ) TO '{output_path}' (FORMAT PARQUET)
    \"\"\"
    conn.execute(query)

    # Get row counts for reporting
    input_rows = conn.execute(f\"SELECT COUNT(*) FROM '{input_path}'\").fetchone()[0]
    output_rows = conn.execute(f\"SELECT COUNT(*) FROM '{output_path}'\").fetchone()[0]

    duration_ms = (time.time() - start) * 1000
    durations.append(duration_ms)
    print(f'  Run {i+1}: {input_rows} -> {output_rows} rows in {duration_ms:.2f}ms')
    conn.close()

if durations:
    print(f'Min: {min(durations):.2f}ms')
    print(f'Max: {max(durations):.2f}ms')
    print(f'Avg: {sum(durations)/len(durations):.2f}ms')
"
}

# --- Commands ---
cmd_bench() {
    local backend="${1:-}" input="${2:-}" output="${3:-}" size="${4:-10mb}" runs="${5:-1}"

    [[ -z "$backend" || -z "$input" || -z "$output" ]] && {
        echo "Usage: $0 <backend> <input> <output> [size] [runs]"
        echo "  backend: native | serverless-rie | serverless-lambda"
        echo "  input:   local | s3"
        echo "  output:  local | s3"
        exit 1
    }

    # Map backend to context
    local context="native"
    local where="local"
    
    case "$backend" in
        native)
            context="native"
            where="local"
            ;;
        serverless-rie)
            context="serverless"
            where="local"
            ;;
        serverless-lambda)
            context="serverless"
            where="lambda"
            ;;
        *)
            echo "Error: Unknown backend: $backend"
            exit 1
            ;;
    esac

    # Validation
    [[ "$where" == "lambda" && "$output" == "local" ]] && { echo "Error: Lambda cannot write to local"; exit 1; }
    [[ "$where" == "lambda" && "$input" == "local" ]] && { echo "Error: Lambda cannot read from local"; exit 1; }

    local input_path=$(get_input_path "$input" "$size" "$context")
    local output_path=$(get_output_path "$output" "$size")

    info "$backend | $input -> $output | $size | $runs runs"
    info "Input:  $input_path"
    info "Output: $output_path"
    echo ""

    case "$backend" in
        native)             run_native "$input_path" "$output_path" "$runs" ;;
        serverless-rie)     run_serverless_local "$input_path" "$output_path" "$runs" ;;
        serverless-lambda)  run_serverless_lambda "$input_path" "$output_path" "$runs" ;;
    esac
}

cmd_engine() {
    local engine="${1:-}" input="${2:-local}" size="${3:-10mb}" runs="${4:-1}"

    [[ -z "$engine" ]] && {
        echo "Usage: $0 engine <engine> [input] [size] [runs]"
        echo "  engine: pyarrow | polars | duckdb"
        echo "  input:  local | s3 (default: local)"
        exit 1
    }

    local input_path=$(get_input_path "$input" "$size" "native")
    local output_path="/tmp/bench_${engine}_${size}_out.parquet"

    case "$engine" in
        pyarrow) run_pyarrow "$input_path" "$output_path" "$runs" ;;
        polars)  run_polars "$input_path" "$output_path" "$runs" ;;
        duckdb)  run_duckdb "$input_path" "$output_path" "$runs" ;;
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
    native|serverless) cmd_bench "$cmd" "$@" ;;
    engine)  cmd_engine "$@" ;;
    compare) cmd_compare "$@" ;;
    help|--help|-h|"")
        cat <<EOF
Usage: $0 <command> [args...]

Commands:
  <what> <input> <where> <output> [size] [runs]  - Run zpq benchmark
      what:   native | serverless
      input:  local | s3
      where:  local | lambda
      output: local | s3
      size:   1mb | 10mb | 100mb (default: 10mb)
      runs:   iterations (default: 1)

  engine <engine> [input] [size] [runs]  - Run competitor engine
      engine: pyarrow | polars | duckdb

  compare [input] [size] [runs]  - Compare all engines

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
