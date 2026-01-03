#!/usr/bin/env bash
# tools/bench.sh - Unified benchmark dispatcher
#
# Usage:
#   ./tools/bench.sh <what> <input> <where> <output> [size] [runs]
#
# Arguments:
#   what:   native | serverless
#   input:  local | s3
#   where:  local | lambda
#   output: local | s3
#   size:   1mb | 10mb | 100mb (default: 10mb)
#   runs:   number of iterations (default: 1)
#
# Examples:
#   ./tools/bench.sh native local local local           # zpq local file→file
#   ./tools/bench.sh native s3 local s3 100mb 3         # zpq S3→S3, 100mb, 3 runs
#   ./tools/bench.sh serverless local local local       # RIE file→file
#   ./tools/bench.sh serverless s3 local s3             # RIE S3→S3
#   ./tools/bench.sh serverless s3 lambda s3 100mb      # Real Lambda S3→S3

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load .env
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# --- Args ---
WHAT="${1:-}"
INPUT="${2:-}"
WHERE="${3:-}"
OUTPUT="${4:-}"
SIZE="${5:-10mb}"
RUNS="${6:-1}"

# --- Validation ---
usage() {
    cat <<EOF
Usage: $0 <what> <input> <where> <output> [size] [runs]

Arguments:
  what:   native | serverless
  input:  local | s3
  where:  local | lambda
  output: local | s3
  size:   1mb | 10mb | 100mb (default: 10mb)
  runs:   number of iterations (default: 1)

Examples:
  $0 native local local local           # zpq local file→file
  $0 native s3 local s3 100mb 3         # zpq S3→S3, 100mb, 3 runs
  $0 serverless local local local       # RIE file→file
  $0 serverless s3 local s3             # RIE S3→S3
  $0 serverless s3 lambda s3 100mb      # Real Lambda S3→S3

Environment (from .env):
  AWS_S3_BUCKET       S3 bucket for test files
  AWS_REGION          AWS region (default: us-west-2)
  LAMBDA_FUNCTION_NAME  Lambda function for 'lambda' where (default: zpq-lambda-bench)
EOF
    exit 1
}

[[ -z "$WHAT" || -z "$INPUT" || -z "$WHERE" || -z "$OUTPUT" ]] && usage

[[ "$WHAT" =~ ^(native|serverless)$ ]] || { echo "Invalid what: $WHAT"; usage; }
[[ "$INPUT" =~ ^(local|s3)$ ]] || { echo "Invalid input: $INPUT"; usage; }
[[ "$WHERE" =~ ^(local|lambda)$ ]] || { echo "Invalid where: $WHERE"; usage; }
[[ "$OUTPUT" =~ ^(local|s3)$ ]] || { echo "Invalid output: $OUTPUT"; usage; }
[[ "$SIZE" =~ ^(1mb|10mb|100mb)$ ]] || { echo "Invalid size: $SIZE"; usage; }

# Invalid combinations
if [[ "$WHERE" == "lambda" && "$OUTPUT" == "local" ]]; then
    echo "Error: Lambda cannot write to local filesystem"
    exit 1
fi
if [[ "$WHERE" == "lambda" && "$INPUT" == "local" ]]; then
    echo "Error: Lambda cannot read from local filesystem"
    exit 1
fi
if [[ "$WHAT" == "native" && "$WHERE" == "lambda" ]]; then
    echo "Error: Native binary cannot run in Lambda (use serverless)"
    exit 1
fi

# --- Build paths ---
BUCKET="${AWS_S3_BUCKET:-}"
FILTER="string_dict_low=category_0001"
SELECT="int32_sorted,string_dict_low,float64"

case "$INPUT" in
    local) INPUT_PATH="/data/benchmark_$SIZE.parquet" ;;
    s3)
        [[ -z "$BUCKET" ]] && { echo "Error: AWS_S3_BUCKET not set"; exit 1; }
        INPUT_PATH="s3://$BUCKET/zpq_test_data/benchmark/benchmark_$SIZE.parquet"
        ;;
esac

case "$OUTPUT" in
    local) OUTPUT_PATH="/tmp/bench_${SIZE}_out.parquet" ;;
    s3)
        [[ -z "$BUCKET" ]] && { echo "Error: AWS_S3_BUCKET not set"; exit 1; }
        OUTPUT_PATH="s3://$BUCKET/zpq_test_data/output/bench_${SIZE}_out.parquet"
        ;;
esac

# For native local paths, adjust to host filesystem
if [[ "$WHAT" == "native" ]]; then
    [[ "$INPUT" == "local" ]] && INPUT_PATH="/tmp/zpq_r2_bucket/benchmark/benchmark_$SIZE.parquet"
    [[ "$OUTPUT" == "local" ]] && OUTPUT_PATH="/tmp/bench_${SIZE}_out.parquet"
fi

# --- Logging ---
info() { echo -e "\033[0;34m[bench]\033[0m $*"; }

info "$WHAT | $INPUT -> $OUTPUT | $WHERE | $SIZE | $RUNS runs"
info "Input:  $INPUT_PATH"
info "Output: $OUTPUT_PATH"
echo ""

# --- Runners ---

run_native() {
    local bin="$PROJECT_ROOT/zig-out/bin/zpq"
    if [[ ! -x "$bin" ]]; then
        echo "Error: zpq not built. Run: zig build -Doptimize=ReleaseFast"
        exit 1
    fi

    for i in $(seq 1 "$RUNS"); do
        [[ "$RUNS" -gt 1 ]] && info "Run $i/$RUNS"
        "$bin" --filter "$FILTER" --select "$SELECT" "$INPUT_PATH" "$OUTPUT_PATH"
    done
}

run_serverless_local() {
    # Ensure RIE container is running
    if ! docker ps --format '{{.Names}}' | grep -q 'lambda-bench'; then
        info "Starting RIE container..."
        (cd "$PROJECT_ROOT/benchmarks" && docker-compose up -d)
        sleep 2
    fi

    local payload="{\"file\": \"$INPUT_PATH\", \"output\": \"$OUTPUT_PATH\", \"filter\": \"$FILTER\", \"select\": \"$SELECT\"}"

    for i in $(seq 1 "$RUNS"); do
        [[ "$RUNS" -gt 1 ]] && info "Run $i/$RUNS"
        curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d "$payload" | jq .
    done
}

run_serverless_lambda() {
    local fn="${LAMBDA_FUNCTION_NAME:-zpq-lambda-bench}"
    local region="${AWS_REGION:-us-west-2}"
    local payload="{\"file\": \"$INPUT_PATH\", \"output\": \"$OUTPUT_PATH\", \"filter\": \"$FILTER\", \"select\": \"$SELECT\"}"

    for i in $(seq 1 "$RUNS"); do
        [[ "$RUNS" -gt 1 ]] && info "Run $i/$RUNS"
        aws lambda invoke \
            --function-name "$fn" \
            --cli-binary-format raw-in-base64-out \
            --payload "$payload" \
            --region "$region" \
            /dev/stdout 2>/dev/null | jq .
    done
}

# --- Dispatch ---
case "$WHAT:$WHERE" in
    native:local)       run_native ;;
    serverless:local)   run_serverless_local ;;
    serverless:lambda)  run_serverless_lambda ;;
    *)
        echo "Error: Unsupported combination: $WHAT + $WHERE"
        exit 1
        ;;
esac
