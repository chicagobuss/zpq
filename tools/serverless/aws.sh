#!/usr/bin/env bash
# tools/serverless/aws.sh
# Helper script for AWS Lambda operations with ZPQ.
# Requires: AWS CLI configured, .env with credentials (optional)

set -euo pipefail

# --- Configuration ---
DEFAULT_REGION="${AWS_REGION:-us-west-2}"
DEFAULT_MEMORY=512
DEFAULT_TIMEOUT=120
DEFAULT_ROLE="${ZPQ_LAMBDA_ROLE:-}"

# --- Logging ---
info() { echo -e "\033[0;34m[lambda]\033[0m $*"; }
warn() { echo -e "\033[0;33m[warn]\033[0m $*"; }
error() { echo -e "\033[0;31m[error]\033[0m $*" >&2; }
success() { echo -e "\033[0;32m[ok]\033[0m $*"; }

# --- Load .env if present ---
load_env() {
    if [[ -f ".env" ]]; then
        # shellcheck disable=SC1091
        source .env
    fi
}

# --- Build Lambda ---
# Usage: build <arch>
# Builds zpq for the target arch and packages as Lambda zip
build() {
    local arch="${1:-arm64}"

    local target
    case "$arch" in
        arm64|aarch64) target="aarch64-linux" ;;
        x86_64|amd64) target="x86_64-linux" ;;
        *) error "Unknown arch: $arch (use arm64 or x86_64)"; exit 1 ;;
    esac

    info "Building zpq for $target..."
    zig build -Dtarget="$target" -Doptimize=ReleaseFast

    local zpq_bin="zig-out/bin/zpq"
    if [[ ! -f "$zpq_bin" ]]; then
        error "Build failed - no zpq at $zpq_bin"
        exit 1
    fi

    # Verify architecture
    local file_arch
    file_arch=$(file "$zpq_bin" 2>/dev/null | grep -oE "x86-64|aarch64|ARM" || echo "unknown")
    info "Built: $file_arch binary ($(du -h "$zpq_bin" | cut -f1))"

    # Create Lambda zip (zpq renamed to bootstrap)
    local zip_dir="zig-out/lambda"
    local zip_path="$zip_dir/zpq-lambda-$arch.zip"
    mkdir -p "$zip_dir"
    cp "$zpq_bin" "$zip_dir/bootstrap"
    (cd "$zip_dir" && zip -j "zpq-lambda-$arch.zip" bootstrap && rm bootstrap)
    success "Created $zip_path ($(du -h "$zip_path" | cut -f1))"

    echo "$zip_path"
}

# --- Deploy Lambda ---
# Usage: deploy <function-name> <zip-path> <arch> [memory] [region]
deploy() {
    local fn_name="$1"
    local zip_path="$2"
    local arch="${3:-arm64}"
    local memory="${4:-$DEFAULT_MEMORY}"
    local region="${5:-$DEFAULT_REGION}"

    load_env

    local aws_arch
    case "$arch" in
        arm64|aarch64) aws_arch="arm64" ;;
        x86_64|amd64) aws_arch="x86_64" ;;
        *) error "Unknown arch: $arch"; exit 1 ;;
    esac

    # Check if function exists
    if aws lambda get-function --function-name "$fn_name" --region "$region" &>/dev/null; then
        info "Updating existing function $fn_name..."
        aws lambda update-function-code \
            --function-name "$fn_name" \
            --zip-file "fileb://$zip_path" \
            --region "$region" \
            --query '{FunctionName: FunctionName, CodeSize: CodeSize}' \
            --output json

        # Update config if memory changed
        aws lambda update-function-configuration \
            --function-name "$fn_name" \
            --memory-size "$memory" \
            --region "$region" \
            --query '{MemorySize: MemorySize}' \
            --output json
    else
        info "Creating new function $fn_name..."

        # Get role
        local role="$DEFAULT_ROLE"
        if [[ -z "$role" ]]; then
            role=$(aws lambda get-function --function-name zpq-filter-s3 --region "$region" --query 'Configuration.Role' --output text 2>/dev/null || true)
        fi
        if [[ -z "$role" ]]; then
            error "No role found. Set ZPQ_LAMBDA_ROLE or ensure zpq-filter-s3 exists"
            exit 1
        fi

        aws lambda create-function \
            --function-name "$fn_name" \
            --runtime provided.al2023 \
            --handler bootstrap \
            --role "$role" \
            --zip-file "fileb://$zip_path" \
            --memory-size "$memory" \
            --timeout "$DEFAULT_TIMEOUT" \
            --architectures "$aws_arch" \
            --region "$region" \
            --query '{FunctionName: FunctionName, MemorySize: MemorySize, Architectures: Architectures}' \
            --output json
    fi

    success "Deployed $fn_name ($aws_arch, ${memory}MB)"
}

# --- Invoke Lambda ---
# Usage: invoke <function-name> <payload-json> [region]
invoke() {
    local fn_name="$1"
    local payload="$2"
    local region="${3:-$DEFAULT_REGION}"

    load_env

    local tmp_out="/tmp/zpq-lambda-$$.json"

    info "Invoking $fn_name..."
    aws lambda invoke \
        --function-name "$fn_name" \
        --cli-binary-format raw-in-base64-out \
        --payload "$payload" \
        --region "$region" \
        "$tmp_out" >/dev/null

    cat "$tmp_out"
    rm -f "$tmp_out"
}

# --- Get Lambda Logs ---
# Usage: logs <function-name> [region]
logs() {
    local fn_name="$1"
    local region="${2:-$DEFAULT_REGION}"

    load_env

    local stream
    stream=$(aws logs describe-log-streams \
        --log-group-name "/aws/lambda/$fn_name" \
        --order-by LastEventTime \
        --descending \
        --limit 1 \
        --region "$region" \
        --query 'logStreams[0].logStreamName' \
        --output text)

    if [[ -z "$stream" || "$stream" == "None" ]]; then
        warn "No log streams found for $fn_name"
        return 1
    fi

    aws logs get-log-events \
        --log-group-name "/aws/lambda/$fn_name" \
        --log-stream-name "$stream" \
        --region "$region" \
        --query 'events[*].message' \
        --output text
}

# --- Get Lambda Metrics ---
# Usage: metrics <function-name> [region]
metrics() {
    local fn_name="$1"
    local region="${2:-$DEFAULT_REGION}"

    load_env

    logs "$fn_name" "$region" | grep -E "(REPORT|Filter completed|Upload complete|Duration|Memory)"
}

# --- Benchmark Matrix ---
# Usage: bench-matrix <payload-json>
# Runs all 4 combinations: arm64/x86_64 x 512MB/1769MB
bench_matrix() {
    local payload="$1"
    local region="${2:-$DEFAULT_REGION}"

    load_env

    info "Building both architectures..."
    local arm_zip x86_zip
    arm_zip=$(build arm64)
    x86_zip=$(build x86_64)

    local configs=(
        "arm-512:arm64:512:$arm_zip"
        "arm-1769:arm64:1769:$arm_zip"
        "x86-512:x86_64:512:$x86_zip"
        "x86-1769:x86_64:1769:$x86_zip"
    )

    info "Deploying 4 Lambda variants..."
    for config in "${configs[@]}"; do
        IFS=':' read -r suffix arch memory zip <<< "$config"
        local fn_name="zpq-bench-$suffix"
        deploy "$fn_name" "$zip" "$arch" "$memory" "$region"
    done

    info "Running benchmarks..."
    echo ""
    printf "%-20s %12s %12s %12s %8s %10s\n" "Config" "Filter" "Upload" "Total" "Memory" "Billed"
    printf "%-20s %12s %12s %12s %8s %10s\n" "------" "------" "------" "-----" "------" "------"

    for config in "${configs[@]}"; do
        IFS=':' read -r suffix arch memory zip <<< "$config"
        local fn_name="zpq-bench-$suffix"

        # Invoke
        invoke "$fn_name" "$payload" "$region" >/dev/null

        # Get metrics
        sleep 2  # Wait for logs
        local log_output
        log_output=$(logs "$fn_name" "$region" 2>/dev/null || echo "")

        local filter_time upload_time total_time mem_used billed
        filter_time=$(echo "$log_output" | grep -oP "Filter completed in \K[0-9.]+" | tail -1 || echo "N/A")
        upload_time=$(echo "$log_output" | grep -oP "Upload complete:.*in \K[0-9.]+" | tail -1 || echo "N/A")
        total_time=$(echo "$log_output" | grep -oP "Duration: \K[0-9.]+" | tail -1 || echo "N/A")
        mem_used=$(echo "$log_output" | grep -oP "Max Memory Used: \K[0-9]+" | tail -1 || echo "N/A")
        billed=$(echo "$log_output" | grep -oP "Billed Duration: \K[0-9]+" | tail -1 || echo "N/A")

        printf "%-20s %10sms %10sms %10sms %6sMB %8sms\n" \
            "$arch-${memory}MB" "$filter_time" "$upload_time" "$total_time" "$mem_used" "$billed"
    done
}

# --- List Lambda Functions ---
# Usage: list [prefix] [region]
list() {
    local prefix="${1:-zpq}"
    local region="${2:-$DEFAULT_REGION}"

    load_env

    aws lambda list-functions \
        --region "$region" \
        --query "Functions[?starts_with(FunctionName, '$prefix')].[FunctionName, Runtime, MemorySize, Architectures[0]]" \
        --output table
}

# --- Delete Lambda Function ---
# Usage: delete <function-name> [region]
delete() {
    local fn_name="$1"
    local region="${2:-$DEFAULT_REGION}"

    load_env

    info "Deleting $fn_name..."
    aws lambda delete-function --function-name "$fn_name" --region "$region"
    success "Deleted $fn_name"
}

# --- Local RIE Benchmark ---
# Usage: local-bench <backend> <size> [runs]
# backend: aws|rustfs|file
# size: 1mb|10mb|100mb
local_bench() {
    local backend="${1:-aws}"
    local size="${2:-10mb}"
    local runs="${3:-1}"

    load_env

    local input output
    case "$backend" in
        aws)
            local bucket="${AWS_S3_BUCKET:?AWS_S3_BUCKET not set in .env}"
            input="s3://$bucket/zpq_test_data/benchmark/benchmark_$size.parquet"
            output="s3://$bucket/zpq_test_data/output/bench_${size}_out.parquet"
            ;;
        rustfs)
            input="s3://zpq-r2-bucket/benchmark/benchmark_$size.parquet"
            output="s3://zpq-r2-bucket/output/bench_${size}_out.parquet"
            export S3_ENDPOINT="${S3_ENDPOINT:-http://host.docker.internal:3000}"
            ;;
        file)
            input="/data/benchmark_$size.parquet"
            output="/tmp/bench_${size}_out.parquet"
            ;;
        *) error "Unknown backend: $backend (use aws|rustfs|file)"; exit 1 ;;
    esac

    local payload="{\"file\": \"$input\", \"output\": \"$output\", \"filter\": \"string_dict_low=category_0001\", \"select\": \"int32_sorted,string_dict_low,float64\"}"

    info "Backend: $backend | Size: $size | Runs: $runs"
    info "Input:  $input"
    info "Output: $output"

    # Ensure RIE container is running
    if ! docker ps --format '{{.Names}}' | grep -q 'lambda-bench'; then
        info "Starting RIE container..."
        (cd "$(dirname "$0")/../../benchmarks" && docker-compose up -d)
        sleep 2
    fi

    for i in $(seq 1 "$runs"); do
        [[ "$runs" -gt 1 ]] && info "Run $i/$runs"
        curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d "$payload" | jq .
    done
}

# --- Help ---
usage() {
    cat <<EOF
ZPQ AWS Lambda Helper

Usage: $0 <command> [args...]

Commands:
  build <arch>                  Build zpq and package as Lambda zip (arch: arm64|x86_64)
  deploy <fn> <zip> <arch> [mem] [region]  Deploy/update Lambda
  invoke <fn> <payload> [region]  Invoke Lambda with JSON payload
  logs <fn> [region]            Get latest logs for Lambda
  metrics <fn> [region]         Get key metrics (duration, memory, etc.)
  bench-matrix <payload>        Run benchmark across all arch/memory combos
  list [prefix] [region]        List Lambda functions
  delete <fn> [region]          Delete Lambda function
  local-bench <backend> <size> [runs]  Run benchmark on local RIE (backend: aws|rustfs|file)

Examples:
  $0 build arm64
  $0 deploy zpq-filter zig-out/lambda/zpq-lambda-arm64.zip arm64 1769
  $0 invoke zpq-filter '{"file": "s3://bucket/file.parquet"}'
  $0 metrics zpq-filter
  $0 bench-matrix '{"file": "s3://bucket/file.parquet"}'
  $0 local-bench aws 100mb 3

Environment:
  AWS_REGION          Default region (default: us-west-2)
  AWS_S3_BUCKET       S3 bucket for benchmark files (required for aws backend)
  ZPQ_LAMBDA_ROLE     IAM role ARN for new functions
  .env                Loaded automatically if present
EOF
}

# --- Dispatcher ---
cmd="${1:-help}"
shift || true

case "$cmd" in
    build) build "$@" ;;
    deploy) deploy "$@" ;;
    invoke) invoke "$@" ;;
    logs) logs "$@" ;;
    local-bench) local_bench "$@" ;;
    metrics) metrics "$@" ;;
    bench-matrix|bench_matrix) bench_matrix "$@" ;;
    list) list "$@" ;;
    delete) delete "$@" ;;
    help|--help|-h) usage ;;
    *)
        error "Unknown command: $cmd"
        usage
        exit 1
        ;;
esac
