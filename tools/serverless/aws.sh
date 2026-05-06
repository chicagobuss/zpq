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

    info "Building zpq-lambda for $target-musl..."
    # Build the lambda-flavored binary (excludes io_uring, etc) and use
    # the linux-musl target so the result is statically linked and
    # runs on Lambda's `provided.al2023` without dynamic-loader help.
    # Without `lambda` as the build step, this would package the CLI
    # binary's `main` (with help-text on missing args) — Lambda runtime
    # then fails with `Runtime.ExitError` because the bootstrap exits
    # before talking to the runtime API. Caught in production by
    # the post-C1 sweep on 2026-05-05.
    zig build -Dtarget="${target}-musl" -Doptimize=ReleaseFast lambda

    local zpq_bin="zig-out/bin/zpq-lambda"
    if [[ ! -f "$zpq_bin" ]]; then
        error "Build failed - no zpq-lambda at $zpq_bin"
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
    # Empty default: when updating an existing function, preserve its
    # configured memory unless the caller explicitly passed `$4`. This
    # closes a long-standing footgun where every code-only redeploy
    # silently reset memory back to 512 MB, regressing benchmarks.
    local memory="${4:-}"
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
        # Pre-flight: refuse to push a zip whose target arch doesn't
        # match the function's configured architecture. Without this
        # check, a wrong-arch deploy succeeds silently and every
        # subsequent invoke fails with `Runtime.ExitError` (exit 126).
        # Cost me an hour on 2026-05-05 — never again.
        local existing_arch
        existing_arch=$(aws lambda get-function-configuration \
            --function-name "$fn_name" --region "$region" \
            --query 'Architectures[0]' --output text)
        if [[ "$existing_arch" != "$aws_arch" ]]; then
            error "Arch mismatch: zip is $aws_arch but function $fn_name is $existing_arch."
            error "Either build for $existing_arch or recreate the function with the new arch."
            exit 1
        fi

        info "Updating existing function $fn_name..."
        aws lambda update-function-code \
            --function-name "$fn_name" \
            --zip-file "fileb://$zip_path" \
            --region "$region" \
            --query '{FunctionName: FunctionName, CodeSize: CodeSize}' \
            --output json

        # Wait for update to complete
        aws lambda wait function-updated --function-name "$fn_name" --region "$region"

        # Only touch memory if the caller passed an explicit value.
        # Otherwise preserve whatever the function is configured for.
        if [[ -n "$memory" ]]; then
            aws lambda update-function-configuration \
                --function-name "$fn_name" \
                --memory-size "$memory" \
                --timeout "$DEFAULT_TIMEOUT" \
                --region "$region" \
                --query '{MemorySize: MemorySize, Timeout: Timeout}' \
                --output json
        else
            info "Memory unchanged (preserving existing config)."
        fi
    else
        info "Creating new function $fn_name..."

        # For new function, use default if no explicit memory.
        local create_memory="${memory:-$DEFAULT_MEMORY}"
        memory="$create_memory"

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

    success "Deployed $fn_name ($aws_arch${memory:+, ${memory}MB})"
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
    if [[ -f "$payload" ]]; then
        payload="fileb://$payload"
    fi

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

# --- Memory Sweep ---
# Usage: memory-sweep <function-name> [payload] [region]
memory_sweep() {
    local fn_name="$1"
    local payload="${2:-'{"event": "bench"}'}"
    local region="${3:-$DEFAULT_REGION}"
    local sizes=(128 512 1024 2048 4096)

    load_env
    info "Starting Memory Sweep for: $fn_name"
    printf "%-8s | %-20s\n" "Memory" "Throughput (RPS)"
    echo "------------------------------------------"

    for mem in "${sizes[@]}"; do
        # 1. Update config
        aws lambda update-function-configuration \
            --function-name "$fn_name" \
            --memory-size "$mem" \
            --region "$region" > /dev/null
        
        aws lambda wait function-updated --function-name "$fn_name" --region "$region"

        # 2. Invoke
        local tmp_out="/tmp/zpq-mem-$$.json"
        aws lambda invoke \
            --function-name "$fn_name" \
            --log-type Tail \
            --payload "$(echo "$payload" | base64)" \
            --region "$region" \
            "$tmp_out" > /tmp/invoke_res.json
        
        # 3. Parse result from tail logs (last 4KB)
        local logs
        logs=$(jq -r '.LogResult' /tmp/invoke_res.json | base64 -d)
        
        # ping-pong result format: "Ping-pong result: 15881.25 roundtrips/s"
        local rps
        rps=$(echo "$logs" | grep -oP "Ping-pong result: \K[0-9.]+" || echo "N/A")

        printf "%-8s | %-20s\n" "${mem}MB" "$rps"
        rm -f "$tmp_out"
    done
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
  memory-sweep <fn> [payload]   Sweep memory sizes for a specific function
  list [prefix] [region]        List Lambda functions
  delete <fn> [region]          Delete Lambda function

Examples:
  $0 build arm64
  $0 deploy zpq-filter zig-out/lambda/zpq-lambda-arm64.zip arm64 1769
  $0 invoke zpq-filter '{"file": "s3://bucket/file.parquet"}'
  $0 metrics zpq-filter
  $0 bench-matrix '{"file": "s3://bucket/file.parquet"}'
  $0 memory-sweep zpq-bench-arm
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
    metrics) metrics "$@" ;;
    bench-matrix|bench_matrix) bench_matrix "$@" ;;
    memory-sweep|memory_sweep) memory_sweep "$@" ;;
    list) list "$@" ;;
    delete) delete "$@" ;;
    help|--help|-h) usage ;;
    *)
        error "Unknown command: $cmd"
        usage
        exit 1
        ;;
esac


