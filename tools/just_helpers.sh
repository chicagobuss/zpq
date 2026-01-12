#!/usr/bin/env bash
# tools/just_helpers.sh
# Universal helper library for justfile recipes.
# Designed to be portable (Bash 3.x+), fail-open on errors, and robust.

set -uo pipefail

# Configuration
DEPS_TAG="deps-v0.2"
GH_OWNER="chicagobuss"
GH_REPO="zpq"

# --- Logging ---
info() { echo -e "\033[0;34m[info]\033[0m $*"; }
warn() { echo -e "\033[0;33m[warn]\033[0m $*"; }
error() { echo -e "\033[0;31m[error]\033[0m $*" >&2; }
success() { echo -e "\033[0;32m[ok]\033[0m $*"; }

# --- Platform Detection ---
# Returns a triple like "aarch64-linux" or "x86_64-macos"
detect_triple() {
    local arch=$(uname -m)
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')

    # Normalize arch
    case "$arch" in
        arm64|aarch64) arch="aarch64" ;;
        x86_64|amd64) arch="x86_64" ;;
        *)
            warn "Unknown architecture: $arch. Assuming native build."
            echo "unknown-unknown"
            return
            ;;
    esac

    # Normalize OS
    case "$os" in
        darwin) os="macos" ;;
        linux) os="linux" ;;
        *)
            warn "Unknown OS: $os. Assuming native build."
            echo "unknown-unknown"
            return
            ;;
    esac

    echo "${arch}-${os}"
}

# --- Fetch Pre-built Dependencies ---
# Refers to tools/r2-fetch-artifacts.sh for the actual logic.
fetch_deps() {
    "$(dirname "$0")/r2-fetch-artifacts.sh"
}

# --- Test against apache/parquet-testing ---
test_parquet_testing() {
    local mode="${1:-schema}"  # schema or scan

    echo "=== Testing against apache/parquet-testing (mode: $mode) ==="

    local passed=0
    local failed=0
    local skipped=0
    local failed_files=""

    for f in references/parquet-testing/data/*.parquet; do
        local name=$(basename "$f")
        printf "%-55s " "$name"

        local output
        output=$(zig build run -- "$mode" "$f" 2>&1) || true
        local exit_code=$?

        if [ $exit_code -eq 0 ]; then
            echo "✓ PASS"
            passed=$((passed + 1))
        else
            if echo "$output" | grep -qE "UnsupportedCompression|not supported|Unsupported|BYTE_STREAM_SPLIT|LZ4_RAW|BROTLI|LZ4"; then
                echo "⊘ SKIP (unsupported)"
                skipped=$((skipped + 1))
            else
                echo "✗ FAIL"
                local error_line=$(echo "$output" | grep -E "error|Error|panic" | tail -1)
                if [ -n "$error_line" ]; then
                    echo "    $error_line"
                fi
                failed=$((failed + 1))
                failed_files="$failed_files $name"
            fi
        fi
    done

    echo ""
    echo "=== Summary ==="
    echo "Passed:  $passed"
    echo "Skipped: $skipped"
    echo "Failed:  $failed"

    if [ -n "$failed_files" ]; then
        echo ""
        echo "Failed files:$failed_files"
    fi

    [ $failed -eq 0 ]
}

# --- Bpftrace Benchmark ---
# Runs zpq with bpftrace syscall tracing. Requires sudo.
# Usage: bpftrace_bench [input] [output] [extra_args...]
# Example: bpftrace_bench s3://bucket/file.parquet /tmp/out.parquet --benchmark
bpftrace_bench() {
    local script_dir="$(dirname "$0")"
    local project_root="$(cd "$script_dir/.." && pwd)"
    
    # Source environment for AWS credentials and export them
    if [ -f "$project_root/.env" ]; then
        set -a  # Automatically export all variables
        source "$project_root/.env"
        set +a
    else
        error ".env file not found at $project_root/.env"
        exit 1
    fi
    
    local input="${1:-}"
    local output="${2:-/tmp/bpftrace_output.parquet}"
    shift 2 || true
    local extra_args="$*"
    
    if [ -z "$input" ]; then
        error "Usage: bpftrace_bench <input> [output] [extra_args...]"
        echo "  Examples:"
        echo "    bpftrace_bench s3://bucket/file.parquet /tmp/out.parquet --benchmark"
        echo "    bpftrace_bench /local/file.parquet /tmp/out.parquet --benchmark"
        exit 1
    fi
    
    info "Bpftrace benchmark: $input -> $output"
    info "Extra args: $extra_args"
    
    # Create a wrapper script that sources .env and runs zpq
    # This lets the root-run bpftrace subprocess source env vars properly
    # Use project dir instead of /tmp to avoid namespace isolation issues
    local wrapper="$project_root/.bpftrace_wrapper.sh"
    cat > "$wrapper" << WRAPPER_EOF
#!/bin/bash
source $project_root/.env
exec $project_root/zig-out/bin/zpq $input $output $extra_args
WRAPPER_EOF
    chmod 755 "$wrapper"
    
    if [ ! -x "$wrapper" ]; then
        error "Failed to create wrapper script at $wrapper"
        exit 1
    fi
    info "Created wrapper: $wrapper"
    
    # Run bpftrace with the wrapper script
    # Note: bpftrace -c can't execute shell scripts directly, must use /bin/bash
    # This traces: reads, writes, sends (TLS/S3), recvs
    sudo bpftrace -e '
tracepoint:syscalls:sys_enter_read /comm == "zpq"/ { @reads = count(); @read_bytes = sum(args->count); }
tracepoint:syscalls:sys_enter_write /comm == "zpq"/ { @writes = count(); @write_bytes = sum(args->count); }
tracepoint:syscalls:sys_enter_sendto /comm == "zpq"/ { @sends = count(); @send_bytes = sum(args->len); }
tracepoint:syscalls:sys_enter_recvfrom /comm == "zpq"/ { @recvs = count(); }
tracepoint:syscalls:sys_enter_openat /comm == "zpq"/ { @opens = count(); }
' -c "/bin/bash $wrapper"
    
    rm -f "$wrapper"
}

# --- Validate against reference implementations ---
validate_parquet() {
    python3 "$(dirname "$0")/validate_parquet.py" "$@"
}

# --- Dispatcher ---
# Allows calling functions by name: ./tools/just_helpers.sh fetch_deps
cmd="${1:-}"
shift || true

case "$cmd" in
    fetch_deps) fetch_deps "$@" ;;
    detect_triple) detect_triple "$@" ;;
    test_parquet_testing) test_parquet_testing "$@" ;;
    validate_parquet) validate_parquet "$@" ;;
    bpftrace_bench) bpftrace_bench "$@" ;;
    *)
        error "Unknown command: $cmd"
        exit 1
        ;;
esac
