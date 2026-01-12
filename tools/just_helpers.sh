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

# --- Flamegraph ---
# Runs zpq with perf and generates a flamegraph.
# Usage: flamegraph [input] [output] [extra_args...]
flamegraph() {
    local script_dir="$(dirname "$0")"
    local project_root="$(cd "$script_dir/.." && pwd)"
    local fg_dir="$project_root/vendor/FlameGraph"
    
    if [ ! -d "$fg_dir" ]; then
        error "FlameGraph tools not found at $fg_dir. Run 'just fetch-flamegraph' first."
        exit 1
    fi

    # Source environment
    if [ -f "$project_root/.env" ]; then
        set -a
        source "$project_root/.env"
        set +a
    else
        error ".env file not found"
        exit 1
    fi

    local input="${1:-}"
    local output="${2:-/tmp/flamegraph.svg}"
    shift 2 || true
    local extra_args="$*"

    if [ -z "$input" ]; then
        error "Usage: flamegraph <input> [output_svg] [extra_args...]"
        exit 1
    fi

    info "Generating flamegraph: $input -> $output"

    local wrapper="$project_root/.perf_wrapper.sh"
    cat > "$wrapper" << 'WRAPPER_EOF'
#!/bin/bash
set -e
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$PROJECT_ROOT/.env"

# If a venv exists, activate it (for polars/python benchmarks)
if [ -d "$PROJECT_ROOT/.venv" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

input="$1"
shift
extra_args="$*"

# Determine if input is a python script, an executable, or a parquet file
if [[ "$input" == *.py ]]; then
    exec python3 "$input" $extra_args
elif [ -x "$input" ]; then
    # If it's an absolute or relative path to an executable, run it
    exec "$input" $extra_args
elif [[ "$input" == *.parquet ]]; then
    # If it's a parquet file, run with zpq
    zpq_bin="$PROJECT_ROOT/zig-out/bin/zpq"
    if [ ! -f "$zpq_bin" ]; then
        echo "zpq binary not found at $zpq_bin. Run 'just build' first." >&2
        exit 1
    fi
    exec "$zpq_bin" "$input" /dev/null $extra_args
else
    # Fallback: check if it's a binary name in zig-out/bin
    probe_bin="$PROJECT_ROOT/zig-out/bin/$input"
    if [ -x "$probe_bin" ]; then
        exec "$probe_bin" $extra_args
    else
        echo "Unknown input type: $input. Must be .py, .parquet, or an executable." >&2
        exit 1
    fi
fi
WRAPPER_EOF
    chmod 755 "$wrapper"

    # Use task-clock to be compatible with most environments (cloud VMs)
    info "Recording with perf..."
    sudo perf record -e task-clock -F 997 -g -o /tmp/perf.data -- /bin/bash "$wrapper" "$input" "$extra_args"
    
    info "Rendering flamegraph..."
    sudo perf script -i /tmp/perf.data | "$fg_dir/stackcollapse-perf.pl" | "$fg_dir/flamegraph.pl" > "$output"
    
    success "Flamegraph generated at $output"
    rm -f "$wrapper"
}

# --- Fetch FlameGraph ---
fetch_flamegraph() {
    local script_dir="$(dirname "$0")"
    local project_root="$(cd "$script_dir/.." && pwd)"
    local dest="$project_root/vendor/FlameGraph"
    
    if [ -d "$dest" ]; then
        info "FlameGraph already exists at $dest"
        return 0
    fi
    
    info "Fetching FlameGraph tools..."
    mkdir -p "$project_root/vendor"
    git clone --depth 1 https://github.com/brendangregg/FlameGraph "$dest"
    success "FlameGraph tools installed to $dest"
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
    flamegraph) flamegraph "$@" ;;
    fetch_flamegraph) fetch_flamegraph "$@" ;;
    *)
        error "Unknown command: $cmd"
        exit 1
        ;;
esac
