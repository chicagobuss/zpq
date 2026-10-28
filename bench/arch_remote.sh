#!/bin/bash
# Cached host setup is separate from the measured runner invocation.
set -euo pipefail
action=$1
shift
hash_file() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi
}
if [ "$action" = setup ]; then
    cache_root=$1
    duck_platform=$2
    archive_sha=$3
    binary_sha=$4
    disk_cap_kb=$5
    mkdir -p "$cache_root"
    cd "$cache_root"
    actual_kb=$(du -sk . | awk '{print $1}')
    if [ "$actual_kb" -gt "$disk_cap_kb" ]; then echo 'campaign disk cap exceeded' >&2; exit 2; fi
    mkdir -p "duckdb-$binary_sha"
    if [ ! -f "duckdb-$binary_sha/duckdb" ]; then
        archive="duckdb-cli-$duck_platform.tar.gz"
        curl -fLsS --connect-timeout 15 --max-time 180 \
            "https://github.com/duckdb/duckdb/releases/download/v2.0.0/$archive" -o "$archive"
        test "$(hash_file "$archive" | awk '{print $1}')" = "$archive_sha"
        tar -xzf "$archive" -C "duckdb-$binary_sha"
        rm "$archive"
    fi
    test "$(hash_file "duckdb-$binary_sha/duckdb" | awk '{print $1}')" = "$binary_sha"
    if [ ! -x polars-2.0.0/bin/python ] && [ ! -f polars-unavailable ]; then
        uv_bin=$(command -v uv || true)
        for candidate in "$HOME/.local/bin/uv" /home/linuxbrew/.linuxbrew/bin/uv /opt/homebrew/bin/uv; do
            if [ -z "$uv_bin" ] && [ -x "$candidate" ]; then uv_bin=$candidate; fi
        done
        if [ -n "$uv_bin" ] && \
            UV_CACHE_DIR="$PWD/uv-cache" "$uv_bin" venv --python 3.12 polars-2.0.0 && \
            UV_CACHE_DIR="$PWD/uv-cache" "$uv_bin" pip install --python polars-2.0.0/bin/python \
                polars==2.0.0 polars-runtime-32==2.0.0; then
            rm -rf uv-cache
        else
            printf '%s\n' 'Polars setup unavailable; rival rows skipped' > polars-unavailable
        fi
    fi
    exit 0
fi
if [ "$action" != run ] && [ "$action" != quick ]; then echo 'expected setup, run or quick' >&2; exit 2; fi
cache_root=$1
memory_max=$2
nice_level=$3
disk_cap_kb=$4
shift 4
case "$cache_root" in /*) ;; *) echo "explicit absolute root required" >&2; exit 2 ;; esac
if [ "$action" = run ]; then cd "$cache_root"; fi
test "$(du -sk "$cache_root" | awk '{print $1}')" -le "$disk_cap_kb" || { echo 'campaign disk cap exceeded' >&2; exit 2; }
unset POLARS_MAX_THREADS
runner_args=("$@")
for ((i=0; i<${#runner_args[@]}; i++)); do
    if [ "${runner_args[$i]}" = --threads ]; then export POLARS_MAX_THREADS="${runner_args[$((i+1))]}"; fi
done
if [ "$action" = run ] && [ -x polars-2.0.0/bin/python ] && [ ! -f polars-unavailable ]; then
    runner_args+=(--polars-python "$PWD/polars-2.0.0/bin/python")
fi
if command -v systemd-run >/dev/null 2>&1 && systemctl --user show-environment >/dev/null 2>&1; then
    exec systemd-run --user --scope --quiet -p "MemoryMax=$memory_max" \
        nice -n "$nice_level" "${runner_args[@]}"
elif [ "$(uname -s)" = Darwin ]; then
    # macOS has no systemd. The fixed tiny fixtures bound memory and outputs.
    exec nice -n "$nice_level" "${runner_args[@]}"
else
    echo "user systemd manager required for configured memory cap" >&2
    exit 2
fi
