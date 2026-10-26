#!/bin/bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
host=${1:?host required}
config=${2:?inventory required}
setup_only=${3:-false}
get() { jq -er --arg h "$host" "$1" "$config"; }
remote() {
    local words=() word
    for word in "$@"; do words+=("$(printf '%q' "$word")"); done
    ssh -o ConnectTimeout=10 "$host" "${words[*]}"
}
notify() {
    local key=$1 message=$2 argv=() word
    while IFS= read -r word; do argv+=("$word"); done < <(jq -r --arg h "$host" "$key // [] | .[]" "$config")
    if [ ${#argv[@]} -gt 0 ]; then "${argv[@]}" "$message"; fi
}
cache_root=$(get '.hosts[$h].root')
target=$(get '.hosts[$h].target')
duck_platform=$(get '.hosts[$h].duckdb_platform')
data=$(get '.data')
binaries=$(get '.binaries')
revision=$(get '.revision')
out=$(get '.out')
memory_max=$(get '.hosts[$h].memory_max // "8G"')
nice_level=$(get '.hosts[$h].nice // 10')
disk_cap_kb=$(get '(.hosts[$h].disk_cap_gb // 20) * 1000000')
budget=$(get '.budget_seconds // 90')
archive_sha=$(jq -er --arg p "$duck_platform" '.platforms[$p].archive_sha256' "$here/arch_duckdb.json")
duck_sha=$(jq -er --arg p "$duck_platform" '.platforms[$p].binary_sha256' "$here/arch_duckdb.json")
manifest_sha=$(sha256sum "$data/manifest.json" | cut -d' ' -f1)
engine_sha=$(sha256sum "$binaries/$target/zpq" | cut -d' ' -f1)
runner_sha=$(sha256sum "$binaries/$target/arch-runner" | cut -d' ' -f1)
remote mkdir -p "$cache_root"
cache_root=$(remote bash -c 'cd "$1" && pwd' bash "$cache_root" | tail -n 1)
rsync -a --checksum "$here/arch_remote.sh" "$here/arch_polars.py" "$here/../release_smoke.sh" "$host:$cache_root/"
remote bash "$cache_root/arch_remote.sh" setup "$cache_root" "$duck_platform" "$archive_sha" "$duck_sha" "$disk_cap_kb"
if ! remote test -f "$cache_root/data-$manifest_sha/manifest.json"; then
    rsync -a "$data/" "$host:$cache_root/data-$manifest_sha/"
fi
if ! remote test -f "$cache_root/engine-$engine_sha/zpq"; then
    rsync -a "$binaries/$target/zpq" "$host:$cache_root/engine-$engine_sha/"
fi
if ! remote test -f "$cache_root/runner-$runner_sha/arch-runner"; then
    rsync -a "$binaries/$target/arch-runner" "$host:$cache_root/runner-$runner_sha/"
fi
if "$setup_only"; then echo "$host setup cached"; exit 0; fi
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$RANDOM"
run_dir="$cache_root/runs/$run_id"
local_dir="$out/$host/$run_id"
mkdir -p "$local_dir"
release_status=elsewhere
if [[ "$target" == aarch64-* ]]; then
    source=$(get '.source')
    if ! remote test -f "$cache_root/source-$revision/.ready"; then
        rsync -a "$source" "$host:$cache_root/source-$revision.tar"
    fi
    set +e
    remote bash "$cache_root/arch_remote.sh" release "$cache_root" "$revision" "$target" \
        "$memory_max" "$nice_level" "$engine_sha" 2>&1 | tee "$local_dir/release-smoke.log"
    release_rc=${PIPESTATUS[0]}
    set -e
    release_status=pass
    if [ "$release_rc" -ne 0 ]; then
        jq -n --arg revision "$revision" \
            '{revision: $revision, release_smoke: {status: "fail", log: "release-smoke.log"}}' \
            > "$local_dir/summary.json"
        exit "$release_rc"
    fi
fi
reference=$(jq -r '.reference // empty' "$config")
extra=()
if [ -n "$reference" ]; then
    ref_sha=$(sha256sum "$reference" | cut -d' ' -f1)
    rsync -a "$reference" "$host:$cache_root/reference-$ref_sha.json"
    extra+=(--reference "$cache_root/reference-$ref_sha.json")
fi
threads=$(get '.hosts[$h].threads // 0')
if [ "$threads" -gt 0 ]; then extra+=(--threads "$threads"); fi
if [ "$(get '.hosts[$h].hash_only // false')" = true ]; then extra+=(--hash-only); fi
if [ "$(get '.hosts[$h].tiny_perf // false')" = true ]; then extra+=(--tiny-perf); fi
notify '.hosts[$h].hold' 'Arch timing window: hold heavy jobs for 2 minutes starting now; release will follow.'
release() { notify '.hosts[$h].hold' 'Arch matrix timing window ended; heavy jobs may resume.'; }
trap release EXIT
remote uptime
top_probe='if [ "$(uname -s)" = Darwin ]; then top -l 1 -n 5; else top -b -n 1 -w 180 | head -14; fi'
remote bash -c "$top_probe" | tee "$local_dir/preflight.txt"
set +e
remote bash "$cache_root/arch_remote.sh" run "$cache_root" "$memory_max" "$nice_level" "$disk_cap_kb" \
    "$cache_root/runner-$runner_sha/arch-runner" --data "$cache_root/data-$manifest_sha" \
    --binary "$cache_root/engine-$engine_sha/zpq" --duckdb "$cache_root/duckdb-$duck_sha/duckdb" \
    --duckdb-sha256 "$duck_sha" --polars-script "$cache_root/arch_polars.py" --revision "$revision" \
    --target "$target" --out "$run_dir" --budget-seconds "$budget" \
    --resources "MemoryMax=$memory_max nice=$nice_level disk_cap_kb=$disk_cap_kb" "${extra[@]}" \
    2>&1 | tee "$local_dir/run.log"
rc=${PIPESTATUS[0]}
set -e
release
trap - EXIT
if [ "$rc" -eq 3 ]; then
    notify '.notify' "P0 arch matrix HASH MISMATCH on $host; $run_dir; retrieving exact output now." || true
fi
if ! rsync -az "$host:$run_dir/summary.json" "$local_dir/"; then
    echo "collection failed; remote evidence retained at $host:$run_dir" >&2
    exit 2
fi
if [ "$rc" -eq 3 ]; then
    while IFS= read -r file; do
        rsync -az "$host:$run_dir/$file" "$local_dir/"
    done < <(jq -r '.queries[] | select(.correctness == "HASH_MISMATCH") |
        .error_text | split("; exact output ")[1] | split("/")[-1]' "$local_dir/summary.json")
fi
jq --arg status "$release_status" --arg comparison "$(get '.comparison // "no cross-host comparison"')" \
    '.release_smoke = {status: $status, log: "release-smoke.log"} | .cross_host_comparison = $comparison |
     if $comparison == "no cross-host comparison" then .exit = 2 else . end' \
    "$local_dir/summary.json" > "$local_dir/summary.tmp"
mv "$local_dir/summary.tmp" "$local_dir/summary.json"
ln -sfn "$run_id/summary.json" "$out/$host/latest.json"
echo "summary $local_dir/summary.json"
if [ "$rc" -eq 0 ]; then remote rm -rf "$run_dir"; fi
exit "$rc"
