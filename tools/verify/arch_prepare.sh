#!/bin/bash
# Run once beside existing study/PDS-H/ClickBench inputs, then copy the sealed set.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
duckdb= study= lineitem= clickbench= output=
while [ $# -gt 0 ]; do
    case "$1" in
        --duckdb) duckdb=$2; shift 2 ;;
        --study) study=$2; shift 2 ;;
        --lineitem) lineitem=$2; shift 2 ;;
        --clickbench) clickbench=$2; shift 2 ;;
        --out) output=$2; shift 2 ;;
        *) echo 'expected --duckdb --study --lineitem --clickbench --out' >&2; exit 2 ;;
    esac
done
[ -x "$duckdb" ] && [ -d "$study" ] && [ -f "$lineitem" ] && [ -f "$clickbench" ] && [ -n "$output" ] || exit 2
"$duckdb" -no-agent --version | grep -q 'v2\.0\.0' || { echo 'DuckDB 2.0.0 required' >&2; exit 2; }
if [ -f "$output/manifest.json" ]; then echo 'sealed set already exists' >&2; exit 2; fi
mkdir -p "$output"
sql_quote() { printf '%s' "$1" | sed "s/'/''/g"; }
copy_subset() {
    local source=$1 name=$2 count=$3
    "$duckdb" -no-agent -init /dev/null -c "SET threads=1; COPY (SELECT * FROM '$(sql_quote "$source")' LIMIT $count) TO '$(sql_quote "$output/$name.parquet")' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 65536);"
}
for name in card100k comp100k lstr100k; do copy_subset "$study/$name.parquet" "$name" 250000; done
copy_subset "$lineitem" lineitem 500000
copy_subset "$clickbench" clickbench 100000
jq '. + {files:{}, preparation:"first 250k study /500k PDS-H SF1 /100k ClickBench sub1 rows; DuckDB 2.0 CLI single-thread COPY once, Snappy 64k row groups"}' \
    "$here/arch_cases.json" > "$output/manifest.tmp"
for file in "$output"/*.parquet; do
    digest=$(sha256sum "$file" | cut -d' ' -f1)
    bytes=$(wc -c < "$file")
    jq --arg file "$(basename "$file")" --arg sha "$digest" --argjson bytes "$bytes" \
        '.files[$file] = {sha256:$sha, bytes:$bytes}' "$output/manifest.tmp" > "$output/manifest.next"
    mv "$output/manifest.next" "$output/manifest.tmp"
done
mv "$output/manifest.tmp" "$output/manifest.json"
sha256sum "$output/manifest.json"
