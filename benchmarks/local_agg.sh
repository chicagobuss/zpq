#!/usr/bin/env bash
# Local CLI aggregate comparison: zpq vs polars vs duckdb.
#
# We measure two things per engine:
#   - process_ms: full wall time of invoking the binary / interpreter
#                 (what a user actually feels when they run a query)
#   - engine_ms:  just the query itself, excluding language startup
#                 (fairer "engine throughput" comparison)
#
# For polars/duckdb-py the engine_ms is reported by the script
# itself via `time.perf_counter()` around `.collect()` /  `.fetchall()`.
# zpq reports its `total_ms` in the response envelope.
# duckdb-cli prints a `.timer on` figure.
#
# Workloads cover:
#   - simple aggs (count, sum, min, max, avg) — like classical DB summary
#   - conditional aggs (FILTER WHERE) — the C4 hot path
#   - mixed numeric+string aggs — close to wide-fact-table queries
#
# Usage: ./benchmarks/local_agg.sh [-n RUNS]

set -euo pipefail
cd "$(dirname "$0")/.."

RUNS=5
while getopts "n:" o; do
  case "$o" in
    n) RUNS="$OPTARG" ;;
    *) echo "usage: $0 [-n RUNS]"; exit 1 ;;
  esac
done

INPUT="data/benchmark_100mb.parquet"
[[ -f "$INPUT" ]] || { echo "missing $INPUT" >&2; exit 1; }

# Build ReleaseFast if needed.
if [[ ! -x ./zig-out/bin/zpq || ./src/cli/main.zig -nt ./zig-out/bin/zpq ]]; then
  echo "# building zpq..." >&2
  zig build -Doptimize=ReleaseFast >&2
fi

# Warm the page cache once at the top so per-run numbers aren't
# dominated by first-read disk latency.
cat "$INPUT" > /dev/null

# ============================================================
# Helpers
# ============================================================

# Run a command N times, parse out two numbers per run (process_ms,
# engine_ms), report best of N for each.
# Args: $1 = label, $2 = command (eval'd), $3 = engine_ms_extractor
#       (a sed/awk/jq pipeline that pulls engine_ms from stdout)
bench_one() {
  local label="$1" cmd="$2" extractor="$3"
  local proc_times=() eng_times=()
  for i in $(seq 1 "$RUNS"); do
    local t0 t1 out
    t0=$(date +%s.%N)
    out=$(eval "$cmd" 2>&1)
    t1=$(date +%s.%N)
    local proc_ms eng_ms
    proc_ms=$(awk -v a="$t1" -v b="$t0" 'BEGIN { printf "%.0f", (a - b) * 1000 }')
    eng_ms=$(echo "$out" | eval "$extractor" || echo "?")
    proc_times+=("$proc_ms")
    eng_times+=("$eng_ms")
  done
  local proc_best eng_best
  proc_best=$(printf '%s\n' "${proc_times[@]}" | sort -n | head -1)
  eng_best=$(printf '%s\n' "${eng_times[@]}" | sort -n | head -1)
  printf '  %-22s  proc_best=%4s ms   engine_best=%4s ms\n' "$label" "$proc_best" "$eng_best"
}

# ============================================================
# Scenarios — three classes of agg query
# ============================================================

scenario_simple() {
  echo "## A1: simple aggregates (count, sum, min, max, avg)"

  local zpq_q='count(*) AS rows, sum(int64_sorted) AS total, min(int8) AS lo, max(int8) AS hi, avg(float64) AS mean'

  bench_one "zpq" \
    "./zig-out/bin/zpq query $INPUT --aggregate '$zpq_q'" \
    "jq -r '.total_ms'"

  bench_one "polars-py" \
    "python3 -c \"import time, polars as pl
t0=time.perf_counter()
df = pl.scan_parquet('$INPUT').select([
  pl.len().alias('rows'),
  pl.col('int64_sorted').sum().alias('total'),
  pl.col('int8').min().alias('lo'),
  pl.col('int8').max().alias('hi'),
  pl.col('float64').mean().alias('mean'),
]).collect()
print(int((time.perf_counter()-t0)*1000))\"" \
    "tail -1"

  bench_one "duckdb-py" \
    "python3 -c \"import time, duckdb
t0=time.perf_counter()
duckdb.sql(\\\"SELECT count(*) AS rows, sum(int64_sorted) AS total, min(int8) AS lo, max(int8) AS hi, avg(float64) AS mean FROM '$INPUT'\\\").fetchall()
print(int((time.perf_counter()-t0)*1000))\"" \
    "tail -1"

  bench_one "duckdb-cli" \
    "duckdb -c \"SELECT count(*) AS rows, sum(int64_sorted) AS total, min(int8) AS lo, max(int8) AS hi, avg(float64) AS mean FROM '$INPUT';\"" \
    "echo ?"
  echo
}

scenario_conditional() {
  echo "## A2: conditional aggregates (FILTER WHERE)"

  local zpq_q="sum(int64_sorted) FILTER (WHERE int8 >= 0) AS pos_sum, sum(int64_sorted) FILTER (WHERE int8 < 0) AS neg_sum, count(*) FILTER (WHERE int8 >= 0) AS pos_count, count(*) AS rows"

  bench_one "zpq" \
    "./zig-out/bin/zpq query $INPUT --aggregate \"$zpq_q\"" \
    "jq -r '.total_ms'"

  bench_one "polars-py" \
    "python3 -c \"import time, polars as pl
t0=time.perf_counter()
df = pl.scan_parquet('$INPUT').select([
  pl.col('int64_sorted').filter(pl.col('int8') >= 0).sum().alias('pos_sum'),
  pl.col('int64_sorted').filter(pl.col('int8') < 0).sum().alias('neg_sum'),
  pl.col('int8').filter(pl.col('int8') >= 0).count().alias('pos_count'),
  pl.len().alias('rows'),
]).collect()
print(int((time.perf_counter()-t0)*1000))\"" \
    "tail -1"

  bench_one "duckdb-py" \
    "python3 -c \"import time, duckdb
t0=time.perf_counter()
duckdb.sql(\\\"SELECT sum(int64_sorted) FILTER (WHERE int8 >= 0) AS pos_sum, sum(int64_sorted) FILTER (WHERE int8 < 0) AS neg_sum, count(*) FILTER (WHERE int8 >= 0) AS pos_count, count(*) AS rows FROM '$INPUT'\\\").fetchall()
print(int((time.perf_counter()-t0)*1000))\"" \
    "tail -1"

  bench_one "duckdb-cli" \
    "duckdb -c \"SELECT sum(int64_sorted) FILTER (WHERE int8 >= 0) AS pos_sum, sum(int64_sorted) FILTER (WHERE int8 < 0) AS neg_sum, count(*) FILTER (WHERE int8 >= 0) AS pos_count, count(*) AS rows FROM '$INPUT';\"" \
    "echo ?"
  echo
}

scenario_string_filter() {
  echo "## A3: 4-way conditional sum on a string-dimension column"
  echo "    sum(value) WHERE dim IN {a, b, c, other}"

  # Use string_dict_low (low-cardinality string) as the dimension,
  # int64_sorted as the value. Approximates "sum value per
  # dimension" without group-by.
  local zpq_q="sum(int64_sorted) FILTER (WHERE string_dict_low = category_0001) AS s1, sum(int64_sorted) FILTER (WHERE string_dict_low = category_0002) AS s2, sum(int64_sorted) FILTER (WHERE string_dict_low = category_0003) AS s3, count(*) FILTER (WHERE string_dict_low = category_0001) AS n1"

  bench_one "zpq" \
    "./zig-out/bin/zpq query $INPUT --aggregate \"$zpq_q\"" \
    "jq -r '.total_ms'"

  bench_one "polars-py" \
    "python3 -c \"import time, polars as pl
t0=time.perf_counter()
df = pl.scan_parquet('$INPUT').select([
  pl.col('int64_sorted').filter(pl.col('string_dict_low') == 'category_0001').sum().alias('s1'),
  pl.col('int64_sorted').filter(pl.col('string_dict_low') == 'category_0002').sum().alias('s2'),
  pl.col('int64_sorted').filter(pl.col('string_dict_low') == 'category_0003').sum().alias('s3'),
  pl.col('string_dict_low').filter(pl.col('string_dict_low') == 'category_0001').count().alias('n1'),
]).collect()
print(int((time.perf_counter()-t0)*1000))\"" \
    "tail -1"

  bench_one "duckdb-py" \
    "python3 -c \"import time, duckdb
t0=time.perf_counter()
duckdb.sql(\\\"SELECT sum(int64_sorted) FILTER (WHERE string_dict_low = 'category_0001') AS s1, sum(int64_sorted) FILTER (WHERE string_dict_low = 'category_0002') AS s2, sum(int64_sorted) FILTER (WHERE string_dict_low = 'category_0003') AS s3, count(*) FILTER (WHERE string_dict_low = 'category_0001') AS n1 FROM '$INPUT'\\\").fetchall()
print(int((time.perf_counter()-t0)*1000))\"" \
    "tail -1"
  echo
}

scenario_simple
scenario_conditional
scenario_string_filter
