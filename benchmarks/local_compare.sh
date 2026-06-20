#!/usr/bin/env bash
# Local CLI comparison: zpq vs duckdb-CLI vs duckdb-Py vs polars-Py.
#
# Goal: read `data/benchmark_100mb.parquet`, do {filter, projection,
# select-arithmetic} variants, write a fresh parquet locally. Time
# wall-clock with /usr/bin/time; report best of N runs per scenario
# per engine.
#
# Apples-to-apples bits:
#   - Same input file (155 MB local).
#   - Same output codec (snappy) on all engines.
#   - Same column subsets / filter predicates across engines.
#   - All engines write to local file (not stdout, not S3).
#
# Usage:
#   ./benchmarks/local_compare.sh [-n RUNS] [-s SCENARIO]
#
# RUNS defaults to 5; SCENARIO can be one of A1..A4, C1..C2, or "all".

set -euo pipefail
cd "$(dirname "$0")/.."

RUNS=5
ONLY=""
while getopts "n:s:" o; do
  case "$o" in
    n) RUNS="$OPTARG" ;;
    s) ONLY="$OPTARG" ;;
    *) echo "usage: $0 [-n RUNS] [-s SCENARIO]"; exit 1 ;;
  esac
done

INPUT="data/benchmark_100mb.parquet"
[[ -f "$INPUT" ]] || { echo "missing $INPUT" >&2; exit 1; }

# Build zpq if needed.
if [[ ! -x ./zig-out/bin/zpq || ./src/cli/main.zig -nt ./zig-out/bin/zpq ]]; then
  echo "# building zpq..." >&2
  zig build -Doptimize=ReleaseFast >&2
fi

# Time helpers — wall-clock seconds with millisecond precision.
time_one() {
  # $1 = command (string, eval'd). Echoes elapsed seconds.
  local t0 t1
  t0=$(date +%s.%N)
  eval "$1" >/dev/null 2>&1
  t1=$(date +%s.%N)
  awk -v a="$t1" -v b="$t0" 'BEGIN { printf "%.3f\n", a - b }'
}

best_of() {
  # $1 = command, $2 = label, $3 = scenario tag. Runs RUNS times,
  # reports best/median.
  local cmd="$1" label="$2" tag="$3"
  local times=()
  for i in $(seq 1 "$RUNS"); do
    times+=("$(time_one "$cmd")")
  done
  # Best (min) and median.
  local sorted
  sorted=$(printf '%s\n' "${times[@]}" | sort -n)
  local best median
  best=$(echo "$sorted" | head -1)
  median=$(echo "$sorted" | awk -v n="$RUNS" 'NR == int((n+1)/2) { print; exit }')
  printf '%-12s %-22s best=%6.3fs median=%6.3fs\n' "$tag" "$label" "$best" "$median"
}

# Output dir.
OUT=/tmp/local_bench_$$
mkdir -p "$OUT"
trap 'rm -rf "$OUT"' EXIT

run_scenario() {
  local tag="$1"
  if [[ -n "$ONLY" && "$ONLY" != "all" && "$ONLY" != "$tag" ]]; then return; fi

  case "$tag" in
    # ============================================================
    # A1 — whole-file copy (no filter, no projection)
    # ============================================================
    A1)
      echo "## A1: whole-file copy (no filter, no projection)"
      best_of "./zig-out/bin/zpq query $INPUT -o $OUT/a1_zpq.parquet" \
              "zpq" "$tag"
      best_of "duckdb -c \"COPY (SELECT * FROM '$INPUT') TO '$OUT/a1_duck.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)\"" \
              "duckdb-cli" "$tag"
      best_of "python3 -c \"import duckdb; duckdb.sql(\\\"COPY (SELECT * FROM '$INPUT') TO '$OUT/a1_duckpy.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)\\\")\"" \
              "duckdb-py" "$tag"
      best_of "python3 -c \"import polars as pl; pl.scan_parquet('$INPUT').sink_parquet('$OUT/a1_polars.parquet', compression='snappy')\"" \
              "polars-py" "$tag"
      ;;

    # ============================================================
    # A2 — projection only (3 cols, no filter)
    # ============================================================
    A2)
      echo "## A2: projection only (3 cols)"
      best_of "./zig-out/bin/zpq query $INPUT -o $OUT/a2_zpq.parquet -c int8,int64_sorted,float64" \
              "zpq" "$tag"
      best_of "duckdb -c \"COPY (SELECT int8, int64_sorted, float64 FROM '$INPUT') TO '$OUT/a2_duck.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)\"" \
              "duckdb-cli" "$tag"
      best_of "python3 -c \"import polars as pl; pl.scan_parquet('$INPUT').select(['int8','int64_sorted','float64']).sink_parquet('$OUT/a2_polars.parquet', compression='snappy')\"" \
              "polars-py" "$tag"
      ;;

    # ============================================================
    # A3 — filter (broad, ~89% surviving) + projection
    # ============================================================
    A3)
      echo "## A3: filter (int8 >= -100, ~89%) + projection"
      best_of "./zig-out/bin/zpq query $INPUT -o $OUT/a3_zpq.parquet -c int8,int64_sorted,float64 --filter 'int8 >= -100'" \
              "zpq" "$tag"
      best_of "duckdb -c \"COPY (SELECT int8, int64_sorted, float64 FROM '$INPUT' WHERE int8 >= -100) TO '$OUT/a3_duck.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)\"" \
              "duckdb-cli" "$tag"
      best_of "python3 -c \"import polars as pl; pl.scan_parquet('$INPUT').filter(pl.col('int8') >= -100).select(['int8','int64_sorted','float64']).sink_parquet('$OUT/a3_polars.parquet', compression='snappy')\"" \
              "polars-py" "$tag"
      ;;

    # ============================================================
    # A4 — filter (selective, ~11% surviving) + projection
    # ============================================================
    A4)
      echo "## A4: filter (int8 >= 100, ~11%) + projection"
      best_of "./zig-out/bin/zpq query $INPUT -o $OUT/a4_zpq.parquet -c int8,int64_sorted,float64 --filter 'int8 >= 100'" \
              "zpq" "$tag"
      best_of "duckdb -c \"COPY (SELECT int8, int64_sorted, float64 FROM '$INPUT' WHERE int8 >= 100) TO '$OUT/a4_duck.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)\"" \
              "duckdb-cli" "$tag"
      best_of "python3 -c \"import polars as pl; pl.scan_parquet('$INPUT').filter(pl.col('int8') >= 100).select(['int8','int64_sorted','float64']).sink_parquet('$OUT/a4_polars.parquet', compression='snappy')\"" \
              "polars-py" "$tag"
      ;;

    # ============================================================
    # C1 — arithmetic select (compute new columns)
    # ============================================================
    C1)
      echo "## C1: --select arithmetic (3 computed cols)"
      best_of "./zig-out/bin/zpq query $INPUT -o $OUT/c1_zpq.parquet --select 'int8, int8 + 100 AS shifted, int64_sorted * 2 AS doubled, int64_random / 1000 AS thousands'" \
              "zpq" "$tag"
      best_of "duckdb -c \"COPY (SELECT int8, int8 + 100 AS shifted, int64_sorted * 2 AS doubled, int64_random / 1000 AS thousands FROM '$INPUT') TO '$OUT/c1_duck.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)\"" \
              "duckdb-cli" "$tag"
      best_of "python3 -c \"import polars as pl; pl.scan_parquet('$INPUT').select([pl.col('int8'), (pl.col('int8') + 100).alias('shifted'), (pl.col('int64_sorted') * 2).alias('doubled'), (pl.col('int64_random') // 1000).alias('thousands')]).sink_parquet('$OUT/c1_polars.parquet', compression='snappy')\"" \
              "polars-py" "$tag"
      ;;

    # ============================================================
    # C2 — arithmetic select + filter
    # ============================================================
    C2)
      echo "## C2: --filter + --select arithmetic"
      best_of "./zig-out/bin/zpq query $INPUT -o $OUT/c2_zpq.parquet --filter 'int8 >= 0' --select 'int8, int8 * int8 AS sq, int64_sorted * 2 AS doubled'" \
              "zpq" "$tag"
      best_of "duckdb -c \"COPY (SELECT int8, int8 * int8 AS sq, int64_sorted * 2 AS doubled FROM '$INPUT' WHERE int8 >= 0) TO '$OUT/c2_duck.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)\"" \
              "duckdb-cli" "$tag"
      best_of "python3 -c \"import polars as pl; pl.scan_parquet('$INPUT').filter(pl.col('int8') >= 0).select([pl.col('int8'), (pl.col('int8') * pl.col('int8')).alias('sq'), (pl.col('int64_sorted') * 2).alias('doubled')]).sink_parquet('$OUT/c2_polars.parquet', compression='snappy')\"" \
              "polars-py" "$tag"
      ;;

    *) echo "unknown scenario: $tag" >&2; exit 1 ;;
  esac
  echo
}

# A scenarios first, then C. Both run by default.
for s in A1 A2 A3 A4 C1 C2; do run_scenario "$s"; done
