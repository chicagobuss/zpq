#!/usr/bin/env bash
# What a query costs through libzpq on a warm context against the CLI, a process per query, in perf counters
# (user + kernel, all threads). Run from the repo root after `zig build -Doptimize=ReleaseFast` and
# `zig build libzpq -Doptimize=ReleaseFast`. CPUS pins both (default 8-15); N sets the calls per library run; SF1 is
# a TPC-H SF1 lineitem file.
set -euo pipefail
CPUS=${CPUS:-8-15}
N=${N:-201}
REPS=${REPS:-5}
ZIG=${ZIG:-zig}
lib=$PWD/zig-out/lib
bench=$(mktemp)
trap 'rm -f "$bench"' EXIT
$ZIG cc -O2 -o "$bench" tests/libzpq/bench_calls.c -Izig-out/include -L"$lib" -lzpq -Wl,-rpath,"$lib"
EV=instructions,cycles,task-clock
Q6F="l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24"
SF1=${SF1:-$HOME/.cache/showcase-data/tpch/lineitem_sf1_zstd.parquet}

# Mean instructions, cycles and task-clock (CPU ms, not wall time) of a command over REPS runs.
stat() {
  perf stat -x, -e $EV -r "$REPS" taskset -c "$CPUS" "$@" 2>&1 >/dev/null |
    awk -F, '{v[$3] = $1} END {printf "%s %s %s\n", v["instructions"], v["cycles"], v["task-clock"]}'
}

row='%-12s %14s %14s %10s | %14s %14s %10s | %8s\n'
printf "$row" query cli_instr cli_cycles cli_cpu_ms lib_instr lib_cycles lib_cpu_ms "cycles%"
while IFS='|' read -r name file agg filter; do
  [[ -z $name ]] && continue
  req="{\"inputs\":[\"$file\"],\"aggregate\":\"$agg\"${filter:+,\"filter\":\"$filter\"}}"
  read -r ci cc ct < <(stat zig-out/bin/zpq query "$file" --aggregate "$agg" ${filter:+--filter "$filter"})
  read -r ai ac at < <(stat "$bench" "$N" "$req")
  read -r oi oc ot < <(stat "$bench" 1 "$req")
  li=$(( (${ai%.*} - ${oi%.*}) / (N - 1) ))
  lc=$(( (${ac%.*} - ${oc%.*}) / (N - 1) ))
  lt=$(awk -v a="$at" -v o="$ot" -v n="$N" 'BEGIN {printf "%.3f", (a - o) / (n - 1)}')
  pct=$(awk -v l="$lc" -v c="${cc%.*}" 'BEGIN {printf "%.1f", 100 * l / c}')
  printf "$row" "$name" "${ci%.*}" "${cc%.*}" "$(printf %.3f "$ct")" "$li" "$lc" "$lt" "$pct%"
done <<QUERIES
count|ci/fixtures/parquet/mixed_codec.parquet|count(*) AS n|
small-scan|ci/fixtures/parquet/mixed_codec.parquet|count(*) AS n, sum(a) AS s, max(b) AS m|a >= 100
sf1-q6ish|$SF1|sum(l_extendedprice * l_discount) AS rev|$Q6F
QUERIES
