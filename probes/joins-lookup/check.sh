#!/usr/bin/env bash
# Exact answers vs DuckDB 2.0.0 on the same SQL text, at -j 1/4/16. usage: check.sh <zpq> <sf...>
set -u
here=$(cd "$(dirname "$0")" && pwd); . "$here/queries.sh"
Z=$1; shift
D=$HOME/.cache/zpq-duckdb2/bin-2.0.0/duckdb
out=/mnt/fast/zpq-out/joins-060/r3/check; mkdir -p $out
for sf in "$@"; do
  T=$HOME/bench/data/pdsh-publisher/source/data/tables/scale-$sf; [ $sf = 100 ] && T=$HOME/bench/data/pdsh-sf100
  for q in ${QUERIES:-q14 j2}; do
    sql=$($q $T)
    timeout 600 taskset -c 16-23 $D -no-agent -csv -c "set threads=8; $sql" > $out/duck-$sf-$q.csv 2>&1
    for j in 1 4 16; do
      timeout 900 taskset -c 8-15 $Z query --query "$sql" -j $j ${ZPQ_ARGS:-} > $out/zpq-$sf-$q-j$j.jsonl 2> $out/zpq-$sf-$q-j$j.err || echo "zpq failed: $(head -c 300 $out/zpq-$sf-$q-j$j.err)"
      echo "sf$sf $q -j$j: $(python3 -I $here/compare.py $out/duck-$sf-$q.csv $out/zpq-$sf-$q-j$j.jsonl)"
    done
    cmp -s $out/zpq-$sf-$q-j1.jsonl $out/zpq-$sf-$q-j4.jsonl && cmp -s $out/zpq-$sf-$q-j1.jsonl $out/zpq-$sf-$q-j16.jsonl && echo "sf$sf $q: -j 1/4/16 byte-identical" || echo "sf$sf $q: -j outputs DIFFER"
  done
done
