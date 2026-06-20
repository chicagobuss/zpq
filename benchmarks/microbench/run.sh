#!/usr/bin/env bash
# Run the decode microbench across every column of benchmark_100mb.parquet.
# Output: one JSON object per line; pipe through jq to format.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
FIXTURE="${1:-${ROOT}/data/benchmark_100mb.parquet}"
RUNS="${2:-7}"
WARMUP="${3:-2}"

if [[ ! -x "${ROOT}/zig-out/bin/microbench" ]]; then
    echo "building microbench (ReleaseFast)..." >&2
    zig build microbench -Doptimize=ReleaseFast
fi

# Discover column names via pyarrow.
COLUMNS=$(python3 -c "
import pyarrow.parquet as pq
md = pq.read_metadata('${FIXTURE}')
for c in md.schema:
    if c.physical_type:
        print(c.name)
")

for col in $COLUMNS; do
    "${ROOT}/zig-out/bin/microbench" "$FIXTURE" "$col" "$RUNS" "$WARMUP"
done
