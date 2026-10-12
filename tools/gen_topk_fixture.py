#!/usr/bin/env python3
"""Generate the skewed GROUP BY fixture for the top-k merge tests in src/core/scan.zig.

16 row groups of 1250 rows. Key `k` takes a few heavy values, a run of values tied at the same count (so a cut at
10 falls inside the tie), and a long tail of values seen once or twice, spread so that no row group holds all of a
heavy key's rows. Every 97th key is NULL.

Columns:
  k  utf8, the skewed key described above
  n  int64, k's number (NULL with k), a second spelling of the same groups
  v  utf8, "v" + row, for string min/max carried through the merge

Usage:  .venv/bin/python tools/gen_topk_fixture.py
"""

import pathlib
import random

import pyarrow as pa
import pyarrow.parquet as pq

ROWS_PER_GROUP = 1250
GROUPS = 16
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "topk_skew.parquet"


def main() -> None:
    keys = []
    for i in range(6):
        keys += [i] * (900 - 120 * i)  # heavy: 900, 780, ..., 300
    for i in range(6, 18):
        keys += [i] * 200  # twelve keys tied at 200: a cut at 10 lands inside them
    n = ROWS_PER_GROUP * GROUPS
    tail = 18
    while len(keys) < n:
        keys += [tail] * (1 + tail % 2)
        tail += 1
    keys = keys[:n]
    random.Random(7).shuffle(keys)
    nulls = [i % 97 == 0 for i in range(n)]
    table = pa.table({
        "k": pa.array([None if z else f"key{x:05d}" for x, z in zip(keys, nulls)], pa.string()),
        "n": pa.array([None if z else x for x, z in zip(keys, nulls)], pa.int64()),
        "v": pa.array([f"v{i:05d}" for i in range(n)], pa.string()),
    })
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP)
    print(f"wrote {OUT} ({n} rows, {GROUPS} row groups, {len(set(keys))} keys)")


if __name__ == "__main__":
    main()
