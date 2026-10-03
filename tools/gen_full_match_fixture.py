#!/usr/bin/env python3
"""Generate the fixture for the full-match (filter-only column skip) tests in src/core/scan.zig.

Eight row groups of 200 rows, sorted on `ts`, so a range predicate gives every outcome at once: `ts >= 500` skips row
groups 0-1, leaves row group 2 partial, and proves row groups 3-7 fully matching from statistics.

Columns:
  ts  int64, 0..1599, sorted, no nulls          the range-filter column
  b   int32, ts // 100 (two values per group)   a filter column that is also a GROUP BY key
  s   utf8, "k{ts:06d}-" + padding, sorted      a filter-only string column (pyarrow writes is_max_value_exact)
  x   double, small repeating values            the aggregate payload
  n   int64, nullable                           group 1 all null, groups 0/2/3 every fifth row null, 4-7 no nulls
  u   uint32 (INTEGER(32, unsigned)), = ts      must never be proven: zpq compares unsigned stats signed

Usage:  .venv/bin/python tools/gen_full_match_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

ROWS_PER_GROUP = 200
GROUPS = 8
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "full_match.parquet"


def nullable(ts: int) -> int | None:
    rg = ts // ROWS_PER_GROUP
    if rg == 1:
        return None
    if rg in (0, 2, 3) and ts % 5 == 0:
        return None
    return ts * 3


def main() -> None:
    ts = list(range(ROWS_PER_GROUP * GROUPS))
    table = pa.table(
        {
            "ts": pa.array(ts, type=pa.int64()),
            "b": pa.array([t // 100 for t in ts], type=pa.int32()),
            "s": pa.array([f"k{t:06d}-" + "p" * 24 for t in ts], type=pa.string()),
            "x": pa.array([(t % 7) * 1.5 - 3.0 for t in ts], type=pa.float64()),
            "n": pa.array([nullable(t) for t in ts], type=pa.int64()),
            "u": pa.array(ts, type=pa.uint32()),
        }
    )
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP, compression="snappy", write_page_index=False)
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
