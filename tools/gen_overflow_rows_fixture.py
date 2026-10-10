#!/usr/bin/env python3
"""Generate the fixture for the "only a consumed row can overflow" tests in src/engine.zig.

1000 rows in one row group. Row 500 holds the faults: a value whose double overflows i64, and a zero divisor. A query
that excludes row 500 (a WHERE, an aggregate's FILTER, the group-key path) must answer as if it were absent; one that
consumes it must fail.

Columns:
  id  int64, 0..999, PLAIN
  p   int64, id % 100, except 9e18 at row 500, PLAIN (the vector kernel)
  a   int64, the same values, dictionary-encoded (the coded path)
  d   int64, id % 7 + 1, except 0 at row 500 (a divisor)
  n   int64, nullable: null on every tenth row, else id % 100

Usage:  .venv/bin/python tools/gen_overflow_rows_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 1000
FAULT = 500
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "overflow_rows.parquet"


def main() -> None:
    vals = [9_000_000_000_000_000_000 if i == FAULT else i % 100 for i in range(ROWS)]
    table = pa.table({
        "id": pa.array(range(ROWS), pa.int64()),
        "p": pa.array(vals, pa.int64()),
        "a": pa.array(vals, pa.int64()),
        "d": pa.array([0 if i == FAULT else i % 7 + 1 for i in range(ROWS)], pa.int64()),
        "n": pa.array([None if i % 10 == 0 else i % 100 for i in range(ROWS)], pa.int64()),
    })
    pq.write_table(table, OUT, row_group_size=ROWS, use_dictionary=["a"], compression="none")


if __name__ == "__main__":
    main()
