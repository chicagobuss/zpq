#!/usr/bin/env python3
"""Generate the many-tiny-row-groups fixture for the write property test in src/engine.zig.

512 row groups of 4 rows, so most row groups lie well before the 64 KiB footer tail an S3 read prefetches, and a copy
that carries Bloom filters (the test adds them) coalesces a row group's chunks with filters at the end of the file.

Columns:
  id  int64, row number
  g   int32, row % 7
  d   decimal(18, 2), (row - 1000) / 100
  s   utf8, "s" + row % 97, null every 11th row

Usage:  .venv/bin/python tools/gen_many_tiny_fixture.py
"""

import pathlib
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

ROWS_PER_GROUP = 4
GROUPS = 512
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "many_tiny.parquet"


def main() -> None:
    n = ROWS_PER_GROUP * GROUPS
    table = pa.table({
        "id": pa.array(range(n), pa.int64()),
        "g": pa.array([i % 7 for i in range(n)], pa.int32()),
        "d": pa.array([Decimal(i - 1000) / 100 for i in range(n)], pa.decimal128(18, 2)),
        "s": pa.array([None if i % 11 == 0 else f"s{i % 97}" for i in range(n)]),
    })
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP, compression="zstd")
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
