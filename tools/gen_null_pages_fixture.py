#!/usr/bin/env python3
"""Generate the fixture for the all-null page tests in src/core/scan.zig.

2000 rows in two row groups, written with a page index and small pages so the ColumnIndex flags whole pages of `v`
and `s.b` all-null. Rows 500..999 of `v` are null, plus every tenth row elsewhere; `s` is null on every seventh row
and on rows 1400..1599; `s.b` equals `v`.

Columns:
  id  int64, 0..1999, never null
  v   int64, nullable (= id where present)
  s   struct<a: int32, b: int64>, nullable
  l   list<int64>, null on every fifth row

Usage:  .venv/bin/python tools/gen_null_pages_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 2000
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "null_pages.parquet"


def v_of(i: int) -> int | None:
    return None if 500 <= i < 1000 or i % 10 == 0 else i


def main() -> None:
    ids = list(range(ROWS))
    v = pa.array([v_of(i) for i in ids], type=pa.int64())
    s_null = pa.array([i % 7 == 0 or 1400 <= i < 1600 for i in ids])
    s = pa.StructArray.from_arrays([pa.array(ids, type=pa.int32()), v], names=["a", "b"], mask=s_null)
    l = pa.array([None if i % 5 == 0 else [i] * (i % 3) for i in ids], type=pa.list_(pa.int64()))
    table = pa.table({"id": pa.array(ids, type=pa.int64()), "v": v, "s": s, "l": l})
    pq.write_table(table, OUT, write_page_index=True, data_page_size=512, write_batch_size=100, use_dictionary=True,
                   row_group_size=1000, compression="snappy")


if __name__ == "__main__":
    main()
