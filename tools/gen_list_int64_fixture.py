#!/usr/bin/env python3
"""Generate the LIST fixture for the repeated-leaf refusal tests in src/core/scan.zig.

Six rows: `id` int64 0..5, and `l` LIST<INT64> holding a two-element list, an empty list, a NULL list, a list of one
NULL, one element and three elements: every repetition/definition shape a list leaf takes.

Usage:  .venv/bin/python tools/gen_list_int64_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "list_int64.parquet"

if __name__ == "__main__":
    t = pa.table({"id": pa.array(range(6), pa.int64()),
                  "l": pa.array([[1, 2], [], None, [None], [3], [4, 5, 6]], pa.list_(pa.int64()))})
    pq.write_table(t, OUT)
