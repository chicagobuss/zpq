#!/usr/bin/env python3
"""Generate the fixture for output naming of a nested leaf whose dotted path a top-level column takes.

Columns (three rows, one row group):
  a.b  int64             a top-level column literally named `a.b`: 1, null, 3
  a    struct<b: int64>  its field b dot-joins to `a.b` too: 10, 20, null

Flat output prints the top-level column as `a.b` and the nested field as `"a"."b"`, the quoted path that binds back
to it. Used by the scan unit tests and tests/lambda_integration.zig.

Usage:  .venv/bin/python tools/gen_dotted_twin_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "dotted_twin.parquet"


def main() -> None:
    table = pa.table({
        "a.b": pa.array([1, None, 3], pa.int64()),
        "a": pa.array([{"b": 10}, {"b": 20}, {"b": None}], pa.struct([("b", pa.int64())])),
    })
    pq.write_table(table, OUT, compression="NONE")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
