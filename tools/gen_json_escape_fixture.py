#!/usr/bin/env python3
"""Generate the fixture for the Lambda JSON-escaping integration test in tests/lambda_integration.zig.

Columns (three rows, one row group):
  s  utf8    a quote, a backslash, a newline and a 0x01 control byte in one value; plus two plain values
  x  double  1.0, NaN, 2.0                      sum(x) is NaN
  y  double  1.0, +inf, 2.0                     max(y) is +Infinity
  b  binary  invalid UTF-8 (0xFF 0xFE, a truncated sequence, a surrogate encoding), plus U+2028

Usage:  .venv/bin/python tools/gen_json_escape_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "json_escape.parquet"


def main() -> None:
    table = pa.table({
        "s": pa.array(['a "q" c:\\d\nnext\x01end', "b", "c"], pa.string()),
        "x": pa.array([1.0, float("nan"), 2.0], pa.float64()),
        "y": pa.array([1.0, float("inf"), 2.0], pa.float64()),
        "b": pa.array([b"\xff\xfeA\xe2\x80", b"\xed\xa0\x80", "\u2028".encode()], pa.binary()),
    })
    pq.write_table(table, OUT, compression="NONE")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
