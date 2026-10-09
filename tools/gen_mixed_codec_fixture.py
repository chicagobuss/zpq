#!/usr/bin/env python3
"""Generate the mixed-codec fixture for the windowed S3 write tests in src/engine.zig.

Six row groups of 8,000 rows whose columns use three codecs, so a recompressing copy rewrites some chunks and copies
others as they are, and every row group but the last lies before the 64 KiB footer tail an S3 read prefetches.

Columns:
  a  int64, row number            SNAPPY
  b  utf8, "s" + row % 5000      ZSTD
  c  double, row / 2             GZIP

Usage:  .venv/bin/python tools/gen_mixed_codec_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

ROWS_PER_GROUP = 8000
GROUPS = 6
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "mixed_codec.parquet"


def main() -> None:
    n = ROWS_PER_GROUP * GROUPS
    table = pa.table({
        "a": pa.array(range(n), pa.int64()),
        "b": pa.array([f"s{i % 5000}" for i in range(n)]),
        "c": pa.array([i * 0.5 for i in range(n)]),
    })
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP, compression={"a": "snappy", "b": "zstd", "c": "gzip"})
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
