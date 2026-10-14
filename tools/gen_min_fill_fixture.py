#!/usr/bin/env python3
"""Generate the fixture for the page min-fill tests in src/core/scan.zig.

One row group of 1000 rows in pages of 50, with a page index, PLAIN and uncompressed so every page carries exact
statistics. `c` alternates 1, 2 and has no nulls, so `c IS NOT NULL` (and, trusting statistics, `c >= 0`) is proven
on every page; a reader that fills such a page with its minimum instead of decoding it loses every `c = 2` row of
`c IS NOT NULL AND (c = 2 OR b = 1)`.

Columns:
  c  int64, 1 + i % 2, no nulls     the column the filter proves and also reads under an OR
  b  int64, 1 on every seventh row  the OR's other operand
  x  int64, i                       the aggregate payload

Usage:  .venv/bin/python tools/gen_min_fill_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 1000
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "min_fill_or.parquet"


def main() -> None:
    i = list(range(ROWS))
    table = pa.table(
        {
            "c": pa.array([1 + v % 2 for v in i], pa.int64()),
            "b": pa.array([1 if v % 7 == 0 else 0 for v in i], pa.int64()),
            "x": pa.array(i, pa.int64()),
        }
    )
    pq.write_table(
        table,
        OUT,
        row_group_size=ROWS,
        data_page_size=1,
        write_batch_size=50,
        write_page_index=True,
        use_dictionary=False,
        compression="NONE",
    )


if __name__ == "__main__":
    main()
