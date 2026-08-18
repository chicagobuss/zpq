#!/usr/bin/env python3
"""Generate the fixture for group-key framing tests.

The relevant nested-schema shape is cheap to reproduce: a MAP column appearing
BEFORE ordinary scalar columns.

`col_idx` counts primitive leaves, but `meta.schema` is a DFS that also holds
the group nodes a MAP introduces (the map node itself and its `key_value`
node). So from the first MAP onward, leaf numbering and DFS numbering drift,
and any code indexing the schema by leaf index reads the wrong element. A
TIMESTAMP column resolving to a STRING element made key deserialization read
the timestamp's own bytes as a length.

The scalar columns after the map cover the framing lanes that can disagree:

  ts    INT64/TIMESTAMP  -> i64 lane   (the column that crashed)
  flag  BOOLEAN          -> i64 lane, though the physical type says boolean;
                           a composite key with this first used to truncate
                           every later column
  name  STRING           -> string lane, the one that got truncated
  ratio DOUBLE           -> f64 lane

Usage:  .venv/bin/python tools/gen_nested_key_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 60
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "nested_key_shape.parquet"


def main() -> None:
    tags = [[("k", f"v{i % 3}")] for i in range(ROWS)]
    table = pa.table(
        {
            # The MAP must come first: everything after it is what shifts.
            "tags": pa.array(tags, type=pa.map_(pa.string(), pa.string())),
            "ts": pa.array(
                [1_750_000_000_000_000 + (i % 5) * 1_000_000 for i in range(ROWS)],
                type=pa.timestamp("us"),
            ),
            "flag": pa.array([i % 2 == 0 for i in range(ROWS)], type=pa.bool_()),
            # Independent of `flag`, so a composite (flag, name) key really
            # has 2 x 4 groups; keying both off i % 2 would collapse it to 4 and
            # a truncated string half would still look plausible.
            "name": pa.array([f"name{(i // 2) % 4}" for i in range(ROWS)], type=pa.string()),
            "ratio": pa.array([(i % 3) + 0.5 for i in range(ROWS)], type=pa.float64()),
        }
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, OUT, compression="snappy")
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
