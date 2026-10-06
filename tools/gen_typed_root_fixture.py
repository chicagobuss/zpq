#!/usr/bin/env python3
"""Generate the typed-root fixture for the schema tests in src/core/parquet/{metadata,column}.zig.

Some writers (segmentio/parquet-go among them) give the schema's root element a physical type and a type_length as
well as its num_children, which the format reserves for leaves. pyarrow writes a plain root, so the footer is
rewritten: the root gets type INT64 and type_length 64 and loses its repetition_type, and every INT64 leaf gets
type_length 64 too, the way such a writer spells them. The data pages are pyarrow's and untouched.

Columns (all REQUIRED, 3 rows, one row group, no dictionary):
  mint   int64   1000, 2000, 3000
  maxt   int64   1999, 2999, 3999
  name   utf8    'a', 'bb', 'ccc'

Usage:  .venv/bin/python tools/gen_typed_root_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

from gen_stats_order_fixtures import T_I32, field, rewrite_footer

OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "typed_root.parquet"
INT64 = 2


def type_the_root(meta: list, _body: bytearray) -> None:
    elements = field(meta, 2)[2][1]
    root = elements[0]
    assert field(root, 5) is not None and field(root, 1) is None, "expected a plain group root"
    root[:] = [f for f in root if f[0] != 3]  # no repetition_type on the root, as such writers leave it
    root += [[1, T_I32, INT64], [2, T_I32, 64]]
    for leaf in elements[1:]:
        if field(leaf, 1)[2] == INT64 and field(leaf, 2) is None:
            leaf.append([2, T_I32, 64])


def main() -> None:
    schema = pa.schema([
        pa.field("mint", pa.int64(), nullable=False),
        pa.field("maxt", pa.int64(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
    ])
    table = pa.table({"mint": [1000, 2000, 3000], "maxt": [1999, 2999, 3999], "name": ["a", "bb", "ccc"]}, schema=schema)
    pq.write_table(table, OUT, compression="snappy", use_dictionary=False)
    rewrite_footer(OUT, type_the_root)
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
