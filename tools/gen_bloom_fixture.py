#!/usr/bin/env python3
"""Generate ci/fixtures/parquet/bloom_keys.parquet, the fixture for row-group pruning by membership (Bloom filters
and dictionaries) in the unit and Lambda integration tests.

Eight row groups of 500 rows, laid out so statistics prune no equality: every group's bounds span the whole domain.

Columns:
  id   int64, even numbers 0..7998; group g holds 2*(8j+g)    Bloom filter; odd numbers are absent everywhere
  s    utf8, "key-{id:06d}"                                  Bloom filter
  cat  utf8, group g holds "a-{g}", "m-{g}", "z-{g}"           dictionary-encoded, no Bloom filter
  x    double, id / 2                                         the aggregate payload

Usage:  .venv/bin/python tools/gen_bloom_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

ROWS_PER_GROUP = 500
GROUPS = 8
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "bloom_keys.parquet"


def main() -> None:
    ids, cats = [], []
    for g in range(GROUPS):
        for j in range(ROWS_PER_GROUP):
            ids.append(2 * (8 * j + g))
            cats.append(("a", "m", "z")[j % 3] + f"-{g}")
    table = pa.table({
        "id": pa.array(ids, pa.int64()),
        "s": pa.array([f"key-{i:06d}" for i in ids], pa.string()),
        "cat": pa.array(cats, pa.string()),
        "x": pa.array([i / 2 for i in ids], pa.float64()),
    })
    pq.write_table(
        table,
        OUT,
        row_group_size=ROWS_PER_GROUP,
        compression="snappy",
        use_dictionary=["cat"],
        bloom_filter_options={c: {"ndv": ROWS_PER_GROUP, "fpp": 0.01} for c in ("id", "s")},
    )
    md = pq.ParquetFile(OUT).metadata
    assert md.num_row_groups == GROUPS
    for g in range(GROUPS):
        rg = md.row_group(g)
        assert rg.column(0).bloom_filter_offset and rg.column(1).bloom_filter_offset
        assert not rg.column(2).bloom_filter_offset
    print(OUT, OUT.stat().st_size, "bytes")


if __name__ == "__main__":
    main()
