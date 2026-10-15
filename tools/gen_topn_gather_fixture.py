#!/usr/bin/env python3
"""Generate the fixtures for the parallel plain-row Top-N and its winner gather (src/engine.zig, src/core/consumer.zig).

Two files of 3000 rows each, row groups of 1000 rows, pages of about 40 rows, so a gathered winner sits among pages
the reader passes over. `topn_gather_a.parquet` writes V1 data pages, `topn_gather_b.parquet` V2. The string column's
dictionary outgrows its page part-way through each chunk, so a chunk turns from dictionary to PLAIN pages.

Columns (i is the row's index in its file):
  id   int64, i + 10000 * file, never null
  k    int32, nullable: i // 37 % 50 (ties within and across row groups); NULL on every eleventh row
  s    string, nullable: 's' + (i * 7919 % 997) padded; NULL on every thirteenth row
  u    uint32: values past 2^31 on odd rows, so signed order would differ
  f    double, nullable: NaN on every 17th row, NULL on every 19th
  dec  decimal(9,2): negative on every third row
  ts   timestamp[ms]
  c    string, nullable, dictionary-encoded throughout: '' on every fifth row, NULL on every seventh, else
       'c' + two digits from a range of ten that rises with the row group (so its footer minimum is '' and proves
       nothing, while its dictionary bounds the group once '' is filtered out)

Usage:  .venv/bin/python tools/gen_topn_gather_fixture.py
"""

import decimal
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 3000
DIR = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet"


def table(file: int) -> pa.Table:
    ids = range(ROWS)
    return pa.table({
        "id": pa.array([i + 10000 * file for i in ids], type=pa.int64()),
        "k": pa.array([None if i % 11 == 0 else (i // 37) % 50 for i in ids], type=pa.int32()),
        "s": pa.array([None if i % 13 == 0 else f"s{(i * 7919) % 997:04d}-{'x' * (i % 5)}" for i in ids]),
        "u": pa.array([(i * 2654435761) % (1 << 32) if i % 2 else i for i in ids], type=pa.uint32()),
        "f": pa.array([None if i % 19 == 0 else float("nan") if i % 17 == 0 else ((i * 31) % 211) / 7.0
                       for i in ids], type=pa.float64()),
        "dec": pa.array([decimal.Decimal((i * 13) % 900 - (600 if i % 3 == 0 else 0)) / 100 for i in ids],
                        type=pa.decimal128(9, 2)),
        "ts": pa.array([1_600_000_000_000 + ((i * 104729) % 5000) * 1000 for i in ids], type=pa.timestamp("ms")),
        "c": pa.array([None if i % 7 == 0 else "" if i % 5 == 0
                       else f"c{(i // 1000) * 10 + (i * 31) % 10 + 30 * file:02d}" for i in ids]),
    })


def main() -> None:
    for file, (name, version) in enumerate((("a", "1.0"), ("b", "2.0"))):
        pq.write_table(table(file), DIR / f"topn_gather_{name}.parquet", row_group_size=1000, data_page_size=256,
                       write_batch_size=40, dictionary_pagesize_limit=2048, data_page_version=version,
                       compression="snappy", write_statistics=True)


if __name__ == "__main__":
    main()
