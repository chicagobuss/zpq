#!/usr/bin/env python3
"""Indexed dictionary keys with sorted nullable predicates and multiple pages per row group."""
import sys
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

path = Path(sys.argv[1] if len(sys.argv) > 1 else "ci/fixtures/parquet/filtered_records_indexed.parquet")
rg_rows = 1281
r = np.arange(16 * rg_rows, dtype=np.int64)
rg = r // rg_rows
k = (r + rg * 17) % 257
k = np.where(k == 0, np.iinfo(np.int64).min,
             np.where(k == 1, np.iinfo(np.int64).max, k * (1 << 45) - (1 << 54)))
words = np.array(["", "a\x00b", "last"] + [f"g{i:03d}" for i in range(13)], dtype=object)
b = words[(r + rg) % len(words)]
f = (r % rg_rows).astype(np.int32)
table = pa.table({
    "k": pa.array(k, mask=r % 17 == 0),
    "b": pa.array(b, mask=r % 19 == 0),
    "f": pa.array(f, mask=r % 23 == 0),
})
pq.write_table(table, path, row_group_size=rg_rows, compression="NONE", write_page_index=True,
               data_page_size=128, write_batch_size=32)
assert all(pq.ParquetFile(path).metadata.row_group(g).column(c).has_offset_index
           for g in range(16) for c in range(3))
assert pq.read_table(path).to_pylist() == table.to_pylist()
expected = list(zip(*(table.column(i).to_pylist() for i in range(3))))
with duckdb.connect() as con:
    actual = con.execute("SELECT k,b,f FROM read_parquet(?,file_row_number=true) ORDER BY file_row_number",
                         [str(path)]).fetchall()
assert expected == actual
print(path)
