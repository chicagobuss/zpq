#!/usr/bin/env python3
"""Indexed, nullable dictionary tuples for default-stats packed COUNT regression tests."""
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

path = Path(sys.argv[1] if len(sys.argv) > 1 else "ci/fixtures/parquet/packed_count_indexed.parquet")
rg_rows = 1281
r = np.arange(16 * rg_rows, dtype=np.int64)
rg = r // rg_rows
k = (r + rg * 17) % 257
k = np.where(k == 0, np.iinfo(np.int64).min,
             np.where(k == 1, np.iinfo(np.int64).max, k * (1 << 45) - (1 << 54)))
b = np.array(["", "a\x00b", "last"], dtype=object)[(r + rg) % 3]
table = pa.table({
    "k": pa.array(k, mask=r % 17 == 0),
    "b": pa.array(b, mask=r % 19 == 0),
    "f": pa.array(((r * 3 + rg) % 263 - 131).astype(np.int32), mask=r % 23 == 0),
})
pq.write_table(table, path, row_group_size=rg_rows, compression="NONE", write_page_index=True,
               data_page_size=512, write_batch_size=128)
assert all(pq.ParquetFile(path).metadata.row_group(g).column(c).has_offset_index
           for g in range(16) for c in range(3))
print(path)
