#!/usr/bin/env python3
"""Clustered integer COUNT keys with NULLs, ties and late out-of-range secondary values."""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    rows = 32768
    k = [i - rows for i in range(rows // 4)]
    k.extend(100 + (i % 3) for i in range(rows - len(k)))
    f = [(i % 5) - 2 for i in range(rows)]
    for i in range(0, rows, 137):
        k[i] = None
    for i in range(0, rows, 3):
        f[i] = None
    # These rows need cold bounds even if a sample learned only the small central f interval.
    f[-1], f[-2] = -(1 << 63), (1 << 63) - 1
    table = pa.table({"k": pa.array(k, pa.int64()), "f": pa.array(f, pa.int64())})
    root = Path(__file__).resolve().parent.parent / "ci/fixtures/parquet"
    for coded in (False, True):
        path = root / f"count_certificate_{'dict' if coded else 'plain'}.parquet"
        pq.write_table(table, path, row_group_size=2048, data_page_size=1024,
                       use_dictionary=coded, dictionary_pagesize_limit=1024, compression="snappy")
        print(path)


if __name__ == "__main__":
    main()
