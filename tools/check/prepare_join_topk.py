"""The join top-K's facts with no statistics: 24,000 rows sorted by `id`, in row groups of 2,048, whose chunks carry no
min or max, so no row group can be ruled out by them. Argument: the file to write."""
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    n = range(24000)
    sku = ["S%03d" % (1 + (i % 8) + (i % 8) // 4) for i in n]
    region = ["nesw"[i % 4] for i in n]
    table = pa.table({"id": pa.array(list(n), type=pa.int64()), "k": pa.array([i // 50 for i in n], type=pa.int64()),
                      "sku": sku, "region": region})
    pq.write_table(table, pathlib.Path(sys.argv[1]), row_group_size=2048, write_statistics=False)


if __name__ == "__main__":
    main()
