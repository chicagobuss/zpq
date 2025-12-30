import pyarrow.parquet as pq
import sys

path = sys.argv[1]
table = pq.ParquetFile(path)
meta = table.metadata
for i in range(meta.num_row_groups):
    rg = meta.row_group(i)
    print(f"Row Group {i}:")
    for j in range(rg.num_columns):
        col = rg.column(j)
        print(f"  Column {j} statistics: {col.statistics}")

