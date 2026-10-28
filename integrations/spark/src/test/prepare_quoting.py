"""A tiny Parquet input whose schema and path must stay data through the Spark readers."""
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
path = root / "input 'double\"; 雪 \\ file.parquet"
names = ["id", 'quote"name', "apostrophe'name", "semi; SELECT 1 --", "space name", "雪😀", "back\\slash", "tick`name"]
columns = [pa.array(range(8), type=pa.int64())]
for i in range(1, len(names)):
    columns.append(pa.array([None if r == 2 else 100 * i + r for r in range(8)], type=pa.int64()))
pq.write_table(pa.Table.from_arrays(columns, names=names), path, row_group_size=2)
# Spark's top-level glob parser treats a backslash as an escape; directory discovery still opens this exact file.
print(root)
