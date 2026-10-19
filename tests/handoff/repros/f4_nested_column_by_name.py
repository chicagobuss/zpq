"""F4 (P2): SQL cannot name a STRUCT or LIST column by its own name: `SELECT st` and `SELECT l` answer
"unknown column referenced in expression" (exit 2). Leaves work (`st.a`), `--columns st` resolves to the leaves, and a
copy keeps both; for a LIST the documented refusal ("column ... is repeated") is what `SELECT *` gives.

    .venv/bin/python -I tests/handoff/repros/f4_nested_column_by_name.py DIR
"""
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import cli  # noqa: E402

d = sys.argv[1]
src = os.path.join(d, "nested.parquet")
pq.write_table(pa.table({"id": [1, 2], "st": [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}], "l": [[1, 2], [3]]}), src)
for sel in ("st", "st.a", "l", "*"):
    print(f"SELECT {sel:5}", cli("query", "--query", f"SELECT {sel} FROM '{src}'"))
print("--columns st", cli("query", src, "--columns", "st"))
