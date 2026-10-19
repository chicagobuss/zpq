"""F3 (P2): a bare BOOLEAN column as a predicate (`WHERE b`, `WHERE NOT b`, `CASE WHEN b`) is refused,
"invalid operator in filter" (exit 2); `WHERE b = true` answers.

    .venv/bin/python -I tests/handoff/repros/f3_bare_boolean_predicate.py DIR
"""
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import cli  # noqa: E402

d = sys.argv[1]
src = os.path.join(d, "bool.parquet")
pq.write_table(pa.table({"b": [True, False, True]}), src)
for where in ("b", "NOT b", "b = true"):
    print(f"WHERE {where:8}", cli("query", "--query", f"SELECT count(*) AS n FROM '{src}' WHERE {where}"))
print("CASE WHEN b  ", cli("query", "--query", f"SELECT CASE WHEN b THEN 1 ELSE 0 END AS c FROM '{src}'"))
