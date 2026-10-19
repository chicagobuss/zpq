"""F1 (P1): a BOOLEAN GROUP BY key, and min/max of a BOOLEAN, print as 0/1 in JSON, where rows print true/false.

    .venv/bin/python -I tests/handoff/repros/f1_bool_key_prints_integer.py DIR
Expected: {"b":false,"n":1} / {"b":true,"n":2} and {"mn":false,"mx":true}, as rows print and as the Parquet write
holds (BOOLEAN). Got 0 / 1 from the CLI, Python's query_raw and the structured request's `agg`. 0.4.1 did the same.
"""
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import cli  # noqa: E402
import zpq  # noqa: E402

d = sys.argv[1]
src = os.path.join(d, "bool.parquet")
pq.write_table(pa.table({"b": [True, False, True]}), src)
print("rows     ", cli("query", "--query", f"SELECT b FROM '{src}' LIMIT 1")[1])
print("group by ", cli("query", "--query", f"SELECT b, count(*) AS n FROM '{src}' GROUP BY b")[1].replace("\n", " "))
print("min/max  ", cli("query", "--query", f"SELECT min(b) AS mn, max(b) AS mx FROM '{src}'")[1])
with zpq.Zpq() as z:
    print("python   ", z.query_raw(f"SELECT b, count(*) AS n FROM '{src}' GROUP BY b").replace("\n", " "))
    print("agg JSON ", json.dumps(z.run({"inputs": [src], "group_by": "b", "aggregate": "count(*) AS n"})["agg"]))
