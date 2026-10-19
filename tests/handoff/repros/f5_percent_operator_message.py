"""F5 (P3): `%` is not an operator zpq has, but a filter using it says "unknown column referenced in expression"
and a SELECT "unexpected character in expression", so the message points at a column, not at the operator.

    .venv/bin/python -I tests/handoff/repros/f5_percent_operator_message.py DIR
"""
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import cli  # noqa: E402

d = sys.argv[1]
src = os.path.join(d, "ints.parquet")
pq.write_table(pa.table({"id": [1, 2, 3]}), src)
print("WHERE ", cli("query", "--query", f"SELECT id FROM '{src}' WHERE id % 2 = 1"))
print("SELECT", cli("query", "--query", f"SELECT id % 2 AS p FROM '{src}'"))
