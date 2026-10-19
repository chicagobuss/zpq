"""F2 (P2): `-o` of min/max over a UINT64 column holding 2^63 or more is refused with `AggIntTooWide` (exit 1).

    .venv/bin/python -I tests/handoff/repros/f2_uint64_minmax_write.py DIR
The JSON answer is right ({"m":18446744073709551615}); min/max keep the column's type (README), so the write could
emit UINT64. The same request through python/zpq.py with `output_url` is refused the same way.
"""
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import cli  # noqa: E402

d = sys.argv[1]
src = os.path.join(d, "u64.parquet")
out = os.path.join(d, "u64_max.parquet")
pq.write_table(pa.table({"u": pa.array([1, (1 << 64) - 1], pa.uint64())}), src)
print("json ", cli("query", "--query", f"SELECT max(u) AS m FROM '{src}'"))
print("write", cli("query", "--query", f"SELECT max(u) AS m FROM '{src}'", "-o", out))
