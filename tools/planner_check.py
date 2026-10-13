#!/usr/bin/env python3
"""Check planner decisions and partitioned answers against DuckDB, without measuring performance."""
import json
import os
from pathlib import Path
import struct
import subprocess
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

zpq, directory = sys.argv[1:]
path = Path(directory) / "planner.parquet"
n = 80_000
keys = [None if i % 97 == 0 else "https://example.org/" + "shared-prefix/" * 8 + str(i % 12_001)
        for i in range(n)]
pq.write_table(pa.table({"k": keys, "v": [(i % 17) - 8 for i in range(n)], "pos": range(n)}),
               path, row_group_size=10_000, use_dictionary=False)
con = duckdb.connect()
env = dict(os.environ, ZPQ_IO_PROFILE="mmap", ZPQ_SKETCH_TOPK="1")

def run(sql, workers=4, explain=False, extra=(), overrides=None):
    command = [zpq, "query", "--query", sql, "-j", str(workers), "--max-memory", "512MB", *extra]
    if explain:
        command.append("--explain")
    result = subprocess.run(command, env=env | (overrides or {}), capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


queries = [
    "SELECT k, count(*) AS c, sum(v) AS s, avg(v) AS av, min(k) AS lo, max(k) AS hi "
    "FROM '{path}' GROUP BY k ORDER BY c DESC LIMIT 10",
    "SELECT k, count(v) FILTER (WHERE v > 0) AS c, sum(v) AS s "
    "FROM '{path}' WHERE v >= -4 GROUP BY k ORDER BY c DESC LIMIT 4 OFFSET 2",
    "SELECT k, count(*) AS c FROM '{path}' GROUP BY k ORDER BY c DESC, k ASC LIMIT 10",
    "SELECT k, count(*) AS c, sum(v) AS s FROM '{path}' GROUP BY k",
]
for template in queries:
    sql = template.format(path=path)
    plan = run(sql, explain=True)
    assert plan["strategy"] == "partitioned_hash", plan
    assert plan["ndv"]["source"] == "unknown" and not plan["sketch"]["census_allowed"], plan
    truth_sql = sql
    if "ORDER BY c DESC," not in sql:
        truth_sql = (sql.replace("ORDER BY c DESC", "ORDER BY c DESC, min(pos) ASC")
                     if "ORDER BY" in sql else sql + " ORDER BY min(pos) ASC")
    cursor = con.execute(truth_sql)
    names = [column[0] for column in cursor.description]
    truth = [dict(zip(names, row)) for row in cursor.fetchall()]
    for workers in (1, 3, 8):
        for encoded in ("0", "1"):
            result = run(sql, workers, overrides={"ZPQ_ENCODED": encoded})
            for row in result["agg"]:
                if isinstance(row.get("av"), dict):
                    avg = row["av"]
                    row["av"] = avg["sum"] / avg["count"] if avg["count"] else None
            assert result["agg"] == truth, (workers, encoded, sql, result["agg"][:3], truth[:3])

# Dictionary statistics must never need a data page. Preserve dictionaries, poison all data-page spans.
bounded = Path(directory) / "planner_bounded.parquet"
pq.write_table(pa.table({"k": [i % 100 for i in range(n)], "v": range(n)}),
               bounded, row_group_size=10_000)
metadata = pq.read_metadata(bounded)
b = bytearray(bounded.read_bytes())
footer = len(b) - 8 - struct.unpack_from("<I", b, len(b) - 8)[0]
for rg in range(metadata.num_row_groups):
    for ci in range(metadata.num_columns):
        col = metadata.row_group(rg).column(ci)
        start = col.data_page_offset
        end = min(footer, col.dictionary_page_offset + col.total_compressed_size)
        b[start:end] = bytes(end - start)
bounded.write_bytes(b)
plan = run(f"SELECT k, count(*) AS c FROM '{bounded}' GROUP BY k ORDER BY c DESC LIMIT 10", explain=True)
assert plan["strategy"] == "plain_hash" and "small_ndv" in plan["reasons"], plan
assert plan["ndv"]["hi"] == 100 and plan["ndv"]["union_complete"], plan
assert len(plan["passes"]) == 1 and plan["count_agg"] is None, plan
plan = run(f"SELECT k, count(*) AS c FROM '{bounded}' GROUP BY k LIMIT 10", explain=True)
assert plan["strategy"] == "plain_hash" and "small_ndv" in plan["reasons"], plan

# Unknown DISTINCT state must not be admitted by the small-group memory estimate.
plan = run(f"SELECT k, count(DISTINCT v) AS c FROM '{bounded}' GROUP BY k", explain=True)
assert plan["plain_bytes_estimate"] is None and "state_memory_unknown" in plan["reasons"], plan
print("planner check: bounded NDV, no data-page reads, and partitioned answers agree with DuckDB")
