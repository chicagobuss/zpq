#!/usr/bin/env python3
"""GROUP BY dependency rewrite: exact bytes/errors under legacy/rewritten paths, workers and budgets.

Usage: group_dependency_check.py CANDIDATE BASE. Requires pyarrow and duckdb.
"""
import decimal
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def run(binary, sql, j, budget, enabled, extra=(), rows="0"):
    env = dict(os.environ, ZPQ_GROUP_KEY_FD=str(enabled), ZPQ_DECODE_BATCH_ROWS=rows)
    return subprocess.run([binary, "query", "--query", sql, "-j", str(j), "--max-memory", budget, *extra],
                          env=env, capture_output=True, timeout=60)


def exact(want, got, context):
    assert (got.returncode, got.stdout, got.stderr) == (want.returncode, want.stdout, want.stderr), (
        context, want.returncode, want.stdout, want.stderr, got.returncode, got.stdout, got.stderr)


def main():
    candidate, base = sys.argv[1:3]
    checks = 0
    with tempfile.TemporaryDirectory(prefix="zpq-group-dependency-") as temp:
        root = Path(temp)
        ks = [-(2**31), -2, -1, 0, 1, 2, 2**31 - 1, None]
        us = [0, 1, 2**31 - 1, 2**31, 2**32 - 1, None]
        n = 2048
        table = pa.table({
            "k": pa.array([ks[i % len(ks)] for i in range(n)], type=pa.int32()),
            "b": pa.array([None if i % 19 == 0 else i % 3 - 1 for i in range(n)], type=pa.int32()),
            "u": pa.array([us[i % len(us)] for i in range(n)], type=pa.uint32()),
            "w": pa.array([-(2**63) if i == 0 else 2**63 - 1 if i == 1 else i % 3 for i in range(n)],
                          type=pa.int64()),
            "flag": pa.array([None if i % 5 == 0 else bool(i % 2) for i in range(n)], type=pa.bool_()),
            "f": pa.array([[1e20, 1.0, -1e20, -0.0, 1e-20, 3.0][i % 6] for i in range(n)], type=pa.float64()),
            "d": pa.array([decimal.Decimal(i % 7) / 100 for i in range(n)], type=pa.decimal128(9, 2)),
            "s": pa.array(["a" if i % 2 else "b" for i in range(n)], type=pa.string()),
        })
        queries = [
            "SELECT k,k-1,k-2,k-3,count(*) AS c FROM '{p}' GROUP BY k,k-1,k-2,k-3 ORDER BY c DESC LIMIT 10",
            "SELECT k,k*k AS square,k*0 AS zero,count(*) AS c FROM '{p}' GROUP BY k,k*k,k*0",
            "SELECT b+k AS both,b,k,count(*) AS c FROM '{p}' GROUP BY b+k,b,k ORDER BY both DESC",
            "SELECT b,k-1 AS p,k,k+b AS plus,count(*) AS c FROM '{p}' GROUP BY b,k-1,k,k+b ORDER BY c DESC",
            "SELECT k-1 AS p,count(*) AS c FROM '{p}' GROUP BY k,k-1 ORDER BY p LIMIT 4 OFFSET 2",
            "SELECT k-1 AS p,count(*) AS c FROM '{p}' GROUP BY k,k-1 HAVING p < 0 ORDER BY c DESC LIMIT 4",
            "SELECT u,u-1,u+1,u*2,count(*) AS c FROM '{p}' GROUP BY u,u-1,u+1,u*2",
            "SELECT k,k-1,sum(f) AS s,avg(f) AS a,min(f) AS m FROM '{p}' GROUP BY k,k-1",
            "SELECT k,k-1,count(DISTINCT b) AS c FROM '{p}' GROUP BY k,k-1 ORDER BY c DESC LIMIT 3",
            "SELECT k,count(*) AS c FROM '{p}' GROUP BY k,k-1 ORDER BY k-1 LIMIT 5",
            "SELECT k,k*k,coalesce(k,0),count(*) AS c FROM '{p}' GROUP BY k,k*k,coalesce(k,0)",
            "SELECT k*k AS square,count(*) AS c FROM '{p}' GROUP BY k*k",
            "SELECT d,d+1,count(*) AS c FROM '{p}' GROUP BY d,d+1",
            "SELECT f,f+1,count(*) AS c FROM '{p}' GROUP BY f,f+1",
            "SELECT k,b,1-k,k-(b-1),count(*) AS c FROM '{p}' GROUP BY k,b,1-k,k-(b-1)",
            "SELECT flag,flag-1,count(*) AS c FROM '{p}' GROUP BY flag,flag-1",
            "SELECT w,w*0,count(*) AS c FROM '{p}' GROUP BY w,w*0",
        ]
        faults = [
            "SELECT w,w-1,count(*) AS c FROM '{p}' GROUP BY w,w-1 ORDER BY c DESC LIMIT 1",
            "SELECT w,w*2,k,k-1,count(*) AS c FROM '{p}' GROUP BY w,w*2,k,k-1 LIMIT 1",
            "SELECT k,k-1,w,w-1,count(*) AS c FROM '{p}' GROUP BY k,k-1,w,w-1",
            "SELECT w,(w+1)-1,count(*) AS c FROM '{p}' GROUP BY w,(w+1)-1 ORDER BY c DESC LIMIT 1",
        ]
        con = duckdb.connect()
        for version in ["1.0", "2.0"]:
            path = root / "input.parquet"
            pq.write_table(table, path, row_group_size=128, write_batch_size=64, data_page_size=256,
                           data_page_version=version, use_dictionary=["k", "u", "s"], write_page_index=True)
            for query in queries:
                sql = query.format(p=path)
                want = run(base, sql, 1, "16M", 0)
                assert want.returncode == 0, (sql, want.stderr)
                for j in range(1, 17):
                    for budget, enabled, rows in [("1M", 0, "0"), ("16M", 1, "0"), ("1M", 1, "64")]:
                        got = run(candidate, sql, j, budget, enabled, rows=rows)
                        exact(want, got, (version, sql, j, budget, enabled, rows))
                        checks += 1
            for query in faults:
                sql = query.format(p=path)
                want = run(base, sql, 1, "16M", 0)
                assert want.returncode != 0 and b"overflow" in want.stderr.lower(), (sql, want.stderr)
                for j in range(1, 17):
                    for budget, enabled, rows in [("1M", 0, "0"), ("16M", 1, "0"), ("1M", 1, "64")]:
                        exact(want, run(candidate, sql, j, budget, enabled, rows=rows), (version, sql, j, rows))
                        checks += 1
            # Typed output must keep computed columns INT64, including beyond INT32 range and all-NULL groups.
            for qi in [1, 6, 7, 14, 15]:
                sql = queries[qi].format(p=path)
                old = root / "old.parquet"
                want = run(base, sql, 1, "16M", 0, ("-o", str(old)))
                assert want.returncode == 0, want.stderr
                truth = pq.read_table(old)
                assert con.execute(f"SELECT count(*) FROM '{old}'").fetchone()[0] == truth.num_rows
                for j in [1, 3, 16]:
                    for enabled in [0, 1]:
                        output = root / "new.parquet"
                        got = run(candidate, sql, j, "16M", enabled, ("-o", str(output)))
                        exact(want, got, (version, sql, j, enabled))
                        assert output.read_bytes() == old.read_bytes(), (version, qi, j, enabled, "parquet bytes")
                        assert pq.read_table(output).equals(truth, check_metadata=False)
                        assert con.execute(f"SELECT count(*) FROM '{output}'").fetchone()[0] == truth.num_rows
                        checks += 1
            # Both projected columns fail at the same first row. Original projection ranks must survive compaction.
            metadata = pq.ParquetFile(path).metadata
            raw = bytearray(path.read_bytes())
            for ci in [0, 1]:
                cm = metadata.row_group(0).column(ci)
                pos = cm.dictionary_page_offset or cm.data_page_offset
                raw[pos] = 0xAF
            path.write_bytes(raw)
            for projection, keys in [("b+k AS both,count(*) AS c", "b+k,b,k"),
                                     ("count(*) AS c", "k+b,b,k")]:
                sql = f"SELECT {projection} FROM '{path}' GROUP BY {keys}"
                want = run(base, sql, 1, "16M", 0)
                assert want.returncode != 0, want.stdout
                for j in range(1, 17):
                    for enabled in [0, 1]:
                        exact(want, run(candidate, sql, j, "1M", enabled), (version, "corruption", j, enabled))
                        checks += 1
            print(f"dependent group keys: V{version} values, float bits, errors and Parquet bytes passed", flush=True)
    print(f"dependent group keys: {checks} exact checks", flush=True)


if __name__ == "__main__":
    main()
