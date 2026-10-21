"""Exact SQL ABI/CLI parity, persistent-session RSS and concurrent diagnostics.

Run from the repository root. ZPQ_CLICKBENCH and ZPQ_CLICKBENCH_QUERIES enable the real ClickBench slice.
The release gate requires both, and runs 50 shapes at j=1/3/16. No timing claims are made here.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
import zpq
import duckdb
import pyarrow.parquet as pq

CLI = os.environ.get("ZPQ_CLI", "zig-out/bin/zpq")
STUDY = [
    "SELECT count(*) AS n FROM {t}",
    "SELECT sum(v) AS s, avg(v) AS a, min(v) AS lo, max(v) AS hi FROM {t}",
    "SELECT count(DISTINCT k) AS n FROM {t}",
    "SELECT count(DISTINCT name) AS n FROM {t}",
    "SELECT k, count(*) AS n FROM {t} GROUP BY k ORDER BY n DESC LIMIT 5",
    "SELECT k, sum(v) AS s FROM {t} GROUP BY k ORDER BY s DESC LIMIT 5 OFFSET 2",
    "SELECT k, count(*) AS n FROM {t} GROUP BY k HAVING count(*) > 20 ORDER BY k",
    "SELECT k, sum(v) AS s FROM {t} GROUP BY k HAVING sum(v) > 0 ORDER BY s DESC LIMIT 9",
    "SELECT k + 1 AS key, count(*) AS n FROM {t} GROUP BY k + 1 ORDER BY key LIMIT 7",
    "SELECT k + 1 AS key, count(*) AS n FROM {t} GROUP BY 1 ORDER BY key LIMIT 7",
    "SELECT CASE WHEN v < 0 THEN -1 ELSE 1 END AS sign, count(*) AS n FROM {t} GROUP BY sign",
    "SELECT sum(CASE WHEN v < 0 THEN v ELSE 0 END) AS s FROM {t}",
    "SELECT name, count(DISTINCT k) AS n FROM {t} GROUP BY name ORDER BY n DESC, name LIMIT 5",
    "SELECT k, name, count(*) AS n FROM {t} GROUP BY k, name LIMIT 8",
    "SELECT k, sum(v) AS s FROM {t} GROUP BY k ORDER BY s DESC LIMIT 0",
    "SELECT count(*) AS n FROM {t} LIMIT 0",
    "SELECT count(*) AS n FROM {t} LIMIT 1 OFFSET 1",
    "SELECT id, name FROM {t} LIMIT 3 OFFSET 2",
    "SELECT id, name FROM {t} ORDER BY v DESC, id LIMIT 9 OFFSET 2",
    "SELECT id FROM {t} WHERE id < 10 ORDER BY v + 1 DESC",
    "SELECT CASE WHEN v < 0 THEN 'negative' ELSE 'positive' END AS sign FROM {t} LIMIT 7",
    "SELECT count(*) AS n FROM {t} WHERE NOT (k IN (1, 2) OR name IS NULL)",
    "SELECT count(*) AS n FROM {t} WHERE name NOT IN ('n1', NULL)",
    "SELECT k, count(*) AS n FROM {t} WHERE k BETWEEN 2 AND 9 GROUP BY k ORDER BY k DESC LIMIT 3",
    "SELECT sum(money) AS s FROM {t}",
    "SELECT avg(money) AS a, avg_state(money) AS st, avg_state(v) AS sv FROM {t}",
    "SELECT k, avg(money) AS a, avg_state(v) AS s FROM {t} GROUP BY k ORDER BY a DESC LIMIT 5",
    "SELECT k, sum(money) AS s FROM {t} GROUP BY k ORDER BY s DESC LIMIT 7",
    "SELECT id AS quoted, v FROM {t} WHERE id < 7 ORDER BY 1 DESC",
    "SELECT id FROM {t} WHERE id < 0",
    "SELECT id, name FROM {t} LIMIT 0",
    "SELECT count(*) AS n FROM {t} WHERE name LIKE 'n1%'",
]
CB_IDS = list(range(18)) + [19, 27]


def cli(sql, j):
    p = subprocess.run([CLI, "query", "--query", sql, "-j", str(j), "--max-memory", "1GB",
                        "--summary=json"], capture_output=True, timeout=120)
    return p


def rss():
    return int(Path('/proc/self/statm').read_text().split()[1]) * os.sysconf('SC_PAGESIZE') // 1024


def main():
    with tempfile.TemporaryDirectory(prefix="zpq-sql-") as d:
        path = str(Path(d) / 'study.parquet')
        con = duckdb.connect()
        con.execute("""COPY (SELECT i AS id, i % 31 AS k, (i % 101 - 50)::DOUBLE / 4 AS v,
            CASE WHEN i % 11 = 0 THEN NULL ELSE 'n' || (i % 17)::VARCHAR END AS name,
            (i % 503 - 250)::DECIMAL(12,2) AS money FROM range(4000) t(i))
            TO ? (FORMAT PARQUET, ROW_GROUP_SIZE 2048)""", [path])
        assert pq.read_table(path).num_rows == 4000
        queries = [(f"study-{i}", q.format(t=f"'{path}'")) for i, q in enumerate(STUDY)]
        cb = os.environ.get("ZPQ_CLICKBENCH")
        cb_queries = os.environ.get("ZPQ_CLICKBENCH_QUERIES")
        if cb and cb_queries:
            lines = Path(cb_queries).read_text().splitlines()
            queries += [(f"cb-{i}", re.sub(r'\bFROM hits\b', f"FROM '{cb}'", lines[i])) for i in CB_IDS]
        else:
            print("ClickBench slice not configured: set ZPQ_CLICKBENCH and ZPQ_CLICKBENCH_QUERIES", flush=True)
        expected = {}
        with zpq.Zpq(max_memory="1GB") as z:
            for j in (1, 3, 16):
                for label, sql in queries:
                    p = cli(sql, j)
                    assert p.returncode == 0, (label, j, p.stderr.decode())
                    got = z.query_raw(sql, parallelism=j, max_memory="1GB").encode()
                    assert got == p.stdout, (label, j, got[:200], p.stdout[:200])
                    if label in expected:
                        assert expected[label] == got, ("worker-count parity", label, j)
                    expected[label] = got
                print(f"SQL differential: {len(queries)} exact matches at j={j}", flush=True)
            # Basename glob expansion and ordering must be the same frontend.
            sql = f"SELECT count(*) AS n FROM '{d}/*.parquet'"
            assert z.query_raw(sql).encode() == cli(sql, 1).stdout
            errors = ["SELECT", f"SELECT absent FROM '{path}'", "SELECT * FROM 'missing.parquet'",
                      f"SELECT * FROM '{d}/missing*.parquet'", "SELECT 1; SELECT 2;",
                      f"SELECT * FROM '{path}' ORDER BY id NULLS FIRST",
                      "SELECT * FROM '" + "x" * 5000 + "'",
                      f"SELECT * FROM '{d}/" + "x" * 5000 + "*'",
                      "SELECT * FROM '" + "x" * 17000 + "'"]
            def check_error(sql):
                p = cli(sql, 1)
                assert p.returncode != 0, sql
                want = json.loads(p.stderr.splitlines()[-1])["message"]
                got = z.run({"sql": sql, "max_memory": "64MB"}, check=False)
                assert got.get("message") == want, (sql, got, want)
                return got
            for sql in errors:
                check_error(sql)
            mixed = queries[:len(STUDY)]
            # Warm every shape before sampling; compare to saved CLI bytes on all 1,000 calls.
            for label, sql in mixed:
                z.query_raw(sql, parallelism=3)
            samples = [rss()]
            for i in range(1000):
                label, sql = mixed[i % len(mixed)]
                assert z.query_raw(sql, parallelism=(1, 3, 16)[i % 3]).encode() == expected[label]
                if (i + 1) % 250 == 0:
                    samples.append(rss())
            assert samples[-1] - samples[2] < 4096, ("SQL RSS growth KiB", samples)
            print(f"SQL session: 1000 exact matches; RSS KiB at 0/250/500/750/1000: {samples}", flush=True)
            # Error text is thread local; successful and failed requests overlap on the same context.
            def concurrent(i):
                if i % 3 == 0:
                    return check_error(errors[i % len(errors)])
                label, sql = mixed[i % len(mixed)]
                assert z.query_raw(sql, max_memory="64MB", parallelism=3).encode() == expected[label]
            with ThreadPoolExecutor(16) as ex:
                list(ex.map(concurrent, range(160)))
            saved_mode = os.environ.get("ZPQ_IO_PROFILE")
            try:
                for mode in ("mmap", "pread", "uring"):
                    os.environ["ZPQ_IO_PROFILE"] = mode
                    for label, sql in mixed[:6] + mixed[17:20]:
                        assert z.query_raw(sql, parallelism=3).encode() == cli(sql, 3).stdout
            finally:
                if saved_mode is None:
                    os.environ.pop("ZPQ_IO_PROFILE", None)
                else:
                    os.environ["ZPQ_IO_PROFILE"] = saved_mode
            over = z.run({"sql": queries[0][1], "max_memory": "2GB"}, check=False)
            assert over["reason"] == "OverCeiling", over
            assert z.query(queries[0][1]) == [{"n": 4000}]
        print("SQL diagnostics, budgets, globbing and concurrent requests passed", flush=True)


if __name__ == '__main__':
    main()
