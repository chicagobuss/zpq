#!/usr/bin/env python3
"""ClickBench's 43 queries, unmodified: does zpq run each, and does it answer as DuckDB does?

DuckDB reads the file through ClickBench's own view (duckdb-parquet/create.sql). zpq reads the file as it is, with
`FROM hits` pointed at the path: no rewrites. A zpq error is a coverage result, not a failure.

Rows compare under tools/oracle_compare.py's policy. ORDER BY ... LIMIT may cut through a run of tied sort keys,
where any choice of tied rows is a right answer; so a result that differs from DuckDB's in order or membership passes
only if every zpq row is a row of DuckDB's whole (unlimited) answer, and the columns that agree position by position
(which must include the sort keys) are reported.

usage: clickbench_check.py ZPQ_BIN PARQUET ZPQ_REPO OUT_JSON [QUERY_NUMBERS...]
"""
import base64
import binascii
import datetime
import json
import os
import re
import subprocess
import sys
import time

ZPQ, PARQUET, REPO, OUT = sys.argv[1:5]
ONLY = {int(q) for q in sys.argv[5:]}
sys.path.insert(0, f"{REPO}/tools")
import duckdb  # noqa: E402
from oracle_compare import canon, same  # noqa: E402
from cli_output import read_rows

CB = os.environ.get("CLICKBENCH_SQL_DIR", os.path.expanduser("~/bench/zpq-fair/repos/ClickBench/duckdb-parquet"))
QUERIES = [q.strip().rstrip(";") for q in open(f"{CB}/queries.sql") if q.strip()]
TIMEOUT_S = 600


def duck():
    con = duckdb.connect(config={"memory_limit": "10GB"})
    con.execute(open(f"{CB}/create.sql").read().replace("'hits.parquet'", f"'{PARQUET}'"))
    return con


def zpq_rows(sql):
    """(rows, None, seconds) or (None, error text, seconds). Rows are lists in SELECT order."""
    zsql = re.sub(r"\bFROM hits\b", f"FROM '{PARQUET}'", sql)
    t = time.monotonic()
    try:
        p = subprocess.run([ZPQ, "query", "--query", zsql], capture_output=True, text=True, timeout=TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return None, f"timeout after {TIMEOUT_S}s", None
    secs = time.monotonic() - t
    if p.returncode != 0:
        err = [l for l in p.stderr.splitlines() if l.strip() and "warn" not in l]
        return None, (" | ".join(err[-3:]) or f"exit {p.returncode}"), secs
    return [list(row.values()) for row in read_rows(p.stdout)], None, secs


def as_answer(v, like):
    """zpq's JSON value as the answer it stands for: AVG is printed as its re-aggregatable {sum, count}; a column the
    file stores as days since the epoch (ClickBench's EventDate, which DuckDB's view turns into a DATE) is that date."""
    if isinstance(v, dict) and set(v) == {"sum", "count"}:
        return None if not v["count"] else v["sum"] / v["count"]
    if isinstance(like, datetime.date) and not isinstance(like, datetime.datetime) and isinstance(v, int):
        return datetime.date(1970, 1, 1) + datetime.timedelta(days=v)
    # ClickBench's text columns have no STRING annotation, so zpq prints their bytes as base64
    # (docs/binary-output.md), where the view reads them as text (`binary_as_string`).
    if isinstance(like, str) and isinstance(v, str) and v != like:
        try:
            return base64.b64decode(v, validate=True).decode()
        except (binascii.Error, UnicodeDecodeError):
            return v
    return v


def rows_equal(a, b):
    return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))


def key(row):
    return tuple(repr(canon(v)) for v in row)


def check(con, n, sql):
    rec = {"q": n, "sql": sql}
    got, err, secs = zpq_rows(sql)
    rec["zpq_s"] = secs
    if err:
        rec["status"] = "zpq-error"
        rec["error"] = err
        return rec
    t = time.monotonic()
    want = [list(r) for r in con.execute(sql).fetchall()]
    rec["duckdb_s"] = time.monotonic() - t
    rec["rows"] = [len(got), len(want)]
    if want and got:
        # Each column's type from a row where it is not NULL.
        like = [next((w[c] for w in want if w[c] is not None), None) for c in range(len(want[0]))]
        got = [[as_answer(v, w) for v, w in zip(g, like)] for g in got]
    if len(got) != len(want) or any(len(g) != len(w) for g, w in zip(got, want)):
        rec["status"] = "FAIL"
        rec["why"] = "shape differs"
        rec["got"], rec["want"] = got[:5], want[:5]
        return rec
    if all(rows_equal(g, w) for g, w in zip(got, want)):
        rec["status"] = "PASS"
        return rec
    agree = [c for c in range(len(got[0]) if got else 0) if all(same(g[c], w[c]) for g, w in zip(got, want))]
    rec["agreeing_columns"] = agree
    if "LIMIT" not in sql.upper():
        if sorted(map(key, got)) == sorted(map(key, want)):
            rec["status"] = "PASS"
            rec["note"] = "same rows, different order (no ORDER BY across all rows)"
        else:
            rec["status"] = "FAIL"
            rec["why"] = "rows differ"
            rec["got"], rec["want"] = got[:10], want[:10]
        return rec
    # A tie at the cut: every zpq row must be a row of DuckDB's whole answer.
    whole = re.sub(r"\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?\s*$", "", sql, flags=re.I)
    cols = ", ".join(f"c{i}" for i in range(len(got[0])))
    missing = []
    for g in got:
        cond = " AND ".join(f"c{i} IS NOT DISTINCT FROM ?" for i in range(len(g)))
        found = con.execute(f"SELECT count(*) FROM ({whole}) AS r({cols}) WHERE {cond}", g).fetchone()[0]
        if not found:
            missing.append(g)
    if missing:
        rec["status"] = "FAIL"
        rec["why"] = "zpq rows not in DuckDB's whole answer"
        rec["missing"] = missing[:5]
        rec["got"], rec["want"] = got[:10], want[:10]
    else:
        rec["status"] = "PASS"
        rec["note"] = "a different, equally valid choice among tied rows at the cut"
    return rec


def main():
    con = duck()
    results = []
    for n, sql in enumerate(QUERIES):
        if ONLY and n not in ONLY:
            continue
        rec = check(con, n, sql)
        results.append(rec)
        tail = rec.get("error") or rec.get("why") or rec.get("note") or ""
        print(f"Q{n:02d} {rec['status']:<9} {tail}"[:220], flush=True)
        json.dump(results, open(OUT, "w"), indent=1, default=str)
    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(counts)


main()
