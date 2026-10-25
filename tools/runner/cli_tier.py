#!/usr/bin/env python3
"""A handful of queries through the real zpq binary.

The referee calls the engine in-process. These cases keep argument parsing and
the CLI's json, jsonl, and parquet formatting covered.
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ZPQ = sys.argv[1] if len(sys.argv) > 1 else "zig-out/bin/zpq"


def run(args, **kw):
    return subprocess.run([ZPQ, *args], capture_output=True, text=True, **kw)


def check(name, ok, detail=""):
    print(f"  {'OK  ' if ok else 'FAIL'} {name} {detail}".rstrip(), flush=True)
    return 0 if ok else 1


def main():
    if not Path(ZPQ).is_file():
        print(f"missing zpq binary: {ZPQ}", file=sys.stderr)
        return 2
    fails = 0
    bare = run([])
    fails += check("usage", bare.returncode == 0 and bare.stdout.startswith("usage:"))
    bogus = run(["query", "--not-a-flag"])
    fails += check("unknown flag", bogus.returncode != 0 and len(bogus.stderr + bogus.stdout) > 0)
    missing = run(["query", "nope.parquet", "--format"])
    fails += check("format needs a value", missing.returncode != 0 and "--format" in missing.stderr + missing.stdout)

    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "in.parquet"
        out = Path(tmp) / "out.parquet"
        n = 20
        pq.write_table(pa.table({"i": pa.array(range(n), type=pa.int32())}), src)
        counted = run(["query", str(src), "--aggregate", "count(*) AS n", "--format", "json"])
        got = None
        if counted.returncode == 0:
            try:
                got = json.loads(counted.stdout)
            except json.JSONDecodeError:
                got = None
        fails += check("json count", got == [{"n": n}], counted.stderr[-160:])
        lines = run(["query", str(src), "--format", "jsonl", "--limit", "3"])
        body = [line for line in lines.stdout.splitlines() if line.strip()]
        fails += check("jsonl limit", lines.returncode == 0 and len(body) == 3 and all(json.loads(line)["i"] == i for i, line in enumerate(body)))
        written = run(["query", str(src), "-o", str(out), "--columns", "i"])
        rows = pq.read_table(out).column("i").to_pylist() if written.returncode == 0 and out.exists() else None
        fails += check("parquet write", rows == list(range(n)))
        sql = run(["query", "--query", f"SELECT count(*) AS n FROM '{src}'", "--format", "json"])
        sql_got = None
        if sql.returncode == 0:
            try:
                sql_got = json.loads(sql.stdout)
            except json.JSONDecodeError:
                sql_got = None
        fails += check("sql json", sql_got == [{"n": n}], sql.stderr[-160:])

    total = 7
    print(f"cli-tier: {total - fails}/{total} checks match")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
