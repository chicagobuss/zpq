#!/usr/bin/env python3
"""
storage_compare.py — workstation-direct head-to-head: zpq vs duckdb vs polars,
across local files, S3, and R2. Same 100 MB fixture at every location.

Output: TSV-style table to stdout. Best-of-N wall times.

Usage:
  python3 benchmarks/storage_compare.py [--runs N] [--engines zpq,duckdb,polars]
                                         [--storages local,s3,r2]
                                         [--queries count_star,sum_int64,filter_count]

Caveats:
  - S3 and R2 numbers include round-trip from your workstation. They reflect
    "developer opens duckdb at home" workflows, not production-network
    throughput. Fair across the three engines, but not directly comparable to
    Lambda numbers.
  - duckdb is invoked via the Python module (one process per query), not the
    CLI, to keep the per-query overhead the same shape across engines. polars
    likewise.
  - zpq is invoked via the CLI binary — that's the fair comparison since
    duckdb/polars are also paying their import + connection-setup costs.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ZPQ = os.path.join(ROOT, "zig-out", "bin", "zpq")
LOCAL_FIXTURE = os.path.join(ROOT, "data", "benchmark_100mb.parquet")


@dataclass
class Query:
    name: str
    # SQL form for duckdb / polars (table name is `t`).
    sql: str
    # zpq CLI args (after the input path).
    zpq_args: list[str]


QUERIES = {
    "count_star": Query(
        name="count_star",
        sql="SELECT count(*) FROM t",
        zpq_args=["--aggregate", "count(*) AS n"],
    ),
    "sum_int64": Query(
        name="sum_int64",
        sql="SELECT sum(int64_sorted) FROM t",
        zpq_args=["--aggregate", "sum(int64_sorted) AS s"],
    ),
    "filter_count": Query(
        name="filter_count",
        sql="SELECT count(*) FROM t WHERE int8 BETWEEN -10 AND 10",
        zpq_args=["--filter", "int8 BETWEEN -10 AND 10",
                  "--aggregate", "count(*) AS n"],
    ),
}


def storage_paths():
    """Resolve {local, s3, r2} -> (zpq_input, sql_input). SQL input is a
    URL DuckDB / Polars can read. zpq input is whatever the CLI accepts."""
    s3_bucket = os.environ.get("AWS_S3_BUCKET")
    r2_bucket = os.environ.get("R2_BUCKET")
    s3_url = (
        f"s3://{s3_bucket}/zpq_test_data/benchmark/benchmark_100mb.parquet"
        if s3_bucket else None
    )
    r2_url = (
        f"s3://{r2_bucket}/testdata/benchmark/benchmark_100mb.parquet"
        if r2_bucket else None
    )
    return {
        "local": (LOCAL_FIXTURE, LOCAL_FIXTURE),
        "s3":    (s3_url, s3_url),
        "r2":    (r2_url, r2_url),
    }


# ----------------------------------------------------------------------
# Engine drivers — return wall-clock seconds.
# ----------------------------------------------------------------------

def run_zpq(storage: str, input_url: str, query: Query) -> float:
    env = os.environ.copy()
    if storage == "r2":
        env["AWS_ACCESS_KEY_ID"] = env["R2_ACCESS_KEY_ID"]
        env["AWS_SECRET_ACCESS_KEY"] = env["R2_SECRET_ACCESS_KEY"]
        env["AWS_REGION"] = env.get("R2_REGION", "auto")
        env["S3_ENDPOINT_URL"] = f"https://{env['R2_ENDPOINT']}"
    elif storage == "s3":
        env["AWS_REGION"] = env.get("AWS_REGION", "us-west-2")
        env.pop("S3_ENDPOINT_URL", None)
    cmd = [ZPQ, "query", input_url] + query.zpq_args
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, env=env, capture_output=True)
    t1 = time.perf_counter()
    if proc.returncode != 0:
        sys.stderr.write(f"zpq failed: {proc.stderr.decode()[:200]}\n")
        return float("nan")
    return t1 - t0


def run_duckdb(storage: str, input_url: str, query: Query) -> float:
    """Spawn a fresh python process so DuckDB pays its connect cost
    every time — apples to apples with the zpq CLI."""
    setup = ""
    if storage == "s3":
        setup = (
            "duckdb.execute(\"INSTALL httpfs; LOAD httpfs;\"); "
            f"duckdb.execute(\"SET s3_region='{os.environ.get('AWS_REGION', 'us-west-2')}'\"); "
            f"duckdb.execute(\"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'\"); "
            f"duckdb.execute(\"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'\"); "
        )
    elif storage == "r2":
        # R2 is region-less; DuckDB's S3 settings still require an
        # explicit region value, so force `auto`.
        setup = (
            "duckdb.execute(\"INSTALL httpfs; LOAD httpfs;\"); "
            f"duckdb.execute(\"SET s3_endpoint='{os.environ['R2_ENDPOINT']}'\"); "
            f"duckdb.execute(\"SET s3_access_key_id='{os.environ['R2_ACCESS_KEY_ID']}'\"); "
            f"duckdb.execute(\"SET s3_secret_access_key='{os.environ['R2_SECRET_ACCESS_KEY']}'\"); "
            "duckdb.execute(\"SET s3_region='auto'\"); "
            "duckdb.execute(\"SET s3_url_style='path'\"); "
            "duckdb.execute(\"SET s3_use_ssl=true\"); "
        )
    sql = query.sql.replace("FROM t", f"FROM read_parquet('{input_url}')")
    code = (
        "import duckdb, time; "
        f"{setup}"
        "t0=time.perf_counter(); "
        f"duckdb.execute(\"\"\"{sql}\"\"\").fetchall(); "
        "print(time.perf_counter() - t0)"
    )
    t0 = time.perf_counter()
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True)
    t1 = time.perf_counter()
    if proc.returncode != 0:
        sys.stderr.write(f"duckdb failed: {proc.stderr.decode()[:200]}\n")
        return float("nan")
    return t1 - t0


def run_polars(storage: str, input_url: str, query: Query) -> float:
    """Same fresh-process pattern as duckdb."""
    storage_options = "{}"
    if storage == "s3":
        storage_options = json.dumps({
            "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "aws_region": os.environ.get("AWS_REGION", "us-west-2"),
        })
    elif storage == "r2":
        storage_options = json.dumps({
            "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "aws_endpoint_url": f"https://{os.environ['R2_ENDPOINT']}",
            "aws_region": "auto",
        })
    # polars' LazyFrame.sql() registers the frame as `self`; rewrite the
    # SQL to match.
    sql = query.sql.replace("FROM t", "FROM self")
    code = (
        "import polars as pl, time, json; "
        f"opts = {storage_options}; "
        f"url = '{input_url}'; "
        "t0=time.perf_counter(); "
        "df = pl.scan_parquet(url, storage_options=opts) if opts else pl.scan_parquet(url); "
        f"df.sql(\"\"\"{sql}\"\"\").collect(); "
        "print(time.perf_counter() - t0)"
    )
    t0 = time.perf_counter()
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True)
    t1 = time.perf_counter()
    if proc.returncode != 0:
        sys.stderr.write(f"polars failed: {proc.stderr.decode()[:200]}\n")
        return float("nan")
    return t1 - t0


ENGINES = {"zpq": run_zpq, "duckdb": run_duckdb, "polars": run_polars}


# ----------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--engines", default="zpq,duckdb,polars")
    ap.add_argument("--storages", default="local,s3,r2")
    ap.add_argument("--queries", default=",".join(QUERIES.keys()))
    args = ap.parse_args()

    engines = args.engines.split(",")
    storages = args.storages.split(",")
    queries = [QUERIES[q] for q in args.queries.split(",")]
    paths = storage_paths()

    print(f"# runs={args.runs} engines={engines} storages={storages}")
    print(f"# fixture = benchmark_100mb.parquet (155 MB) at each storage")
    print()
    print("storage\tengine\tquery\tmin_ms\tmedian_ms\tp95_ms")

    for storage in storages:
        zpq_in, sql_in = paths[storage]
        if zpq_in is None:
            sys.stderr.write(f"# skipping {storage}: no env config\n")
            continue
        for query in queries:
            for engine in engines:
                runner = ENGINES[engine]
                input_url = zpq_in if engine == "zpq" else sql_in
                # Warm-up (1 untimed run to populate caches / TLS sessions).
                runner(storage, input_url, query)
                samples_ms = []
                for _ in range(args.runs):
                    t = runner(storage, input_url, query)
                    samples_ms.append(t * 1000.0)
                samples_ms.sort()
                p95 = samples_ms[int(len(samples_ms) * 0.95)] if len(samples_ms) > 1 else samples_ms[0]
                print(f"{storage}\t{engine}\t{query.name}\t"
                      f"{min(samples_ms):.1f}\t{statistics.median(samples_ms):.1f}\t{p95:.1f}",
                      flush=True)


if __name__ == "__main__":
    main()
