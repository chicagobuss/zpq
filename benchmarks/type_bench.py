#!/usr/bin/env python3
"""Cross-engine perf comparison for ZPQ's type/operator surface.

Filter/aggregate queries over the newer types (decimal, string min/max,
date/timestamp literals, bool aggregates, IN) against the same local parquet,
best-of-N wall-clock, across:

  - zpq          : the ZPQ CLI (native; cold process, but startup is ~ms)
  - duckdb-cli   : the DuckDB CLI (native; cold process)
  - polars-cold  : polars via a fresh python subprocess — pays interpreter +
                   `import polars` (~100 ms) every time (the serverless/cold axis)
  - polars-warm  : polars in-process, import amortized — the Rust core's actual
                   compute, "rust without the python tax". (PyPolars is a thin
                   wrapper over the same Rust kernels a standalone binary would
                   call, so this *is* the native-compute number.)

zpq/duckdb-cli are cold but start in ~ms, so their numbers ≈ compute; polars-warm
is compute with import removed. That makes zpq vs duckdb-cli vs polars-warm a
~compute comparison (where to find perf gains), while polars-cold shows the
cold-start floor ZPQ's serverless pitch targets.

Usage: .venv/bin/python benchmarks/type_bench.py [-n RUNS]
"""
import subprocess, sys, time, shutil, os, datetime

FILE = "data/bench_types.parquet"
RUNS = 5
VENV_PY = os.path.join(os.getcwd(), ".venv", "bin", "python")
ZPQ = "zig-out/bin/zpq"
DUCKDB = shutil.which("duckdb")

# (label, zpq-args, duckdb-sql, polars-fn). polars-fn(pl, scan) -> a frame to
# .collect(); used both in-process (warm) and re-invoked cold via --polars-one.
QUERIES = [
    ("sum(price)  [decimal]",
     ["--aggregate", "sum(price) AS s"],
     f"SELECT sum(price) FROM '{FILE}'",
     lambda pl, s: s.select(pl.col("price").sum())),
    ("min/max(name)  [string]",
     ["--aggregate", "min(name) AS lo, max(name) AS hi"],
     f"SELECT min(name), max(name) FROM '{FILE}'",
     lambda pl, s: s.select(pl.col("name").min().alias("lo"), pl.col("name").max().alias("hi"))),
    ("count WHERE d >= 2018  [date lit]",
     ["--filter", "d >= '2018-01-01'", "--aggregate", "count(id) AS c"],
     f"SELECT count(id) FROM '{FILE}' WHERE d >= DATE '2018-01-01'",
     lambda pl, s: s.filter(pl.col("d") >= datetime.date(2018, 1, 1)).select(pl.len())),
    ("count WHERE ts < 2015-06  [ts lit]",
     ["--filter", "ts < '2015-06-01 00:00:00'", "--aggregate", "count(id) AS c"],
     f"SELECT count(id) FROM '{FILE}' WHERE ts < TIMESTAMP '2015-06-01 00:00:00'",
     lambda pl, s: s.filter(pl.col("ts") < datetime.datetime(2015, 6, 1)).select(pl.len())),
    ("sum(flag)  [bool agg]",
     ["--aggregate", "sum(flag) AS s"],
     f"SELECT sum(flag::INTEGER) FROM '{FILE}'",
     lambda pl, s: s.select(pl.col("flag").cast(pl.Int64).sum())),
    ("count WHERE id IN (...)  [IN]",
     ["--filter", "id IN (1, 500, 1000000, 4999999)", "--aggregate", "count(id) AS c"],
     f"SELECT count(id) FROM '{FILE}' WHERE id IN (1, 500, 1000000, 4999999)",
     lambda pl, s: s.filter(pl.col("id").is_in([1, 500, 1000000, 4999999])).select(pl.len())),
    ("sum(id)  [numeric baseline]",
     ["--aggregate", "sum(id) AS s"],
     f"SELECT sum(id) FROM '{FILE}'",
     lambda pl, s: s.select(pl.col("id").sum())),
]


def best_ms_subprocess(cmd):
    best = None
    for _ in range(RUNS):
        t0 = time.monotonic()
        r = subprocess.run(cmd, capture_output=True)
        dt = (time.monotonic() - t0) * 1000.0
        if r.returncode != 0:
            return None
        best = dt if best is None else min(best, dt)
    return best


def best_ms_polars_warm(pl_fn):
    import polars as pl
    best = None
    for _ in range(RUNS):
        t0 = time.monotonic()
        pl_fn(pl, pl.scan_parquet(FILE)).collect()  # re-scan each run (warm engine, incl. IO)
        dt = (time.monotonic() - t0) * 1000.0
        best = dt if best is None else min(best, dt)
    return best


def main():
    global RUNS
    args = sys.argv[1:]
    # Cold-polars worker: import + scan + run one query once, then exit. Invoked
    # as a subprocess so its full wall-clock includes interpreter + import.
    if args and args[0] == "--polars-one":
        import polars as pl
        QUERIES[int(args[1])][3](pl, pl.scan_parquet(FILE)).collect()
        return
    if args and args[0] == "-n":
        RUNS = int(args[1]); args = args[2:]

    rows = []
    for i, (label, zargs, sql, pl_fn) in enumerate(QUERIES):
        z = best_ms_subprocess([ZPQ, "query", FILE, *zargs])
        d = best_ms_subprocess([DUCKDB, "-c", sql]) if DUCKDB else None
        pc = best_ms_subprocess([VENV_PY, __file__, "--polars-one", str(i)])
        pw = best_ms_polars_warm(pl_fn)
        rows.append((label, z, d, pc, pw))

    def fmt(x):
        return f"{x:7.1f}" if x is not None else "   n/a "
    print(f"# ZPQ type/operator perf — best of {RUNS}, wall-clock (ms)")
    print(f"# file: {FILE}  ({os.path.getsize(FILE)//1024//1024} MB, 5M rows)\n")
    print(f"| {'query':38} | {'zpq':>7} | {'duckdb-cli':>10} | {'polars-cold':>11} | {'polars-warm':>11} |")
    print(f"|{'-'*40}|{'-'*9}|{'-'*12}|{'-'*13}|{'-'*13}|")
    for label, z, d, pc, pw in rows:
        print(f"| {label:38} | {fmt(z)} | {fmt(d):>10} | {fmt(pc):>11} | {fmt(pw):>11} |")
    print("\n_zpq/duckdb-cli: native, cold (but ~ms startup → ≈ compute). "
          "polars-cold: subprocess incl. python+import (cold-start axis). "
          "polars-warm: in-process, import amortized = the Rust core's compute._")


if __name__ == "__main__":
    main()
