#!/usr/bin/env python3
"""Re-encoding writes: the layout contract and the memory ladder, checked from the outside.

1. Bytes: a write without --max-memory is the same file at every -j (the T9 repro: one row group of 100,000 equal
   short strings, then 100,000 unique 1 KiB ones).
2. Budgets: with an explicit --max-memory from 200MB to 1GB, every codec writes at -j1 as at -j16 (the T9/T10
   repros: -j1 refused budgets -j16 met, and SNAPPY/GZIP reported a refused allocation as CompressionFailed), and the
   file reads back as its input.
3. Targets: output row groups close at --row-group-size rows or --row-group-bytes PLAIN bytes, whatever the input's
   row groups; data pages close at 20,000 rows or 1 MiB; every chunk's page index matches its pages and values
   (tools/page_index_check.py), and pyarrow and DuckDB read the same rows.

Usage: .venv/bin/python tools/write_layout_check.py [path-to-zpq]   (default zig-out/bin/zpq)
"""
import decimal
import hashlib
import os
import random
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from page_index_check import check_file  # noqa: E402

ZPQ = sys.argv[1] if len(sys.argv) > 1 else "zig-out/bin/zpq"
fails = 0


def report(ok, label, detail=""):
    global fails
    fails += not ok
    print(f"  {'OK  ' if ok else 'FAIL'}  {label}  {detail}")


def zpq(*args):
    return subprocess.run([ZPQ, "query", *args], capture_output=True, text=True)


def digest(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def rg_rows(path):
    m = pq.ParquetFile(path).metadata
    return [m.row_group(i).num_rows for i in range(m.num_row_groups)]


def main():
    tmp = tempfile.mkdtemp(prefix="zpq-layout.")
    t9 = os.path.join(tmp, "t9.parquet")
    rng = random.Random(5)
    vals = ["same-value"] * 100_000 + ["".join(rng.choice("0123456789abcdef") for _ in range(32)) * 32
                                       for _ in range(100_000)]
    pq.write_table(pa.table({"id": pa.array(range(len(vals)), pa.int64()), "s": pa.array(vals)}), t9,
                   row_group_size=len(vals), compression="zstd")
    want = pq.read_table(t9)

    # 1. No --max-memory: one file at every -j.
    seen = {}
    for j in (1, 3, 16):
        out = os.path.join(tmp, f"t9_j{j}.parquet")
        r = zpq(t9, "--scan-all", "--filter", "id >= 0", "-j", str(j), "-o", out)
        seen[j] = digest(out) if r.returncode == 0 else r.stderr.strip()[:60]
    # 131,072 rows, then the 1 KiB strings reach 64 MiB.
    report(len(set(seen.values())) == 1 and rg_rows(os.path.join(tmp, "t9_j1.parquet")) == [131_072, 64_777, 4_151],
           "t9:bytes-at-every-j", str(rg_rows(os.path.join(tmp, "t9_j1.parquet"))))

    # 2. Explicit budgets, every codec, -j1 against -j16.
    for budget in ("200MB", "400MB", "1GB"):
        for codec in ("snappy", "gzip", "zstd", "lz4_raw", "uncompressed"):
            outs = []
            err = ""
            for j in (1, 16):
                out = os.path.join(tmp, f"t9_{budget}_{codec}_{j}.parquet")
                r = zpq(t9, "--scan-all", "--filter", "id >= 0", "-j", str(j), "--max-memory", budget,
                        "--codec", codec, "-o", out)
                if r.returncode != 0:
                    err = r.stderr.strip()[:80]
                    break
                outs.append(out)
            ok = len(outs) == 2 and digest(outs[0]) == digest(outs[1]) and pq.read_table(outs[0]).equals(want)
            report(ok, f"t9:{budget}:{codec}", err or f"{len(rg_rows(outs[0]))} row groups")

    # 3. Targets, cut across the input's row groups; pages and their index; two readers.
    src = os.path.join(tmp, "src.parquet")
    n = 230_001
    r2 = random.Random(7)
    t = pa.table({
        "k": pa.array(range(n), pa.int64()),
        "v": pa.array([r2.random() if i % 13 else None for i in range(n)], pa.float64()),
        "s": pa.array([f"name-{i % 5000}" if i % 7 else None for i in range(n)], pa.string()),
        "d": pa.array([decimal.Decimal(r2.randrange(-10**9, 10**9)).scaleb(-2) for _ in range(n)], pa.decimal128(12, 2)),
    })
    pq.write_table(t, src, row_group_size=33_333)
    cases = [
        ("rows", ["--row-group-size", "100000"], [100_000, 100_000, 30_001]),
        # k and d (INT64-backed) 8 bytes each, v 8 when present, s 4 + 9 when present: about 32 bytes a row, so
        # 1MB closes at about 32,000.
        ("bytes", ["--row-group-bytes", "1MB"], None),
        ("default", [], [131_072, n - 131_072]),
    ]
    for label, extra, want_rows in cases:
        out = os.path.join(tmp, f"src_{label}.parquet")
        r = zpq(src, "--filter", "k >= 0", "--scan-all", "-j", "4", "-o", out, *extra)
        if r.returncode != 0:
            report(False, f"targets:{label}", r.stderr.strip()[:80])
            continue
        rows = rg_rows(out)
        ok = sum(rows) == n and (want_rows is None or rows == want_rows)
        if label == "bytes":
            # Every row group but the last reached the target; the last row closed it.
            ok = ok and len(rows) > 3 and len(set(rows[:-1])) <= 3
        n_rg, n_pages = check_file(out, True, True)
        got = pq.read_table(out)
        ok = ok and got.equals(t.combine_chunks()) and duckdb.sql(
            f"SELECT count(*) FROM (SELECT * FROM '{out}' EXCEPT ALL SELECT * FROM '{src}')").fetchone()[0] == 0
        report(ok, f"targets:{label}", f"row groups {rows[:4]}{'...' if len(rows) > 4 else ''}, {n_pages} pages")
    out = os.path.join(tmp, "src_default.parquet")
    pages_ok = all(pq.ParquetFile(out).metadata.row_group(0).column(c).has_offset_index for c in range(4))
    report(pages_ok, "targets:page-index-present")

    print(f"write_layout_check: {'all ok' if fails == 0 else f'{fails} FAILED'}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
