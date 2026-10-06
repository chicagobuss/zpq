#!/usr/bin/env python3
"""Bloom filter interop between zpq, pyarrow, DuckDB and Hardwood.

  1. Other readers on zpq-written filters (re-encoded and byte-copied): DuckDB's parquet_bloom_probe never excludes a
     row group holding the value and excludes most that do not, and its counts match; Hardwood (tools/bloom/
     hwbloom.sh) returns the right rows and drops row groups on the filters.
  2. zpq on other writers' filters: zpq's counts match DuckDB's and --scan-all on files pyarrow and DuckDB wrote with
     filters, and on any `--extra` files (e.g. one Hardwood wrote), and its filters prune there.
  3. Every file passes the independent oracle (tools/bloom/sbbf.py).

Needs duckdb in the Python environment; Hardwood runs when ~/.cache/zpq-bloom/hardwood.classpath exists.

usage: tools/bloom/interop.py [ZPQ] [--extra FILE ...] [--keep DIR]
"""
import argparse
import datetime as dt
import os
import random
import shutil
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import check  # noqa: E402
import sbbf  # noqa: E402

# Columns DuckDB and Hardwood probe and how to spell a value for each: (DuckDB SQL literal, Hardwood --type).
SPELL = {
    "i32": (str, "int"),
    "i64": (str, "long"),
    "f32": (lambda v: f"{v!r}::FLOAT", "float"),
    "f64": (lambda v: f"{v!r}::DOUBLE", "double"),
    "s": (lambda v: f"'{v}'", "str"),
    "n_s": (lambda v: f"'{v}'", "str"),
    "low": (lambda v: f"'{v}'", "str"),
    "d": (lambda v: f"DATE '{v.isoformat()}'", "date"),
    "n_i64": (str, "long"),
}


def chunk_values(path, name):
    """Per row group, the set of the column's non-null values."""
    pf = pq.ParquetFile(path)
    return [set(v for v in pf.read_row_group(i, columns=[name]).column(0).to_pylist() if v is not None)
            for i in range(pf.metadata.num_row_groups)]


def probes(rnd, table, name):
    values = [v for v in table.column(name).to_pylist() if v is not None]
    present = [rnd.choice(values) for _ in range(3)]
    return present, check.absent_like(rnd, name, present, values), values


def duckdb_reads(con, path, table, rnd):
    """parquet_bloom_probe on zpq's filters: no row group holding a value excluded; most others are."""
    excluded = possible = 0
    for name, (lit, _) in SPELL.items():
        present, absent, values = probes(rnd, table, name)
        groups = chunk_values(path, name)
        for v in present + absent:
            rows = con.execute(f"SELECT row_group_id, bloom_filter_excludes FROM parquet_bloom_probe(?, ?, {lit(v)})",
                               [path, name]).fetchall()
            assert len(rows) == len(groups), (path, name, rows)
            for rg, excludes in rows:
                if v in groups[rg]:
                    assert not excludes, f"DuckDB excludes {name} = {v!r} from row group {rg} of {path}, which holds it"
                else:
                    possible += 1
                    excluded += bool(excludes)
            got = con.execute(f"SELECT count(*) FROM read_parquet(?) WHERE {name} = {lit(v)}", [path]).fetchone()[0]
            want = sum(1 for x in values if x == v)
            # DuckDB 1.5.6 compares FLOAT zero by its bits: it finds +0.0 but not -0.0 (fixed upstream).
            if not (name == "f32" and v == 0.0):
                assert got == want, f"DuckDB {name} = {v!r} on {path}: {got}, want {want}"
    assert excluded > possible * 0.8, f"DuckDB excluded only {excluded} of {possible} row groups on {path}"
    return excluded, possible


def hardwood_reads(path, table, rnd):
    """Hardwood's rows and row groups dropped on zpq's filters and dictionaries."""
    dropped = possible = 0
    for name, (_, hw_type) in SPELL.items():
        present, absent, values = probes(rnd, table, name)
        groups = chunk_values(path, name)
        spelled = [v.isoformat() if isinstance(v, dt.date) else repr(v) if isinstance(v, float) else str(v)
                   for v in present + absent]
        out = subprocess.run(["nice", "-n", "10", os.path.join(HERE, "hwbloom.sh"), "--file", path, "--col", name,
                              "--type", hw_type, "--values", ",".join(spelled)],
                             capture_output=True, text=True, check=True).stdout
        for v, line in zip(present + absent, [ln for ln in out.splitlines() if ln.startswith("VALUE ")]):
            f = line.split()
            rows, bloom, dct, stats = int(f[3]), int(f[5]), int(f[7]), int(f[9])
            want = sum(1 for x in values if x == v)
            assert rows == want, f"Hardwood {name} = {v!r} on {path}: {rows} rows, want {want}"
            holding = sum(1 for g in groups if v in g)
            assert bloom + dct + stats <= len(groups) - holding, f"Hardwood dropped a group holding {name} = {v!r}"
            possible += len(groups) - holding
            dropped += bloom + dct + stats
    assert dropped > possible * 0.8, f"Hardwood dropped only {dropped} of {possible} row groups on {path}"
    return dropped, possible


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("zpq", nargs="?", default="zig-out/bin/zpq")
    ap.add_argument("--extra", nargs="*", default=[], help="more files with filters to look up through zpq")
    ap.add_argument("--keep")
    ap.add_argument("--seed", type=int, default=11)
    a = ap.parse_args()
    zpq = check.Zpq(a.zpq)
    rnd = random.Random(a.seed)
    d = a.keep or tempfile.mkdtemp(prefix="zpq-bloom-interop-")
    os.makedirs(d, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads=2")
    try:
        names = list(check.column_kinds(rnd))
        table = check.build_table(rnd, 12_000, names)
        rg = 2000
        src = os.path.join(d, "src.parquet")
        pq.write_table(table, src, row_group_size=rg)
        flags = [x for n in check.bloomable(table) for x in ("--bloom-filter", n)]
        zenc = os.path.join(d, "zpq_reencode.parquet")
        zpq.write(src, zenc, "--filter", "f64 > -1e300", *flags)
        zcopy = os.path.join(d, "zpq_copy.parquet")
        zpq.write(src, zcopy, *flags)
        pa_path = os.path.join(d, "pyarrow.parquet")
        pq.write_table(table, pa_path, row_group_size=rg,
                       bloom_filter_options={n: {"ndv": rg, "fpp": 0.01} for n in names})
        # DuckDB writes filters only for dictionary-encoded chunks; a high dictionary limit makes every chunk one.
        dd_path = os.path.join(d, "duckdb.parquet")
        con.register("t", table.drop_columns(["ts_ns"]))
        con.execute(f"COPY (SELECT * FROM t) TO '{dd_path}' (FORMAT parquet, ROW_GROUP_SIZE {rg}, "
                    "DICTIONARY_SIZE_LIMIT 100000, BLOOM_FILTER_FALSE_POSITIVE_RATIO 0.01)")

        for f in (zenc, zcopy, pa_path, dd_path, *a.extra):
            nf, nv = check.oracle(f)
            print(f"oracle    {os.path.basename(f):22} filters={nf:4} values={nv:7} all present")
        for f in (zenc, zcopy):
            ex, pos = duckdb_reads(con, f, table, rnd)
            print(f"duckdb    {os.path.basename(f):22} excluded {ex}/{pos} row groups without the value, none with it")
        if os.path.exists(os.path.expanduser("~/.cache/zpq-bloom/hardwood.classpath")):
            for f in (zenc, zcopy):
                dr, pos = hardwood_reads(f, table, rnd)
                print(f"hardwood  {os.path.basename(f):22} dropped {dr}/{pos} row groups without the value, rows match")
        else:
            print("hardwood  skipped: no ~/.cache/zpq-bloom/hardwood.classpath")
        # zpq on other writers' filters, against Python's counts and --scan-all (check.lookups).
        others = [pa_path, dd_path]
        cases, pruned = check.lookups(rnd, zpq, others, table, [n for n in names if n != "ts_ns"], others)
        print(f"zpq reads {cases} lookups agree; pruned: "
              + ", ".join(f"{os.path.basename(f)}={p}" for f, p in pruned.items()))
        for f in a.extra:
            t = pq.read_table(f)
            cols = [n for n in t.column_names if n in names and not pa.types.is_timestamp(t.schema.field(n).type)]
            cases, pruned = check.lookups(rnd, zpq, [f], t, cols, [f])
            print(f"zpq reads {cases} lookups agree on {os.path.basename(f)}; pruned {pruned[f]}")
    finally:
        if not a.keep:
            shutil.rmtree(d, ignore_errors=True)
    print("bloom interop: ok")


if __name__ == "__main__":
    main()
