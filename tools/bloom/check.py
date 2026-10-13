#!/usr/bin/env python3
"""Bloom filter correctness end to end, with pyarrow alone. CI runs it from tools/cli_smoke.sh.

  1. Writer oracle: every stored value of every chunk with a filter tests present in it under the spec's hashing
     (sbbf.py, written from the spec, independently of zpq). Files: pyarrow-written with filters; zpq-written from
     the same rows by byte copy (filters built by decoding), by re-encode, and by carrying pyarrow's filters through.
  2. Lookup parity: zpq's `col = v`, `IN` and `OR IS NULL` counts equal Python's for present and absent values, on
     every file, with filters and under --scan-all; and on files with filters, filters prune.
  3. Fuzz: tables of random columns, sizes and probabilities through zpq's writer, then 1 and 2.

usage: tools/bloom/check.py [ZPQ] [--rounds N] [--keep DIR]
"""
import argparse
import datetime as dt
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sbbf  # noqa: E402
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cli_output import read_one, read_summary

EPOCH = dt.datetime(2020, 1, 1)


def alnum(rnd, k):
    return "".join(rnd.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(k))


# name -> (arrow type, value for row key i, zpq literal for a value). Keys are distinct per row unless the column is
# low-cardinality, and are shuffled across row groups so statistics cannot prune equality.
def column_kinds(rnd):
    def ts_lit(unit):
        def f(v):
            if unit == "ns":
                return str(v)  # bare ticks: the binder's back-compat form
            return "'" + v.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
        return f

    return {
        "i8": (pa.int8(), lambda i: (i % 251) - 125, str),
        "i32": (pa.int32(), lambda i: i * 7919 - 400_000_000, str),
        "i64": (pa.int64(), lambda i: i * 1_000_000_007 - 2**52, str),
        "u16": (pa.uint16(), lambda i: (i * 7) % 65536, str),
        "u32": (pa.uint32(), lambda i: (i * 45007) % 2**32, str),
        "u64": (pa.uint64(), lambda i: 2**63 + i * 3 if i % 2 else i, str),
        "f32": (pa.float32(), lambda i: -0.0 if i == 7 else ((i % 4001) - 2000) / 4.0, repr),
        "f64": (pa.float64(), lambda i: (i * 37 % 100003) / 8.0 - 3000.0, repr),
        "s": (pa.string(), lambda i: f"k{i:07d}{alnum(rnd, 4)}", lambda v: f"'{v}'"),
        "bin": (pa.binary(), lambda i: f"b{i}".encode(), lambda v: f"'{v.decode()}'"),
        "fsb": (pa.binary(12), lambda i: f"{i:012d}".encode(), lambda v: f"'{v.decode()}'"),
        "d": (pa.date32(), lambda i: dt.date(2000, 1, 1) + dt.timedelta(days=i % 9000),
              lambda v: f"'{v.isoformat()}'"),
        "ts_us": (pa.timestamp("us"), lambda i: EPOCH + dt.timedelta(microseconds=i * 1_000_003), ts_lit("us")),
        "ts_ms": (pa.timestamp("ms"), lambda i: EPOCH + dt.timedelta(milliseconds=i * 1009), ts_lit("ms")),
        "ts_ns": (pa.timestamp("ns"), lambda i: 1_600_000_000_000_000_000 + i * 1_000_000_007, ts_lit("ns")),
        "t_us": (pa.time64("us"), lambda i: dt.time(i % 24, i % 60, (i // 7) % 60, (i * 13) % 1_000_000),
                 lambda v: f"'{v.isoformat()}'"),
        "n_i64": (pa.int64(), lambda i: None if i % 19 == 3 else i * 13, str),
        "n_s": (pa.string(), lambda i: None if i % 23 == 5 else f"n{i}", lambda v: f"'{v}'"),
        "low": (pa.string(), lambda i: f"cat{i % 37}", lambda v: f"'{v}'"),
    }


def build_table(rnd, n, names):
    kinds = column_kinds(rnd)
    keys = list(range(n))
    rnd.shuffle(keys)
    cols = {}
    for name in names:
        t, gen, _ = kinds[name]
        cols[name] = pa.array([gen(i) for i in keys], t)
    return pa.table(cols)


def physical_values(arr, physical):
    """A chunk's non-null values as Python values of its physical type, for `sbbf.plain`."""
    t = arr.type
    arr = arr.drop_null()
    if physical in ("INT32", "INT64"):
        if not pa.types.is_integer(t):
            arr = arr.cast(pa.int64() if physical == "INT64" else pa.int32())
        elif pa.types.is_unsigned_integer(t) and t.bit_width == 64:
            arr = pc.cast(arr, pa.int64(), safe=False)
        return [int(v) for v in arr.to_pylist()]
    return arr.to_pylist()


def oracle(path):
    """Every stored value of every chunk with a filter tests present. Returns (filters, values checked)."""
    pf = pq.ParquetFile(path)
    filters = values = 0
    rnd = random.Random(1)
    for ri, chunks in enumerate(sbbf.footer_filters(path)):
        for col_path, physical, offset, length in chunks:
            if not offset:
                continue
            bits = sbbf.load(path, offset, length)
            filters += 1
            name = ".".join(col_path)
            arr = pf.read_row_group(ri, columns=[name]).column(0).combine_chunks()
            for v in physical_values(arr, physical):
                values += 1
                if not sbbf.check(bits, sbbf.xxh64(sbbf.plain(physical, v))):
                    raise AssertionError(f"{path}: rg {ri} {name} value {v!r} tests absent")
            # A filter that says "maybe" to everything would pass the above.
            hits = sum(sbbf.check(bits, rnd.getrandbits(64)) for _ in range(200))
            assert hits < 200, f"{path}: rg {ri} {name} admits everything"
    return filters, values


class Zpq:
    def __init__(self, path):
        self.path = path
        self.calls = 0

    def run(self, *args):
        self.calls += 1
        r = subprocess.run([self.path, "query", *args], capture_output=True, text=True)
        if r.returncode != 0:
            raise AssertionError(f"zpq {' '.join(args)}: {(r.stdout + r.stderr).strip()[:400]}")
        return r

    def count(self, path, filt, scan_all=False):
        extra = ["--scan-all"] if scan_all else []
        result = self.run(path, "--filter", filt, "--aggregate", "count(*) AS n", "--summary=json", *extra)
        return int(read_one(result.stdout)["n"]), read_summary(result.stderr)["membership"]

    def write(self, src, out, *args):
        self.run(src, "-o", out, *args)


def absent_like(rnd, name, present, values):
    """Values of the column's type stored nowhere in it, inside its range where that matters."""
    have = set(values)
    out = []
    for _ in range(200):
        if len(out) == 3:
            break
        v = rnd.choice(present)
        if isinstance(v, bool) or v is None:
            continue
        if isinstance(v, int):
            lo, hi = min(values), max(values)
            c = rnd.randint(lo, hi)
        elif isinstance(v, float):
            c = v + 0.0625
        elif isinstance(v, str):
            c = v[:-1] + "~"
        elif isinstance(v, bytes):
            c = v[:-1] + b"~"
        elif isinstance(v, dt.datetime):
            c = v + dt.timedelta(microseconds=1)
        elif isinstance(v, dt.date):
            c = v + dt.timedelta(days=20000)
        elif isinstance(v, dt.time):
            c = v.replace(microsecond=(v.microsecond + 1) % 1_000_000)
        else:
            continue
        if c not in have:
            out.append(c)
    return out


def lookups(rnd, zpq, files, table, names, want_pruning):
    """zpq's counts against Python's, for present and absent literals, on each file."""
    kinds = column_kinds(rnd)
    cases = 0
    pruned = {f: 0 for f in files}
    for name in names:
        lit = kinds[name][2]
        col = table.column(name)
        # Python's datetime stops at microseconds; nanosecond timestamps are compared as their ticks.
        if pa.types.is_timestamp(col.type) and col.type.unit == "ns":
            col = col.cast(pa.int64())
        values = [v for v in col.to_pylist() if v is not None]
        if not values:
            continue
        present = [rnd.choice(values) for _ in range(2)]
        absent = absent_like(rnd, name, present, values)
        exprs = []
        for v in present + absent:
            exprs.append((f"{name} = {lit(v)}", sum(1 for x in values if x == v)))
        mix = present[:1] + absent[:2]
        exprs.append((f"{name} IN ({', '.join(lit(v) for v in mix)})", sum(1 for x in values if x in mix)))
        if absent:
            nulls = table.column(name).null_count
            exprs.append((f"{name} = {lit(absent[0])} OR {name} IS NULL", nulls))
        for f in files:
            if name not in pq.ParquetFile(f).schema_arrow.names:
                continue
            for e, want in exprs:
                got, m = zpq.count(f, e)
                full, _ = zpq.count(f, e, scan_all=True)
                cases += 1
                if got != want or full != want:
                    raise AssertionError(f"{os.path.basename(f)} [{e}]: zpq {got}, --scan-all {full}, want {want}")
                pruned[f] += m["bloom_pruned"] + m["dict_pruned"]
    for f in want_pruning:
        assert pruned[f] > 0, f"{f}: no row group pruned by membership"
    return cases, pruned


def bloomable(table):
    return [n for n in table.column_names if not pa.types.is_boolean(table.schema.field(n).type)]


def run_fixed(zpq, d, rnd):
    names = list(column_kinds(rnd))
    table = build_table(rnd, 12_000, names)
    rg = 2000
    src = os.path.join(d, "src.parquet")
    pq.write_table(table, src, row_group_size=rg, compression="snappy")
    pa_path = os.path.join(d, "pyarrow.parquet")
    pq.write_table(table, pa_path, row_group_size=rg, compression="snappy",
                   bloom_filter_options={n: {"ndv": rg, "fpp": 0.01} for n in names})
    zcopy = os.path.join(d, "zpq_copy.parquet")
    flags = [a for n in bloomable(table) for a in ("--bloom-filter", n)]
    zpq.write(src, zcopy, *flags)
    zenc = os.path.join(d, "zpq_reencode.parquet")
    # Floats are never proven fully matching, so every row group re-encodes.
    zflags = [a for n in bloomable(table) for a in ("--bloom-filter", n + ":0.05")]
    zpq.write(src, zenc, "--filter", "f64 > -1e300", *zflags)
    zcarry = os.path.join(d, "zpq_carry.parquet")
    zpq.write(pa_path, zcarry, "--columns", "i64,s,fsb,ts_us,low")
    files = [pa_path, zcopy, zenc, zcarry]
    # The filters change nothing a reader sees: pyarrow reads zpq's files back value for value.
    for f in (zcopy, zenc):
        got = pq.read_table(f)
        for name in names:
            a, b = got.column(name), table.column(name)
            if pa.types.is_timestamp(b.type) and b.type.unit == "ns":
                a, b = a.cast(pa.int64()), b.cast(pa.int64())
            assert a.to_pylist() == b.to_pylist(), f"{f}: column {name} reads back different values"
    for f in files:
        nf, nv = oracle(f)
        assert nf > 0, f"{f}: no filters written"
        print(f"oracle   {os.path.basename(f):24} filters={nf:4} values={nv:7} all present")
    # zpq's own filters are on every requested column of every row group.
    for f in (zcopy, zenc):
        for ri, chunks in enumerate(sbbf.footer_filters(f)):
            for col_path, _, offset, length in chunks:
                assert offset and length, (f, ri, col_path)
    cases, pruned = lookups(rnd, zpq, files, table, names, files)
    print(f"lookups  {cases} cases agree; pruned by membership: "
          + ", ".join(f"{os.path.basename(f)}={p}" for f, p in pruned.items()))


def run_fuzz(zpq, d, rnd, rounds):
    pool = list(column_kinds(rnd))
    for k in range(rounds):
        names = rnd.sample(pool, rnd.randint(2, 6)) + ["f64"]
        names = list(dict.fromkeys(names))
        n = rnd.randint(50, 6000)
        table = build_table(rnd, n, names)
        rg = rnd.choice([97, 500, 2048, n])
        src = os.path.join(d, f"fz{k}.parquet")
        pq.write_table(table, src, row_group_size=rg, compression=rnd.choice(["snappy", "zstd", "none"]))
        fpp = rnd.choice([0.001, 0.01, 0.1, 0.4])
        out = os.path.join(d, f"fz{k}_z.parquet")
        args = [a for c in bloomable(table) for a in ("--bloom-filter", f"{c}:{fpp}")]
        if rnd.random() < 0.5:
            args = ["--filter", "f64 > -1e300"] + args
        zpq.write(src, out, *args)
        nf, nv = oracle(out)
        cases, _ = lookups(rnd, zpq, [out], table, names, [])
        print(f"fuzz {k:2} rows={n:5} rg={rg:5} fpp={fpp:<5} cols={','.join(names)}: "
              f"filters={nf} values={nv} lookups={cases}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("zpq", nargs="?", default="zig-out/bin/zpq")
    ap.add_argument("--rounds", type=int, default=4)
    ap.add_argument("--seed", type=int, default=20261006)
    ap.add_argument("--keep", help="write the files here and keep them")
    a = ap.parse_args()
    zpq = Zpq(a.zpq)
    rnd = random.Random(a.seed)
    d = a.keep or tempfile.mkdtemp(prefix="zpq-bloom-")
    os.makedirs(d, exist_ok=True)
    try:
        run_fixed(zpq, d, rnd)
        run_fuzz(zpq, d, rnd, a.rounds)
    finally:
        if not a.keep:
            shutil.rmtree(d, ignore_errors=True)
    print(f"bloom check: ok ({zpq.calls} zpq runs)")


if __name__ == "__main__":
    main()
