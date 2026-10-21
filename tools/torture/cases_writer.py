"""Part 2: writer stress tests. Each case writes an input with pyarrow (or by hand), has zpq write it at -j 1, 3 and
16, and requires: byte-identical outputs, every value read back by pyarrow (floats bit for bit), DuckDB and zpq
itself, and the statistics zpq wrote to be correct under the total-order NaN policy."""

import io
import math
import os
import random
import struct

import pyarrow as pa
import pyarrow.parquet as pq

import tpq as T
from common import E, case, run

JS = (1, 3, 16)


# ---------------------------------------------------------------- comparison


def _bits(v, width):
    return struct.pack("<f" if width == 4 else "<d", v)


def same_value(a, b, float_width=None, exact_bits=True):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) and isinstance(b, float):
        if exact_bits and float_width:
            return _bits(a, float_width) == _bits(b, float_width)
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return a == b and math.copysign(1, a) == math.copysign(1, b)
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(same_value(a[k], b[k], None, False) for k in a)
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(same_value(x, y, None, False) for x, y in zip(a, b))
    return a == b


def float_width(t):
    if pa.types.is_float32(t):
        return 4
    if pa.types.is_float64(t):
        return 8
    return None


def f32_bits_column(col):
    """Exact float32 bit patterns of a pyarrow column (to_pylist widens to f64, which may quiet a signaling NaN)."""
    import numpy as np

    arr = col.combine_chunks()
    vals = np.frombuffer(arr.buffers()[1], dtype=np.uint32, count=len(arr) + arr.offset)[arr.offset:]
    return [None if not arr[i].is_valid else int(vals[i]) for i in range(len(arr))]


def compare_tables(want, got, exact_bits=True):
    """First difference between two pyarrow tables (by column name), or None."""
    if want.num_rows != got.num_rows:
        return f"rows {got.num_rows} != {want.num_rows}"
    for name in want.column_names:
        if name not in got.column_names:
            return f"column {name} missing"
        w, g = want.column(name), got.column(name)
        fw = float_width(w.type)
        if fw == 4 and exact_bits and pa.types.is_float32(g.type):
            wb, gb = f32_bits_column(w), f32_bits_column(g)
            for i, (x, y) in enumerate(zip(wb, gb)):
                if x != y:
                    return f"{name}[{i}]: bits {y if y is None else hex(y)} != {x if x is None else hex(x)}"
            continue
        wl, gl = w.to_pylist(), g.to_pylist()
        for i, (x, y) in enumerate(zip(wl, gl)):
            if not same_value(x, y, fw, exact_bits):
                return f"{name}[{i}]: {str(y)[:80]!r} != {str(x)[:80]!r}"
    return None


def duck_table(path):
    import duckdb

    con = duckdb.connect()
    return con.sql(f"SELECT * FROM read_parquet('{path}')").fetch_arrow_table()


def duck_count(path):
    import duckdb

    return duckdb.connect().sql(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]


# ---------------------------------------------------------------- the writer check


def write_check(args, want, stats_check=None, mem="4G", js=JS, duck=True, zpq_rows=True, expect_rc=0):
    """A `check` run: zpq writes `{F}` with `args` at every -j; see the module docstring."""

    def check(ctx):
        outs = []
        last = {}
        rss = 0
        for j in js:
            o = ctx["cdir"] / f"{ctx['name']}.j{j}.parquet"
            if o.exists():
                o.unlink()
            sql = bool(args) and args[0] == "--query"
            argv = ([ctx["bin"], "query"] + ([] if sql else [str(ctx["path"])])
                    + [a.replace("{F}", str(ctx["path"])) for a in args] + ["-j", str(j), "-o", str(o)])
            res = ctx["bounded"](argv, mem=mem)
            last = res
            rss = max(rss, res["rss_kb"] or 0)
            err = res["err"].decode(errors="replace")
            if res["rc"] != expect_rc or "panic:" in err:
                return False, f"-j{j}: rc {res['rc']} sig {res['sig']}: {err.strip()[-300:]!r}", res
            if expect_rc != 0:
                continue
            outs.append(o)
        last["rss_kb"] = rss
        if expect_rc != 0:
            return True, f"refused rc {expect_rc}: {err.strip().splitlines()[-1][:120] if err.strip() else ''}", last
        blobs = [open(o, "rb").read() for o in outs]
        for j, b in zip(js[1:], blobs[1:]):
            if b != blobs[0]:
                return False, f"NONDETERMINISTIC: -j{js[0]} ({len(blobs[0])} B) != -j{j} ({len(b)} B)", last
        o = outs[0]
        try:
            got = pq.read_table(o)
        except Exception as e:  # noqa: BLE001
            return False, f"PYARROW cannot read: {str(e)[:200]}", last
        w = want() if callable(want) else want
        d = compare_tables(w, got)
        if d:
            return False, f"PYARROW value mismatch: {d}", last
        if duck:
            try:
                d = compare_tables(w, duck_table(o), exact_bits=False)
            except Exception as e:  # noqa: BLE001
                d = f"cannot read: {str(e)[:200]}"
                if "Recursion level in ArrowSchema" in str(e):
                    # DuckDB reads the file but cannot export so deep a type to Arrow: compare the row count only.
                    d = None if duck_count(o) == w.num_rows else f"count {duck_count(o)} != {w.num_rows}"
            if d:
                return False, f"DUCKDB {d}", last
        res = ctx["bounded"]([ctx["bin"], "query", str(o), "--scan-all", "--aggregate", "count(*) AS n"], mem=mem)
        if res["rc"] != 0 or res["out"].strip() != f'{{"n":{w.num_rows}}}'.encode():
            return False, f"ZPQ readback count: rc {res['rc']} {res['out'][:80]!r} {res['err'][-200:]!r}", last
        if zpq_rows:
            res = ctx["bounded"]([ctx["bin"], "query", str(o), "--format", "csv"], mem=mem)
            if res["rc"] != 0:
                return False, f"ZPQ readback rows: rc {res['rc']} {res['err'][-200:]!r}", last
        if stats_check:
            msg = stats_check(o, pq.ParquetFile(o).metadata)
            if msg:
                return False, f"STATS {msg}", last
        for p in outs[1:]:
            p.unlink()
        return True, f"ok ({len(blobs[0])} B, {len(js)} -j identical, pyarrow+duckdb+zpq agree)", last

    return check


def wrun(label, args, want, **kw):
    return run(label, [], E("eq"), check=write_check(args, want, **kw))


def pq_bytes(t, **kw):
    b = io.BytesIO()
    pq.write_table(t, b, **kw)
    return b.getvalue()


def filt(t, mask):
    return t.filter(pa.array(mask))


# ================================================================ 1. pathological shapes

B11, B12, B13 = "2.1.1 deep nesting", "2.1.2 50,000 columns", "2.1.3 zero rows / all NULL"


def deep_struct_type(depth):
    t = pa.int64()
    for i in range(depth):
        t = pa.struct([pa.field(f"f{depth - i}", t)])
    return t


def deep_struct_value(depth, row):
    """Row r: NULL at depth (r % (depth+2)) (0 = the column itself), else a leaf value of r."""
    cut = row % (depth + 2)
    def build(level):
        if level == cut:
            return None
        if level == depth:
            return row
        return {f"f{level + 1}": build(level + 1)}
    return build(0)


@case("w_deep_struct_64", B11)
def _():
    # A 64-deep struct (no repetition): def levels to 64, so the level bit width is 7. Re-encoded by a filter.
    depth, n = 64, 300
    t = pa.table({"id": pa.array(range(n), pa.int64()),
                  "s": pa.array([deep_struct_value(depth, r) for r in range(n)], deep_struct_type(depth))})
    return {"file": pq_bytes(t), "runs": [
        wrun("filter-write", ["--filter", "id NOT IN (1,4,7)"],
             lambda: filt(t, [r not in (1, 4, 7) for r in range(n)])),
        wrun("copy", [], t),
        wrun("select-refused", ["--select", "id, s"], t, expect_rc=2),
        wrun("sql-select", ["--query", "SELECT id, s FROM '{F}'"], t),
        wrun("sql-where", ["--query", "SELECT id, s FROM '{F}' WHERE id > 3"], lambda: t.slice(4)),
    ]}


def deep_list_type(depth):
    t = pa.int64()
    for i in range(depth):
        t = pa.list_(pa.struct([pa.field("a", pa.int64()), pa.field("x", t)])) if i % 2 else pa.list_(t)
    return t


def deep_list_value(depth, row, rng):
    """Each row descends to a random cut depth, where it ends in NULL, [] or [NULL] (or the leaf at full depth);
    near the root lists hold two entries, and struct entries are sometimes NULL."""
    cut = depth if row % 4 == 0 else rng.randrange(depth)
    kind = rng.randrange(3)

    def build(level):
        if level == depth:
            return None if row % 8 == 4 else row
        if level == cut:
            return (None, [], [None])[kind]
        k = 2 if level < 3 else 1
        if (depth - level - 1) % 2:  # this level holds structs
            return [{"a": row + level, "x": build(level + 1)} if i == 0 or rng.random() < 0.7 else None
                    for i in range(k)]
        return [build(level + 1) for _ in range(k)]
    return build(0)


@case("w_deep_list_64", B11)
def _():
    # LIST<STRUCT<a, LIST<...>>> alternating, 64 levels: empty lists, [NULL], NULL structs at every depth. zpq cannot
    # re-encode repeated columns: a copy must round-trip and a filter must be refused (exit 2).
    depth, n = 64, 60
    rng = random.Random(64)
    ty = deep_list_type(depth)
    t = pa.table({"id": pa.array(range(n), pa.int64()), "l": pa.array([deep_list_value(depth, r, rng)
                                                                         for r in range(n)], ty)})
    return {"file": pq_bytes(t), "runs": [
        wrun("copy", [], t),
        wrun("columns-copy", ["--columns", "id,l"], t),
        wrun("filter-refused", ["--filter", "id > 3"], t, expect_rc=2),
        run("agg-leaf", ["query", "{F}", "--aggregate", "count(*) AS n"], E("eq", [{"n": n}])),
    ]}


@case("w_deep_struct_199", B11)
def _():
    # The schema depth cap is 200 (metadata.zig:162): 199 struct levels plus the root.
    depth, n = 198, 50
    t = pa.table({"id": pa.array(range(n), pa.int64()),
                  "s": pa.array([deep_struct_value(depth, r) for r in range(n)], deep_struct_type(depth))})
    return {"file": pq_bytes(t), "runs": [wrun("filter-write", ["--scan-all", "--filter", "id >= 1"],
                                               lambda: filt(t, [r >= 1 for r in range(n)])),
                                          wrun("copy", [], t)]}


@case("w_wide_50k", B12, big=True)
def _():
    ncols, n = 50_000, 10
    cols = {f"c{i}": pa.array([i * 100 + r if (i + r) % 7 else None for r in range(n)], pa.int64())
            for i in range(ncols)}
    t = pa.table(cols)

    def gen(path):
        pq.write_table(t, path, compression="snappy")

    def footer_check(o, md):
        b = open(o, "rb").read()
        flen = struct.unpack("<I", b[-8:-4])[0]
        return None if md.num_columns == ncols else f"{md.num_columns} columns (footer {flen} B)"

    return {"file": gen, "cleanup": True, "runs": [
        wrun("copy", [], t, mem="8G", stats_check=footer_check),
        wrun("filter-write", ["--filter", "c0 IS NOT NULL"], lambda: filt(t, [r % 7 != 0 for r in range(n)]),
             mem="8G", stats_check=footer_check),
        run("agg", ["query", "{F}", "--aggregate", "count(*) AS n, sum(c49999) AS s"],
            E("eq", [{"n": n, "s": sum(49999 * 100 + r for r in range(n) if (49999 + r) % 7)}]), mem="8G"),
    ]}


def empty_table():
    return pa.table({"a": pa.array([], pa.int64()), "s": pa.array([], pa.string()), "d": pa.array([], pa.float64())})


@case("w_zero_row_groups", B13)
def _():
    # A file with a schema and no row group at all.
    schema = [T.schema_elem("schema", num_children=3), T.schema_elem("a", ty=T.INT64, rep=T.OPTIONAL),
              T.schema_elem("s", ty=T.BYTE_ARRAY, rep=T.OPTIONAL, converted=T.CT_UTF8),
              T.schema_elem("d", ty=T.DOUBLE, rep=T.OPTIONAL)]
    fmd = T.file_meta(schema, [], 0)
    b = T.finish(T.MAGIC, fmd)
    return {"file": b, "runs": [
        wrun("copy", [], empty_table()),
        wrun("filter-write", ["--filter", "a > 0"], empty_table()),
        wrun("select-write", ["--select", "a, s, d"], empty_table()),
        run("agg", ["query", "{F}", "--aggregate", "count(*) AS n, sum(a) AS s, max(d) AS m"],
            E("eq", [{"n": 0, "s": None, "m": None}])),
        run("agg-write", ["query", "{F}", "--aggregate", "count(*) AS n, sum(a) AS s", "-o", "{O}"],
            E("eq", [{"n": 0, "s": None}])),
    ]}


@case("w_one_row_group_zero_rows", B13)
def _():
    t = empty_table()
    return {"file": pq_bytes(t), "runs": [
        wrun("copy", [], t), wrun("filter-write", ["--filter", "a > 0"], t),
        run("count", ["query", "{F}", "--aggregate", "count(*) AS n"], E("eq", [{"n": 0}]))]}


@case("w_filter_matches_nothing", B13)
def _():
    t = pa.table({"a": pa.array(range(1000), pa.int64()), "s": pa.array([str(i) for i in range(1000)]),
                  "d": pa.array([float(i) for i in range(1000)])})
    return {"file": pq_bytes(t, row_group_size=300), "runs": [
        wrun("filter-none", ["--filter", "a < 0"], lambda: t.slice(0, 0)),
        wrun("filter-none-scanall", ["--scan-all", "--filter", "s = 'zzz'"], lambda: t.slice(0, 0)),
        wrun("limit0", ["--limit", "0"], lambda: t.slice(0, 0))]}


@case("w_single_row", B13)
def _():
    t = pa.table({"a": pa.array([7], pa.int64()), "s": pa.array(["x"]), "d": pa.array([float("nan")])})
    return {"file": pq_bytes(t), "runs": [wrun("filter", ["--filter", "a = 7"], t), wrun("copy", [], t)]}


def all_null_check(cols, n):
    def check(o, md):
        for rg in range(md.num_row_groups):
            for ci in range(md.num_columns):
                c = md.row_group(rg).column(ci)
                if c.path_in_schema not in cols:
                    continue
                st = c.statistics
                if st is None:
                    return f"{c.path_in_schema}: no statistics"
                if st.null_count != md.row_group(rg).num_rows:
                    return f"{c.path_in_schema}: null_count {st.null_count} != {md.row_group(rg).num_rows}"
                if st.has_min_max:
                    return f"{c.path_in_schema}: an all-NULL chunk has min/max {st.min!r}/{st.max!r}"
        return None
    return check


@case("w_all_null_pages", B13)
def _():
    n = 5000
    t = pa.table({"k": pa.array(range(n), pa.int64()), "i": pa.array([None] * n, pa.int64()),
                  "s": pa.array([None] * n, pa.string()), "d": pa.array([None] * n, pa.float64()),
                  "f": pa.array([None] * n, pa.float32()), "b": pa.array([None] * n, pa.bool_()),
                  "dec": pa.array([None] * n, pa.decimal128(20, 3)), "ts": pa.array([None] * n, pa.timestamp("us"))})
    return {"file": pq_bytes(t, row_group_size=2000), "runs": [
        wrun("filter-write", ["--scan-all", "--filter", "k >= 0"], t,
             stats_check=all_null_check({"i", "s", "d", "f", "b", "dec", "ts"}, n)),
        wrun("select-write", ["--select", "k, i, s, d, f, b, dec, ts"], t,
             stats_check=all_null_check({"i", "s", "d", "f", "b", "dec", "ts"}, n)),
        run("isnull", ["query", "{F}", "--filter", "i IS NULL", "--aggregate", "count(*) AS n"],
            E("eq", [{"n": n}]))]}


# ================================================================ 2. encoding & dictionary blowups

B21, B22, B23 = "2.2.1 mid-page dictionary explosion", "2.2.2 500 MB single value", "2.2.3 incompressible entropy"


def uuidish(rng, k):
    return "".join(rng.choice("0123456789abcdef") for _ in range(k))


@case("w_dict_explosion", B21)
def _():
    # One row group: 100,000 identical strings then 100,000 unique 1 KiB strings (~100 MB). The writer decides the
    # dictionary per chunk, so the chunk must come out PLAIN/delta, value-identical.
    rng = random.Random(5)
    vals = ["same-value"] * 100_000 + [uuidish(rng, 32) * 32 for _ in range(100_000)]
    t = pa.table({"id": pa.array(range(len(vals)), pa.int64()), "s": pa.array(vals)})

    def gen(path):
        pq.write_table(t, path, row_group_size=len(vals), compression="zstd")

    return {"file": gen, "cleanup": True, "runs": [
        wrun("filter-write", ["--scan-all", "--filter", "id >= 0"], t, mem="8G", duck=True),
        wrun("filter-write-1g", ["--scan-all", "--filter", "id >= 0", "--max-memory", "256MB"], t, mem="8G"),
        wrun("filter-half", ["--filter", "id >= 50000"], lambda: t.slice(50_000), mem="8G")]}


@case("w_dict_explosion_reversed", B21)
def _():
    rng = random.Random(6)
    vals = [uuidish(rng, 32) * 32 for _ in range(100_000)] + ["same-value"] * 100_000
    t = pa.table({"id": pa.array(range(len(vals)), pa.int64()), "s": pa.array(vals)})

    def gen(path):
        pq.write_table(t, path, row_group_size=len(vals), compression="zstd")

    return {"file": gen, "cleanup": True, "runs": [wrun("filter-write", ["--scan-all", "--filter", "id >= 0"], t, mem="8G")]}


@case("w_dict_int_float_boundary", B21)
def _():
    # Int and float dictionaries are kept at <= num_present/10 entries: exactly at, and one past, the limit.
    n = 10_000
    t = pa.table({"i_at": pa.array([r % 1000 for r in range(n)], pa.int64()),
                  "i_past": pa.array([r % 1001 for r in range(n)], pa.int64()),
                  "f_at": pa.array([float(r % 1000) + 0.5 for r in range(n)], pa.float64()),
                  "f_past": pa.array([float(r % 1001) + 0.5 for r in range(n)], pa.float32())})
    return {"file": pq_bytes(t, row_group_size=n), "runs": [wrun("select-write", ["--select", "i_at, i_past, f_at, f_past"], t)]}


@case("w_huge_value_500mb", B22, big=True, known="T5")
def _():
    size = 500_000_000
    head = b'{"blob":0,"x":"'

    def want():
        return pa.table({"id": pa.array([0], pa.int64()),
                         "j": pa.array([head + b"x" * (size - len(head) - 2) + b'"}'], pa.large_binary())})

    def gen(path):
        pq.write_table(want(), path, compression="zstd", use_dictionary=False)

    def footer(o, md):
        b = open(o, "rb")
        b.seek(-8, 2)
        flen = struct.unpack("<I", b.read(4))[0]
        return None if flen < 100_000_000 else f"footer {flen} B: full min/max of the 500 MB value"

    return {"file": gen, "cleanup": True, "runs": [
        wrun("filter-write", ["--scan-all", "--filter", "id >= 0"], want, mem="14G", js=(1,), stats_check=footer, zpq_rows=False)]}


def entropy_table(n=20_000, seed=9):
    rng = random.Random(seed)
    return pa.table({"id": pa.array(range(n), pa.int64()),
                     "blob": pa.array([rng.randbytes(1024) for _ in range(n)], pa.binary()),
                     "d": pa.array([struct.unpack("<d", rng.randbytes(8))[0] for _ in range(n)], pa.float64()),
                     "i": pa.array([rng.getrandbits(63) for _ in range(n)], pa.int64())})


def entropy_case(codec, extra=()):
    t = entropy_table()

    def ratio_check(o, md):
        # Incompressible pages may come out larger than raw; they must still be framed right (read back above).
        return None

    return {"file": pq_bytes(t, compression="none", row_group_size=8000), "runs": [
        wrun(f"{codec}", ["--codec", codec, "--filter", "id >= 0"] + list(extra), t, stats_check=ratio_check),
        wrun(f"{codec}-copy", ["--codec", codec] + list(extra), t)]}


for _codec in ("snappy", "zstd", "gzip", "lz4", "uncompressed"):
    case(f"w_entropy_{_codec}", B23)(lambda c=_codec: entropy_case(c))
case("w_entropy_zstd_speed", B23)(lambda: entropy_case("zstd", ("--compress-priority", "speed")))


# ================================================================ 3. floating point & sort-order statistics

B31, B32, B33 = "2.3.1 IEEE-754 minefield", "2.3.2 unsigned byte-array order", "2.3.3 statistics truncation"

F64 = {"qnan": 0x7FF8000000000000, "snan": 0x7FF0000000000001, "snan_payload": 0x7FF4DEADBEEF0001,
       "neg_qnan": 0xFFF8000000000000, "neg_snan": 0xFFF0000000000123, "qnan_payload": 0x7FF8000000C0FFEE}
F32 = {"qnan": 0x7FC00000, "snan": 0x7F800001, "snan_payload": 0x7FA0BEEF, "neg_qnan": 0xFFC00000,
       "neg_snan": 0xFF800123, "qnan_payload": 0x7FC0C0FE}


def f64(bits):
    return struct.unpack("<d", struct.pack("<Q", bits))[0]


def f32_array(bit_list):
    """float32 array from exact bit patterns (None = NULL), without a round trip through Python floats."""
    import numpy as np

    raw = np.array([0 if b is None else b for b in bit_list], dtype=np.uint32).view(np.float32)
    mask = np.array([b is None for b in bit_list])
    return pa.array(raw, pa.float32(), mask=mask)


def f32_bits(v):
    return struct.unpack("<I", struct.pack("<f", v))[0]


SPECIALS64 = [0.0, -0.0, math.inf, -math.inf, 1.5, -2.5] + [f64(b) for b in F64.values()]
SPECIALS32 = [f32_bits(x) for x in (0.0, -0.0, math.inf, -math.inf, 1.5, -2.5)] + list(F32.values())


def float_rg_layout():
    """Row groups of 8 rows each with a distinct float shape; returns (f64 values, f32 bit patterns, labels)."""
    groups = [
        ("all_nan", [f64(F64["qnan"])] * 8, [F32["qnan"]] * 8),
        ("all_snan", [f64(F64["snan_payload"])] * 8, [F32["snan_payload"]] * 8),
        ("zeros", [-0.0, 0.0] * 4, [f32_bits(-0.0), f32_bits(0.0)] * 4),
        ("neg_zero_only", [-0.0] * 8, [f32_bits(-0.0)] * 8),
        ("pos_zero_only", [0.0] * 8, [f32_bits(0.0)] * 8),
        ("inf_and_nan", [math.inf, -math.inf, f64(F64["neg_qnan"]), 1.0] * 2,
         [f32_bits(math.inf), f32_bits(-math.inf), F32["neg_qnan"], f32_bits(1.0)] * 2),
        ("mixed", SPECIALS64[:8], SPECIALS32[:8]),
        ("mixed2", SPECIALS64[4:12], SPECIALS32[4:12]),
        ("nulls_and_nan", [None, f64(F64["snan"]), None, -0.0, None, 0.0, None, f64(F64["qnan_payload"])],
         [None, F32["snan"], None, f32_bits(-0.0), None, f32_bits(0.0), None, F32["qnan_payload"]]),
    ]
    return groups


def ieee_table():
    groups = float_rg_layout()
    d, f, g = [], [], []
    for name, dv, fv in groups:
        d += dv
        f += fv
        g += [name] * len(dv)
    return pa.table({"id": pa.array(range(len(d)), pa.int64()), "g": pa.array(g), "d": pa.array(d, pa.float64()),
                     "f": f32_array(f)})


def ieee_stats_check(o, md):
    """Per row group of 8: written float stats obey the spec and zpq's NaN policy."""
    t = pq.read_table(o)
    groups = float_rg_layout()
    # zpq may merge row groups; check each chunk against the values it holds.
    start = 0
    raw = open(o, "rb").read()
    _, fmd, _ = T.read_file(raw)
    for rgi, rg in enumerate(fmd.get(T.FMD_ROW_GROUPS).items):
        n = rg.get(T.RG_NUM_ROWS)
        for ci, c in enumerate(rg.get(T.RG_COLUMNS).items):
            m = c.get(T.CC_META)
            ty = m.get(T.CMD_TYPE)
            if ty not in (T.FLOAT, T.DOUBLE):
                continue
            name = m.get(T.CMD_PATH).items[0].decode()
            vals = t.column(name).to_pylist()[start:start + n]
            if ty == T.FLOAT:
                bits = f32_bits_column(t.column(name).slice(start, n))
                vals = [None if b is None else struct.unpack("<f", struct.pack("<I", b))[0] for b in bits]
            present = [v for v in vals if v is not None]
            nans = sum(1 for v in present if math.isnan(v))
            nonnan = [v for v in present if not math.isnan(v)]
            st = m.get(T.CMD_STATS)
            if st is None:
                return f"rg{rgi} {name}: no statistics"
            if st.get(T.ST_NAN) is not None and st.get(T.ST_NAN) != nans:
                return f"rg{rgi} {name}: nan_count {st.get(T.ST_NAN)} != {nans}"
            if st.get(T.ST_NAN) is None:
                return f"rg{rgi} {name}: no nan_count (zpq writes one for every float chunk it encodes)"
            if st.get(T.ST_NULLS) != n - len(present):
                return f"rg{rgi} {name}: null_count {st.get(T.ST_NULLS)} != {n - len(present)}"
            fmt = "<f" if ty == T.FLOAT else "<d"
            mn, mx = st.get(T.ST_MIN_VALUE), st.get(T.ST_MAX_VALUE)
            if not nonnan:
                if mn is not None or mx is not None:
                    dec = [struct.unpack(fmt, x)[0] for x in (mn, mx) if x is not None]
                    if any(not math.isnan(x) for x in dec):
                        return f"rg{rgi} {name}: all-NaN/NULL chunk has non-NaN bounds {dec}"
                    # Writers that emit NaN bounds for all-NaN chunks: legal only with nan_count; record it.
                continue
            if mn is None or mx is None:
                return f"rg{rgi} {name}: missing bounds for non-NaN values"
            lo, hi = struct.unpack(fmt, mn)[0], struct.unpack(fmt, mx)[0]
            if math.isnan(lo) or math.isnan(hi):
                return f"rg{rgi} {name}: NaN in bounds {lo}/{hi} with non-NaN values present"
            if lo > min(nonnan) or hi < max(nonnan):
                return f"rg{rgi} {name}: bounds {lo}/{hi} do not cover [{min(nonnan)}, {max(nonnan)}]"
            if lo == 0 and math.copysign(1, lo) > 0:
                return f"rg{rgi} {name}: zero min written as +0.0 (spec: write -0.0)"
            if hi == 0 and math.copysign(1, hi) < 0:
                return f"rg{rgi} {name}: zero max written as -0.0 (spec: write +0.0)"
            # The deprecated pair must not be written for floats (or must match).
            for dep, typed in ((T.ST_MIN, mn), (T.ST_MAX, mx)):
                if st.get(dep) is not None and st.get(dep) != typed:
                    return f"rg{rgi} {name}: deprecated bound differs from typed bound"
        start += n
    return None


def ieee_queries():
    """(label, filter, predicate over (d, f-as-float))."""
    def nan(x):
        return x is not None and math.isnan(x)
    return [
        ("d_eq_0", "d = 0.0", lambda d, f: d is not None and not nan(d) and d == 0),
        ("d_eq_neg0", "d = -0.0", lambda d, f: d is not None and not nan(d) and d == 0),
        ("f_eq_0", "f = 0.0", lambda d, f: f is not None and not nan(f) and f == 0),
        ("d_gt_1", "d > 1.0", lambda d, f: d is not None and (nan(d) or d > 1)),
        ("f_ge_inf", "f >= 1e39", lambda d, f: f is not None and (nan(f) or f == math.inf)),
        ("d_lt_0", "d < 0.0", lambda d, f: d is not None and not nan(d) and d < 0),
        ("d_le_neg0", "d <= -0.0", lambda d, f: d is not None and not nan(d) and d <= 0),
    ]


@case("w_ieee754_minefield", B31)
def _():
    t = ieee_table()
    rows = t.to_pylist()
    fbits = f32_bits_column(t.column("f"))
    fv = [None if b is None else struct.unpack("<f", struct.pack("<I", b))[0] for b in fbits]
    runs = [wrun("filter-write", ["--scan-all", "--filter", "id >= 0"], t, stats_check=ieee_stats_check),
            wrun("select-write", ["--select", "id, g, d, f"], t, stats_check=ieee_stats_check),
            wrun("copy", [], t)]
    # Pushdown on a zpq-written file must not skip a page that a ±0 / NaN row satisfies: query the -j1 output.
    for label, fl, pred in ieee_queries():
        want = [{"id": r["id"]} for r, f in zip(rows, fv) if pred(r["d"], f)]
        runs.append(run(f"zpqout-{label}", ["query", "{O}", "--filter", fl, "--columns", "id"], E("eq", want),
                        needs_out=["--scan-all", "--filter", "id >= 0"]))
    return {"file": pq_bytes(t, row_group_size=8, write_statistics=False), "runs": runs}


@case("w_ieee754_dictionary_path", B31)
def _():
    # Many repeats so the float dictionary is taken (<= num_present/10 entries): stats come from the dictionary path.
    base = SPECIALS64 * 100
    t = pa.table({"id": pa.array(range(len(base)), pa.int64()), "d": pa.array(base, pa.float64()),
                  "f": f32_array(SPECIALS32 * 100)})
    return {"file": pq_bytes(t, row_group_size=len(base)), "runs": [
        wrun("filter-write", ["--scan-all", "--filter", "id >= 0"], t, stats_check=ieee_stats_check)]}


def bytes_order_check(cols):
    def check(o, md):
        t = pq.read_table(o)
        start = 0
        for rg in range(md.num_row_groups):
            n = md.row_group(rg).num_rows
            for ci in range(md.num_columns):
                c = md.row_group(rg).column(ci)
                if c.path_in_schema not in cols:
                    continue
                vals = [v for v in t.column(c.path_in_schema).to_pylist()[start:start + n] if v is not None]
                vals = [v.encode() if isinstance(v, str) else v for v in vals]
                st = c.statistics
                if not vals:
                    continue
                if not st or not st.has_min_max:
                    return f"{c.path_in_schema}: no bounds"
                mn = st.min.encode() if isinstance(st.min, str) else st.min
                mx = st.max.encode() if isinstance(st.max, str) else st.max
                if mn != min(vals) or mx != max(vals):
                    return f"{c.path_in_schema} rg{rg}: bounds {mn[:12]!r}/{mx[:12]!r} != unsigned {min(vals)[:12]!r}/{max(vals)[:12]!r}"
            start += n
        return None
    return check


@case("w_bytes_unsigned_order", B32)
def _():
    utf = ["a", "\u0080", "ÿ", "été", "z", "\U0001F600", "", "A", "߿", "~"]
    binv = [b"\x7f", b"\x80", b"\xff", b"\x00", b"\xfe\xff", b"\x80\x00", b"", b"\x01", b"\xff\xff\xff", b"a"]
    t = pa.table({"id": pa.array(range(10), pa.int64()), "u": pa.array(utf), "b": pa.array(binv, pa.binary()),
                  "fx": pa.array([bytes([x, 255 - x]) for x in (0, 127, 128, 255, 1, 200, 129, 126, 64, 192)],
                                 pa.binary(2))})
    big = pa.concat_tables([t] * 20)
    big = big.set_column(0, "id", pa.array(range(big.num_rows), pa.int64()))
    q = [("u_gt", "u > '\u007f'", [r["id"] for r in big.to_pylist() if r["u"].encode() > b"\x7f"])]
    runs = [wrun("filter-write", ["--scan-all", "--filter", "id >= 0"], t, stats_check=bytes_order_check({"u", "b", "fx"})),
            wrun("filter-write-dict", ["--scan-all", "--filter", "id >= 0"], big, stats_check=bytes_order_check({"u", "b", "fx"}))]
    return {"file": pq_bytes(t), "runs": runs}


@case("w_bytes_unsigned_order_dict", B32)
def _():
    utf = ["a", "\u0080", "ÿ", "été", "z", "\U0001F600", "", "A", "߿", "~"] * 100
    binv = [b"\x7f", b"\x80", b"\xff", b"\x00", b"\xfe\xff", b"\x80\x00", b"", b"\x01", b"\xff\xff\xff", b"a"] * 100
    t = pa.table({"id": pa.array(range(1000), pa.int64()), "u": pa.array(utf), "b": pa.array(binv, pa.binary())})
    want_gt = [{"id": i} for i, u in enumerate(utf) if u.encode() > b"\x7f"]
    return {"file": pq_bytes(t, row_group_size=1000), "runs": [
        wrun("filter-write", ["--scan-all", "--filter", "id >= 0"], t, stats_check=bytes_order_check({"u", "b"})),
        run("zpqout-u-gt", ["query", "{O}", "--filter", "u > '\u007f'", "--columns", "id"], E("eq", want_gt),
            needs_out=["--scan-all", "--filter", "id >= 0"]),
        run("zpqout-u-eq-ff", ["query", "{O}", "--filter", "u = 'ÿ'", "--aggregate", "count(*) AS n"],
            E("eq", [{"n": 100}]), needs_out=["--scan-all", "--filter", "id >= 0"])]}


@case("w_stats_long_shared_prefix", B33)
def _():
    # Every value shares a 10 KiB prefix, then 'a' or 'b' (and one with 0xFF bytes at the cut point). zpq writes full
    # bounds (no truncation): they must be exact, and pushdown on the zpq output must find both kinds of row.
    pre = "p" * 10240
    vals = [pre + ("a" if i % 2 else "b") for i in range(200)]
    bins = [b"q" * 63 + b"\xff" * 2 + (b"\x01" if i % 2 else b"\x02") for i in range(200)]
    t = pa.table({"id": pa.array(range(200), pa.int64()), "s": pa.array(vals), "b": pa.array(bins, pa.binary())})
    return {"file": pq_bytes(t), "runs": [
        wrun("filter-write", ["--scan-all", "--filter", "id >= 0"], t, stats_check=bytes_order_check({"s", "b"})),
        run("zpqout-eq-b", ["query", "{O}", "--filter", f"s = '{pre}b'", "--aggregate", "count(*) AS n"],
            E("eq", [{"n": 100}]), needs_out=["--scan-all", "--filter", "id >= 0"]),
        run("zpqout-gt-a", ["query", "{O}", "--filter", f"s > '{pre}a'", "--aggregate", "count(*) AS n"],
            E("eq", [{"n": 100}]), needs_out=["--scan-all", "--filter", "id >= 0"])]}


def truncated_stats_file(max_bytes, exact):
    """A foreign file whose chunk max is a truncated prefix (64 bytes) — incremented or not — of the true max."""
    pre = "p" * 100
    vals = [pre + ("a" if i % 2 else "b") for i in range(100)]
    t = pa.table({"s": pa.array(vals)})
    b = pq_bytes(t, write_statistics=False)
    raw, fmd, fs = T.read_file(b)
    m = fmd.get(T.FMD_ROW_GROUPS).items[0].get(T.RG_COLUMNS).items[0].get(T.CC_META)
    st = T.Struct()
    st.set(T.ST_MIN_VALUE, (pre + "a")[:64].encode(), T.T_BIN).set(T.ST_MAX_VALUE, max_bytes, T.T_BIN)
    st.set(T.ST_NULLS, 0, T.T_I64).set(7, exact, T.T_TRUE).set(8, False, T.T_TRUE)
    m.set(T.CMD_STATS, st, T.T_STRUCT)
    return T.finish(raw[:fs], fmd), pre


@case("w_read_foreign_truncated_max_incremented", B33)
def _():
    # max truncated to 64 bytes and incremented ('p'*63 + 'q'): a correct writer's output; zpq must find every row.
    b, pre = truncated_stats_file(b"p" * 63 + b"q", False)
    return {"file": b, "runs": [
        run("eq-b", ["query", "{F}", "--filter", f"s = '{pre}b'", "--aggregate", "count(*) AS n"], E("eq", [{"n": 50}])),
        run("gt", ["query", "{F}", "--filter", f"s > '{pre}a'", "--aggregate", "count(*) AS n"], E("eq", [{"n": 50}])),
        run("max-trust", ["query", "{F}", "--trust-stats", "--aggregate", "max(s) AS m"], E("err|eq", [{"m": pre + "b"}])),
        run("max", ["query", "{F}", "--aggregate", "max(s) AS m"], E("eq", [{"m": pre + "b"}]))]}


@case("w_read_foreign_truncated_max_not_incremented", B33)
def _():
    # A buggy writer: max truncated to 64 bytes but NOT incremented, is_max_value_exact = false. The bound is below
    # the real max, but the file says it is inexact, so zpq must not prune on it.
    b, pre = truncated_stats_file(b"p" * 64, False)
    return {"file": b, "runs": [
        run("eq-b", ["query", "{F}", "--filter", f"s = '{pre}b'", "--aggregate", "count(*) AS n"], E("eq", [{"n": 50}])),
        run("gt", ["query", "{F}", "--filter", f"s > '{'p' * 64}'", "--aggregate", "count(*) AS n"],
            E("eq", [{"n": 100}]))]}


@case("w_read_foreign_truncated_max_all_ff", B33)
def _():
    # Truncation point holds 0xFF bytes: a correct writer cannot increment and must drop max (or keep it longer).
    # Here a buggy writer kept the 0xFF prefix as max with is_max_value_exact = false.
    vals = [b"\xff" * 64 + bytes([i]) for i in range(1, 50)]
    t = pa.table({"s": pa.array(vals, pa.binary())})
    raw, fmd, fs = T.read_file(pq_bytes(t, write_statistics=False))
    m = fmd.get(T.FMD_ROW_GROUPS).items[0].get(T.RG_COLUMNS).items[0].get(T.CC_META)
    st = T.Struct()
    st.set(T.ST_MIN_VALUE, b"\xff" * 64, T.T_BIN).set(T.ST_MAX_VALUE, b"\xff" * 64, T.T_BIN).set(T.ST_NULLS, 0, T.T_I64)
    st.set(7, False, T.T_TRUE).set(8, False, T.T_TRUE)
    m.set(T.CMD_STATS, st, T.T_STRUCT)
    b = T.finish(raw[:fs], fmd)
    return {"file": b, "runs": [
        run("count-all", ["query", "{F}", "--aggregate", "count(s) AS n"], E("eq", [{"n": 49}])),
        run("ne", ["query", "{F}", "--filter", "s != 'x'", "--aggregate", "count(*) AS n"], E("eq", [{"n": 49}]))]}
