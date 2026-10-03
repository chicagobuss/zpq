#!/usr/bin/env python3
"""Differential test: ZPQ vs DuckDB, ROW-BY-ROW, across a matrix of
filter × projection query shapes.

Adopted from Hardwood's DifferentialReadTest: the bug class this catches is
*composition* defects — a filter or projection that's correct in isolation but
wrong once combined (e.g. filter + column subset + null column). Our existing
duckdb_smoke only compares aggregate scalars; this compares the actual decoded
rows ZPQ writes out against DuckDB's answer for the same query.

For each (filter, projection): ZPQ writes filtered+projected parquet; we read it
back and diff (order-independent, float/decimal tolerant) against DuckDB. DuckDB
is the oracle (same role Hardwood gives it).

Usage: .venv/bin/python tools/differential.py
"""
import duckdb, subprocess, os, sys, tempfile, json
import pyarrow as pa
import pyarrow.parquet as pq  # strict reader: DuckDB is lenient, pyarrow is not

ZPQ = "zig-out/bin/zpq"
# Physical column order of the fixture (gen_fixture below). `--columns` projects
# a subset in THIS order; `--select` projects in the requested order.
SCHEMA_ORDER = ["id", "amt", "price", "name", "nname", "d", "flag"]

# (label, zpq_filter | None, duckdb_where | None) — split because date literals
# differ (ZPQ '2020-06-01' vs DuckDB DATE '2020-06-01').
FILTERS = [
    ("none",          None,                     None),
    ("id_gt",         "id > 500",               "id > 500"),
    ("amt_lt",        "amt < 100.5",            "amt < 100.5"),
    ("price_gt",      "price > 100.50",         "price > 100.50"),
    ("name_like",     "name LIKE 'n1%'",        "name LIKE 'n1%'"),
    ("nname_isnull",  "nname IS NULL",          "nname IS NULL"),
    ("nname_notnull", "nname IS NOT NULL",      "nname IS NOT NULL"),
    ("flag_true",     "flag = true",            "flag = true"),
    ("id_in",         "id IN (1, 5, 999)",      "id IN (1, 5, 999)"),
    ("id_between",    "id BETWEEN 100 AND 200", "id BETWEEN 100 AND 200"),
    ("and",           "id > 500 AND name LIKE 'n9%'", "id > 500 AND name LIKE 'n9%'"),
    ("or",            "id < 100 OR id > 1900",  "id < 100 OR id > 1900"),
    ("date_ge",       "d >= '2020-06-01'",      "d >= DATE '2020-06-01'"),
    ("not_in",        "id NOT IN (1,2,3)",      "id NOT IN (1,2,3)"),
    ("notnull_and_like", "nname IS NOT NULL AND name LIKE 'n5%'", "nname IS NOT NULL AND name LIKE 'n5%'"),
    ("id_neg",        "id < 0",                 "id < 0"),
    ("amt_isnull",    "amt IS NULL",            "amt IS NULL"),
    ("flag_isnull",   "flag IS NULL",           "flag IS NULL"),
    ("empty_name",    "name = ''",              "name = ''"),
]

# (label, zpq_select_expr, duckdb_select_expr) — projection of COMPUTED columns,
# aliased to the same name on both sides. Exercises the expression evaluator +
# null propagation, which plain --columns doesn't.
SELECTS = [
    ("id_plus1",  "id + 1 AS x",            "id + 1 AS x"),
    ("concat",    "name || '_z' AS x",      "name || '_z' AS x"),
    ("coalesce",  "coalesce(nname, 'NA') AS x", "coalesce(nname, 'NA') AS x"),
    # Raw arithmetic over a NULLABLE column (e.g. `amt * 2`) is a documented
    # limitation: eval.zig returns NullableNotSupported (clean error, not wrong
    # answer). Coalesce IS null-aware.
]

# (label, zpq_agg, duckdb_select) — filter × aggregate, compared as scalars.
AGGS = [
    ("sum_count", "sum(id) AS s, count(id) AS c", "sum(id), count(id)"),
    ("minmax",    "min(amt) AS mn, max(amt) AS mx", "min(amt), max(amt)"),
    ("decimal_sum", "sum(price) AS s", "sum(price)"),
    # count(*) alone needs no column at all in a row group statistics prove fully matching.
    ("count_star", "count(*) AS n, sum(amt) AS s", "count(*), sum(amt)"),
]
# (label, columns_csv | None) — None = all columns (passthrough fast path).
PROJECTIONS = [
    ("all",        None),
    ("id_name",    ["id", "name"]),
    ("name_id",    ["name", "id"]),     # reordered
    ("nname_only", ["nname"]),          # the nullable column alone
    ("price_d",    ["price", "d"]),     # decimal + date
    ("flag_amt",   ["flag", "amt"]),
]


def gen_fixture(con, path, n=4000):
    # Adversarial on purpose: small ROW_GROUP_SIZE → multiple row groups (so the
    # RG-prune + cross-RG-merge paths run, not a single-RG happy path); negative
    # ids; nulls across numeric/bool (not just string); empty strings.
    con.execute(f"""COPY (SELECT
        (i - 2000)::INTEGER AS id,
        (CASE WHEN i % 11 = 0 THEN NULL ELSE (i - 2000) * 1.5 END)::DOUBLE AS amt,
        CAST(i AS DECIMAL(18,2)) AS price,
        (CASE WHEN i % 13 = 0 THEN '' ELSE 'n' || i END) AS name,
        (CASE WHEN i % 7 = 0 THEN NULL ELSE 'v' || i END) AS nname,
        (DATE '2020-01-01' + i::INTEGER) AS d,
        (CASE WHEN i % 5 = 0 THEN NULL ELSE (i % 3 = 0) END) AS flag
      FROM range(1, {n}) t(i)) TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE 500)""")


from decimal import Decimal


def norm(v):
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, float) and v != v:
        return "NaN"  # as zpq's jsonl prints it, and so NaN rows compare equal
    if isinstance(v, (float, Decimal)):
        return round(float(v), 4)
    return v


def rows_sorted(con, sql):
    rs = con.execute(sql).fetchall()
    return sorted([tuple(norm(c) for c in row) for row in rs], key=repr)


# --- statistics order: columns whose stats order differs from the signed order of their bytes ---
# (label, fixture, zpq_filter | None, oracle). Fixtures come from tools/gen_stats_order_fixtures.py. Each case diffs
# jsonl rows, written parquet, and aggregates. The oracle is a DuckDB WHERE clause, or, where DuckDB itself trusts
# statistics it should not, a Python predicate over every row decoded by pyarrow, which reads no statistics at all.
UNSIGNED_FIXTURE = "ci/fixtures/parquet/unsigned_order.parquet"
DEPRECATED_FIXTURE = "ci/fixtures/parquet/deprecated_stats.parquet"
COLUMN_ORDER_FIXTURE = "ci/fixtures/parquet/column_order.parquet"
NAN_FIXTURE = "ci/fixtures/parquet/nan_stats.parquet"
STATS_ORDER = [
    ("u_all",          UNSIGNED_FIXTURE, None,                          None),
    ("u32_ge_2^31",    UNSIGNED_FIXTURE, "u32 >= 2147483648",           "u32 >= 2147483648"),
    ("u32_eq_2^31",    UNSIGNED_FIXTURE, "u32 = 2147483648",            "u32 = 2147483648"),
    ("u32_in",         UNSIGNED_FIXTURE, "u32 IN (0, 4294967295)",      "u32 IN (0, 4294967295)"),
    ("u32_gt_neg",     UNSIGNED_FIXTURE, "u32 > -1",                    "u32 > -1"),
    ("u64_gt_2^63-1",  UNSIGNED_FIXTURE, "u64 > 9223372036854775807",   "u64 > 9223372036854775807"),
    ("u64_eq_max",     UNSIGNED_FIXTURE, "u64 = 18446744073709551615",  "u64 = 18446744073709551615"),
    ("u8_ge_128",      UNSIGNED_FIXTURE, "u8 >= 128",                   "u8 >= 128"),
    ("u16_between",    UNSIGNED_FIXTURE, "u16 BETWEEN 32768 AND 65535", "u16 BETWEEN 32768 AND 65535"),
    ("n32_gt_2^31-1",  UNSIGNED_FIXTURE, "n32 > 2147483647",            "n32 > 2147483647"),
    # DuckDB prunes these on the signed-ordered deprecated min/max and returns no rows.
    ("depr_s_eq",      DEPRECATED_FIXTURE, "s = 'a'",        lambda r: r["s"] == "a"),
    ("depr_s_lt",      DEPRECATED_FIXTURE, "s < 'b'",        lambda r: r["s"].encode() < b"b"),
    ("depr_dec_eq",    DEPRECATED_FIXTURE, "dec = 1.00",     lambda r: r["dec"] == Decimal("1.00")),
    ("depr_u_eq",      DEPRECATED_FIXTURE, "u = 1",          lambda r: r["u"] == 1),
    # Signed order is these columns' own order; DuckDB is right and the deprecated pair still prunes.
    ("depr_i_gt",      DEPRECATED_FIXTURE, "i > 100",        "i > 100"),
    ("depr_d_lt",      DEPRECATED_FIXTURE, "d < 1",          "d < 1"),
    # `s` declares a column order neither reader implements; DuckDB prunes on its bounds anyway.
    ("order_s_eq",     COLUMN_ORDER_FIXTURE, "s = 'B'",      lambda r: r["s"] == "B"),
    ("order_s_lt",     COLUMN_ORDER_FIXTURE, "s < 'a'",      lambda r: r["s"].encode() < b"a"),
    ("order_i_gt",     COLUMN_ORDER_FIXTURE, "i > 100",      "i > 100"),
    # Bounds leave NaN out of [0, 0]; DuckDB trusts them both ways, dropping NaN rows from `!= 0` and keeping them
    # for `= 0`.
    ("nan_ne",         NAN_FIXTURE,          "d != 0",       lambda r: r["d"] != 0),
    ("nan_eq",         NAN_FIXTURE,          "d = 0",        lambda r: r["d"] == 0),
]
# fixture → (zpq aggregate, DuckDB select, the same aggregate over pyarrow rows)
STATS_ORDER_AGGS = {
    UNSIGNED_FIXTURE: ("count(*) AS n, sum(u32) AS s32, min(u64) AS mn, max(u64) AS mx, sum(u64) AS s64",
                       "count(*), sum(u32), min(u64), max(u64), sum(u64)", None),
    DEPRECATED_FIXTURE: ("count(*) AS n, sum(i) AS si, min(dec) AS lo, max(dec) AS hi, max(u) AS mu",
                         "count(*), sum(i), min(dec), max(dec), max(u)",
                         lambda rows: [len(rows), sum(r["i"] for r in rows) if rows else None,
                                       min((r["dec"] for r in rows), default=None),
                                       max((r["dec"] for r in rows), default=None),
                                       max((r["u"] for r in rows), default=None)]),
    NAN_FIXTURE: ("count(*) AS n, sum(i) AS si", "count(*), sum(i)",
                  lambda rows: [len(rows), sum(r["i"] for r in rows) if rows else None]),
    COLUMN_ORDER_FIXTURE: ("count(*) AS n, sum(i) AS si, min(s) AS lo", "count(*), sum(i), min(s)",
                           lambda rows: [len(rows), sum(r["i"] for r in rows) if rows else None,
                                         min((r["s"].encode() for r in rows), default=b"").decode() or None]),
}


def declared_column_orders(path):
    """FileMetaData.column_orders as ColumnOrder union member ids (0 = an empty union), or None when absent."""
    from gen_stats_order_fixtures import Reader, T_STRUCT, field
    data = open(path, "rb").read()
    n = int.from_bytes(data[-8:-4], "little")
    orders = field(Reader(data[-8 - n:-8]).value(T_STRUCT), 7)
    return None if orders is None else [u[0][0] if u else 0 for u in orders[2][1]]


def stats_order_checks(con, tmp):
    """Rows (jsonl), written parquet, and aggregates per case. Returns (total, fails)."""
    total = fails = 0

    def report(ok, label, detail):
        nonlocal fails
        if not ok:
            fails += 1
        print(f"  {'OK  ' if ok else 'FAIL'}  stats-order:{label:28} {detail}")

    for label, fixture, zf, oracle in STATS_ORDER:
        cols = [d[0] for d in con.execute(f"SELECT * FROM '{fixture}' LIMIT 0").description]
        za, da, py_agg = STATS_ORDER_AGGS[fixture]
        if callable(oracle):
            truth = [r for r in pq.read_table(fixture).to_pylist() if oracle(r)]
            want = sorted([tuple(norm(r[c]) for c in cols) for r in truth], key=repr)
            want_agg = [norm(v) for v in py_agg(truth)]
            src = "truth"
        else:
            where = f" WHERE {oracle}" if oracle is not None else ""
            want = rows_sorted(con, f"SELECT {', '.join(cols)} FROM '{fixture}'{where}")
            want_agg = [norm(v) for v in con.execute(f"SELECT {da} FROM '{fixture}'{where}").fetchone()]
            src = "duck"
        filt = ["--filter", zf] if zf is not None else []

        total += 1
        r = subprocess.run([ZPQ, "query", fixture, "--format", "jsonl"] + filt, capture_output=True, text=True)
        if r.returncode != 0:
            report(False, f"{label} × jsonl", f"ZPQ error: {r.stderr.strip()[:70]}")
        else:
            got = sorted([tuple(norm(json.loads(l)[c]) for c in cols) for l in r.stdout.splitlines() if l.strip()],
                         key=repr)
            report(got == want, f"{label} × jsonl", f"zpq={len(got)} {src}={len(want)} rows")

        total += 1
        out = os.path.join(tmp, "zstats.parquet")
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", fixture, "-o", out] + filt, capture_output=True, text=True)
        if r.returncode != 0 or not os.path.exists(out):
            report(False, f"{label} × parquet", f"ZPQ error: {r.stderr.strip()[:70]}")
        else:
            tbl = pq.read_table(out)
            got = sorted([tuple(norm(row[c]) for c in cols) for row in tbl.to_pylist()], key=repr)
            report(got == want, f"{label} × parquet", f"zpq={len(got)} {src}={len(want)} rows")

        # Unfiltered aggregates may be answered from statistics under --trust-stats; check that path too.
        for flags in ([[]] if zf is not None else [[], ["--trust-stats"]]):
            total += 1
            tag = f"{label} × agg{' trust' if flags else ''}"
            cmd = [ZPQ, "query", fixture, "--aggregate", za] + filt + flags
            r = subprocess.run(cmd, capture_output=True, text=True)
            if r.returncode != 0:
                report(False, tag, f"ZPQ error: {r.stderr.strip()[:70]}")
                continue
            # An empty input sums to 0 in zpq and NULL in SQL; compare everything else exactly.
            zvals = [norm(v) for v in json.loads(r.stdout)["agg"].values()]
            ok = len(zvals) == len(want_agg) and all(
                z == w or (w is None and z in (0, None)) for z, w in zip(zvals, want_agg))
            report(ok, tag, f"zpq={zvals} {src}={want_agg}")

    # Aliased unsigned references keep their values through --select; arithmetic on UINT64 is refused, not wrapped.
    where = "u64 >= 9223372036854775806 AND i < 400"
    want = rows_sorted(con, f"SELECT u64 AS b, u32 AS c, u32 + 1 AS d FROM '{UNSIGNED_FIXTURE}' WHERE {where}")
    sel = ["--select", "u64 AS b, u32 AS c, u32 + 1 AS d", "--filter", where]
    for fmt in ("jsonl", "parquet"):
        total += 1
        out = os.path.join(tmp, "zsel_u.parquet")
        if os.path.exists(out):
            os.remove(out)
        cmd = [ZPQ, "query", UNSIGNED_FIXTURE] + sel + (["--format", "jsonl"] if fmt == "jsonl" else ["-o", out])
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            report(False, f"select_u64_alias × {fmt}", f"ZPQ error: {r.stderr.strip()[:70]}")
            continue
        if fmt == "jsonl":
            rows = [json.loads(l) for l in r.stdout.splitlines() if l.strip()]
        else:
            rows = pq.read_table(out).to_pylist()
        got = sorted([(row["b"], row["c"], row["d"]) for row in rows], key=repr)
        report(got == want, f"select_u64_alias × {fmt}", f"zpq={len(got)} duck={len(want)} rows")
    # zpq's own output declares TYPE_DEFINED_ORDER, so a strict reader trusts its unsigned and string bounds and its
    # pushdown still answers right; a byte copy keeps its source's declared orders.
    for olabel, args in [("reencode", ["--filter", "u32 >= 2147483648"]), ("bytecopy", []),
                         ("project", ["--columns", "u32,u64"])]:
        out = os.path.join(tmp, f"zorder_{olabel}.parquet")
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", UNSIGNED_FIXTURE, "-o", out] + args, capture_output=True, text=True)
        total += 1
        if r.returncode != 0:
            report(False, f"output_orders × {olabel}", f"ZPQ error: {r.stderr.strip()[:70]}")
            continue
        md = pq.ParquetFile(out).metadata
        usable = all(md.row_group(g).column(c).statistics.has_min_max
                     for g in range(md.num_row_groups) for c in range(md.num_columns)
                     if md.schema.column(c).name in ("u32", "u64"))
        rows = pq.read_table(out).to_pylist()
        pushed = [pq.read_table(out, filters=[("u64", ">=", pa.scalar(2**63, pa.uint64()))]).num_rows,
                  pq.read_table(out, filters=[("u32", "<", 2**31)]).num_rows]
        truth = [sum(r["u64"] >= 2**63 for r in rows), sum(r["u32"] < 2**31 for r in rows)]
        report(usable and pushed == truth, f"output_orders × {olabel}",
               f"has_min_max={usable} pushdown={pushed} truth={truth}")
    total += 1
    out = os.path.join(tmp, "zorder_str.parquet")
    r = subprocess.run([ZPQ, "query", DEPRECATED_FIXTURE, "-o", out, "--filter", "i > 0"], capture_output=True)
    md = pq.ParquetFile(out).metadata
    usable = all(md.row_group(g).column(0).statistics.has_min_max for g in range(md.num_row_groups))
    pushed = pq.read_table(out, filters=[("s", "==", "a")]).num_rows
    report(r.returncode == 0 and usable and pushed == 1, "output_orders × string",
           f"has_min_max={usable} s='a' -> {pushed}")

    # Inputs declaring different orders for one leaf: the merged output must declare one order true of every row
    # group (zpq re-encodes rather than byte-copying), never an empty union, and still prune right.
    normalized = os.path.join(tmp, "zorder_norm.parquet")
    mixed = os.path.join(tmp, "zorder_mixed.parquet")
    for p in (normalized, mixed):
        if os.path.exists(p):
            os.remove(p)
    subprocess.run([ZPQ, "query", COLUMN_ORDER_FIXTURE, "-o", normalized, "--filter", "i > -1"], capture_output=True)
    r = subprocess.run([ZPQ, "query", COLUMN_ORDER_FIXTURE, normalized, "-o", mixed], capture_output=True, text=True)
    total += 1
    if r.returncode != 0:
        report(False, "output_orders × mixed inputs", f"ZPQ error: {r.stderr.strip()[:70]}")
    else:
        orders = declared_column_orders(mixed)
        rows = pq.read_table(mixed).to_pylist()
        pushed = pq.read_table(mixed, filters=[("s", "==", "B")]).num_rows
        truth = sum(r["s"] == "B" for r in rows)
        report(0 not in (orders or []) and pushed == truth == 2, "output_orders × mixed inputs",
               f"column_orders={orders} s='B' -> {pushed} truth={truth}")

    total += 1
    r = subprocess.run([ZPQ, "query", UNSIGNED_FIXTURE, "--select", "u64 + 1 AS x", "--format", "jsonl"],
                       capture_output=True, text=True)
    report(r.returncode != 0 and "UINT64" in r.stderr, "select_u64_arith refused", r.stderr.strip()[:60])
    return total, fails


def main():
    con = duckdb.connect()
    tmp = tempfile.mkdtemp()
    fixture = os.path.join(tmp, "diff_fixture.parquet")
    gen_fixture(con, fixture)

    fails, total = 0, 0
    for flabel, zf, dw in FILTERS:
        for plabel, cols in PROJECTIONS:
            total += 1
            out = os.path.join(tmp, "zout.parquet")
            if os.path.exists(out):
                os.remove(out)
            cmd = [ZPQ, "query", fixture, "-o", out]
            if zf is not None:
                cmd += ["--filter", zf]
            if cols is not None:
                cmd += ["--columns", ",".join(cols)]
            r = subprocess.run(cmd, capture_output=True, text=True)
            label = f"{flabel:18} × {plabel:10}"
            if r.returncode != 0 or not os.path.exists(out):
                print(f"  FAIL  {label}  ZPQ error: {(r.stderr or r.stdout).strip()[:70]}")
                fails += 1
                continue
            # STRICT reader gate: pyarrow must accept ZPQ's output. DuckDB is
            # lenient (it tolerated the bit-width-0 corruption pyarrow rejects),
            # so a pure DuckDB diff gives false confidence. Also pins projection
            # COLUMN ORDER (read_table preserves physical schema order).
            try:
                tbl = pq.read_table(out)
            except Exception as e:
                print(f"  FAIL  {label}  pyarrow rejected ZPQ output: {str(e)[:55]}")
                fails += 1
                continue
            # `--columns` is a SUBSET selector: it preserves SCHEMA order by
            # design (projectSubset by index), NOT the requested order. Explicit
            # reordering is `--select`'s job (checked separately below). So the
            # expectation is the requested subset in schema order.
            if cols is not None:
                want_order = [c for c in SCHEMA_ORDER if c in cols]
                if list(tbl.column_names) != want_order:
                    print(f"  FAIL  {label}  column order: got {list(tbl.column_names)} want {want_order}")
                    fails += 1
                    continue
            # Value diff vs the DuckDB oracle. Order-independent (DuckDB doesn't
            # guarantee row order); row-order determinism is checked separately.
            canon = cols if cols is not None else \
                ["amt", "d", "flag", "id", "name", "nname", "price"]
            sel = ", ".join(canon)
            where = f" WHERE {dw}" if dw is not None else ""
            try:
                zrows = rows_sorted(con, f"SELECT {sel} FROM '{out}'")
                drows = rows_sorted(con, f"SELECT {sel} FROM '{fixture}'{where}")
            except Exception as e:
                print(f"  FAIL  {label}  compare error: {e}")
                fails += 1
                continue
            if zrows != drows:
                fails += 1
                # find first divergence to report
                diff = "row-count" if len(zrows) != len(drows) else "values"
                detail = f"zpq={len(zrows)} duck={len(drows)}"
                if diff == "values":
                    for i, (a, b) in enumerate(zip(zrows, drows)):
                        if a != b:
                            detail = f"row[{i}] zpq={a} duck={b}"
                            break
                print(f"  FAIL  {label}  [{diff}] {detail}")
            else:
                print(f"  OK    {label}  ({len(zrows)} rows)")
    # --- --select expressions × a couple filters ---
    for slabel, zs, ds in SELECTS:
        for flabel, zf, dw in [("none", None, None), ("id_gt", "id > 500", "id > 500")]:
            total += 1
            out = os.path.join(tmp, "zout.parquet")
            if os.path.exists(out):
                os.remove(out)
            cmd = [ZPQ, "query", fixture, "-o", out, "--select", zs]
            if zf is not None:
                cmd += ["--filter", zf]
            r = subprocess.run(cmd, capture_output=True, text=True)
            label = f"select:{slabel:10} × {flabel:6}"
            if r.returncode != 0 or not os.path.exists(out):
                print(f"  FAIL  {label}  ZPQ error: {(r.stderr or r.stdout).strip()[:70]}")
                fails += 1
                continue
            where = f" WHERE {dw}" if dw is not None else ""
            zrows = rows_sorted(con, f"SELECT x FROM '{out}'")
            drows = rows_sorted(con, f"SELECT {ds} FROM '{fixture}'{where}")
            if zrows != drows:
                fails += 1
                detail = f"zpq={len(zrows)} duck={len(drows)}" if len(zrows) != len(drows) else \
                    next(f"row zpq={a} duck={b}" for a, b in zip(zrows, drows) if a != b)
                print(f"  FAIL  {label}  {detail}")
            else:
                print(f"  OK    {label}  ({len(zrows)} rows)")

    # --- filter × aggregate (scalars, tolerant) ---
    for alabel, za, ds in AGGS:
        for flabel, zf, dw in [("none", None, None), ("id_gt", "id > 500", "id > 500"),
                               ("notnull", "nname IS NOT NULL", "nname IS NOT NULL"),
                               ("date_ge", "d >= '2020-06-01'", "d >= DATE '2020-06-01'")]:
            total += 1
            cmd = [ZPQ, "query", fixture, "--aggregate", za]
            if zf is not None:
                cmd += ["--filter", zf]
            r = subprocess.run(cmd, capture_output=True, text=True)
            label = f"agg:{alabel:11} × {flabel:8}"
            if r.returncode != 0:
                print(f"  FAIL  {label}  ZPQ error: {(r.stderr or r.stdout).strip()[:70]}")
                fails += 1
                continue

            try:
                zout = json.loads(r.stdout)
                zvals = [norm(v) for v in zout["agg"].values()]
                proven = zout.get("row_groups_full_match", 0)
            except Exception as e:
                print(f"  FAIL  {label}  bad json: {e}")
                fails += 1
                continue
            where = f" WHERE {dw}" if dw is not None else ""
            dvals = [norm(v) for v in con.execute(f"SELECT {ds} FROM '{fixture}'{where}").fetchone()]
            # tolerant scalar compare
            ok = len(zvals) == len(dvals) and all(
                (abs(float(a) - float(b)) <= 1e-4 * max(1.0, abs(float(b)))) if isinstance(b, (int, float)) and b is not None
                else str(a) == str(b)
                for a, b in zip(zvals, dvals))
            if ok:
                print(f"  OK    {label}  {zvals}  (full-match row groups: {proven})")
            else:
                fails += 1
                print(f"  FAIL  {label}  zpq={zvals} duck={dvals}")

    # --- GROUP BY × aggregate matrix ---
    GROUP_BYS = [
        ("gb_simple", "flag", "count(id) AS c", None, 
         "SELECT flag, count(id) AS c FROM '{fixture}' GROUP BY flag"),
        ("gb_arith", "id + 1 AS id_plus1", "sum(price) AS s", None, 
         "SELECT (id + 1) AS id_plus1, sum(price) AS s FROM '{fixture}' GROUP BY id_plus1"),
        ("gb_concat", "name || '_z' AS concat", "max(price) AS mx, min(price) AS mn", None, 
         "SELECT (name || '_z') AS concat, max(price) AS mx, min(price) AS mn FROM '{fixture}' GROUP BY concat"),
        ("gb_coalesce", "coalesce(nname, 'NA') AS coal", "count(id) AS c", "c,coal",
         "SELECT count(id) AS c, coalesce(nname, 'NA') AS coal FROM '{fixture}' GROUP BY coal"),
    ]
    for glabel, gby, agg, order, d_sql in GROUP_BYS:
        for flabel, zf, dw in [("none", None, None), ("id_gt", "id > 500", "id > 500")]:
            total += 1
            cmd = [ZPQ, "query", fixture, "--group-by", gby]
            if agg:
                cmd += ["--aggregate", agg]
            if order:
                cmd += ["--column-order", order]
            if zf is not None:
                cmd += ["--filter", zf]
            r = subprocess.run(cmd, capture_output=True, text=True)
            label = f"groupby:{glabel:15} × {flabel:6}"
            if r.returncode != 0:
                print(f"  FAIL  {label}  ZPQ error: {(r.stderr or r.stdout).strip()[:70]}")
                fails += 1
                continue

            try:
                zrows_raw = json.loads(r.stdout)["agg"]
            except Exception as e:
                print(f"  FAIL  {label}  bad json: {e}")
                fails += 1
                continue
            
            where = f" WHERE {dw}" if dw is not None else ""
            duck_query = d_sql.format(fixture=fixture)
            if where:
                parts = duck_query.split(" GROUP BY ")
                duck_query = f"{parts[0]}{where} GROUP BY {parts[1]}"
            
            try:
                duck_rel = con.execute(duck_query)
                col_names = [desc[0] for desc in duck_rel.description]
                drows = sorted([tuple(norm(c) for c in row) for row in duck_rel.fetchall()], key=repr)
                
                zrows_list = []
                for row_dict in zrows_raw:
                    row_tuple = tuple(norm(row_dict.get(c)) for c in col_names)
                    zrows_list.append(row_tuple)
                zrows = sorted(zrows_list, key=repr)
            except Exception as e:
                print(f"  FAIL  {label}  compare setup error: {e}")
                fails += 1
                continue
                
            if zrows != drows:
                fails += 1
                detail = f"zpq={len(zrows)} duck={len(drows)}" if len(zrows) != len(drows) else \
                    next(f"row zpq={a} duck={b}" for a, b in zip(zrows, drows) if a != b)
                print(f"  FAIL  {label}  {detail}")
            else:
                print(f"  OK    {label}  ({len(zrows)} rows)")
    # ZPQ must preserve INPUT row order through filter+write (Iceberg compaction /
    # S3-to-S3 passthrough expect determinism; parallel per-RG workers must not
    # scramble). Compared in FILE order vs the input filtered in file order — this
    # can't go through DuckDB (SQL gives no order guarantee), so it's a separate
    # ZPQ-vs-input check, not a ZPQ-vs-DuckDB one.
    total += 1
    out = os.path.join(tmp, "zord.parquet")
    if os.path.exists(out):
        os.remove(out)
    subprocess.run([ZPQ, "query", fixture, "-o", out, "--filter", "id > 0", "--columns", "id"],
                   capture_output=True)
    zt = pq.read_table(out).column("id").to_pylist()
    it = pq.read_table(fixture).column("id").to_pylist()
    expected = [v for v in it if v is not None and v > 0]
    if zt == expected:
        print(f"  OK    row-order preserved (filter, {len(zt)} rows in input order)")
    else:
        fails += 1
        d = next((i for i, (a, b) in enumerate(zip(zt, expected)) if a != b), None)
        extra = f" first diff @ {d}: {zt[d]} vs {expected[d]}" if d is not None else ""
        print(f"  FAIL  row-order: zpq={len(zt)} expected={len(expected)}{extra}")

    # --- --select honors explicit column ORDER (the reorder mechanism) ---
    total += 1
    out = os.path.join(tmp, "zsel.parquet")
    if os.path.exists(out):
        os.remove(out)
    subprocess.run([ZPQ, "query", fixture, "-o", out,
                    "--select", "name AS name, id AS id"], capture_output=True)
    got = list(pq.read_table(out).column_names)
    if got == ["name", "id"]:
        print("  OK    --select honors column order (name, id)")
    else:
        fails += 1
        print(f"  FAIL  --select column order: got {got} want ['name', 'id']")

    # No nested (struct/list/map) coverage here. ZPQ decodes nested but rejects
    # nested re-encode, so a write-based differential cannot exercise it. Nested
    # read coverage belongs in a read/aggregate harness.

    so_total, so_fails = stats_order_checks(con, tmp)
    total += so_total
    fails += so_fails

    print(f"\ndifferential: {total - fails}/{total} checks match (strict pyarrow read + DuckDB value diff + order)")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
