#!/usr/bin/env python3
"""Differential test: ZPQ vs DuckDB, ROW-BY-ROW, across a matrix of
filter × projection query shapes.

Adopted from Hardwood's DifferentialReadTest: the bug class this catches is
*composition* defects — a filter or projection that's correct in isolation but
wrong once combined (e.g. filter + column subset + null column). Our existing
duckdb_smoke only compares aggregate scalars; this compares the actual decoded
rows ZPQ writes out against DuckDB's answer for the same query.

For each (filter, projection): ZPQ writes filtered+projected parquet; we read it
back and diff it, order-independent, against DuckDB. DuckDB is the oracle (same
role Hardwood gives it). Values compare under tools/oracle_compare.py: exact for
NULL, integers and DECIMAL (zpq's JSON digits read as Decimal), tolerant for floats.

Usage: .venv/bin/python tools/differential.py
"""
import duckdb, subprocess, os, sys, tempfile, json
import pyarrow as pa
import pyarrow.parquet as pq  # strict reader: DuckDB is lenient, pyarrow is not
from oracle_compare import DUCKDB_STATS_DIVERGENCES, canon, same, self_check

ZPQ = "zig-out/bin/zpq"
# Physical column order of the fixture (gen_fixture below). `--columns` projects
# a subset in THIS order; `--select` projects in the requested order.
SCHEMA_ORDER = ["id", "amt", "price", "name", "nname", "d", "flag", "ts"]

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
    ("price_between", "price BETWEEN 100.5 AND 200", "price BETWEEN 100.5 AND 200"),
    ("date_between",  "d BETWEEN '2020-06-01' AND '2020-07-01'",
                      "d BETWEEN DATE '2020-06-01' AND DATE '2020-07-01'"),
    ("and",           "id > 500 AND name LIKE 'n9%'", "id > 500 AND name LIKE 'n9%'"),
    ("or",            "id < 100 OR id > 1900",  "id < 100 OR id > 1900"),
    ("date_ge",       "d >= '2020-06-01'",      "d >= DATE '2020-06-01'"),
    ("not_in",        "id NOT IN (1,2,3)",      "id NOT IN (1,2,3)"),
    # Literals finer than the column's millisecond unit must compare exactly, not truncated to the unit.
    ("ts_ge_sub_ms",  "ts >= '2020-01-01 00:00:00.0995'", "ts >= TIMESTAMP '2020-01-01 00:00:00.0995'"),
    ("ts_lt_sub_ms",  "ts < '2020-01-01 00:00:00.0995'",  "ts < TIMESTAMP '2020-01-01 00:00:00.0995'"),
    ("ts_eq_sub_ms",  "ts = '2020-01-01 00:00:00.0995'",  "ts = TIMESTAMP '2020-01-01 00:00:00.0995'"),
    ("ts_ne_sub_ms",  "ts != '2020-01-01 00:00:00.0995'", "ts != TIMESTAMP '2020-01-01 00:00:00.0995'"),
    ("ts_between_sub_ms", "ts BETWEEN '2020-01-01 00:00:00.0995' AND '2020-01-01 00:00:00.2005'",
                      "ts BETWEEN TIMESTAMP '2020-01-01 00:00:00.0995' AND TIMESTAMP '2020-01-01 00:00:00.2005'"),
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
    ("avg",       "avg(amt) AS a, min(name) AS mn", "avg(amt), min(name)"),
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
        (CASE WHEN i % 5 = 0 THEN NULL ELSE (i % 3 = 0) END) AS flag,
        (CASE WHEN i % 17 = 0 THEN NULL ELSE TIMESTAMP '2020-01-01' + to_milliseconds(i) END)::TIMESTAMP_MS AS ts
      FROM range(1, {n}) t(i)) TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE 500)""")


from decimal import Decimal


def fold_avg(v):
    """avg arrives as its re-aggregatable {sum, count} pair; no values averaged means NULL."""
    if isinstance(v, dict):
        return v["sum"] / v["count"] if v["count"] else None
    return v


def canon_rows(rows, lanes=None):
    """Rows as sorted tuples of canonical values (oracle_compare.canon), `lanes` giving each column's lane."""
    return sorted((tuple(canon(v, lanes[i] if lanes else None) for i, v in enumerate(row)) for row in rows), key=repr)


def rows_sorted(con, sql, lanes=None):
    return canon_rows(con.execute(sql).fetchall(), lanes)


# A DECIMAL result column: zpq prints it exactly, so its JSON digits are read as Decimal and compared exactly.
EXACT = "exact"


def json_lanes(rel):
    """Per result column of a DuckDB query: EXACT for DECIMAL, else None."""
    return [EXACT if str(d[1]).startswith("DECIMAL") else None for d in rel.description]


def zjson(text):
    """zpq's JSON with every number's digits kept: a DECIMAL answer is exact text."""
    return json.loads(text, parse_float=Decimal)


def from_zpq(vals, lanes):
    """zpq values for comparison: Decimal in an EXACT column, the double it prints anywhere else."""
    return [float(v) if isinstance(v, Decimal) and (lanes is None or lanes[i] != EXACT) else v
            for i, v in enumerate(vals)]


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
                                         next((m.decode() for m in [min((r["s"].encode() for r in rows),
                                                                         default=None)] if m is not None), None)]),
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
        rel = con.execute(f"SELECT * FROM '{fixture}' LIMIT 0")
        cols = [d[0] for d in rel.description]
        row_lanes = json_lanes(rel)
        za, da, py_agg = STATS_ORDER_AGGS[fixture]
        agg_lanes = json_lanes(con.execute(f"SELECT {da} FROM '{fixture}' LIMIT 0"))
        if callable(oracle):
            assert fixture in DUCKDB_STATS_DIVERGENCES, f"{label}: DuckDB is overruled only where it is known to diverge"
            truth = [r for r in pq.read_table(fixture).to_pylist() if oracle(r)]
            want_rows = [[r[c] for c in cols] for r in truth]
            want_agg = py_agg(truth)
            src = "truth"
        else:
            where = f" WHERE {oracle}" if oracle is not None else ""
            want_rows = con.execute(f"SELECT {', '.join(cols)} FROM '{fixture}'{where}").fetchall()
            want_agg = list(con.execute(f"SELECT {da} FROM '{fixture}'{where}").fetchone())
            src = "duck"
        want = canon_rows(want_rows)
        filt = ["--filter", zf] if zf is not None else []

        total += 1
        r = subprocess.run([ZPQ, "query", fixture, "--format", "jsonl"] + filt, capture_output=True, text=True)
        if r.returncode != 0:
            report(False, f"{label} × jsonl", f"ZPQ error: {r.stderr.strip()[:70]}")
        else:
            got = canon_rows([from_zpq([zjson(l)[c] for c in cols], row_lanes)
                              for l in r.stdout.splitlines() if l.strip()], row_lanes)
            want_json = canon_rows(want_rows, row_lanes)
            report(got == want_json, f"{label} × jsonl", f"zpq={len(got)} {src}={len(want_json)} rows")

        total += 1
        out = os.path.join(tmp, "zstats.parquet")
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", fixture, "-o", out] + filt, capture_output=True, text=True)
        if r.returncode != 0 or not os.path.exists(out):
            report(False, f"{label} × parquet", f"ZPQ error: {r.stderr.strip()[:70]}")
        else:
            tbl = pq.read_table(out)
            got = canon_rows([[row[c] for c in cols] for row in tbl.to_pylist()])
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
            zvals = from_zpq(list(zjson(r.stdout)["agg"].values()), agg_lanes)
            ok = len(zvals) == len(want_agg) and all(same(z, w, lane) for z, w, lane in zip(zvals, want_agg, agg_lanes))
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
    self_check()
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
            sel_cols = cols if cols is not None else \
                ["amt", "d", "flag", "id", "name", "nname", "price", "ts"]
            sel = ", ".join(sel_cols)
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
                               ("date_ge", "d >= '2020-06-01'", "d >= DATE '2020-06-01'"),
                               ("ts_ne_sub_ms", "ts != '2020-01-01 00:00:00.0995'",
                                "ts != TIMESTAMP '2020-01-01 00:00:00.0995'"),
                               # Over no rows / only nulls: sum, avg, min, max are NULL, count is 0.
                               ("no_rows", "id < -100000", "id < -100000"),
                               ("amt_isnull", "amt IS NULL", "amt IS NULL")]:
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
                zout = zjson(r.stdout)
                zvals = [fold_avg(v) for v in zout["agg"].values()]
                proven = zout.get("row_groups_full_match", 0)
            except Exception as e:
                print(f"  FAIL  {label}  bad json: {e}")
                fails += 1
                continue
            where = f" WHERE {dw}" if dw is not None else ""
            duck = con.execute(f"SELECT {ds} FROM '{fixture}'{where}")
            lanes = json_lanes(duck)
            zvals = from_zpq(zvals, lanes)
            dvals = list(duck.fetchone())
            ok = len(zvals) == len(dvals) and all(same(a, b, lane) for a, b, lane in zip(zvals, dvals, lanes))
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
        # Keys mixing integer and float operands compute in DOUBLE; reading the integer side's bits as a double
        # used to turn 2 into 1e-323 and collapse distinct keys.
        ("gb_int_plus_float", "id + 0.5 AS h", "count(*) AS n", None,
         "SELECT (id + 0.5) AS h, count(*) AS n FROM '{fixture}' GROUP BY h"),
        ("gb_float_times_int", "amt * 2 AS a2", "count(*) AS n", None,
         "SELECT (amt * 2) AS a2, count(*) AS n FROM '{fixture}' GROUP BY a2"),
        ("gb_int_times_float", "id * amt AS p", "count(*) AS n", None,
         "SELECT (id * amt) AS p, count(*) AS n FROM '{fixture}' GROUP BY p"),
        ("gb_decimal_plus_int", "price + id AS q", "count(*) AS n", None,
         "SELECT (price + id) AS q, count(*) AS n FROM '{fixture}' GROUP BY q"),
        ("gb_null_input", "flag", "sum(amt) AS s, min(amt) AS mn, count(amt) AS c, avg(amt) AS a, max(name) AS mx",
         None, "SELECT flag, sum(amt) AS s, min(amt) AS mn, count(amt) AS c, avg(amt) AS a, max(name) AS mx "
         "FROM '{fixture}' GROUP BY flag"),
    ]
    for glabel, gby, agg, order, d_sql in GROUP_BYS:
        for flabel, zf, dw in [("none", None, None), ("id_gt", "id > 500", "id > 500"),
                               ("amt_isnull", "amt IS NULL", "amt IS NULL"),
                               ("no_rows", "id < -100000", "id < -100000")]:
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
                zrows_raw = zjson(r.stdout)["agg"]
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
                lanes = json_lanes(duck_rel)
                drows = canon_rows(duck_rel.fetchall(), lanes)
                zrows = canon_rows([from_zpq([fold_avg(row_dict.get(c)) for c in col_names], lanes)
                                    for row_dict in zrows_raw], lanes)
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

            # `-o` writes the same groups as parquet: same column order, same values and row order as the JSON, avg
            # split into <name>__sum / <name>__count. pyarrow is the strict reader.
            total += 1
            out = os.path.join(tmp, "zgroup.parquet")
            if os.path.exists(out):
                os.remove(out)
            r = subprocess.run(cmd + ["-o", out], capture_output=True, text=True)
            label = f"groupby-o:{glabel:13} × {flabel:6}"
            want_cols, want_rows = [], []
            for c, v in (zrows_raw[0].items() if zrows_raw else []):
                want_cols += [f"{c}__sum", f"{c}__count"] if isinstance(v, dict) else [c]
            json_rows = []
            for row_dict in zrows_raw:
                vals = []
                for v in row_dict.values():
                    vals += [v["sum"], v["count"]] if isinstance(v, dict) else [v]
                json_rows.append(vals)
            try:
                if r.returncode != 0:
                    raise RuntimeError((r.stderr or r.stdout).strip()[:70])
                tbl = pq.read_table(out)
                # A DECIMAL column's JSON digits are its exact value; any other number is the double it prints.
                exact = [str(f.type).startswith("decimal") for f in tbl.schema]
                want_rows = [tuple(canon(float(v) if isinstance(v, Decimal) and not (i < len(exact) and exact[i])
                                         else v) for i, v in enumerate(vals)) for vals in json_rows]
                got_rows = [tuple(canon(v.decode() if isinstance(v, bytes) else v) for v in row.values())
                            for row in tbl.to_pylist()]
                problem = None
                if zrows_raw and list(tbl.column_names) != want_cols:
                    problem = f"columns {tbl.column_names} want {want_cols}"
                elif got_rows != want_rows:
                    problem = (f"rows zpq-parquet={got_rows[:2]}.. ({len(got_rows)}) "
                               f"json={want_rows[:2]}.. ({len(want_rows)})")
            except Exception as e:
                problem = f"error: {e}"
            if problem:
                fails += 1
                print(f"  FAIL  {label}  {problem}")
            else:
                print(f"  OK    {label}  ({len(got_rows)} rows match the JSON)")
    # `-o` keeps the parquet type of pass-through columns: bare GROUP BY keys and min/max of a column come back as
    # the source type (DATE, STRING, INT8, UINT64, ...), not the evaluation lane's plain INT64 / BYTE_ARRAY. DECIMAL
    # stays DOUBLE (its f64 value); computed columns keep their computed type.
    import datetime as dt
    import decimal
    import pyarrow as pa
    typed = os.path.join(tmp, "typed.parquet")
    k = range(6)
    pq.write_table(pa.table({
        "s": pa.array(["a", "b", "a", "b", "a", None], pa.string()),
        "bin": pa.array([b"x", b"y"] * 3, pa.binary()),
        "d": pa.array([dt.date(2024, 1, 1 + i % 2) for i in k], pa.date32()),
        "ts": pa.array([dt.datetime(2024, 1, 1, 0, 0, i % 2) for i in k], pa.timestamp("ms")),
        "tsz": pa.array([dt.datetime(2024, 1, 1, 0, 0, i % 2) for i in k], pa.timestamp("us", tz="UTC")),
        "b": pa.array([i % 2 == 0 for i in k], pa.bool_()),
        "i8": pa.array([i % 2 - 1 for i in k], pa.int8()),
        "u32": pa.array([4294967295 if i % 2 else 1 for i in k], pa.uint32()),
        "u64": pa.array([2**64 - 1 if i % 2 else 1 for i in k], pa.uint64()),
        "f": pa.array([0.5 if i % 2 else 1.5 for i in k], pa.float32()),
        "tm": pa.array([dt.time(1, i % 2) for i in k], pa.time32("ms")),
        "dec": pa.array([decimal.Decimal("1.25") * (i % 2) for i in k], pa.decimal128(9, 2)),
    }), typed)
    src_types = pq.read_schema(typed)
    for col in src_types.names:
        total += 1
        out = os.path.join(tmp, "ztyped.parquet")
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", typed, "--group-by", col, "-o", out, "--aggregate",
                            f"min({col}) AS lo, max({col}) AS hi, count(*) AS n, sum(i8) AS si"],
                           capture_output=True, text=True)
        label = f"typed-o:{col}"
        want = src_types.field(col).type
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:70])
            got = pq.read_table(out)
            types = [got.schema.field(c).type for c in (col, "lo", "hi", "n", "si")]
            if types != [want, want, want, pa.int64(), pa.int64()]:
                raise RuntimeError(f"types {[str(t) for t in types]} want {want} x3, int64 x2")
            # DuckDB hands back tz-aware timestamps only through pytz; it reads them as naive UTC instead, which is
            # what canon() turns zpq's tz-aware values into.
            lanes = None
            zrows = canon_rows([list(row.values()) for row in got.to_pylist()], lanes)
            c = f"timezone('UTC', {col})" if getattr(want, "tz", None) else col
            drows = rows_sorted(con, f"SELECT {c}, min({c}), max({c}), count(*), sum(i8) FROM '{typed}' GROUP BY 1",
                                lanes)
            if zrows != drows:
                raise RuntimeError(f"rows zpq={zrows[:2]} duck={drows[:2]}")
            print(f"  OK    {label}  ({want})")
        except Exception as e:
            fails += 1
            print(f"  FAIL  {label}  {e}")

    # Exact DECIMAL aggregates written with -o: DECIMAL columns of the type SQL gives the result (sum DECIMAL(38, s),
    # min/max the argument's, avg's exact sum beside its count), read back by pyarrow and DuckDB value for value.
    dfix = "ci/fixtures/parquet/decimal_exact.parquet"
    DEC_O = [
        ("scalar", [], "sum(d9) AS s, min(d38) AS mn, sum(d9 * d18) AS p, avg(dn) AS a",
         "SELECT sum(d9) AS s, min(d38) AS mn, sum(d9 * d18) AS p, sum(dn) AS a__sum, count(dn) AS a__count FROM '{f}'",
         ["decimal128(38, 2)", "decimal128(38, 10)", "decimal128(38, 6)", "decimal128(38, 2)", "int64"]),
        ("grouped", ["--group-by", "k"], "sum(d18 * q) AS p, max(dn) AS mx",
         "SELECT k, sum(d18 * q) AS p, max(dn) AS mx FROM '{f}' GROUP BY k",
         ["string", "decimal128(38, 6)", "decimal128(15, 2)"]),
    ]
    for dlabel, extra, za, dsql, want_types in DEC_O:
        total += 1
        out = os.path.join(tmp, "zdec.parquet")
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", dfix, "-o", out, "--aggregate", za] + extra, capture_output=True, text=True)
        label = f"decimal-o:{dlabel}"
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:70])
            got = pq.read_table(out)
            types = [str(f.type) for f in got.schema]
            if types != want_types:
                raise RuntimeError(f"types {types} want {want_types}")
            want = rows_sorted(con, dsql.format(f=dfix))
            if canon_rows([list(row.values()) for row in got.to_pylist()]) != want:
                raise RuntimeError(f"pyarrow rows {got.to_pylist()[:2]} want {want[:2]}")
            if rows_sorted(con, f"SELECT * FROM '{out}'") != want:
                raise RuntimeError("DuckDB reads other values back")
            print(f"  OK    {label}  {types}")
        except Exception as e:
            fails += 1
            print(f"  FAIL  {label}  {e}")

    # A row group bigger than --max-memory lets one producer hold: written, printed and aggregated a slice of rows at a
    # time, with the same answers as DuckDB, down to a budget of a few slices.
    big = os.path.join(tmp, "one_big_rg.parquet")
    con.execute(f"""COPY (SELECT i::BIGINT AS id, (i % 97)::INTEGER AS g, CAST(i * 0.01 AS DECIMAL(18,2)) AS price,
        'n' || (i % 1000) AS name, (CASE WHEN i % 7 = 0 THEN NULL ELSE i * 0.5 END)::DOUBLE AS amt,
        CAST(i * 1000000007.01 AS DECIMAL(38,2)) AS wide,
        CASE WHEN i % 5 = 0 THEN NULL ELSE repeat('x', i % 300) END AS pad
        FROM range(0, 50000) t(i)) TO '{big}' (FORMAT PARQUET, ROW_GROUP_SIZE 100000)""")
    tiny = ["--max-memory", "2M", "-j", "2"]
    out = os.path.join(tmp, "zbig.parquet")
    for blabel, args, dsql in [
        ("write", ["-o", out, "--filter", "id >= 100", "--select", "id, price * 2 AS p, name, amt, wide, pad"],
         "SELECT id, price * 2 AS p, name, amt, wide, pad FROM '{f}' WHERE id >= 100"),
        ("print", ["--format", "jsonl", "--filter", "g = 3"], "SELECT * FROM '{f}' WHERE g = 3"),
        ("print-limit", ["--format", "jsonl", "--limit", "7"], "SELECT * FROM '{f}' LIMIT 7"),
        ("aggregate", ["--aggregate", "sum(price) AS s, count(amt) AS c, max(name) AS m, sum(price * 3) AS t, "
                       "sum(wide) AS w, max(pad) AS x, count(pad) AS n"],
         "SELECT sum(price) AS s, count(amt) AS c, max(name) AS m, sum(price * 3) AS t, "
         "sum(wide) AS w, max(pad) AS x, count(pad) AS n FROM '{f}'"),
        ("grouped", ["--aggregate", "sum(price) AS s, min(amt) AS a", "--group-by", "g"],
         "SELECT g, sum(price) AS s, min(amt) AS a FROM '{f}' GROUP BY g"),
    ]:
        total += 1
        label = f"sliced:{blabel}"
        if os.path.exists(out):
            os.remove(out)
        r = subprocess.run([ZPQ, "query", big] + args + tiny, capture_output=True, text=True)
        try:
            if r.returncode != 0:
                raise RuntimeError((r.stderr or r.stdout).strip()[:90])
            rel = con.execute(dsql.format(f=big))
            lanes = json_lanes(rel)
            want = canon_rows(rel.fetchall(), lanes)
            cols = [d[0] for d in rel.description]
            if blabel == "write":
                if pq.ParquetFile(out).metadata.num_row_groups < 2:
                    raise RuntimeError("one row group out: the write was not sliced")
                got = canon_rows([[row[c] for c in cols] for row in pq.read_table(out).to_pylist()], lanes)
            elif blabel.startswith("print"):
                got = canon_rows([from_zpq([zjson(l)[c] for c in cols], lanes)
                                  for l in r.stdout.splitlines() if l.strip()], lanes)
                if blabel == "print-limit":
                    # LIMIT without ORDER BY: compare to the file's first rows in order.
                    want = canon_rows(con.execute(f"SELECT * FROM '{big}'").fetchmany(7), lanes)
            else:
                agg = zjson(r.stdout)["agg"]
                rows = agg if isinstance(agg, list) else [agg]
                got = canon_rows([from_zpq([fold_avg(row.get(c)) for c in cols], lanes) for row in rows], lanes)
            if got != want:
                raise RuntimeError(f"zpq={got[:2]} duck={want[:2]} ({len(got)} vs {len(want)})")
            print(f"  OK    {label}  ({len(got)} rows)")
        except Exception as e:
            fails += 1
            print(f"  FAIL  {label}  {e}")
    total += 1
    r = subprocess.run([ZPQ, "query", big, "--aggregate", "sum(price) AS s", "--max-memory", "100K"],
                       capture_output=True, text=True)
    want_s = con.execute(f"SELECT sum(price) FROM '{big}'").fetchone()[0]
    streamed = r.returncode == 0 and zjson(r.stdout)["agg"]["s"] == want_s
    print(f"  {'OK  ' if streamed else 'FAIL'}  sliced:tiny-budget  {(r.stderr or r.stdout).strip()[:70]}")
    fails += 0 if streamed else 1
    # INT96 (no DuckDB writer) and FLOAT16 lanes: printed in slices exactly as printed whole.
    total += 1
    import numpy as np
    legacy = os.path.join(tmp, "one_big_rg_int96.parquet")
    n = 20000
    pq.write_table(pa.table({
        "ts": pa.array([i * 1_000_003_000 for i in range(n)], pa.timestamp("ns")),
        "h": pa.array(np.arange(n, dtype=np.float16) / 8),
    }), legacy, row_group_size=n, data_page_size=4096, use_deprecated_int96_timestamps=True)
    whole = subprocess.run([ZPQ, "query", legacy, "--format", "jsonl"], capture_output=True, text=True)
    sliced = subprocess.run([ZPQ, "query", legacy, "--format", "jsonl", "--max-memory", "200K"],
                            capture_output=True, text=True)
    matched = whole.returncode == 0 and sliced.returncode == 0 and whole.stdout == sliced.stdout and \
        len(whole.stdout.splitlines()) == n
    print(f"  {'OK  ' if matched else 'FAIL'}  sliced:int96-float16  {(sliced.stderr or '').strip()[:70]}")
    fails += 0 if matched else 1

    # count/min/max over nullable columns whose pages a reader of another type would refuse: narrow FIXED_LEN_BYTE_ARRAY
    # dictionaries (FLOAT16's 2-byte entries) and DELTA-encoded strings. Encoded execution (count reads levels only)
    # must answer as ZPQ_ENCODED=0 does, and the counts as DuckDB's.
    import numpy as np
    n = 3000
    nar = pa.table({
        "h": pa.array(np.array([(i % 8) - 4 for i in range(n)], dtype=np.float16) / 2, pa.float16(),
                      mask=np.array([i % 5 == 0 for i in range(n)])),
        "s": pa.array([None if i % 7 == 0 else f"v{i % 13}" for i in range(n)]),
        "f": pa.array([None if i % 3 == 0 else bytes([i % 250, 1, 2]) for i in range(n)], pa.binary(3)),
    })
    for elabel, kw in [("dictionary", {"use_dictionary": True}),
                       ("delta", {"use_dictionary": ["h", "f"], "column_encoding": {"s": "DELTA_BYTE_ARRAY"}}),
                       ("delta-length", {"use_dictionary": ["h", "f"],
                                         "column_encoding": {"s": "DELTA_LENGTH_BYTE_ARRAY"}})]:
        path = os.path.join(tmp, f"levels_{elabel}.parquet")
        pq.write_table(nar, path, **kw)
        for col in ["h", "s", "f"]:
            total += 1
            agg = f"count({col}) AS n, min({col}) AS lo, max({col}) AS hi"
            outs = [subprocess.run([ZPQ, "query", path, "--aggregate", agg, "--scan-all"], capture_output=True,
                                   text=True, env=dict(os.environ, ZPQ_ENCODED=e)) for e in ["1", "0"]]
            label = f"levels-only:{elabel}:{col}"
            try:
                for r in outs:
                    if r.returncode != 0:
                        raise RuntimeError((r.stderr or r.stdout).strip()[:70])
                enc, plain = (zjson(r.stdout)["agg"] for r in outs)
                if enc != plain:
                    raise RuntimeError(f"encoded {enc} vs ZPQ_ENCODED=0 {plain}")
                want = con.execute(f"SELECT count({col}) FROM '{path}'").fetchone()[0]
                if enc["n"] != want:
                    raise RuntimeError(f"count {enc['n']} want {want}")
                print(f"  OK    {label}  ({want})")
            except Exception as e:
                fails += 1
                print(f"  FAIL  {label}  {e}")

    # Clashing output names fail loudly, naming the column, instead of one column showing another's values.
    CLASHES = [
        ("key_vs_alias", ["--group-by", "name AS k", "--aggregate", "sum(amt) AS name"], "name"),
        ("dup_alias", ["--aggregate", "sum(amt) AS a, min(amt) AS a"], "a"),
        ("dup_alias_gb", ["--group-by", "flag", "--aggregate", "count(*) AS c, sum(amt) AS C"], "C"),
        ("sql_key_vs_alias", ["--query", "SELECT name AS k, sum(amt) AS name FROM '{fixture}' GROUP BY name"], "name"),
        ("sql_dup_alias", ["--query", "SELECT flag, count(*) AS c, sum(amt) AS c FROM '{fixture}' GROUP BY flag"], "c"),
    ]
    for clabel, cargs, col in CLASHES:
        total += 1
        cargs = [a.format(fixture=fixture) for a in cargs]
        cmd = [ZPQ, "query"] + ([] if cargs[0] == "--query" else [fixture]) + cargs
        r = subprocess.run(cmd, capture_output=True, text=True)
        label = f"name-clash:{clabel}"
        if r.returncode != 0 and not r.stdout and f"output column `{col}`" in r.stderr:
            print(f"  OK    {label}  (rejected: `{col}`)")
        else:
            fails += 1
            print(f"  FAIL  {label}  rc={r.returncode} stdout={r.stdout[:60]!r} stderr={r.stderr.strip()[:80]!r}")

    # Unaliased aggregates: a unique function name stays the output name (as before); clashing ones take the call
    # text, as DuckDB names them, instead of erroring or printing duplicate JSON keys.
    NAMES = [
        ("lone", ["--aggregate", "sum(amt)"], ["sum"]),
        ("two_sums", ["--aggregate", "sum(amt), sum(id)"], ["sum(amt)", "sum(id)"]),
        ("gb_two_mins", ["--group-by", "flag", "--aggregate", "min(amt), min(id)"], ["flag", "min(amt)", "min(id)"]),
        ("sql_two_sums", ["--query", "SELECT sum(amt), sum(id) FROM '{fixture}'"], ["sum(amt)", "sum(id)"]),
        ("sql_gb_unaliased", ["--query", "SELECT flag, count(*), sum(id) FROM '{fixture}' GROUP BY flag"],
         ["flag", "count(*)", "sum(id)"]),
    ]
    for nlabel, nargs, want in NAMES:
        total += 1
        nargs = [a.format(fixture=fixture) for a in nargs]
        cmd = [ZPQ, "query"] + ([] if nargs[0] == "--query" else [fixture]) + nargs
        r = subprocess.run(cmd, capture_output=True, text=True)
        label = f"agg-names:{nlabel}"
        try:
            # object_pairs_hook keeps duplicate keys visible instead of letting the last one win.
            class Pairs(list):
                pass
            agg = dict(json.loads(r.stdout, object_pairs_hook=Pairs))["agg"]
            got = [k for k, _ in (agg if isinstance(agg, Pairs) else agg[0])]
        except Exception as e:
            got = f"error: {e}; {r.stderr.strip()[:60]}"
        if got == want:
            print(f"  OK    {label}  {got}")
        else:
            fails += 1
            print(f"  FAIL  {label}  got {got} want {want}")

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
