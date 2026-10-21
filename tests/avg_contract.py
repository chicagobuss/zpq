#!/usr/bin/env python3
"""The avg / avg_state answer contract, end to end through the CLI; invoked by cli_smoke.sh.

avg(x) is one value in every format: a DOUBLE for integer and floating x (the avg_state sum over its count, bit for
bit), a DECIMAL(q, s + q - p), q = min(38, p + 4), for DECIMAL(p, s), the exact mean rounded half away from zero; NULL
over no values. avg_state(x) is the mergeable pair: {"sum", "count"} in JSON and NDJSON, <name>__sum and
<name>__count in CSV and Parquet. Checked for every input type, in --aggregate and SQL, scalar and GROUP BY, at -j 1 and
-j 16 and under --unordered, with Parquet read back by pyarrow and DuckDB. `--report PATH` writes the read-back table.
"""
import argparse
import csv
import decimal
from decimal import Decimal
from fractions import Fraction
import io
import json
import math
import random
import struct
import subprocess
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Sums of 38-digit values have up to 40 digits; the default context's 28 would round them.
decimal.getcontext().prec = 120

# (column, precision, scale): s = 0 and s = p - 1 at p = 34, 35 and 38; DECIMAL(36,35), whose mean of values >= 1 the
# (p + 4, s + 4) rule capped at 38 could not hold; and a narrow everyday type.
DECIMALS = [("d9_2", 9, 2), ("d34_0", 34, 0), ("d34_33", 34, 33), ("d35_0", 35, 0), ("d35_34", 35, 34),
            ("d36_35", 36, 35), ("d38_0", 38, 0), ("d38_37", 38, 37), ("d38_38", 38, 38)]
NUMERIC = [("i32", pa.int32()), ("i64", pa.int64()), ("u64", pa.uint64()), ("f32", pa.float32()),
           ("f64", pa.float64())]
KEYS = ["b", "a", "z", "c"]  # first-appearance order; every value in group "z" is NULL


def avg_type(p, s):
    q = min(38, p + 4)
    return q, s + q - p


def bits(x):
    return struct.pack("<d", x)


def half_away(fr, scale):
    """`fr` rounded half away from zero to `scale` digits, exactly (no Decimal context rounding)."""
    n = abs(fr) * 10 ** scale
    whole, rem = divmod(n.numerator, n.denominator)
    if 2 * rem >= n.denominator:
        whole += 1
    return (Decimal(-whole if fr < 0 else whole)).scaleb(-scale)


def build_fixtures(tmp):
    r = random.Random(0xA5C0)
    n = 96
    keys = [KEYS[i % len(KEYS)] for i in range(n)]
    cols = {"k": pa.array(keys, pa.string())}
    for name, ty in NUMERIC:
        vals = []
        for i, k in enumerate(keys):
            if k == "z" or i % 7 == 3:
                vals.append(None)
            elif name == "i32":
                vals.append(r.randint(-2**31, 2**31 - 1))
            elif name == "i64":
                vals.append(r.randint(-2**62, 2**62))
            elif name == "u64":
                vals.append(r.randint(2**63, 2**64 - 1) if i % 2 else r.randint(0, 2**40))
            elif name == "f32":
                vals.append(struct.unpack("<f", struct.pack("<f", r.uniform(-1e6, 1e6)))[0])
            else:
                vals.append(r.uniform(-1e3, 1e3) * 10 ** r.randint(-3, 12))
        cols[name] = pa.array(vals, ty)
    for name, p, s in DECIMALS:
        # Unscaled values under 10^min(p, 36): 96 of them sum under 10^38, so avg_state's DECIMAL(38, s) column holds
        # every sum. DECIMAL(36, 35) values are >= 1, so every mean is too.
        top = 10 ** min(p, 36) - 1
        vals = []
        for i, k in enumerate(keys):
            if k == "z" or i % 5 == 2:
                vals.append(None)
                continue
            u = r.randint(10 ** 35, top) if name == "d36_35" else r.randint(-top, top)
            vals.append(Decimal(u).scaleb(-s))
        cols[name] = pa.array(vals, pa.decimal128(p, s))
    cols["nulls"] = pa.array([None] * n, pa.int64())
    main = tmp / "avg_types.parquet"
    pq.write_table(pa.table(cols), main, row_group_size=8)
    # Values near 10^37 and 10^38 - 1 in DECIMAL(38, 0): their sum passes 38 digits, their mean does not.
    wide = tmp / "avg_wide.parquet"
    big = [10 ** 38 - 1, 10 ** 38 - 3, 10 ** 37 + 5, 10 ** 38 - 2]
    pq.write_table(pa.table({"w": pa.array([Decimal(v) for v in big], pa.decimal128(38, 0))}), wide, row_group_size=1)
    return main, wide, pq.read_table(main).to_pydict()


def expected(data, col, rows):
    """(avg, (sum, count), tol) of `col` over row indices `rows`: avg a float, Decimal or None; a float column's sum is
    None and its avg is compared within `tol`, as its fold order rounds; every other one is exact."""
    vals = [data[col][i] for i in rows if data[col][i] is not None]
    dec = next((d for d in DECIMALS if d[0] == col), None)
    if not vals:
        return None, (None, 0), 0
    if dec:
        _, p, s = dec
        total = sum(vals)
        return half_away(Fraction(total) / len(vals), avg_type(p, s)[1]), (total, len(vals)), 0
    if col in ("f32", "f64"):
        return math.fsum(vals) / len(vals), (None, len(vals)), 1e-13 * max(abs(v) for v in vals)
    total = sum(vals)
    return float(total) / len(vals), (float(total), len(vals)), 0


class Run:
    def __init__(self, zpq):
        self.zpq = zpq

    def __call__(self, *args, ok=True):
        p = subprocess.run([self.zpq, "query", *args], capture_output=True, timeout=120)
        if ok:
            assert p.returncode == 0, (args, p.returncode, p.stderr.decode())
        else:
            assert p.returncode != 0, (args, p.stdout)
        return p


def parse_text(fmt, out):
    """Rows of a JSON / NDJSON / CSV answer as dicts of exact values (Decimal for numbers with a point)."""
    text = out.decode()
    if fmt == "json":
        return json.loads(text, parse_float=Decimal)
    if fmt == "ndjson":
        return [json.loads(line, parse_float=Decimal) for line in text.splitlines()]
    rows = list(csv.reader(io.StringIO(text)))
    return [dict(zip(rows[0], r)) for r in rows[1:]]


def num(v):
    """A JSON / CSV cell as a number: '' and None are NULL, "NaN" a float."""
    if v is None or v == "":
        return None
    if isinstance(v, str):
        return float(v) if v in ("NaN", "Infinity", "-Infinity") else Decimal(v)
    return v


def check_double(got, want, tol, ctx):
    if want is None:
        assert got is None, (ctx, got)
        return
    g = float(got)
    if tol == 0:
        assert bits(g) == bits(want), (ctx, g, want)
    else:
        assert abs(g - want) <= tol, (ctx, g, want)


def check_decimal(got, want, scale, ctx):
    if want is None:
        assert got is None, (ctx, got)
        return
    assert isinstance(got, (Decimal, int)), (ctx, got)
    assert Decimal(got) == want, (ctx, got, want)
    if isinstance(got, Decimal):
        assert -got.as_tuple().exponent == scale, (ctx, "printed scale", got, scale)


def check_answer(data, rows_by_key, cols, got_rows, fmt, ctx, unordered):
    """Every avg / avg_state of `got_rows` (dicts, a scalar answer has key None) against the exact expectation."""
    for row in got_rows:
        key = row.get("k")
        idx = rows_by_key[key]
        for col in cols:
            want_avg, (want_sum, want_count), tol = expected(data, col, idx)
            dec = next((d for d in DECIMALS if d[0] == col), None)
            a = row[f"a_{col}"]
            c = f"{ctx} k={key} {col}"
            if dec:
                check_decimal(num(a), want_avg, avg_type(dec[1], dec[2])[1], c)
            else:
                # Integer sums are exact, so their mean is exact at every -j and order.
                check_double(num(a), want_avg, tol, c)
            if fmt == "csv":
                s, n = num(row[f"st_{col}__sum"]), int(row[f"st_{col}__count"])
            else:
                st = row[f"st_{col}"]
                assert set(st) == {"sum", "count"}, (c, st)
                s, n = num(st["sum"]), st["count"]
            assert n == want_count, (c, n, want_count)
            if dec:
                assert (s is None and want_sum is None) or Decimal(s) == want_sum, (c, s, want_sum)
            elif want_sum is not None:
                assert bits(float(s)) == bits(want_sum), (c, s, want_sum)
            if not dec and n:
                # A DOUBLE avg is its avg_state's sum over its count, bit for bit, on every path.
                assert bits(float(num(a))) == bits(float(s) / n) or unordered, (c, a, s, n)
            if not n:
                assert s is None and num(a) is None, (c, s, a)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("zpq")
    ap.add_argument("--report")
    args = ap.parse_args()
    run = Run(str(Path(args.zpq).resolve()))
    with tempfile.TemporaryDirectory(prefix="zpq-avg-") as d:
        tmp = Path(d)
        main_f, wide_f, data = build_fixtures(tmp)
        n = len(data["k"])
        cols = [c for c, _ in NUMERIC] + [c for c, _, _ in DECIMALS] + ["nulls"]
        agg = ", ".join(f"avg({c}) AS a_{c}, avg_state({c}) AS st_{c}" for c in cols)
        all_rows = {None: list(range(n))}
        by_key = {k: [i for i in range(n) if data["k"][i] == k] for k in KEYS}
        checked = 0
        texts = {}
        for grouped in (False, True):
            rows_by_key = by_key if grouped else all_rows
            for via in ("flags", "sql"):
                if via == "flags":
                    q = [str(main_f), "--aggregate", agg] + (["--group-by", "k"] if grouped else [])
                else:
                    sel = ("k, " if grouped else "") + agg
                    q = ["--query", f"SELECT {sel} FROM '{main_f}'" + (" GROUP BY k" if grouped else "")]
                for j in ("1", "16"):
                    for unordered in ((False, True) if grouped else (False,)):
                        extra = ["-j", j] + (["--unordered"] if unordered else [])
                        for fmt in ("json", "ndjson", "csv"):
                            out = run(*q, *extra, "--format", fmt).stdout
                            rows = parse_text(fmt, out)
                            assert len(rows) == (len(KEYS) if grouped else 1), (fmt, rows)
                            if grouped and not unordered:
                                assert [r["k"] for r in rows] == KEYS, ("first-appearance order", fmt, rows)
                            ctx = f"{via} grouped={grouped} -j{j} unordered={unordered} {fmt}"
                            check_answer(data, rows_by_key, cols, rows, fmt, ctx, unordered)
                            if fmt == "csv":
                                header = out.decode().splitlines()[0].split(",")
                                assert "a_i64" in header and "a_i64__sum" not in header, header
                                assert "st_i64__sum" in header and "st_i64__count" in header, header
                            if not unordered:
                                texts.setdefault((grouped, via, fmt), set()).add(out)
                            checked += 1
        # Ordered answers: the same bytes at -j 1 and -j 16.
        for key, outs in texts.items():
            assert len(outs) == 1, ("answers differ across -j or path", key)

        # Empty input: a filter that keeps nothing, scalar and grouped.
        for grouped in (False, True):
            q = [str(main_f), "--aggregate", "avg(i64) AS a, avg_state(i64) AS st, avg(d9_2) AS ad, "
                 "avg_state(d9_2) AS sd", "--filter", "i64 > 9223372036854775806"]
            q += ["--group-by", "k"] if grouped else []
            got = parse_text("ndjson", run(*q).stdout)
            if grouped:
                assert got == [], got
            else:
                assert got == [{"a": None, "st": {"sum": None, "count": 0}, "ad": None,
                                "sd": {"sum": None, "count": 0}}], got
                csv_out = run(*q, "--format", "csv").stdout.decode()
                assert csv_out == "a,st__sum,st__count,ad,sd__sum,sd__count\n,,0,,,0\n", csv_out

        # A DECIMAL(38, 0) mean whose sum passes 38 digits: avg answers it everywhere, -o included; avg_state answers
        # in JSON and refuses `-o`, whose __sum column is DECIMAL(38, 0).
        w = [10 ** 38 - 1, 10 ** 38 - 3, 10 ** 37 + 5, 10 ** 38 - 2]
        want_w = half_away(Fraction(sum(w), len(w)), 0)
        got = parse_text("ndjson", run(str(wide_f), "--aggregate", "avg(w) AS a, avg_state(w) AS st").stdout)[0]
        assert Decimal(got["a"]) == want_w and got["st"] == {"sum": sum(w), "count": 4}, got
        run(str(wide_f), "--aggregate", "avg(w) AS a", "-o", str(tmp / "wide.parquet"))
        assert pq.read_table(tmp / "wide.parquet").column("a").to_pylist() == [want_w]
        refused = run(str(wide_f), "--aggregate", "avg_state(w) AS st", "-o", str(tmp / "wide_st.parquet"), ok=False)
        assert refused.returncode == 1, refused.stderr

        report = parquet_readback(run, tmp, main_f, data, cols, by_key, all_rows)
        if args.report:
            Path(args.report).write_text(report)
        print(f"avg contract: PASS ({checked} text answers, Parquet read back by pyarrow and DuckDB)")


def parquet_readback(run, tmp, main_f, data, cols, by_key, all_rows):
    """`-o` answers, scalar and grouped, read by pyarrow and DuckDB: types and values per input type."""
    lines = ["| input | avg column (pyarrow / DuckDB) | avg_state columns (pyarrow) | values |", "|---|---|---|---|"]
    agg = ", ".join(f"avg({c}) AS a_{c}, avg_state({c}) AS st_{c}" for c in cols)
    for grouped in (False, True):
        for j in ("1", "16"):
            out = tmp / f"avg_{grouped}_{j}.parquet"
            run(str(main_f), "--aggregate", agg, *(["--group-by", "k"] if grouped else []), "-j", j, "-o", str(out))
            t = pq.read_table(out)
            with duckdb.connect(config={"threads": "1"}) as con:
                described = {r[0]: r[1] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(out)])
                             .fetchall()}
                duck_rows = con.execute("SELECT * FROM read_parquet(?)", [str(out)]).fetchall()
            names = t.column_names
            duck = [dict(zip(names, r)) for r in duck_rows]
            arrow = t.to_pylist()
            assert arrow == duck or all(_same_row(a, b) for a, b in zip(arrow, duck)), ("pyarrow vs DuckDB", out)
            rows_by_key = by_key if grouped else all_rows
            for col in cols:
                dec = next((x for x in DECIMALS if x[0] == col), None)
                field = t.schema.field(f"a_{col}")
                assert f"a_{col}__sum" not in names
                if dec:
                    q, s = avg_type(dec[1], dec[2])
                    assert field.type == pa.decimal128(q, s), (col, field.type)
                    assert described[f"a_{col}"] == f"DECIMAL({q},{s})", (col, described[f"a_{col}"])
                    assert t.schema.field(f"st_{col}__sum").type == pa.decimal128(38, dec[2]), col
                else:
                    assert field.type == pa.float64() and described[f"a_{col}"] == "DOUBLE", (col, field.type)
                    assert t.schema.field(f"st_{col}__sum").type == pa.float64(), col
                assert field.nullable
                count = t.schema.field(f"st_{col}__count")
                assert count.type == pa.int64() and not count.nullable, (col, count)
                for row in arrow:
                    idx = rows_by_key[row.get("k")]
                    want_avg, (want_sum, want_count), tol = expected(data, col, idx)
                    ctx = f"parquet grouped={grouped} -j{j} {col} k={row.get('k')}"
                    if dec:
                        assert row[f"a_{col}"] == want_avg, (ctx, row[f"a_{col}"], want_avg)
                        assert row[f"st_{col}__sum"] == want_sum, ctx
                    else:
                        check_double(row[f"a_{col}"], want_avg, tol, ctx)
                        if want_count:
                            assert bits(row[f"a_{col}"]) == bits(row[f"st_{col}__sum"] / want_count), ctx
                    assert row[f"st_{col}__count"] == want_count, ctx
                if grouped and j == "16":
                    sample = next(r for r in arrow if r["k"] == "b")[f"a_{col}"]
                    st = (f"DECIMAL(38,{dec[2]}) + BIGINT" if dec else "DOUBLE + BIGINT")
                    src = f"DECIMAL({dec[1]},{dec[2]})" if dec else str(data_type(col))
                    lines.append(f"| {col} {src} | {field.type} / {described[f'a_{col}']} | {st} | "
                                 f"group b: {sample}; group z: {next(r for r in arrow if r['k'] == 'z')[f'a_{col}']} |")
    return "\n".join(lines) + "\n"


def data_type(col):
    return dict(NUMERIC).get(col, pa.int64())


def _same_row(a, b):
    for k in a:
        x, y = a[k], b[k]
        if isinstance(x, float) and isinstance(y, float) and math.isnan(x) and math.isnan(y):
            continue
        if x != y:
            return False
    return True


if __name__ == "__main__":
    main()
