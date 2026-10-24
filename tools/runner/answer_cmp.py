#!/usr/bin/env python3
"""Compare one in-process aggregate answer with DuckDB, using tools/oracle_compare.py.

The referee writes zpq's JSON (the same text the CLI prints) and passes the DuckDB
SQL. This does not run zpq. A decimal average is zpq's mean, rounded half away from
zero, which DuckDB's own avg is not: the SQL supplies the exact sum and count.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from oracle_compare import canon, same  # noqa: E402

EXACT = "exact"


def avg_dec(total, n, scale):
    """zpq's avg over a DECIMAL: exact sum over n, half away from zero, at `scale` digits."""
    if not n:
        return None
    q = Fraction(Decimal(str(total))) / n * 10**scale
    m = (2 * abs(q.numerator) + q.denominator) // (2 * q.denominator)
    return Decimal(f"{'-' if q < 0 else ''}{m}E-{scale}")


def split_state(row):
    out = {}
    for k, v in row.items():
        if isinstance(v, dict) and set(v) == {"sum", "count"}:
            out[f"{k}__sum"], out[f"{k}__count"] = v["sum"], v["count"]
        else:
            out[k] = v
    return out


def duck_json(duckdb, sql, stable=False):
    # The 2.0 CLI sorts NaN with more than one thread in an order that changes between runs. One thread
    # matches the in-process DuckDB oracle the existing differential uses, and zpq's tie break.
    text = f"SET threads=1; {sql}" if stable else sql
    r = subprocess.run([duckdb, "-no-agent", "-json", "-c", text], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(r.stderr.strip().splitlines()[-1][:300] if r.stderr.strip() else "duckdb failed")
    return json.loads(r.stdout)


def describe(duckdb, sql, stable=False):
    rows = duck_json(duckdb, f"DESCRIBE {sql}", stable=stable)
    return [(row["column_name"], row["column_type"]) for row in rows]


def coerce(val, typ):
    if val is None:
        return None
    t = typ.upper()
    if t.startswith("DECIMAL"):
        return Decimal(str(val))
    if t.startswith(("DOUBLE", "FLOAT", "REAL")):
        return float(val)
    if t.startswith("BOOL"):
        return bool(val)
    if t.startswith(("HUGEINT", "BIGINT", "INTEGER", "SMALLINT", "TINYINT", "INT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT")):
        return int(val)
    return val


def load_zpq(path):
    return json.loads(Path(path).read_text(), parse_float=Decimal)


def lanes_of(types):
    return [EXACT if t.upper().startswith("DECIMAL") else None for t in types]


def from_zpq(vals, lanes):
    return [float(v) if isinstance(v, Decimal) and (lanes is None or lanes[i] != EXACT) else v for i, v in enumerate(vals)]


def apply_mean(rows, names, lanes, args):
    """Replace the decimal-average column with zpq's mean of the sum and count columns."""
    if args.mean_scale is None:
        return rows, names, lanes
    scale = args.mean_scale
    if args.mean_alias:
        si, ci = names.index(args.mean_sum), names.index(args.mean_count)
        mi = names.index(args.mean_alias)
        lanes = list(lanes)
        lanes[mi] = EXACT
        out = []
        for row in rows:
            row = list(row)
            row[mi] = avg_dec(row[si], row[ci], scale)
            out.append(row)
        return out, names, lanes
    # Scalar form: the select starts with the sum and the count the mean is taken from.
    out = []
    for row in rows:
        out.append([avg_dec(row[0], row[1], scale), *row[2:]])
    return out, names[2:], [EXACT, *lanes[2:]]


def duck_rows(args):
    stable = getattr(args, "stable", False)
    spec = describe(args.duckdb, args.sql, stable=stable)
    names = [n for n, _ in spec]
    types = [t for _, t in spec]
    raw = duck_json(args.duckdb, args.sql, stable=stable)
    rows = [[coerce(row.get(name), typ) for name, typ in spec] for row in raw]
    return apply_mean(rows, names, lanes_of(types), args)


def canon_rows(rows, lanes):
    return sorted(
        (tuple(canon(v, lanes[i] if lanes else None) for i, v in enumerate(row)) for row in rows),
        key=repr,
    )


def cmd_agg(args):
    zrows = load_zpq(args.zpq)
    if len(zrows) != 1:
        raise SystemExit(f"zpq rows {len(zrows)}")
    drows, _, lanes = duck_rows(args)
    if len(drows) != 1:
        raise SystemExit(f"duck rows {len(drows)}")
    got = from_zpq(list(split_state(zrows[0]).values()), lanes)
    want = drows[0]
    if len(got) != len(want) or not all(same(a, b, lane) for a, b, lane in zip(got, want, lanes)):
        raise SystemExit(f"zpq={got} duck={want}")


def cmd_group(args):
    zraw = load_zpq(args.zpq)
    drows, names, lanes = duck_rows(args)
    zrows = canon_rows(
        [from_zpq([split_state(row).get(c) for c in names], lanes) for row in zraw],
        lanes,
    )
    want = canon_rows(drows, lanes)
    if zrows != want:
        if len(zrows) != len(want):
            raise SystemExit(f"zpq={len(zrows)} duck={len(want)}")
        for a, b in zip(zrows, want):
            if a != b:
                raise SystemExit(f"row zpq={a} duck={b}")


def load_rows(path):
    text = Path(path).read_text()
    if not text.strip():
        return []
    if text.lstrip().startswith("["):
        rows = json.loads(text, parse_float=Decimal)
        return rows if isinstance(rows, list) else [rows]
    return [json.loads(line, parse_float=Decimal) for line in text.splitlines() if line.strip()]


def to_epoch(val, typ):
    """JSONL prints a DATE as days and a TIMESTAMP as milliseconds, both since 1970-01-01."""
    import datetime

    if val is None:
        return None
    t = typ.upper()
    if t.startswith("TIMESTAMP"):
        text = str(val)
        if "." in text:
            head, frac = text.split(".", 1)
            dt = datetime.datetime.strptime(head, "%Y-%m-%d %H:%M:%S").replace(microsecond=int(frac.ljust(6, "0")[:6]))
        else:
            dt = datetime.datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        return (dt - datetime.datetime(1970, 1, 1)) // datetime.timedelta(milliseconds=1)
    if t.startswith("DATE"):
        day = datetime.date.fromisoformat(str(val)[:10])
        return (day - datetime.date(1970, 1, 1)).days
    return val


def cmd_rows(args):
    """Plain ORDER BY rows, in order. Parquet is read back through DuckDB after pyarrow accepts it."""
    if args.parquet:
        import pyarrow.parquet as pq

        pq.read_table(args.parquet)
        written = argparse.Namespace(**vars(args))
        written.sql = f"SELECT * FROM '{args.parquet}'"
        written.mean_scale = None
        got_rows, _, _ = duck_rows(written)
        want_rows, _, lanes = duck_rows(args)
        got = [tuple(canon(v, lanes[i]) for i, v in enumerate(row)) for row in got_rows]
        want = [tuple(canon(v, lanes[i]) for i, v in enumerate(row)) for row in want_rows]
    else:
        zraw = load_rows(args.zpq)
        spec = describe(args.duckdb, args.sql, stable=True)
        want_rows, _, lanes = duck_rows(args)
        if args.epoch:
            types = [t for _, t in spec]
            want_rows = [[to_epoch(v, types[i]) for i, v in enumerate(row)] for row in want_rows]
        got = []
        for row in zraw:
            vals = from_zpq(list(row.values()), lanes)
            got.append(tuple(canon(v, lanes[i] if i < len(lanes) else None) for i, v in enumerate(vals)))
        want = [tuple(canon(v, lanes[i]) for i, v in enumerate(row)) for row in want_rows]
    if got != want:
        if len(got) != len(want):
            raise SystemExit(f"zpq={len(got)} duck={len(want)}")
        for a, b in zip(got, want):
            if a != b:
                raise SystemExit(f"row zpq={a} duck={b}"[:400])


def cmd_ordered(args):
    """Row order is the answer: ORDER BY, HAVING, spill and distinct."""
    zraw = load_zpq(args.zpq)
    if isinstance(zraw, dict):
        zraw = [zraw]
    drows, _, lanes = duck_rows(args)
    got = []
    for row in zraw:
        vals = from_zpq(list(row.values()), lanes)
        got.append(tuple(canon(v, lanes[i] if i < len(lanes) else None) for i, v in enumerate(vals)))
    want = [tuple(canon(v, lanes[i]) for i, v in enumerate(row)) for row in drows]
    if got != want:
        if len(got) != len(want):
            raise SystemExit(f"zpq={len(got)} duck={len(want)}")
        for a, b in zip(got, want):
            if a != b:
                raise SystemExit(f"row zpq={a} duck={b}"[:400])


def stats_sql(args, select):
    where = f" WHERE {args.where}" if args.where else ""
    return f"SELECT {select} FROM '{args.fixture}'{where}"


def row_pred(row, args):
    """The pyarrow predicate today's differential uses where DuckDB trusts statistics it should not."""
    value = row[args.pred_col]
    if args.pred_op == "lt_bytes":
        return value.encode() < args.pred_value.encode()
    same_value = _pred_eq(value, args.pred_value)
    if args.pred_op == "eq":
        return same_value
    if args.pred_op == "ne":
        return not same_value
    raise SystemExit(f"unknown pred {args.pred_op}")


def _pred_eq(value, text):
    if value is None:
        return False
    if isinstance(value, Decimal):
        return value == Decimal(text)
    if isinstance(value, bool):
        return value == (text == "true")
    if isinstance(value, int):
        return value == int(text)
    if isinstance(value, float):
        return value == float(text)
    if isinstance(value, str):
        return value == text
    return str(value) == text


def truth_rows(args):
    import pyarrow.parquet as pq

    if args.pred_col:
        table = pq.read_table(args.fixture)
        cols = list(table.column_names)
        rows = [r for r in table.to_pylist() if row_pred(r, args)]
        spec = describe(args.duckdb, f"SELECT * FROM '{args.fixture}'", stable=True)
        lanes = lanes_of([t for _, t in spec])
        return cols, [[r[c] for c in cols] for r in rows], lanes
    typed = argparse.Namespace(**vars(args))
    typed.sql = stats_sql(args, "*")
    typed.stable = True
    rows, names, lanes = duck_rows(typed)
    return names, rows, lanes


def cmd_stats_rows(args):
    """Unordered rows. Parquet is the written file; JSONL is the printed rows. A predicate replaces DuckDB."""
    import pyarrow.parquet as pq

    cols, want_rows, lanes = truth_rows(args)
    if args.parquet:
        got_rows = [[row[c] for c in cols] for row in pq.read_table(args.zpq).to_pylist()]
        got, want = canon_rows(got_rows, lanes), canon_rows(want_rows, lanes)
    else:
        got = canon_rows(
            [from_zpq([row[c] for c in cols], lanes) for row in load_rows(args.zpq)],
            lanes,
        )
        want = canon_rows(want_rows, lanes)
    if got != want:
        if len(got) != len(want):
            raise SystemExit(f"zpq={len(got)} truth={len(want)}")
        for a, b in zip(got, want):
            if a != b:
                raise SystemExit(f"row zpq={a} truth={b}"[:400])


def py_aggregate(name, rows):
    if name == "nan":
        return [len(rows), sum(r["i"] for r in rows) if rows else None]
    if name == "deprecated":
        return [
            len(rows),
            sum(r["i"] for r in rows) if rows else None,
            min((r["dec"] for r in rows), default=None),
            max((r["dec"] for r in rows), default=None),
            max((r["u"] for r in rows), default=None),
        ]
    if name == "column_order":
        encoded = [r["s"].encode() for r in rows]
        least = min(encoded) if encoded else None
        return [
            len(rows),
            sum(r["i"] for r in rows) if rows else None,
            None if least is None else least.decode(),
        ]
    raise SystemExit(f"unknown py agg {name}")


def cmd_stats_agg(args):
    """One aggregate row. `py_agg` is the truth where DuckDB's filtered answer is not."""
    import pyarrow.parquet as pq

    spec = describe(args.duckdb, f"SELECT {args.sql} FROM '{args.fixture}'", stable=True)
    lanes = lanes_of([t for _, t in spec])
    if args.py_agg:
        rows = [r for r in pq.read_table(args.fixture).to_pylist() if row_pred(r, args)]
        want = py_aggregate(args.py_agg, rows)
    else:
        typed = argparse.Namespace(**vars(args))
        typed.sql = stats_sql(args, args.sql)
        typed.stable = True
        drows, _, lanes = duck_rows(typed)
        if len(drows) != 1:
            raise SystemExit(f"duck rows {len(drows)}")
        want = drows[0]
    zraw = load_zpq(args.zpq)
    if isinstance(zraw, list):
        if len(zraw) != 1:
            raise SystemExit(f"zpq rows {len(zraw)}")
        zraw = zraw[0]
    got = from_zpq(list(split_state(zraw).values()), lanes)
    if len(got) != len(want) or not all(same(a, b, lane) for a, b, lane in zip(got, want, lanes)):
        raise SystemExit(f"zpq={got} truth={want}")


def cmd_stats_select(args):
    """Aliased unsigned columns, compared as a sorted set."""
    import pyarrow.parquet as pq

    typed = argparse.Namespace(**vars(args))
    typed.sql = stats_sql(args, args.sql)
    typed.stable = True
    want_rows, names, _ = duck_rows(typed)
    want = sorted((tuple(row) for row in want_rows), key=repr)
    if args.parquet:
        pq.read_table(args.parquet)
        raw = pq.read_table(args.zpq).to_pylist()
    else:
        raw = load_rows(args.zpq)
    got = sorted((tuple(row[name] for name in names) for row in raw), key=repr)
    if got != want:
        if len(got) != len(want):
            raise SystemExit(f"zpq={len(got)} truth={len(want)}")
        for a, b in zip(got, want):
            if a != b:
                raise SystemExit(f"row zpq={a} truth={b}"[:400])


def declared_column_orders(path):
    """FileMetaData.column_orders member ids (0 = an empty union), or None when the file declares none."""
    from gen_stats_order_fixtures import Reader, T_STRUCT, field

    data = open(path, "rb").read()
    n = int.from_bytes(data[-8:-4], "little")
    orders = field(Reader(data[-8 - n:-8]).value(T_STRUCT), 7)
    return None if orders is None else [u[0][0] if u else 0 for u in orders[2][1]]


def cmd_stats_orders(args):
    """Written statistics a strict reader can trust: min/max present, and pushdown keeps the right rows."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    out = args.zpq
    if args.orders == "mixed":
        orders = declared_column_orders(out)
        rows = pq.read_table(out).to_pylist()
        pushed = pq.read_table(out, filters=[("s", "==", "B")]).num_rows
        truth = sum(r["s"] == "B" for r in rows)
        if not (0 not in (orders or []) and pushed == truth == 2):
            raise SystemExit(f"column_orders={orders} s='B' -> {pushed} truth={truth}")
        return
    if args.orders == "string":
        md = pq.ParquetFile(out).metadata
        usable = all(
            md.row_group(g).column(0).statistics is not None and md.row_group(g).column(0).statistics.has_min_max
            for g in range(md.num_row_groups)
        )
        pushed = pq.read_table(out, filters=[("s", "==", "a")]).num_rows
        if not (usable and pushed == 1):
            raise SystemExit(f"has_min_max={usable} s='a' -> {pushed}")
        return
    md = pq.ParquetFile(out).metadata
    names = [md.schema.column(c).name for c in range(md.num_columns)]
    usable = all(
        md.row_group(g).column(c).statistics is not None and md.row_group(g).column(c).statistics.has_min_max
        for g in range(md.num_row_groups)
        for c in range(md.num_columns)
        if names[c] in ("u32", "u64")
    )
    rows = pq.read_table(out).to_pylist()
    pushed = [
        pq.read_table(out, filters=[("u64", ">=", pa.scalar(2**63, pa.uint64()))]).num_rows,
        pq.read_table(out, filters=[("u32", "<", 2**31)]).num_rows,
    ]
    truth = [sum(r["u64"] >= 2**63 for r in rows), sum(r["u32"] < 2**31 for r in rows)]
    if not (usable and pushed == truth):
        raise SystemExit(f"has_min_max={usable} pushdown={pushed} truth={truth}")


def cmd_parquet(args):
    import pyarrow.parquet as pq

    zraw = load_zpq(args.zpq)
    tbl = pq.read_table(args.parquet)
    want_cols = list(split_state(zraw[0])) if zraw else []
    exact = [str(f.type).startswith("decimal") for f in tbl.schema]
    json_rows = [list(split_state(row).values()) for row in zraw]
    want = [
        tuple(
            canon(float(v) if isinstance(v, Decimal) and not (i < len(exact) and exact[i]) else v)
            for i, v in enumerate(vals)
        )
        for vals in json_rows
    ]
    got = [
        tuple(canon(v.decode() if isinstance(v, bytes) else v) for v in row.values())
        for row in tbl.to_pylist()
    ]
    if zraw and list(tbl.column_names) != want_cols:
        raise SystemExit(f"columns {tbl.column_names} want {want_cols}")
    if got != want:
        raise SystemExit(f"rows parquet={got[:2]} ({len(got)}) json={want[:2]} ({len(want)})")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["agg", "group", "parquet", "ordered", "rows", "stats_rows", "stats_agg", "stats_select", "stats_orders"])
    p.add_argument("--zpq", required=True)
    p.add_argument("--duckdb")
    p.add_argument("--sql")
    p.add_argument("--parquet")
    p.add_argument("--mean-scale", type=int)
    p.add_argument("--mean-sum")
    p.add_argument("--mean-count")
    p.add_argument("--mean-alias")
    p.add_argument("--epoch", action="store_true")
    p.add_argument("--fixture")
    p.add_argument("--where")
    p.add_argument("--pred-col")
    p.add_argument("--pred-op")
    p.add_argument("--pred-value")
    p.add_argument("--py-agg")
    p.add_argument("--orders")
    args = p.parse_args()
    args.stable = args.mode in ("ordered", "rows", "stats_rows", "stats_agg", "stats_select", "stats_orders")
    if args.mode == "agg":
        cmd_agg(args)
    elif args.mode == "group":
        cmd_group(args)
    elif args.mode == "ordered":
        cmd_ordered(args)
    elif args.mode == "rows":
        cmd_rows(args)
    elif args.mode == "stats_rows":
        cmd_stats_rows(args)
    elif args.mode == "stats_agg":
        cmd_stats_agg(args)
    elif args.mode == "stats_select":
        cmd_stats_select(args)
    elif args.mode == "stats_orders":
        cmd_stats_orders(args)
    else:
        cmd_parquet(args)


if __name__ == "__main__":
    main()
