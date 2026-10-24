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


def duck_json(duckdb, sql):
    r = subprocess.run([duckdb, "-no-agent", "-json", "-c", sql], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(r.stderr.strip().splitlines()[-1][:300] if r.stderr.strip() else "duckdb failed")
    return json.loads(r.stdout)


def describe(duckdb, sql):
    rows = duck_json(duckdb, f"DESCRIBE {sql}")
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
    spec = describe(args.duckdb, args.sql)
    names = [n for n, _ in spec]
    types = [t for _, t in spec]
    raw = duck_json(args.duckdb, args.sql)
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
    p.add_argument("mode", choices=["agg", "group", "parquet"])
    p.add_argument("--zpq", required=True)
    p.add_argument("--duckdb")
    p.add_argument("--sql")
    p.add_argument("--parquet")
    p.add_argument("--mean-scale", type=int)
    p.add_argument("--mean-sum")
    p.add_argument("--mean-count")
    p.add_argument("--mean-alias")
    args = p.parse_args()
    if args.mode == "agg":
        cmd_agg(args)
    elif args.mode == "group":
        cmd_group(args)
    else:
        cmd_parquet(args)


if __name__ == "__main__":
    main()
