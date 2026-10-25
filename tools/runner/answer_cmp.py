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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from oracle_compare import canon, canon_rows, decimal_mean as avg_dec, require_rows, same  # noqa: E402

EXACT = "exact"


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
    # Preserve the CLI's decimal digits before the DESCRIBE type chooses a lane.
    return json.loads(r.stdout, parse_float=Decimal)


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
    if t.startswith(("HUGEINT", "BIGINT", "INTEGER", "SMALLINT", "TINYINT", "INT",
                     "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT")):
        return int(val)
    return val


def load_zpq(path):
    return json.loads(Path(path).read_text(), parse_float=Decimal)


def lanes_of(types):
    return [EXACT if t.upper().startswith("DECIMAL") else None for t in types]


def from_zpq(vals, lanes):
    return [float(v) if isinstance(v, Decimal) and (lanes is None or lanes[i] != EXACT) else v
            for i, v in enumerate(vals)]


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
        [from_zpq([split_state(row)[c] for c in names], lanes) for row in zraw],
        lanes,
    )
    want = canon_rows(drows, lanes)
    require_rows(zrows, want)


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
        want_rows, _, lanes = duck_rows(args)
        if args.epoch:
            spec = describe(args.duckdb, args.sql, stable=True)
            types = [t for _, t in spec]
            want_rows = [[to_epoch(v, types[i]) for i, v in enumerate(row)] for row in want_rows]
        got = []
        for row in zraw:
            vals = from_zpq(list(row.values()), lanes)
            got.append(tuple(canon(v, lanes[i] if i < len(lanes) else None) for i, v in enumerate(vals)))
        want = [tuple(canon(v, lanes[i]) for i, v in enumerate(row)) for row in want_rows]
    require_rows(got, want)


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
    require_rows(got, want, "truth")


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
    require_rows(got, want, "truth")


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


def wire_expr(name, typ):
    quoted = '"' + name.replace('"', '""') + '"'
    kind = typ.upper()
    if kind.startswith("BLOB"):
        return f"lower(hex({quoted})) AS {quoted}"
    if kind.startswith("UUID"):
        return f"lower({quoted}::VARCHAR) AS {quoted}"
    return quoted


def wire_float(value):
    import math

    if isinstance(value, float) and math.isnan(value):
        return ("nan",)
    if isinstance(value, float):
        return (value, math.copysign(1.0, value))
    return value


def wire_pa_cell(value):
    import uuid

    if isinstance(value, uuid.UUID):
        return str(value).lower()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return wire_float(value)


def wire_duck_cell(value, typ):
    if value is None:
        return None
    kind = typ.upper()
    if kind.startswith(("DOUBLE", "FLOAT", "REAL")):
        return wire_float(float(value))
    if kind.startswith(("BLOB", "UUID")):
        return str(value).lower()
    return value


def wire_rows(args, sql, cols, types):
    raw = duck_json(args.duckdb, sql, stable=True)
    return [tuple(wire_duck_cell(row.get(col), typ) for col, typ in zip(cols, types)) for row in raw]


def cmd_wire(args):
    """Written parquet: values via both readers, uncompressed size, FLBA encodings, float bounds."""
    import math

    import pyarrow.parquet as pq

    out = args.zpq
    check = args.check
    if check == "bytes":
        meta = pq.ParquetFile(out).metadata
        ok = all(
            meta.row_group(g).total_byte_size
            == sum(meta.row_group(g).column(c).total_uncompressed_size for c in range(meta.num_columns))
            for g in range(meta.num_row_groups)
        )
        if not ok:
            raise SystemExit("total_byte_size")
        return
    if check == "encodings":
        meta = pq.ParquetFile(out).metadata
        bad = []
        for g in range(meta.num_row_groups):
            for c in range(meta.num_columns):
                col = meta.row_group(g).column(c)
                if col.physical_type != "FIXED_LEN_BYTE_ARRAY":
                    continue
                if "DELTA_LENGTH_BYTE_ARRAY" in col.encodings:
                    bad.append(f"rg{g}.{col.path_in_schema}")
        if bad:
            raise SystemExit("FLBA " + ",".join(bad))
        return
    if check == "bounds":
        meta = pq.ParquetFile(out).metadata
        tbl = pq.read_table(out)
        bad = []
        for c in range(meta.num_columns):
            cc = meta.row_group(0).column(c)
            if cc.physical_type not in ("FLOAT", "DOUBLE"):
                continue
            vals = [v for v in tbl.column(cc.path_in_schema).to_pylist() if v is not None and not math.isnan(v)]
            st = cc.statistics
            if not vals:
                if st is not None and st.has_min_max:
                    bad.append(f"{cc.path_in_schema}: bounds on an all-NaN chunk")
                continue
            lo, hi = min(vals), max(vals)
            lo = -0.0 if lo == 0 else lo
            hi = 0.0 if hi == 0 else hi
            if wire_float(st.min) != wire_float(lo) or wire_float(st.max) != wire_float(hi):
                bad.append(f"{cc.path_in_schema}: [{st.min}, {st.max}] want [{lo}, {hi}]")
        if bad:
            raise SystemExit("; ".join(bad)[:300])
        return

    pf = pq.ParquetFile(out)
    cols = list(pf.schema_arrow.names)
    spec = describe(args.duckdb, f"SELECT * FROM '{out}'", stable=True)
    types = [dict(spec)[col] for col in cols]
    select = ", ".join(wire_expr(col, typ) for col, typ in zip(cols, types))
    where = f" WHERE {args.where}" if args.where else ""
    want = sorted(wire_rows(args, f"SELECT {select} FROM '{args.fixture}'{where}", cols, types), key=repr)
    if check == "duck":
        got = sorted(wire_rows(args, f"SELECT {select} FROM '{out}'", cols, types), key=repr)
    else:
        pq.read_table(out)
        got = sorted(
            (tuple(wire_pa_cell(row[col]) for col in cols) for row in pq.read_table(out).to_pylist()),
            key=repr,
        )
    require_rows(got, want, "truth")


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


def nulls_first(row):
    return tuple((x is not None, x if x is not None else 0) for x in row)


def cmd_dense(args):
    zraw = load_zpq(args.zpq)
    if isinstance(zraw, dict):
        zraw = [zraw]
    got = sorted((tuple(row.values()) for row in zraw), key=nulls_first)
    drows, _, _ = duck_rows(args)
    want = sorted((tuple(row) for row in drows), key=nulls_first)
    if got != want:
        raise SystemExit(f"duckdb differs: zpq={got[:2]} duck={want[:2]}")


def cmd_usame(args):
    zraw = load_zpq(args.zpq)
    if isinstance(zraw, dict):
        zraw = [zraw]
    drows, names, lanes = duck_rows(args)
    got = sorted((tuple(from_zpq([row[c] for c in names], lanes)) for row in zraw), key=repr)
    want = sorted((tuple(row) for row in drows), key=repr)
    if len(got) != len(want) or not all(same(g, w) for gr, wr in zip(got, want) for g, w in zip(gr, wr)):
        raise SystemExit(f"zpq={got[:2]} duck={want[:2]}")


def cmd_tail(args):
    import datetime
    import pyarrow.parquet as pq

    check = args.check
    if check == "names":
        rows = load_zpq(args.zpq)
        if isinstance(rows, dict):
            rows = [rows]
        want = args.names.split("|")
        for row in rows:
            got = list(row.keys())
            if got != want:
                raise SystemExit(f"names {got} want {want}")
        return
    if check == "order":
        got = pq.read_table(args.parquet or args.zpq).column("id").to_pylist()
        src = pq.read_table(args.fixture).column("id").to_pylist()
        want = [v for v in src if v is not None and v > 0]
        if got != want:
            raise SystemExit(f"order zpq={len(got)} input={len(want)}")
        return
    if check == "select_order":
        got = list(pq.read_table(args.parquet or args.zpq).column_names)
        if got != ["name", "id"]:
            raise SystemExit(f"columns {got}")
        return
    if check == "avg_exact":
        rows = load_zpq(args.zpq)
        by_g = {row["g"]: row for row in rows}
        for drow in duck_json(args.duckdb, args.sql, stable=True):
            g = drow["g"]
            # The exact sum is wider than f64. The check is the float of that sum, which is what zpq prints.
            sv, nv = float(int(drow["sv"])), int(drow["nv"])
            sw, nw = float(int(drow["sw"])), int(drow["nw"])
            got = by_g[g]
            if float(got["a"]) != sv / nv or float(got["b"]) != sw / nw:
                raise SystemExit(f"avg g={g} zpq={got['a']},{got['b']} want {sv / nv},{sw / nw}")
            st = got["st"]
            if float(st["sum"]) != sv or st["count"] != nv:
                raise SystemExit(f"avg_state g={g} {st} want {sv},{nv}")
        if len(by_g) != len(duck_json(args.duckdb, args.sql, stable=True)):
            raise SystemExit("avg group count")
        return
    if check == "levels":
        raw = load_zpq(args.zpq)
        row = raw[0] if isinstance(raw, list) else raw
        want = duck_json(args.duckdb, args.sql, stable=True)[0]["n"]
        if int(row["n"]) != int(want):
            raise SystemExit(f"count {row['n']} want {want}")
        return
    if check in ("typed", "decimal"):
        tbl = pq.read_table(args.parquet)
        got_types = [str(field.type) for field in tbl.schema]
        want_types = args.types.split("|")
        if got_types != want_types:
            raise SystemExit(f"types {got_types} want {want_types}")
        lanes = [EXACT if t.startswith("decimal") else None for t in got_types]

        def norm_cell(v):
            if isinstance(v, (bytes, bytearray)):
                return v.hex()
            if isinstance(v, datetime.datetime):
                if v.tzinfo is not None:
                    v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                return v.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
            if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
                return v.isoformat()
            if isinstance(v, datetime.time):
                return v.isoformat()
            return v

        zrows = canon_rows([[norm_cell(v) for v in row.values()] for row in tbl.to_pylist()], lanes)
        drows, names, dlane = duck_rows(args)
        if check == "decimal" and args.orders:
            scales = {}
            for part in args.orders.split("|"):
                name, scale = part.split(":")
                scales[name] = int(scale)
            fixed = []
            for row in drows:
                row = list(row)
                for name, scale in scales.items():
                    si, ci = names.index(f"{name}__sum"), names.index(f"{name}__count")
                    mi = names.index(name)
                    row[mi] = avg_dec(row[si], row[ci], scale)
                fixed.append(row[: len(want_types)])
            drows = fixed
            dlane = lanes
        want = canon_rows([[norm_cell(v) for v in row] for row in drows], dlane if check == "decimal" else lanes)
        if zrows != want:
            raise SystemExit(f"rows zpq={zrows[:2]} duck={want[:2]}")
        back = duck_json(args.duckdb, f"SELECT * FROM '{args.parquet}'", stable=True)
        if check == "decimal":
            # The file DuckDB reads back must carry the same values pyarrow did.
            spec = describe(args.duckdb, f"SELECT * FROM '{args.parquet}'", stable=True)
            brows = [[coerce(row.get(n), t) for n, t in spec] for row in back]
            if canon_rows(brows, lanes) != zrows:
                raise SystemExit("DuckDB reads other values back")
        return
    if check == "sliced_write":
        import pyarrow.parquet as pq2

        pf = pq2.ParquetFile(args.parquet)
        if pf.metadata.num_row_groups < 2:
            raise SystemExit("one row group out: the write was not sliced")
        drows, names, lanes = duck_rows(args)
        tbl = pq2.read_table(args.parquet)
        zrows = canon_rows([[row[c] for c in names] for row in tbl.to_pylist()], lanes)
        if zrows != canon_rows(drows, lanes):
            raise SystemExit(f"sliced write zpq={len(zrows)} duck={len(drows)}")
        return
    if check in ("sliced_print", "sliced_limit"):
        text = Path(args.zpq).read_text().splitlines()
        drows, names, lanes = duck_rows(args)
        if check == "sliced_limit":
            drows = drows[:7]
        zraw = [json.loads(line, parse_float=Decimal) for line in text if line.strip()]
        zrows = canon_rows([from_zpq([row[c] for c in names], lanes) for row in zraw], lanes)
        want = canon_rows(drows, lanes)
        if zrows != want:
            raise SystemExit(f"sliced print zpq={len(zrows)} duck={len(want)}")
        return
    raise SystemExit(f"bad tail check {check}")


def cmd_product(args):
    """Parquet written by the first product shape, in order, and DuckDB's read of that file."""
    import pyarrow.parquet as pq

    truth = load_zpq(args.zpq)
    if isinstance(truth, dict):
        truth = [truth]
    want = [tuple(row[c] for c in ("a", "b", "c")) for row in truth]
    table = pq.read_table(args.parquet, columns=["a", "b", "c"])
    got = list(zip(*(table[c].to_pylist() for c in ("a", "b", "c"))))
    if got != want:
        raise SystemExit(f"product parquet zpq={got[:2]} json={want[:2]}")
    spec = describe(args.duckdb, f"SELECT * FROM '{args.parquet}'", stable=True)
    raw = duck_json(args.duckdb, f"SELECT * FROM '{args.parquet}'", stable=True)
    back = [tuple(coerce(row.get(name), typ) for name, typ in spec) for row in raw]
    if back != want:
        raise SystemExit(f"product readback duck={back[:2]} json={want[:2]}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["agg", "group", "parquet", "ordered", "rows", "stats_rows", "stats_agg",
                                    "stats_select", "stats_orders", "wire", "dense", "usame", "tail", "product"])
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
    p.add_argument("--check")
    p.add_argument("--names")
    p.add_argument("--types")
    args = p.parse_args()
    args.stable = args.mode in ("ordered", "rows", "stats_rows", "stats_agg", "stats_select", "stats_orders",
                               "wire", "dense", "usame", "tail")
    if args.mode == "agg":
        cmd_agg(args)
    elif args.mode == "group":
        cmd_group(args)
    elif args.mode in ("ordered", "rows"):
        cmd_rows(args)
    elif args.mode == "stats_rows":
        cmd_stats_rows(args)
    elif args.mode == "stats_agg":
        cmd_stats_agg(args)
    elif args.mode == "stats_select":
        cmd_stats_select(args)
    elif args.mode == "stats_orders":
        cmd_stats_orders(args)
    elif args.mode == "wire":
        cmd_wire(args)
    elif args.mode == "dense":
        cmd_dense(args)
    elif args.mode == "usame":
        cmd_usame(args)
    elif args.mode == "tail":
        cmd_tail(args)
    elif args.mode == "product":
        cmd_product(args)
    else:
        cmd_parquet(args)


if __name__ == "__main__":
    main()
