#!/usr/bin/env python3
"""Read a Parquet file with PyArrow's strict reader and print what it saw as JSON. No comparison policy lives here.

  readback.py FILE [--filter COL OP VALUE]...
  readback.py FILE --stored

`--stored` prints {names, types, annotations, rows} as zpq's row output names them: structs flattened to dotted leaf names, and
dates, times and timestamps as their stored integers (days, or the column's unit).

Prints {names, types, rows, row_groups (per-column metadata), column_orders, pushed}. Values are JSON scalars, or
tagged objects for types JSON lacks: {"$dec"}, {"$bytes"} (hex), {"$ts"}, {"$date"}, {"$time"}, {"$uuid"}; NaN and
the infinities are the strings "NaN", "Infinity", "-Infinity". A file PyArrow rejects exits 1 with its error.
"""
import datetime
import decimal
import json
import math
import sys
import uuid

import pyarrow as pa
import pyarrow.parquet as pq


def cell(v):
    if isinstance(v, bool) or v is None or isinstance(v, (int, str)):
        return v
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "Infinity" if v > 0 else "-Infinity"
        return v
    if isinstance(v, decimal.Decimal):
        return {"$dec": str(v)}
    if isinstance(v, (bytes, bytearray)):
        return {"$bytes": bytes(v).hex()}
    if isinstance(v, uuid.UUID):
        return {"$uuid": str(v).lower()}
    if isinstance(v, datetime.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return {"$ts": v.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")}
    if isinstance(v, datetime.date):
        return {"$date": v.isoformat()}
    if isinstance(v, datetime.time):
        return {"$time": v.isoformat()}
    if isinstance(v, dict):
        return {k: cell(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [cell(x) for x in v]
    raise SystemExit(f"readback: no JSON form for {type(v).__name__}")


def scalar(value, typ):
    if pa.types.is_unsigned_integer(typ) or pa.types.is_signed_integer(typ):
        return pa.scalar(int(value), typ)
    return value


def column_orders(path):
    """FileMetaData.column_orders member ids (0 = an empty union), or None when the file declares none."""
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
    from datagen.parquet import Reader, T_STRUCT, field

    data = open(path, "rb").read()
    n = int.from_bytes(data[-8:-4], "little")
    orders = field(Reader(data[-8 - n:-8]).value(T_STRUCT), 7)
    return None if orders is None else [u[0][0] if u else 0 for u in orders[2][1]]


def stored(path):
    import pyarrow.compute as pc

    table = pq.read_table(path)
    while any(pa.types.is_struct(f.type) for f in table.schema):
        table = table.flatten()
    cols = []
    for f, col in zip(table.schema, table.columns):
        if pa.types.is_date32(f.type) or pa.types.is_time32(f.type):
            col = pc.cast(col, pa.int32())
        elif pa.types.is_date(f.type) or pa.types.is_time(f.type) or pa.types.is_timestamp(f.type):
            col = pc.cast(col, pa.int64())
        cols.append(col)
    table = pa.Table.from_arrays(cols, names=table.column_names)
    leaves = pq.ParquetFile(path).schema
    notes = {}
    for i in range(len(leaves)):
        c = leaves.column(i)
        notes[c.path] = f"{c.logical_type} {c.converted_type}"
    print(json.dumps({
        "names": table.column_names,
        "types": [str(f.type) for f in table.schema],
        "annotations": [notes.get(n, "") for n in table.column_names],
        "rows": [[cell(row[n]) for n in table.column_names] for row in table.to_pylist()],
    }))


def main():
    path = sys.argv[1]
    if sys.argv[2:] == ["--stored"]:
        try:
            return stored(path)
        except Exception as e:  # noqa: BLE001 - the reader's verdict is the output
            print(f"pyarrow rejected {path}: {e}", file=sys.stderr)
            sys.exit(1)
    filters = []
    rest = sys.argv[2:]
    while rest:
        if rest[0] != "--filter" or len(rest) < 4:
            raise SystemExit("usage: readback.py FILE [--filter COL OP VALUE]...")
        filters.append(tuple(rest[1:4]))
        rest = rest[4:]
    try:
        pf = pq.ParquetFile(path)
        table = pq.read_table(path)
    except Exception as e:  # noqa: BLE001 - the reader's verdict is the output
        print(f"pyarrow rejected {path}: {e}", file=sys.stderr)
        sys.exit(1)
    md = pf.metadata
    names = list(table.column_names)
    meta = []
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        cols = []
        for c in range(md.num_columns):
            cc = rg.column(c)
            st = cc.statistics
            bounded = st is not None and st.has_min_max
            floats = cc.physical_type in ("FLOAT", "DOUBLE")
            cols.append({
                "path": cc.path_in_schema,
                "physical": cc.physical_type,
                "encodings": list(cc.encodings),
                "uncompressed": cc.total_uncompressed_size,
                "has_min_max": bounded,
                "min": cell(st.min) if bounded and floats else None,
                "max": cell(st.max) if bounded and floats else None,
            })
        meta.append({"total_byte_size": rg.total_byte_size, "columns": cols})
    pushed = []
    for col, op, value in filters:
        typ = table.schema.field(col).type
        if value.startswith("$pow2:"):
            value = 2 ** int(value[6:])
        pushed.append(pq.read_table(path, filters=[(col, op, scalar(value, typ))]).num_rows)
    print(json.dumps({
        "names": names,
        "types": [str(f.type) for f in table.schema],
        "rows": [[cell(row[n]) for n in names] for row in table.to_pylist()],
        "row_groups": meta,
        "column_orders": column_orders(path),
        "pushed": pushed,
    }))


if __name__ == "__main__":
    main()
