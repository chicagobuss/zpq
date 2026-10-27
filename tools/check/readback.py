#!/usr/bin/env python3
"""Read a Parquet file with PyArrow's strict reader and print what it saw as JSON. No comparison policy lives here.

  readback.py FILE [--filter COL OP VALUE]...
  readback.py FILE --stored
  readback.py FILE --bloom
  readback.py FILE --footer

`--bloom` prints, per row group, each column chunk's physical type, its non-null values in physical form (integers,
floats, or {"$bytes"} for the bytes a BYTE_ARRAY, FLBA or INT96 stores), whether it has a dictionary page, and its
Bloom filter's bitset as hex (null without one), read from the footer directly. `--footer` prints {num_rows, leaves}
from the footer alone (leaf column paths, dotted). `--stored` prints {names, types, annotations, rows} as zpq's row
output names them: structs flattened to dotted leaf names, and dates, times and timestamps as their stored integers
(days, or the column's unit).

Prints {names, types, nullable, rows, row_groups (per-column metadata, with the raw footer's statistics bounds and
exactness flags), column_orders, stats_kept (PyArrow's metadata keeps those flags), pushed}. Values are JSON scalars, or
tagged objects for types JSON lacks: {"$dec"}, {"$bytes"} (hex), {"$ts"}, {"$date"}, {"$time"}, {"$uuid"}, and
{"$float": "NaN" | "Infinity" | "-Infinity"}. A file PyArrow rejects exits 1 with its error.
"""
import datetime
import decimal
import json
import math
import struct
import sys
import uuid

import pyarrow as pa
import pyarrow.parquet as pq


def cell(v):
    if isinstance(v, bool) or v is None or isinstance(v, (int, str)):
        return v
    if isinstance(v, float):
        if math.isnan(v):
            return {"$float": "NaN"}
        if math.isinf(v):
            return {"$float": "Infinity" if v > 0 else "-Infinity"}
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


def column_cells(col):
    """A column's cells; nanosecond timestamps from their ticks, which Python's datetime cannot hold."""
    t = col.type
    if pa.types.is_timestamp(t) and t.unit == "ns":
        out = []
        for v in col.cast(pa.int64()).to_pylist():
            if v is None:
                out.append(None)
                continue
            s, ns = divmod(v, 10**9)
            frac = f"{ns:09d}".rstrip("0")
            text = (datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=s)).strftime("%Y-%m-%d %H:%M:%S")
            out.append({"$ts": text + (f".{frac}" if frac else "")})
        return out
    return [cell(v) for v in col.to_pylist()]


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


# The footer walk and filter header parser, from parquet-format's BloomFilter.md (no reader's own Bloom code).
def _varint(b, i):
    r = s = 0
    while True:
        c = b[i]
        i += 1
        r |= (c & 0x7F) << s
        s += 7
        if c < 0x80:
            return r, i


def parse_header(b):
    """(numBytes, header length) of a BLOCK / XXHASH / UNCOMPRESSED filter header. Asserts on anything else."""
    i = last = 0
    nbytes = None
    seen = set()
    while True:
        h = b[i]
        i += 1
        if h == 0:
            break
        delta, t = h >> 4, h & 0xF
        assert delta, "long-form field header not expected"
        last += delta
        if last == 1:
            assert t == 5
            z, i = _varint(b, i)
            nbytes = (z >> 1) ^ -(z & 1)
        else:
            # A union with one member, field 1, an empty struct.
            assert t == 12 and b[i] == 0x1C and b[i + 1] == 0 and b[i + 2] == 0, (last, t)
            i += 3
            seen.add(last)
    assert seen == {2, 3, 4}, seen
    assert nbytes > 0 and nbytes % 32 == 0
    return nbytes, i


def load(path, offset, length=None):
    """The bitset of the filter at `offset`, checked against `length` when the footer gives it."""
    with open(path, "rb") as f:
        f.seek(offset)
        head = f.read(64)
        n, hl = parse_header(head)
        if length is not None:
            assert length == hl + n, (length, hl, n)
        f.seek(offset + hl)
        bits = f.read(n)
    assert len(bits) == n
    return bits


class _Thrift:
    """Just enough of the compact protocol to walk a Parquet footer."""

    def __init__(self, b, i=0):
        self.b, self.i = b, i

    def byte(self):
        c = self.b[self.i]
        self.i += 1
        return c

    def varint(self):
        r, self.i = _varint(self.b, self.i)
        return r

    def zigzag(self):
        z = self.varint()
        return (z >> 1) ^ -(z & 1)

    def binary(self):
        n = self.varint()
        v = self.b[self.i:self.i + n]
        self.i += n
        return v

    def list_header(self):
        h = self.byte()
        n = h >> 4
        if n == 15:
            n = self.varint()
        return n, h & 0xF

    def fields(self):
        """(id, type) of each field of a struct, until its stop byte."""
        last = 0
        while True:
            h = self.byte()
            if h == 0:
                return
            delta, t = h >> 4, h & 0xF
            last = last + delta if delta else self.zigzag()
            yield last, t

    def skip(self, t):
        if t in (1, 2):  # bool true/false live in the type nibble
            return
        if t == 3:
            self.i += 1
        elif t in (4, 5, 6):
            self.varint()
        elif t == 7:
            self.i += 8
        elif t == 8:
            self.binary()
        elif t in (9, 10):
            n, et = self.list_header()
            for _ in range(n):
                self.skip(et)
        elif t == 11:
            n = self.varint()
            if n:
                kv = self.byte()
                for _ in range(n):
                    self.skip(kv >> 4)
                    self.skip(kv & 0xF)
        elif t == 12:
            for _, ft in self.fields():
                self.skip(ft)
        else:
            raise ValueError(f"thrift type {t}")


PHYSICAL = ("BOOLEAN", "INT32", "INT64", "INT96", "FLOAT", "DOUBLE", "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY")


def footer_filters(path):
    """Per row group, per column chunk: (path, physical type, bloom_filter_offset, bloom_filter_length), read from the
    footer directly, so any pyarrow will do."""
    with open(path, "rb") as f:
        data = f.read()
    assert data[-4:] == b"PAR1"
    n = struct.unpack_from("<I", data, len(data) - 8)[0]
    r = _Thrift(data, len(data) - 8 - n)
    groups = []
    for fid, t in r.fields():
        if fid != 4:
            r.skip(t)
            continue
        nrg, _ = r.list_header()
        for _ in range(nrg):
            chunks = []
            for rfid, rt in r.fields():
                if rfid != 1:
                    r.skip(rt)
                    continue
                ncol, _ = r.list_header()
                for _ in range(ncol):
                    meta = {}
                    for cfid, ct in r.fields():
                        if cfid != 3:
                            r.skip(ct)
                            continue
                        for mfid, mt in r.fields():
                            if mfid == 1:
                                meta["type"] = PHYSICAL[r.zigzag()]
                            elif mfid == 3:
                                k, _ = r.list_header()
                                meta["path"] = tuple(r.binary().decode() for _ in range(k))
                            elif mfid == 14:
                                meta["offset"] = r.zigzag()
                            elif mfid == 15:
                                meta["length"] = r.zigzag()
                            else:
                                r.skip(mt)
                    chunks.append((meta["path"], meta["type"], meta.get("offset"), meta.get("length")))
            groups.append(chunks)
    return groups




def footer_stats(data):
    """Per row group, per column: the Statistics fields a footer sets (min_value 6, max_value 5) and its exactness
    flags (is_min_value_exact 8, is_max_value_exact 7; None when absent), walked from the raw thrift."""
    n = struct.unpack_from("<I", data, len(data) - 8)[0]
    r = _Thrift(data, len(data) - 8 - n)
    groups = []
    for fid, t in r.fields():
        if fid != 4:
            r.skip(t)
            continue
        for _ in range(r.list_header()[0]):
            chunks = []
            for rfid, rt in r.fields():
                if rfid != 1:
                    r.skip(rt)
                    continue
                for _ in range(r.list_header()[0]):
                    st = {"has_min": False, "has_max": False, "min_exact": None, "max_exact": None}
                    for cfid, ct in r.fields():
                        if cfid != 3:
                            r.skip(ct)
                            continue
                        for mfid, mt in r.fields():
                            if mfid != 12:
                                r.skip(mt)
                                continue
                            for sfid, stt in r.fields():
                                if sfid in (7, 8):
                                    st["max_exact" if sfid == 7 else "min_exact"] = stt == 1
                                    continue
                                if sfid in (5, 6):
                                    st["has_max" if sfid == 5 else "has_min"] = True
                                r.skip(stt)
                    chunks.append(st)
            groups.append(chunks)
    return groups


def physical_values(arr, physical):
    """A chunk's non-null values as its physical type stores them."""
    import pyarrow.compute as pc

    while pa.types.is_list(arr.type) or pa.types.is_large_list(arr.type):
        arr = arr.flatten()
    t = arr.type
    arr = arr.drop_null()
    if physical == "INT96":
        # nanoseconds of the day, then the Julian day; 2440588 is 1970-01-01
        ns = arr.cast(pa.timestamp("ns")).cast(pa.int64()).to_pylist()
        day = 86_400 * 10**9
        return [{"$bytes": struct.pack("<qI", v % day, v // day + 2440588).hex()} for v in ns]
    if physical in ("INT32", "INT64"):
        if not pa.types.is_integer(t):
            arr = arr.cast(pa.int64() if physical == "INT64" else pa.int32())
        elif pa.types.is_unsigned_integer(t) and t.bit_width == 64:
            arr = pc.cast(arr, pa.int64(), safe=False)
        return [int(v) for v in arr.to_pylist()]
    out = []
    for v in arr.to_pylist():
        if isinstance(v, str):
            v = v.encode()
        out.append({"$bytes": v.hex()} if isinstance(v, (bytes, bytearray)) else cell(v))
    return out


def bloom(path):
    pf = pq.ParquetFile(path)
    groups = []
    for ri, chunks in enumerate(footer_filters(path)):
        out = []
        for ci, (col_path, physical, offset, length) in enumerate(chunks):
            name = ".".join(col_path)
            arr = pf.read_row_group(ri, columns=[name]).column(0).combine_chunks()
            bits = load(path, offset, length).hex() if offset else None
            out.append({"path": name, "physical": physical, "bits": bits, "values": physical_values(arr, physical),
                        "dictionary": pf.metadata.row_group(ri).column(ci).has_dictionary_page})
        groups.append(out)
    print(json.dumps(groups))


def footer(path):
    """The footer alone, as PyArrow reads it: no page is decoded."""
    md = pq.read_metadata(path)
    print(json.dumps({"num_rows": md.num_rows,
                      "leaves": [md.schema.column(i).path for i in range(md.num_columns)]}))


def main():
    path = sys.argv[1]
    if sys.argv[2:] == ["--bloom"]:
        return bloom(path)
    if sys.argv[2:] == ["--footer"]:
        try:
            return footer(path)
        except Exception as e:  # noqa: BLE001 - the reader's verdict is the output
            print(f"pyarrow rejected {path}: {e}", file=sys.stderr)
            sys.exit(1)
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
    with open(path, "rb") as f:
        raw = footer_stats(f.read())
    # PyArrow has no Python exactness properties: reserialize its parsed metadata to see which flags it kept.
    sink = pa.BufferOutputStream()
    md.write_metadata_file(sink)
    kept = footer_stats(sink.getvalue().to_pybytes()) == raw
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
                "dictionary": cc.has_dictionary_page,
                "codec": cc.compression,
                "column_index": cc.has_column_index,
                "offset_index": cc.has_offset_index,
                **raw[g][c],
            })
        meta.append({"rows": rg.num_rows, "total_byte_size": rg.total_byte_size, "columns": cols})
    pushed = []
    for col, op, value in filters:
        typ = table.schema.field(col).type
        if value.startswith("$pow2:"):
            value = 2 ** int(value[6:])
        pushed.append(pq.read_table(path, filters=[(col, op, scalar(value, typ))]).num_rows)
    print(json.dumps({
        "names": names,
        "types": [str(f.type) for f in table.schema],
        "nullable": [f.nullable for f in table.schema],
        "rows": [list(r) for r in zip(*(column_cells(table.column(n)) for n in names))] if names else [],
        "row_groups": meta,
        "column_orders": column_orders(path),
        "stats_kept": kept,
        "pushed": pushed,
    }))


if __name__ == "__main__":
    main()
