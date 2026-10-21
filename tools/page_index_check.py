#!/usr/bin/env python3
"""Check a Parquet file's page layout and page index against its values, read with pyarrow.

For every column chunk: walk its pages from the footer's offsets (headers parsed here, a small Thrift compact reader),
then check the OffsetIndex (one location per data page: offset, size with header, first row) and the ColumnIndex (per
page null flag, min/max of the page's present values, null count, NaN count, boundary order) against the values
pyarrow decodes for each page's rows. Flat columns only, which is what zpq re-encodes.

    page_index_check.py FILE [--require-index] [--quiet]

Prints one summary line per file (row groups, pages, rows per page) and exits 1 on the first mismatch.
"""
import argparse
import decimal
import math
import struct
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq


class Compact:
    """Just enough of the Thrift compact protocol to read structs as {field id: value}."""

    def __init__(self, buf, pos=0):
        self.buf, self.pos = buf, pos

    def byte(self):
        b = self.buf[self.pos]
        self.pos += 1
        return b

    def varint(self):
        shift = out = 0
        while True:
            b = self.byte()
            out |= (b & 0x7F) << shift
            if not b & 0x80:
                return out
            shift += 7

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def value(self, t):
        if t in (1, 2):
            return t == 1
        if t == 3:
            return struct.unpack("b", bytes([self.byte()]))[0]
        if t in (4, 5, 6):
            return self.zigzag()
        if t == 7:
            v = struct.unpack("<d", self.buf[self.pos:self.pos + 8])[0]
            self.pos += 8
            return v
        if t == 8:
            n = self.varint()
            v = bytes(self.buf[self.pos:self.pos + n])
            self.pos += n
            return v
        if t in (9, 10):
            h = self.byte()
            n, et = h >> 4, h & 0x0F
            if n == 15:
                n = self.varint()
            if et in (1, 2):
                return [self.byte() == 1 for _ in range(n)]
            return [self.value(et) for _ in range(n)]
        if t == 11:
            n = self.varint()
            if n == 0:
                return {}
            kv = self.byte()
            return {self.value(kv >> 4): self.value(kv & 0x0F) for _ in range(n)}
        if t == 12:
            return self.struct()
        raise ValueError(f"thrift type {t} at {self.pos}")

    def struct(self):
        out, last = {}, 0
        while True:
            h = self.byte()
            if h == 0:
                return out
            delta, t = h >> 4, h & 0x0F
            fid = last + delta if delta else self.zigzag()
            out[fid] = self.value(t)
            last = fid


def footer(data):
    if data[:4] != b"PAR1" or data[-4:] != b"PAR1":
        raise SystemExit("not a parquet file")
    n = struct.unpack("<I", data[-8:-4])[0]
    return Compact(data, len(data) - 8 - n).struct()


DATA_PAGE, DICTIONARY_PAGE, DATA_PAGE_V2 = 0, 2, 3
PHYS = {0: "BOOLEAN", 1: "INT32", 2: "INT64", 3: "INT96", 4: "FLOAT", 5: "DOUBLE", 6: "BYTE_ARRAY", 7: "FLBA"}


def decode_bound(phys, raw, signed=True):
    if phys == "INT32":
        return struct.unpack("<i" if signed else "<I", raw)[0]
    if phys == "INT64":
        return struct.unpack("<q" if signed else "<Q", raw)[0]
    if phys == "FLOAT":
        return struct.unpack("<f", raw)[0]
    if phys == "DOUBLE":
        return struct.unpack("<d", raw)[0]
    if phys == "BOOLEAN":
        return raw[0] != 0
    return raw


def as_key(phys, v):
    """A Python value from pyarrow in the order the stats use (bytes for byte arrays)."""
    if phys in ("BYTE_ARRAY", "FLBA"):
        return v.encode() if isinstance(v, str) else bytes(v)
    return v


def check_file(path, require_index, quiet):
    data = open(path, "rb").read()
    meta = footer(data)
    pf = pq.ParquetFile(path)
    schema_elems = meta[2]
    leaves = [e for e in schema_elems[1:] if 5 not in e or not e.get(5)]  # num_children unset → leaf
    total_pages = 0
    rows_per_page = []
    row_base = 0
    errors = []

    def fail(msg):
        errors.append(msg)
        raise SystemExit(f"{path}: {msg}")

    for rgi, rg in enumerate(meta[4]):
        rg_rows = rg[3]
        for ci, chunk in enumerate(rg[1]):
            cm = chunk[3]
            phys = PHYS[cm[1]]
            elem = leaves[ci]
            unsigned = elem.get(6) in (11, 12, 13, 14) or (10 in elem and 10 in elem[10] and not elem[10][10].get(2, True))
            start = cm.get(11) if cm.get(11) is not None else cm[9]
            end = start + cm[7]
            pos = start
            pages = []
            while pos < end:
                r = Compact(data, pos)
                h = r.struct()
                body = r.pos
                ptype, csize = h[1], h[3]
                n = (h.get(5) or h.get(8) or h.get(7) or {}).get(1, 0)
                pages.append({"type": ptype, "offset": pos, "size": body - pos + csize, "values": n})
                pos = body + csize
            if pos != end:
                fail(f"rg {rgi} col {ci}: pages end at {pos}, chunk at {end}")
            data_pages = [p for p in pages if p["type"] in (DATA_PAGE, DATA_PAGE_V2)]
            if sum(p["values"] for p in data_pages) != cm[5]:
                fail(f"rg {rgi} col {ci}: pages hold {sum(p['values'] for p in data_pages)} values, chunk {cm[5]}")
            if cm[5] != rg_rows:
                fail(f"rg {rgi} col {ci}: {cm[5]} values for {rg_rows} rows (flat columns only)")
            if (cm.get(11) is not None) != any(p["type"] == DICTIONARY_PAGE for p in pages):
                fail(f"rg {rgi} col {ci}: dictionary_page_offset disagrees with the pages")
            if data_pages and data_pages[0]["offset"] != cm[9]:
                fail(f"rg {rgi} col {ci}: data_page_offset {cm[9]} is not the first data page {data_pages[0]['offset']}")
            total_pages += len(data_pages)
            rows_per_page += [p["values"] for p in data_pages]
            if 4 not in chunk and 6 not in chunk:
                if require_index:
                    fail(f"rg {rgi} col {ci}: no page index")
                continue
            # OffsetIndex: one location per data page, in order.
            oi = Compact(data, chunk[4]).struct()
            if len(Compact(data, chunk[4]).buf) and chunk.get(5) is not None:
                r = Compact(data, chunk[4])
                r.struct()
                if r.pos - chunk[4] != chunk[5]:
                    fail(f"rg {rgi} col {ci}: offset index length {chunk[5]}, parsed {r.pos - chunk[4]}")
            locs = oi[1]
            if len(locs) != len(data_pages):
                fail(f"rg {rgi} col {ci}: offset index has {len(locs)} pages, chunk {len(data_pages)}")
            first = 0
            for loc, p in zip(locs, data_pages):
                if loc[1] != p["offset"] or loc[2] != p["size"] or loc[3] != first:
                    fail(f"rg {rgi} col {ci}: location {loc} vs page at {p['offset']} size {p['size']} row {first}")
                first += p["values"]
            if 6 not in chunk:
                if require_index:
                    fail(f"rg {rgi} col {ci}: no column index")
                continue
            r = Compact(data, chunk[6])
            ci_s = r.struct()
            if chunk.get(7) is not None and r.pos - chunk[6] != chunk[7]:
                fail(f"rg {rgi} col {ci}: column index length {chunk[7]}, parsed {r.pos - chunk[6]}")
            null_pages, mins, maxs, order = ci_s[1], ci_s[2], ci_s[3], ci_s[4]
            null_counts = ci_s.get(5)
            nan_counts = ci_s.get(8)
            if not (len(null_pages) == len(mins) == len(maxs) == len(data_pages)):
                fail(f"rg {rgi} col {ci}: column index lists disagree with {len(data_pages)} pages")
            # One column chunk at a time: memory stays one chunk's values, whatever the file's size.
            col = pf.read_row_group(rgi, columns=[pf.schema_arrow.names[ci]]).column(0)
            first = 0
            prev = None
            asc = desc = True
            for pi, p in enumerate(data_pages):
                vals = col.slice(first, p["values"])
                first += p["values"]
                nulls = vals.null_count
                present = vals.drop_null()
                if null_counts is not None and null_counts[pi] != nulls:
                    fail(f"rg {rgi} col {ci} page {pi}: null count {null_counts[pi]}, values {nulls}")
                if null_pages[pi] != (len(present) == 0):
                    fail(f"rg {rgi} col {ci} page {pi}: null page {null_pages[pi]}, {len(present)} present")
                if len(present) == 0:
                    continue
                raw = present.to_pylist()
                if raw and isinstance(raw[0], decimal.Decimal):
                    scale = elem.get(7) or 0
                    ints = [int(v.scaleb(scale)) for v in raw]
                    if phys == "FLBA":
                        py = [v.to_bytes(elem[2], "big", signed=True) for v in ints]
                    else:
                        py = ints
                else:
                    py = [as_key(phys, v) for v in raw]
                if phys in ("FLOAT", "DOUBLE"):
                    nans = sum(1 for v in py if math.isnan(v))
                    if nan_counts is not None and nan_counts[pi] != nans:
                        fail(f"rg {rgi} col {ci} page {pi}: nan count {nan_counts[pi]}, values {nans}")
                    py = [v for v in py if not math.isnan(v)]
                    if not py:
                        continue
                if hasattr(py[0], "timestamp") or not isinstance(py[0], (int, float, bytes, bool)):
                    py = [as_key(phys, v) for v in pc.cast(present, "int64" if phys == "INT64" else "int32").to_pylist()]
                lo, hi = decode_bound(phys, mins[pi], not unsigned), decode_bound(phys, maxs[pi], not unsigned)
                if phys == "FLBA" and py and isinstance(py[0], bytes) and raw and isinstance(raw[0], decimal.Decimal):
                    # Decimal bounds order as signed integers, not bytes.
                    key = lambda b: int.from_bytes(b, "big", signed=True)
                    lo, hi = key(lo), key(hi)
                    py = [key(b) for b in py]
                if phys in ("INT32", "INT64") and unsigned:
                    width = 32 if phys == "INT32" else 64
                    py = [v % (1 << width) for v in py]
                if lo != min(py) or hi != max(py):
                    if not (phys in ("FLOAT", "DOUBLE") and lo == min(py) == 0 and hi == max(py)):
                        fail(f"rg {rgi} col {ci} page {pi}: bounds {lo!r}..{hi!r}, values {min(py)!r}..{max(py)!r}")
                if prev is not None:
                    asc = asc and prev[0] <= lo and prev[1] <= hi
                    desc = desc and prev[0] >= lo and prev[1] >= hi
                prev = (lo, hi)
            if order == 1 and not asc:
                fail(f"rg {rgi} col {ci}: boundary order ASCENDING, pages are not")
            if order == 2 and not desc:
                fail(f"rg {rgi} col {ci}: boundary order DESCENDING, pages are not")
        row_base += rg_rows
    if pf.metadata.num_rows != row_base:
        fail(f"footer rows {pf.metadata.num_rows}, row groups {row_base}")
    if not quiet:
        rp = sorted(rows_per_page)
        med = rp[len(rp) // 2] if rp else 0
        print(f"{path}: ok, {len(meta[4])} row groups, {total_pages} data pages, rows/page median {med} "
              f"min {rp[0] if rp else 0} max {rp[-1] if rp else 0}")
    return len(meta[4]), total_pages


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--require-index", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    for f in a.files:
        check_file(f, a.require_index, a.quiet)


if __name__ == "__main__":
    main()
