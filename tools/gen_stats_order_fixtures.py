#!/usr/bin/env python3
"""Generate the statistics-order fixtures for the pruning tests in src/core/scan.zig and tools/differential.py.

unsigned_order.parquet: four row groups of 256 rows, 64-row pages with a page index. Every unsigned width holds values
on both sides of its signed range, so stats or rows read as signed put them in the wrong order.

  i    int32, 0..1023                     row number, for spotting rows
  u8   uint8, i % 256                     crosses 2^7 inside every row group
  u16  uint16, (i * 257) % 65536          crosses 2^15
  u32  uint32                             rg0 0..255, rg1 2^31-128..2^31+127, rg2 3e9+, rg3 2^32-256..2^32-1
  u64  uint64                             the same layout around 2^63 and 2^64
  n32  uint32, nullable                   u32 with every third row null; rg3 all null

deprecated_stats.parquet: two row groups of three rows whose chunks carry ONLY the deprecated Statistics.min/max
(fields 1/2), computed as the spec defines them: by signed comparison, whatever the type. pyarrow writes min_value/
max_value instead, so the footer is rewritten. Row group 0 is chosen so the signed pair is a plausible-looking range
that excludes a stored value; a reader that trusts it there drops matching rows.

  s    utf8            rg0 'é','b','a'     signed bytewise: 'é' (0xC3..) is the minimum, so 'a' is "below" the range
  dec  decimal(9, 2)   rg0 1.00,1.28,2.56  FLBA(4); 1.28 ends in 0x80, so signed bytes put it below 1.00
  u    uint32          rg0 1,2,3e9         3e9 is negative as i32
  i    int64           rg0 1,2,3           signed order is the type order: the deprecated pair stays usable
  d    double          rg0 0.5,1.5,2.5     likewise
  idec decimal(9, 2)   rg0 1.00,1.28,2.56  over INT32: signed is its numeric order, so the pair stays usable

column_order.parquet: two row groups of three rows, with a page index. The footer declares a column order this reader
does not implement for `s` (ColumnOrder union member 3), standing in for a writer with a collation of its own, and
its chunk and page bounds are written in that case-insensitive order: row group 0 holds 'B','a','c' and records
['a', 'c'], which read bytewise excludes 'B'. `i` keeps TYPE_DEFINED_ORDER and must still prune.

  s    utf8     rg0 'B','a','c'    rg1 'x','Y','z'
  i    int64    rg0 1,2,3          rg1 101,102,103

nan_stats.parquet: three row groups of three rows, `d` double and `i` int64. Bounds leave NaN out, so every row group
records [0, 0]; `d != 0` holds for the NaN rows. The footer adds Statistics.nan_count (field 9) where noted.

  d    double   rg0 0,NaN,0 (no nan_count)   rg1 0,0,0 (nan_count 0)   rg2 0,NaN,0 (nan_count 1)

Usage:  .venv/bin/python tools/gen_stats_order_fixtures.py
"""

import decimal
import pathlib
import struct

import pyarrow as pa
import pyarrow.parquet as pq

OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet"
ROWS_PER_GROUP = 256

U32_BASES = [0, 2**31 - 128, 3_000_000_000, 2**32 - 256]
U64_BASES = [0, 2**63 - 128, 18_000_000_000_000_000_000, 2**64 - 256]


def unsigned_table() -> pa.Table:
    n = ROWS_PER_GROUP * len(U32_BASES)
    rows = range(n)
    u32 = [U32_BASES[i // ROWS_PER_GROUP] + i % ROWS_PER_GROUP for i in rows]
    u64 = [U64_BASES[i // ROWS_PER_GROUP] + i % ROWS_PER_GROUP for i in rows]
    n32 = [None if (i % 3 == 0 or i // ROWS_PER_GROUP == 3) else v for i, v in zip(rows, u32)]
    return pa.table(
        {
            "i": pa.array(rows, type=pa.int32()),
            "u8": pa.array([i % 256 for i in rows], type=pa.uint8()),
            "u16": pa.array([(i * 257) % 65536 for i in rows], type=pa.uint16()),
            "u32": pa.array(u32, type=pa.uint32()),
            "u64": pa.array(u64, type=pa.uint64()),
            "n32": pa.array(n32, type=pa.uint32()),
        }
    )


# ---- thrift compact protocol, just enough to rewrite a parquet footer ----
# A struct decodes to a list of [field_id, type, value]; a list to (elem_type, [values]). Unknown fields round-trip.
T_TRUE, T_FALSE, T_BYTE, T_I16, T_I32, T_I64, T_DOUBLE, T_BINARY, T_LIST, T_SET, T_MAP, T_STRUCT = range(1, 13)


class Reader:
    def __init__(self, buf: bytes):
        self.buf, self.pos = buf, 0

    def byte(self) -> int:
        self.pos += 1
        return self.buf[self.pos - 1]

    def varint(self) -> int:
        out = shift = 0
        while True:
            b = self.byte()
            out |= (b & 0x7F) << shift
            shift += 7
            if not b & 0x80:
                return out

    def zigzag(self) -> int:
        v = self.varint()
        return (v >> 1) ^ -(v & 1)

    def value(self, t: int):
        if t in (T_TRUE, T_FALSE):
            return t == T_TRUE
        if t == T_BYTE:
            return self.byte()
        if t in (T_I16, T_I32, T_I64):
            return self.zigzag()
        if t == T_DOUBLE:
            self.pos += 8
            return self.buf[self.pos - 8:self.pos]
        if t == T_BINARY:
            n = self.varint()
            self.pos += n
            return self.buf[self.pos - n:self.pos]
        if t in (T_LIST, T_SET):
            h = self.byte()
            n, et = h >> 4, h & 0x0F
            if n == 15:
                n = self.varint()
            if et in (T_TRUE, T_FALSE):
                return et, [self.byte() == 1 for _ in range(n)]
            return et, [self.value(et) for _ in range(n)]
        if t == T_STRUCT:
            fields, last = [], 0
            while True:
                h = self.byte()
                if h == 0:
                    return fields
                delta, ft = h >> 4, h & 0x0F
                fid = last + delta if delta else self.zigzag()
                last = fid
                fields.append([fid, ft, self.value(ft)])
        raise ValueError(f"unsupported thrift type {t}")


def _varint(v: int) -> bytes:
    out = bytearray()
    while True:
        if v < 0x80:
            out.append(v)
            return bytes(out)
        out.append((v & 0x7F) | 0x80)
        v >>= 7


def _zigzag(v: int) -> bytes:
    return _varint((v << 1) ^ (v >> 63))


def write_value(t: int, v) -> bytes:
    if t == T_BYTE:
        return bytes([v])
    if t in (T_I16, T_I32, T_I64):
        return _zigzag(v)
    if t == T_DOUBLE:
        return bytes(v)
    if t == T_BINARY:
        return _varint(len(v)) + bytes(v)
    if t in (T_LIST, T_SET):
        et, items = v
        head = bytes([(len(items) << 4) | et]) if len(items) < 15 else bytes([0xF0 | et]) + _varint(len(items))
        if et in (T_TRUE, T_FALSE):
            return head + bytes(1 if x else 2 for x in items)
        return head + b"".join(write_value(et, x) for x in items)
    if t == T_STRUCT:
        out, last = bytearray(), 0
        for fid, ft, fv in sorted(v, key=lambda f: f[0]):
            wire_t = (T_TRUE if fv else T_FALSE) if ft in (T_TRUE, T_FALSE) else ft
            out += bytes([((fid - last) << 4) | wire_t]) if 0 < fid - last <= 15 else bytes([wire_t]) + _zigzag(fid)
            last = fid
            if ft not in (T_TRUE, T_FALSE):
                out += write_value(ft, fv)
        return bytes(out) + b"\x00"
    raise ValueError(f"unsupported thrift type {t}")


def field(fields: list, fid: int):
    return next((f for f in fields if f[0] == fid), None)


def rewrite_footer(path: pathlib.Path, edit) -> None:
    """Decode the FileMetaData of `path`, let `edit(meta, body)` mutate it in place, and write it back. `body` is the
    file up to the footer; `edit` may append to it, e.g. a replacement page index whose offset it records in `meta`."""
    data = path.read_bytes()
    assert data[:4] == data[-4:] == b"PAR1"
    footer_len = struct.unpack("<I", data[-8:-4])[0]
    start = len(data) - 8 - footer_len
    meta = Reader(data[start:len(data) - 8]).value(T_STRUCT)
    body = bytearray(data[:start])
    edit(meta, body)
    footer = write_value(T_STRUCT, meta)
    path.write_bytes(bytes(body) + footer + struct.pack("<I", len(footer)) + b"PAR1")


def signed_bytes_key(b: bytes) -> bytes:
    """Orders byte strings as a signed-byte comparison would (0x80..0xFF before 0x00..0x7F)."""
    return bytes(x ^ 0x80 for x in b)


# Per column: plain encoding of one value, and the signed order the deprecated min/max use.
DEPRECATED_COLUMNS = {
    "s": (lambda v: v.encode(), lambda v: signed_bytes_key(v.encode())),
    "dec": (lambda v: int(v * 100).to_bytes(4, "big", signed=True),
            lambda v: signed_bytes_key(int(v * 100).to_bytes(4, "big", signed=True))),
    "u": (lambda v: struct.pack("<I", v), lambda v: v - 2**32 if v >= 2**31 else v),
    "i": (lambda v: struct.pack("<q", v), lambda v: v),
    "d": (lambda v: struct.pack("<d", v), lambda v: v),
    "idec": (lambda v: struct.pack("<i", v), lambda v: v),  # unscaled; the footer rewrite adds DECIMAL(9, 2)
}
DEPRECATED_ROW_GROUPS = [
    {"s": ["é", "b", "a"], "dec": ["1.00", "1.28", "2.56"], "u": [1, 2, 3_000_000_000], "i": [1, 2, 3],
     "d": [0.5, 1.5, 2.5], "idec": [100, 128, 256]},
    {"s": ["x", "y", "z"], "dec": ["5.00", "6.00", "7.00"], "u": [10, 11, 12], "i": [101, 102, 103],
     "d": [10.5, 11.5, 12.5], "idec": [500, 600, 700]},
]


def deprecated_table() -> pa.Table:
    cols = {name: [v for rg in DEPRECATED_ROW_GROUPS for v in rg[name]] for name in DEPRECATED_COLUMNS}
    return pa.table(
        {
            "s": pa.array(cols["s"], type=pa.string()),
            "dec": pa.array([decimal.Decimal(v) for v in cols["dec"]], type=pa.decimal128(9, 2)),
            "u": pa.array(cols["u"], type=pa.uint32()),
            "i": pa.array(cols["i"], type=pa.int64()),
            "d": pa.array(cols["d"], type=pa.float64()),
            "idec": pa.array(cols["idec"], type=pa.int32()),
        }
    )


def to_deprecated_stats(meta: list, _body: bytearray) -> None:
    """Replace every chunk's Statistics with the deprecated pair only: max (1), min (2), null_count (3). Also annotate
    `idec` as DECIMAL(9, 2) over INT32 (pyarrow would store every decimal of the table one way or the other)."""
    idec = next(e for e in field(meta, 2)[2][1] if field(e, 4)[2] == b"idec")
    decimal_type = [[1, T_I32, 2], [2, T_I32, 9]]  # LogicalType.DECIMAL (5) {scale, precision}
    idec += [[6, T_I32, 5], [7, T_I32, 2], [8, T_I32, 9], [10, T_STRUCT, [[5, T_STRUCT, decimal_type]]]]
    for rg_idx, rg in enumerate(field(meta, 4)[2][1]):
        for chunk in field(rg, 1)[2][1]:
            cmd = field(chunk, 3)[2]
            name = field(cmd, 3)[2][1][0].decode()
            encode, key = DEPRECATED_COLUMNS[name]
            values = DEPRECATED_ROW_GROUPS[rg_idx][name]
            if name == "dec":
                values = [decimal.Decimal(v) for v in values]
            stats = field(cmd, 12)[2]
            stats[:] = [f for f in stats if f[0] == 3]
            stats.append([1, T_BINARY, encode(max(values, key=key))])
            stats.append([2, T_BINARY, encode(min(values, key=key))])


COLLATED_ROW_GROUPS = [{"s": ["B", "a", "c"], "i": [1, 2, 3]}, {"s": ["x", "Y", "z"], "i": [101, 102, 103]}]
UNKNOWN_COLUMN_ORDER = 3


def to_collated_order(meta: list, body: bytearray) -> None:
    """Declare an unknown column order for `s` and write its chunk and page bounds in case-insensitive order."""
    names = [f[2][1][0].decode() for f in [field(c, 3)[2] for c in field(field(meta, 4)[2][1][0], 1)[2][1]]
             for f in [field(f, 3)]]
    orders = field(meta, 7)[2][1]
    orders[names.index("s")] = [[UNKNOWN_COLUMN_ORDER, T_STRUCT, []]]
    for rg_idx, rg in enumerate(field(meta, 4)[2][1]):
        chunk = field(rg, 1)[2][1][names.index("s")]
        values = COLLATED_ROW_GROUPS[rg_idx]["s"]
        lo, hi = (v.encode() for v in (min(values, key=str.lower), max(values, key=str.lower)))
        stats = field(field(chunk, 3)[2], 12)[2]
        stats[:] = [f for f in stats if f[0] in (3, 4)] + [[5, T_BINARY, hi], [6, T_BINARY, lo]]
        # One page per row group: replace its ColumnIndex bounds and append the re-encoded index.
        ci_off, ci_len = field(chunk, 6)[2], field(chunk, 7)[2]
        ci = Reader(bytes(body[ci_off:ci_off + ci_len])).value(T_STRUCT)
        assert len(field(ci, 2)[2][1]) == 1, "expected one page per row group"
        field(ci, 2)[2] = (T_BINARY, [lo])
        field(ci, 3)[2] = (T_BINARY, [hi])
        encoded = write_value(T_STRUCT, ci)
        field(chunk, 6)[2], field(chunk, 7)[2] = len(body), len(encoded)
        body += encoded


def write_collated() -> None:
    out = OUT_DIR / "column_order.parquet"
    table = pa.table(
        {
            "s": pa.array([v for rg in COLLATED_ROW_GROUPS for v in rg["s"]], type=pa.string()),
            "i": pa.array([v for rg in COLLATED_ROW_GROUPS for v in rg["i"]], type=pa.int64()),
        }
    )
    pq.write_table(table, out, row_group_size=3, compression="snappy", use_dictionary=False, write_page_index=True)
    rewrite_footer(out, to_collated_order)
    print(f"wrote {out} ({out.stat().st_size} bytes)")


NAN_ROW_GROUPS = [[0.0, float("nan"), 0.0], [0.0, 0.0, 0.0], [0.0, float("nan"), 0.0]]
NAN_COUNTS = [None, 0, 1]


def add_nan_counts(meta: list, _body: bytearray) -> None:
    for rg, nan_count in zip(field(meta, 4)[2][1], NAN_COUNTS):
        stats = field(field(field(rg, 1)[2][1][0], 3)[2], 12)[2]
        stats[:] = [f for f in stats if f[0] != 9]
        if nan_count is not None:
            stats.append([9, T_I64, nan_count])


def write_nan() -> None:
    out = OUT_DIR / "nan_stats.parquet"
    d = [v for rg in NAN_ROW_GROUPS for v in rg]
    table = pa.table({"d": pa.array(d, type=pa.float64()), "i": pa.array(range(len(d)), type=pa.int64())})
    pq.write_table(table, out, row_group_size=3, compression="snappy", use_dictionary=False, write_page_index=True)
    rewrite_footer(out, add_nan_counts)
    print(f"wrote {out} ({out.stat().st_size} bytes)")


def main() -> None:
    write_unsigned()
    write_deprecated()
    write_collated()
    write_nan()


def write_deprecated() -> None:
    out = OUT_DIR / "deprecated_stats.parquet"
    pq.write_table(deprecated_table(), out, row_group_size=3, compression="snappy", use_dictionary=False)
    rewrite_footer(out, to_deprecated_stats)
    print(f"wrote {out} ({out.stat().st_size} bytes)")


def write_unsigned() -> None:
    out = OUT_DIR / "unsigned_order.parquet"
    pq.write_table(
        unsigned_table(),
        out,
        row_group_size=ROWS_PER_GROUP,
        compression="snappy",
        use_dictionary=False,
        write_page_index=True,
        write_batch_size=64,
        data_page_size=64,
    )
    print(f"wrote {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
