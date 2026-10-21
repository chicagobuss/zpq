"""Minimal Thrift compact codec and Parquet file surgery for the torture generators.

Decoded Thrift structs are `Struct` objects: an ordered list of (field id, compact type, value) that re-encodes to the
same bytes, so a generator can change one field of a valid pyarrow footer and leave every other byte alone. `build()`
lays out a whole file from page lists and recomputes the offsets and sizes the caller did not override, so a lie is
only ever the one a generator injects on purpose.
"""

import io
import struct as _st

# Compact protocol type ids.
T_STOP, T_TRUE, T_FALSE, T_I8, T_I16, T_I32, T_I64, T_DOUBLE, T_BIN, T_LIST, T_SET, T_MAP, T_STRUCT = range(13)


class Struct:
    def __init__(self, fields=None):
        self.fields = list(fields or [])  # [fid, type, value]

    def get(self, fid, default=None):
        for f in self.fields:
            if f[0] == fid:
                return f[2]
        return default

    def has(self, fid):
        return any(f[0] == fid for f in self.fields)

    def set(self, fid, value, ty=None):
        for f in self.fields:
            if f[0] == fid:
                f[2] = value
                if ty is not None:
                    f[1] = ty
                return self
        if ty is None:
            ty = _infer(value)
        self.fields.append([fid, ty, value])
        self.fields.sort(key=lambda f: f[0])
        return self

    def drop(self, fid):
        self.fields = [f for f in self.fields if f[0] != fid]
        return self

    def __repr__(self):
        return "S{" + ", ".join(f"{f[0]}:{f[2]!r}" for f in self.fields) + "}"


class List:
    def __init__(self, ety, items):
        self.ety = ety
        self.items = list(items)

    def __repr__(self):
        return f"L<{self.ety}>{self.items!r}"


def _infer(v):
    if isinstance(v, bool):
        return T_TRUE
    if isinstance(v, int):
        return T_I64
    if isinstance(v, float):
        return T_DOUBLE
    if isinstance(v, (bytes, bytearray, str)):
        return T_BIN
    if isinstance(v, List):
        return T_LIST
    if isinstance(v, Struct):
        return T_STRUCT
    raise TypeError(v)


# ---------------------------------------------------------------- decode


class Reader:
    def __init__(self, buf, pos=0):
        self.b = buf
        self.p = pos

    def byte(self):
        v = self.b[self.p]
        self.p += 1
        return v

    def varint(self):
        shift = 0
        out = 0
        while True:
            c = self.byte()
            out |= (c & 0x7F) << shift
            if not c & 0x80:
                return out
            shift += 7

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def value(self, ty):
        if ty == T_TRUE:
            return True
        if ty == T_FALSE:
            return False
        if ty == T_I8:
            v = self.byte()
            return v - 256 if v > 127 else v
        if ty in (T_I16, T_I32, T_I64):
            return self.zigzag()
        if ty == T_DOUBLE:
            v = _st.unpack_from("<d", self.b, self.p)[0]
            self.p += 8
            return v
        if ty == T_BIN:
            n = self.varint()
            v = bytes(self.b[self.p : self.p + n])
            self.p += n
            return v
        if ty in (T_LIST, T_SET):
            h = self.byte()
            n = h >> 4
            ety = h & 0xF
            if n == 15:
                n = self.varint()
            items = []
            for _ in range(n):
                if ety in (T_TRUE, T_FALSE):  # list bools are one byte each, 1 = true
                    items.append(self.byte() == 1)
                else:
                    items.append(self.value(ety))
            return List(ety, items)
        if ty == T_STRUCT:
            return self.struct()
        raise ValueError(f"type {ty} at {self.p}")

    def struct(self):
        s = Struct()
        last = 0
        while True:
            h = self.byte()
            if h == T_STOP:
                return s
            ty = h & 0xF
            d = h >> 4
            fid = last + d if d else self.zigzag()
            last = fid
            s.fields.append([fid, ty, self.value(ty)])


def decode(buf, pos=0):
    """Decode one struct; returns (Struct, end position)."""
    r = Reader(buf, pos)
    s = r.struct()
    return s, r.p


# ---------------------------------------------------------------- encode


def uvarint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def zz(n):
    return uvarint((n << 1) ^ (n >> 63)) if n >= 0 else uvarint(((-n) << 1) - 1)


def enc_value(ty, v, out):
    if ty in (T_TRUE, T_FALSE):
        return  # carried in the field header
    if ty == T_I8:
        out.append(v & 0xFF)
    elif ty in (T_I16, T_I32, T_I64):
        out += zz(v)
    elif ty == T_DOUBLE:
        out += _st.pack("<d", v)
    elif ty == T_BIN:
        if isinstance(v, str):
            v = v.encode()
        out += uvarint(len(v))
        out += v
    elif ty in (T_LIST, T_SET):
        n = len(v.items)
        if n < 15:
            out.append((n << 4) | v.ety)
        else:
            out.append(0xF0 | v.ety)
            out += uvarint(n)
        for it in v.items:
            if v.ety in (T_TRUE, T_FALSE):
                out.append(1 if it else 2)
            else:
                enc_value(v.ety, it, out)
    elif ty == T_STRUCT:
        enc_struct(v, out)
    else:
        raise ValueError(ty)


def enc_struct(s, out):
    last = 0
    for fid, ty, v in s.fields:
        wty = ty
        if ty in (T_TRUE, T_FALSE):
            wty = T_TRUE if v else T_FALSE
        d = fid - last
        if 0 < d <= 15:
            out.append((d << 4) | wty)
        else:
            out.append(wty)
            out += zz(fid)
        last = fid
        enc_value(ty, v, out)
    out.append(T_STOP)


def encode(s):
    out = bytearray()
    enc_struct(s, out)
    return bytes(out)


# ---------------------------------------------------------------- parquet helpers

MAGIC = b"PAR1"

# Field ids used by the generators (parquet.thrift).
FMD_VERSION, FMD_SCHEMA, FMD_NUM_ROWS, FMD_ROW_GROUPS, FMD_KV, FMD_CREATED_BY, FMD_COLUMN_ORDERS = 1, 2, 3, 4, 5, 6, 7
RG_COLUMNS, RG_TOTAL_BYTE_SIZE, RG_NUM_ROWS, RG_FILE_OFFSET, RG_TOTAL_COMPRESSED = 1, 2, 3, 5, 6
CC_FILE_PATH, CC_FILE_OFFSET, CC_META = 1, 2, 3
CC_OI_OFF, CC_OI_LEN, CC_CI_OFF, CC_CI_LEN = 4, 5, 6, 7
CMD_TYPE, CMD_ENCODINGS, CMD_PATH, CMD_CODEC, CMD_NUM_VALUES = 1, 2, 3, 4, 5
CMD_TOTAL_UNCOMP, CMD_TOTAL_COMP, CMD_DATA_OFF, CMD_INDEX_OFF, CMD_DICT_OFF, CMD_STATS = 6, 7, 9, 10, 11, 12
CMD_ENC_STATS, CMD_BLOOM_OFF, CMD_BLOOM_LEN = 13, 14, 15
ST_MAX, ST_MIN, ST_NULLS, ST_DISTINCT, ST_MAX_VALUE, ST_MIN_VALUE, ST_NAN = 1, 2, 3, 4, 5, 6, 9
PH_TYPE, PH_UNCOMP, PH_COMP, PH_CRC, PH_DATA, PH_INDEX, PH_DICT, PH_DATA_V2 = 1, 2, 3, 4, 5, 6, 7, 8
DPH_NUM_VALUES, DPH_ENCODING, DPH_DEF_ENC, DPH_REP_ENC, DPH_STATS = 1, 2, 3, 4, 5
DICT_NUM_VALUES, DICT_ENCODING, DICT_SORTED = 1, 2, 3
V2_NUM_VALUES, V2_NUM_NULLS, V2_NUM_ROWS, V2_ENCODING, V2_DEF_LEN, V2_REP_LEN, V2_COMPRESSED, V2_STATS = range(1, 9)
(SE_TYPE, SE_TYPE_LEN, SE_REP, SE_NAME, SE_NUM_CHILDREN, SE_CONVERTED, SE_SCALE, SE_PRECISION, SE_FIELD_ID,
 SE_LOGICAL) = range(1, 11)
CI_NULL_PAGES, CI_MINS, CI_MAXS, CI_BOUNDARY, CI_NULL_COUNTS, CI_NAN_COUNTS = 1, 2, 3, 4, 5, 8
OI_LOCATIONS = 1
PL_OFFSET, PL_SIZE, PL_FIRST_ROW = 1, 2, 3

# Enums.
BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BYTE_ARRAY, FLBA = range(8)
REQUIRED, OPTIONAL, REPEATED = range(3)
PLAIN, _GROUP_VAR_INT, PLAIN_DICTIONARY, RLE, BIT_PACKED, DELTA_BINARY_PACKED = 0, 1, 2, 3, 4, 5
DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, RLE_DICTIONARY, BYTE_STREAM_SPLIT = 6, 7, 8, 9
UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD, LZ4_RAW = range(8)
DATA_PAGE, INDEX_PAGE, DICTIONARY_PAGE, DATA_PAGE_V2 = range(4)
CT_UTF8, CT_MAP, CT_MAP_KV, CT_LIST, CT_ENUM, CT_DECIMAL = range(6)


class Page:
    """A page: header Struct plus payload bytes. Sizes in the header are filled at build time unless pinned."""

    def __init__(self, header, payload, pin_sizes=False):
        self.header = header
        self.payload = payload
        self.pin_sizes = pin_sizes

    def bytes(self):
        if not self.pin_sizes:
            self.header.set(PH_COMP, len(self.payload), T_I32)
            if not self.header.has(PH_UNCOMP):
                self.header.set(PH_UNCOMP, len(self.payload), T_I32)
        return encode(self.header) + self.payload


def read_file(path_or_bytes):
    b = path_or_bytes if isinstance(path_or_bytes, (bytes, bytearray)) else open(path_or_bytes, "rb").read()
    assert b[:4] == MAGIC and b[-4:] == MAGIC, "not parquet"
    flen = _st.unpack_from("<I", b, len(b) - 8)[0]
    fstart = len(b) - 8 - flen
    fmd, end = decode(b, fstart)
    assert end == len(b) - 8, (end, len(b) - 8)
    return bytes(b), fmd, fstart


def chunk_pages(b, cmd):
    """Split a column chunk into Pages using the chunk's own offsets and total_compressed_size."""
    start = cmd.get(CMD_DICT_OFF) if cmd.has(CMD_DICT_OFF) and cmd.get(CMD_DICT_OFF) else cmd.get(CMD_DATA_OFF)
    end = start + cmd.get(CMD_TOTAL_COMP)
    pages = []
    p = start
    while p < end:
        h, q = decode(b, p)
        n = h.get(PH_COMP)
        pages.append(Page(h, bytes(b[q : q + n])))
        p = q + n
    return pages


def file_bytes(chunks_by_rg, fmd, prefix=MAGIC, fix_offsets=True):
    """Lay out a file. `chunks_by_rg[rg][col]` is a list of Pages; `fmd` is the FileMetaData Struct whose column
    chunk metadata gets fresh offsets/sizes (unless fix_offsets is False). Returns bytes."""
    out = bytearray(prefix)
    rgs = fmd.get(FMD_ROW_GROUPS).items
    for rgi, cols in enumerate(chunks_by_rg):
        rg = rgs[rgi]
        ccs = rg.get(RG_COLUMNS).items
        rg_start = len(out)
        rg_comp = 0
        for ci, pages in enumerate(cols):
            cc = ccs[ci]
            cmd = cc.get(CC_META)
            start = len(out)
            dict_off = None
            data_off = None
            uncomp = 0
            for pg in pages:
                off = len(out)
                raw = pg.bytes()
                hlen = len(raw) - len(pg.payload)
                ty = pg.header.get(PH_TYPE)
                if ty == DICTIONARY_PAGE and dict_off is None and data_off is None:
                    dict_off = off
                elif ty in (DATA_PAGE, DATA_PAGE_V2) and data_off is None:
                    data_off = off
                uncomp += hlen + pg.header.get(PH_UNCOMP)
                out += raw
            if fix_offsets:
                cmd.set(CMD_DATA_OFF, data_off if data_off is not None else start, T_I64)
                if dict_off is not None:
                    cmd.set(CMD_DICT_OFF, dict_off, T_I64)
                else:
                    cmd.drop(CMD_DICT_OFF)
                cmd.set(CMD_TOTAL_COMP, len(out) - start, T_I64)
                cmd.set(CMD_TOTAL_UNCOMP, uncomp, T_I64)
                cc.set(CC_FILE_OFFSET, start, T_I64)
                cmd.drop(CMD_INDEX_OFF)
            rg_comp += len(out) - start
        if fix_offsets:
            if rg.has(RG_FILE_OFFSET):
                rg.set(RG_FILE_OFFSET, rg_start, T_I64)
            if rg.has(RG_TOTAL_COMPRESSED):
                rg.set(RG_TOTAL_COMPRESSED, rg_comp, T_I64)
    return out


def finish(out, fmd, footer=None):
    f = footer if footer is not None else encode(fmd)
    out = bytearray(out)
    out += f
    out += _st.pack("<I", len(f))
    out += MAGIC
    return bytes(out)


def strip_indexes(fmd):
    """Drop page-index and bloom references (their bytes are not carried by file_bytes)."""
    for rg in fmd.get(FMD_ROW_GROUPS).items:
        for cc in rg.get(RG_COLUMNS).items:
            for fid in (CC_OI_OFF, CC_OI_LEN, CC_CI_OFF, CC_CI_LEN):
                cc.drop(fid)
            cc.get(CC_META).drop(CMD_BLOOM_OFF).drop(CMD_BLOOM_LEN)


def rebuild(path_or_bytes, mutate_pages=None, mutate_fmd=None, keep_index=False):
    """Read a file, optionally mutate its pages (callback(rg, col, pages) -> pages) and footer, and lay it out again."""
    b, fmd, _ = read_file(path_or_bytes)
    chunks = []
    for rgi, rg in enumerate(fmd.get(FMD_ROW_GROUPS).items):
        cols = []
        for ci, cc in enumerate(rg.get(RG_COLUMNS).items):
            pages = chunk_pages(b, cc.get(CC_META))
            if mutate_pages:
                pages = mutate_pages(rgi, ci, pages)
            cols.append(pages)
        chunks.append(cols)
    if not keep_index:
        strip_indexes(fmd)
    out = file_bytes(chunks, fmd)
    if mutate_fmd:
        mutate_fmd(fmd, out)
    return finish(out, fmd)


# ---------------------------------------------------------------- page index


def add_page_index(out, fmd, rg, col, column_index=None, offset_index=None):
    """Append ColumnIndex/OffsetIndex Structs to `out` (before the footer) and point column `col` of row group `rg` at
    them. Returns the new bytes."""
    out = bytearray(out)
    cc = fmd.get(FMD_ROW_GROUPS).items[rg].get(RG_COLUMNS).items[col]
    if column_index is not None:
        e = encode(column_index)
        cc.set(CC_CI_OFF, len(out), T_I64).set(CC_CI_LEN, len(e), T_I32)
        out += e
    if offset_index is not None:
        e = encode(offset_index)
        cc.set(CC_OI_OFF, len(out), T_I64).set(CC_OI_LEN, len(e), T_I32)
        out += e
    return out


def page_locations(out_prefix_offsets):
    return List(T_STRUCT, [Struct([[PL_OFFSET, T_I64, o], [PL_SIZE, T_I32, s], [PL_FIRST_ROW, T_I64, r]])
                           for o, s, r in out_prefix_offsets])


# ---------------------------------------------------------------- hand-made files


def schema_elem(name, ty=None, rep=None, num_children=None, converted=None, type_length=None, scale=None,
                precision=None, logical=None):
    s = Struct()
    if ty is not None:
        s.set(SE_TYPE, ty, T_I32)
    if type_length is not None:
        s.set(SE_TYPE_LEN, type_length, T_I32)
    if rep is not None:
        s.set(SE_REP, rep, T_I32)
    s.set(SE_NAME, name.encode(), T_BIN)
    if num_children is not None:
        s.set(SE_NUM_CHILDREN, num_children, T_I32)
    if converted is not None:
        s.set(SE_CONVERTED, converted, T_I32)
    if scale is not None:
        s.set(SE_SCALE, scale, T_I32)
    if precision is not None:
        s.set(SE_PRECISION, precision, T_I32)
    if logical is not None:
        s.set(SE_LOGICAL, logical, T_STRUCT)
    return s


def data_page_v1(payload, num_values, encoding=PLAIN, uncomp=None, stats=None):
    dph = Struct([[DPH_NUM_VALUES, T_I32, num_values], [DPH_ENCODING, T_I32, encoding], [DPH_DEF_ENC, T_I32, RLE],
                  [DPH_REP_ENC, T_I32, RLE]])
    if stats is not None:
        dph.set(DPH_STATS, stats, T_STRUCT)
    h = Struct([[PH_TYPE, T_I32, DATA_PAGE], [PH_UNCOMP, T_I32, len(payload) if uncomp is None else uncomp],
                [PH_COMP, T_I32, len(payload)], [PH_DATA, T_STRUCT, dph]])
    return Page(h, payload)


def data_page_v2(payload, num_values, num_nulls, num_rows, def_len, rep_len, encoding=PLAIN, compressed=False,
                 uncomp=None):
    v2 = Struct([[V2_NUM_VALUES, T_I32, num_values], [V2_NUM_NULLS, T_I32, num_nulls], [V2_NUM_ROWS, T_I32, num_rows],
                 [V2_ENCODING, T_I32, encoding], [V2_DEF_LEN, T_I32, def_len], [V2_REP_LEN, T_I32, rep_len],
                 [V2_COMPRESSED, T_TRUE, compressed]])
    h = Struct([[PH_TYPE, T_I32, DATA_PAGE_V2], [PH_UNCOMP, T_I32, len(payload) if uncomp is None else uncomp],
                [PH_COMP, T_I32, len(payload)], [PH_DATA_V2, T_STRUCT, v2]])
    return Page(h, payload)


def dict_page(payload, num_values, encoding=PLAIN, uncomp=None):
    d = Struct([[DICT_NUM_VALUES, T_I32, num_values], [DICT_ENCODING, T_I32, encoding]])
    h = Struct([[PH_TYPE, T_I32, DICTIONARY_PAGE], [PH_UNCOMP, T_I32, len(payload) if uncomp is None else uncomp],
                [PH_COMP, T_I32, len(payload)], [PH_DICT, T_STRUCT, d]])
    return Page(h, payload)


def column_meta(ty, path, num_values, codec=UNCOMPRESSED, encodings=(PLAIN, RLE), stats=None):
    m = Struct([[CMD_TYPE, T_I32, ty], [CMD_ENCODINGS, T_LIST, List(T_I32, list(encodings))],
                [CMD_PATH, T_LIST, List(T_BIN, [p.encode() for p in path])], [CMD_CODEC, T_I32, codec],
                [CMD_NUM_VALUES, T_I64, num_values], [CMD_TOTAL_UNCOMP, T_I64, 0], [CMD_TOTAL_COMP, T_I64, 0],
                [CMD_DATA_OFF, T_I64, 0]])
    if stats is not None:
        m.set(CMD_STATS, stats, T_STRUCT)
    return m


def file_meta(schema, row_groups, num_rows, created_by="torture"):
    """`row_groups`: list of (num_rows, [ColumnMetaData Struct, ...])."""
    rgs = []
    for n, cmds in row_groups:
        ccs = [Struct([[CC_FILE_OFFSET, T_I64, 0], [CC_META, T_STRUCT, m]]) for m in cmds]
        rgs.append(Struct([[RG_COLUMNS, T_LIST, List(T_STRUCT, ccs)], [RG_TOTAL_BYTE_SIZE, T_I64, 0],
                           [RG_NUM_ROWS, T_I64, n]]))
    return Struct([[FMD_VERSION, T_I32, 1], [FMD_SCHEMA, T_LIST, List(T_STRUCT, schema)],
                   [FMD_NUM_ROWS, T_I64, num_rows], [FMD_ROW_GROUPS, T_LIST, List(T_STRUCT, rgs)],
                   [FMD_CREATED_BY, T_BIN, created_by.encode()]])


def make_file(schema, row_groups_pages, num_rows=None, rg_rows=None, metas=None, created_by="torture"):
    """Simple builder: `row_groups_pages[rg][col]` = Pages; `metas[rg][col]` = ColumnMetaData Structs."""
    if rg_rows is None:
        rg_rows = [0] * len(row_groups_pages)
    fmd = file_meta(schema, [(rg_rows[i], metas[i]) for i in range(len(row_groups_pages))],
                    sum(rg_rows) if num_rows is None else num_rows, created_by)
    out = file_bytes(row_groups_pages, fmd)
    return out, fmd


# ---------------------------------------------------------------- encodings


def rle_run(value, count, bit_width):
    nbytes = (bit_width + 7) // 8
    return uvarint(count << 1) + int(value).to_bytes(nbytes, "little") if nbytes else uvarint(count << 1)


def bitpack(values, bit_width):
    """One bit-packed run; values padded to a multiple of 8."""
    vals = list(values) + [0] * (-len(values) % 8)
    acc = 0
    for i, v in enumerate(vals):
        acc |= (v & ((1 << bit_width) - 1)) << (i * bit_width)
    nbytes = len(vals) * bit_width // 8
    return uvarint(((len(vals) // 8) << 1) | 1) + acc.to_bytes(nbytes, "little")


def levels_v1(body):
    """v1 data-page level stream: 4-byte little-endian length prefix + RLE/bit-packed hybrid body."""
    return _st.pack("<I", len(body)) + body


def plain_i32(vals):
    return b"".join(_st.pack("<i", v) for v in vals)


def plain_i64(vals):
    return b"".join(_st.pack("<q", v) for v in vals)


def plain_f32(vals):
    return b"".join(_st.pack("<f", v) for v in vals)


def plain_f64(vals):
    return b"".join(_st.pack("<d", v) for v in vals)


def plain_ba(vals):
    return b"".join(_st.pack("<I", len(v)) + v for v in vals)


def stats(min_value=None, max_value=None, null_count=None, nan_count=None, legacy=False, distinct=None):
    s = Struct()
    if max_value is not None:
        s.set(ST_MAX if legacy else ST_MAX_VALUE, max_value, T_BIN)
    if min_value is not None:
        s.set(ST_MIN if legacy else ST_MIN_VALUE, min_value, T_BIN)
    if null_count is not None:
        s.set(ST_NULLS, null_count, T_I64)
    if distinct is not None:
        s.set(ST_DISTINCT, distinct, T_I64)
    if nan_count is not None:
        s.set(ST_NAN, nan_count, T_I64)
    return s
