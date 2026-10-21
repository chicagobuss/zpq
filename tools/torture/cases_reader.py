"""Part 1: the reader torture chamber. Every case builds a small file from a deterministic pyarrow base or from
hand-made pages, injects one lie, and states what zpq must do with it."""

import io
import os
import struct
import zlib

import pyarrow as pa
import pyarrow.parquet as pq

import tpq as T
from common import E, battery, case, run

N = 1000
A = list(range(N))
S = [None if i % 3 == 0 else f"s{i}" for i in range(N)]
TRUE_ROWS = [{"a": a, "s": s} for a, s in zip(A, S)]
AGG = "count(*) AS n, sum(a) AS sa, count(s) AS cs, max(s) AS ms, min(a) AS mn"
AGG_TRUE = {"n": N, "sa": sum(A), "cs": sum(s is not None for s in S), "ms": max(s for s in S if s), "mn": 0}


def base(**kw):
    """Two flat columns (REQUIRED int64 a, OPTIONAL string s), 1000 rows, 2 row groups of 500."""
    t = pa.table({"a": pa.array(A, pa.int64()), "s": pa.array(S, pa.string())},
                 schema=pa.schema([pa.field("a", pa.int64(), nullable=False), pa.field("s", pa.string())]))
    b = io.BytesIO()
    opts = dict(row_group_size=500, data_page_size=1024, compression="snappy", write_statistics=True)
    opts.update(kw)
    pq.write_table(t, b, **opts)
    return b.getvalue()


def footer_len_lie(b, new_len):
    return b[:-8] + struct.pack("<I", new_len & 0xFFFFFFFF) + T.MAGIC


def cmd(fmd, rg, col):
    return fmd.get(T.FMD_ROW_GROUPS).items[rg].get(T.RG_COLUMNS).items[col].get(T.CC_META)


def cc(fmd, rg, col):
    return fmd.get(T.FMD_ROW_GROUPS).items[rg].get(T.RG_COLUMNS).items[col]


def refooter(b, mutate):
    """Re-encode the footer of `b` after `mutate(fmd)`; the column data stays byte-identical."""
    raw, fmd, fstart = T.read_file(b)
    mutate(fmd)
    return T.finish(raw[:fstart], fmd)


def footer_bytes(b, footer):
    _, _, fstart = T.read_file(b)
    return T.finish(b[:fstart], None, footer=footer)


# ================================================================ 1. structural & Thrift lies

B1 = "1.1 Ouroboros footer"


@case("footer_len_before_start", B1)
def _():
    b = base()
    return {"file": footer_len_lie(b, len(b)), "runs": battery("err")}


@case("footer_len_into_magic", B1)
def _():
    b = base()
    return {"file": footer_len_lie(b, len(b) - 10), "runs": battery("err")}


@case("footer_len_whole_file", B1)
def _():
    b = base()
    return {"file": footer_len_lie(b, len(b) - 8), "runs": battery("err")}


@case("footer_len_4gib", B1)
def _():
    return {"file": footer_len_lie(base(), 0xFFFFFFFF), "runs": battery("err")}


@case("footer_len_zero", B1)
def _():
    return {"file": footer_len_lie(base(), 0), "runs": battery("err")}


@case("footer_len_short_by_one", B1)
def _():
    b = base()
    flen = struct.unpack("<I", b[-8:-4])[0]
    return {"file": footer_len_lie(b, flen - 1), "runs": battery("err")}


@case("footer_len_long_into_data", B1)
def _():
    b = base()
    flen = struct.unpack("<I", b[-8:-4])[0]
    return {"file": footer_len_lie(b, flen + 37), "runs": battery("err")}


@case("tiny_file_par1par1", B1)
def _():
    return {"file": b"PAR1PAR1", "runs": battery("err")}


@case("tiny_file_12_bytes", B1)
def _():
    return {"file": b"PAR1\x00\x00\x00\x00PAR1", "runs": battery("err")}


@case("empty_file", B1)
def _():
    return {"file": b"", "runs": battery("err")}


@case("truncated_upload_half", B1)
def _():
    b = base()
    return {"file": b[: len(b) // 2], "runs": battery("err")}


@case("truncated_upload_mid_footer", B1)
def _():
    b = base()
    return {"file": b[:-20], "runs": battery("err")}


@case("footer_magic_par_e", B1)
def _():
    b = base()
    return {"file": b[:-4] + b"PARE", "runs": battery("err")}  # encrypted-footer magic on a plain file


@case("leading_magic_wrong", B1)
def _():
    b = base()
    return {"file": b"PAR0" + b[4:], "runs": battery("err|eq", rows=TRUE_ROWS, agg=AGG, agg_value=AGG_TRUE, count=N)}


B2 = "1.2 Thrift bombing"


def fmd_prefix(b):
    """FileMetaData bytes of `b` with the trailing STOP removed, so a generator can append bomb fields."""
    _, fmd, _ = T.read_file(b)
    e = T.encode(fmd)
    assert e[-1] == 0
    return e[:-1], fmd


@case("thrift_varint_never_ends_fmd", B2)
def _():
    # version field header then continuation bits to EOF of the footer.
    return {"file": footer_bytes(base(), bytes([0x15]) + b"\xff" * 4000), "runs": battery("err")}


@case("thrift_varint_overlong_num_rows", B2)
def _():
    e, _ = fmd_prefix(base())
    # Append a duplicate num_rows (field 3, i64) whose varint is 12 bytes long: past 64 bits.
    return {"file": footer_bytes(base(), e + bytes([0x06, 0x03]) + b"\xff" * 11 + b"\x01\x00"),
            "runs": battery("err|eq", rows=TRUE_ROWS, count=N)}


@case("thrift_nested_struct_bomb_fmd", B2)
def _():
    e, _ = fmd_prefix(base())
    # Unknown field 99 of struct type, then 200k nested structs each opening field 1 of struct type.
    bomb = bytes([0x0C]) + T.zz(99) + bytes([0x1C]) * 200_000 + b"\x00" * 200_001
    return {"file": footer_bytes(base(), e + bomb + b"\x00"), "runs": battery("err|eq", rows=TRUE_ROWS, count=N)}


@case("thrift_nested_list_bomb_fmd", B2)
def _():
    e, _ = fmd_prefix(base())
    # Unknown field 99: list<list<list<...>>> with one element per level, 200k levels.
    bomb = bytes([0x09]) + T.zz(99) + bytes([0x19]) * 200_000 + bytes([0x15])
    return {"file": footer_bytes(base(), e + bomb + b"\x00"), "runs": battery("err|eq", rows=TRUE_ROWS, count=N)}


@case("thrift_list_count_2g_fmd", B2)
def _():
    e, _ = fmd_prefix(base())
    # Unknown field 99: list of 2^31-1 structs on a ~2 KB footer.
    bomb = bytes([0x09]) + T.zz(99) + bytes([0xFC]) + T.uvarint(2**31 - 1) + b"\x00" * 16
    return {"file": footer_bytes(base(), e + bomb + b"\x00"), "runs": battery("err")}


@case("thrift_schema_list_count_huge", B2)
def _():
    b = base()
    _, fmd, _ = T.read_file(b)
    e = T.encode(fmd)
    # field 2 (schema) header is 0x29 after version: rewrite its list header to claim 2^40 structs.
    i = e.index(bytes([0x29]))
    e2 = e[: i + 1] + bytes([0xFC]) + T.uvarint(2**40) + e[i + 2 :]
    return {"file": footer_bytes(b, e2), "runs": battery("err")}


@case("thrift_binary_len_huge", B2)
def _():
    e, _ = fmd_prefix(base())
    # created_by again (field 6, binary) with length 2^62.
    return {"file": footer_bytes(base(), e + bytes([0x08, 0x06]) + T.uvarint(2**62) + b"x" * 8 + b"\x00"),
            "runs": battery("err")}


@case("thrift_invalid_type_id", B2)
def _():
    e, _ = fmd_prefix(base())
    return {"file": footer_bytes(base(), e + bytes([0x0E]) + T.zz(99) + b"\x00" * 8 + b"\x00"), "runs": battery("err")}


@case("thrift_map_bomb_fmd", B2)
def _():
    e, _ = fmd_prefix(base())
    # Unknown field 99: map<struct,struct> claiming 2^31 entries.
    bomb = bytes([0x0B]) + T.zz(99) + T.uvarint(2**31) + bytes([0xCC]) + b"\x00" * 32
    return {"file": footer_bytes(base(), e + bomb + b"\x00"), "runs": battery("err")}


def first_page_header_bomb(tail):
    """Replace column a / row group 0's first page header bytes with `tail` (same total length)."""
    b = base(compression="none")
    raw, fmd, fstart = T.read_file(b)
    off = cmd(fmd, 0, 0).get(T.CMD_DATA_OFF)
    return raw[:off] + tail + raw[off + len(tail) :]


@case("thrift_varint_never_ends_page_header", B2)
def _():
    return {"file": first_page_header_bomb(bytes([0x15]) + b"\xff" * 600),
            "runs": battery("err", count=N, count_kind="eq")}


@case("thrift_nested_struct_bomb_page_header", B2)
def _():
    # One hand-made page whose header carries unknown field 99: 3000 nested structs. Offsets and sizes are exact.
    pg = T.data_page_v1(T.plain_i64(range(8)), 8)
    pg.bytes()
    hdr = T.encode(pg.header)[:-1] + bytes([0x0C]) + T.zz(99) + bytes([0x1C]) * 3000 + b"\x00" * 3001 + b"\x00"
    schema = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED)]
    m = T.column_meta(T.INT64, ["z"], 8)
    m.set(T.CMD_DATA_OFF, 4, T.T_I64).set(T.CMD_TOTAL_COMP, len(hdr) + 64, T.T_I64)
    m.set(T.CMD_TOTAL_UNCOMP, len(hdr) + 64, T.T_I64)
    fmd = T.file_meta(schema, [(8, [m])], 8)
    return {"file": T.finish(T.MAGIC + hdr + pg.payload, fmd),
            "runs": battery("err|eq", count=8, count_kind="eq", select="z", agg="sum(z) AS s", agg_value={"s": 28},
                            rows=[{"z": i} for i in range(8)], write_kind="safe")}


B3 = "1.3 Decompression bombs"


def zstd_rle_frame(total, claim_content=None):
    """A zstd frame of RLE blocks that inflates to `total` zero bytes from ~4 bytes per 128 KiB."""
    out = bytearray(struct.pack("<I", 0xFD2FB528))
    if claim_content is None:
        out.append(0x00)  # FHD: no checksum, no single segment, no FCS
        out.append(0x38)  # window descriptor: 2^17
    else:
        out.append(0xC0)  # FCS 8 bytes, not single segment
        out.append(0x38)
        out += struct.pack("<Q", claim_content)
    left = total
    while left > 0:
        n = min(left, 128 * 1024)
        left -= n
        last = 1 if left == 0 else 0
        hdr = last | (1 << 1) | (n << 3)
        out += hdr.to_bytes(3, "little") + b"\x00"
    return bytes(out)


def zero_page_file(codec, payload, claim_uncomp, num_values, ty=T.INT64):
    """One REQUIRED column `z`, one row group, one data page carrying `payload` with the claimed sizes."""
    pg = T.data_page_v1(payload, num_values, uncomp=claim_uncomp)
    schema = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=ty, rep=T.REQUIRED)]
    m = T.column_meta(ty, ["z"], num_values, codec=codec)
    out, fmd = T.make_file(schema, [[[pg]]], rg_rows=[num_values], metas=[[m]])
    return T.finish(out, fmd)


@case("zstd_bomb_10g_claims_64k", B3)
def _():
    # Page says 64 KiB (8192 int64 zeros); the frame inflates to 10 GiB.
    return {"file": zero_page_file(T.ZSTD, zstd_rle_frame(10 << 30), 65536, 8192),
            "runs": battery("err", count=8192, count_kind="eq", select="z", agg="sum(z) AS s", agg_value={"s": 0})}


@case("zstd_bomb_fcs_lies_small", B3)
def _():
    # Frame header claims 64 KiB of content; blocks inflate to 1 GiB.
    return {"file": zero_page_file(T.ZSTD, zstd_rle_frame(1 << 30, claim_content=65536), 65536, 8192),
            "runs": battery("err", count=8192, count_kind="eq", select="z", agg="sum(z) AS s", agg_value={"s": 0})}


@case("gzip_bomb_1g_claims_64k", B3)
def _():
    co = zlib.compressobj(9, zlib.DEFLATED, 31)
    chunk = b"\x00" * (1 << 20)
    parts = [co.compress(chunk) for _ in range(1024)]
    parts.append(co.flush())
    return {"file": zero_page_file(T.GZIP, b"".join(parts), 65536, 8192),
            "runs": battery("err", count=8192, count_kind="eq", select="z", agg="sum(z) AS s", agg_value={"s": 0})}


def snappy_zeros(n, claim=None):
    """Raw snappy: preamble claims `claim` (default n); a literal zero then 64-byte copies at offset 1."""
    out = bytearray(T.uvarint(n if claim is None else claim))
    out += bytes([0x00, 0x00])  # literal of length 1: tag (len-1)<<2 = 0, then the byte
    left = n - 1
    while left > 0:
        k = min(64, left)
        if k < 4:
            out += bytes([((k - 1) << 2), 0x00])  # literal of k zeros
            out += b"\x00" * (k - 1)
            left -= k
            continue
        out += bytes([((k - 1) << 2) | 2]) + struct.pack("<H", 1)  # copy-2: len k, offset 1
        left -= k
    return bytes(out)


@case("snappy_preamble_huge_page_small", B3)
def _():
    # Snappy preamble says 1 GiB; the page header and stream are 64 KiB.
    return {"file": zero_page_file(T.SNAPPY, snappy_zeros(65536, claim=1 << 30), 65536, 8192),
            "runs": battery("err", count=8192, count_kind="eq", select="z", agg="sum(z) AS s", agg_value={"s": 0})}


@case("snappy_inflates_past_claim", B3)
def _():
    # Stream inflates to 4 MiB (its preamble honest); page header claims 64 KiB.
    return {"file": zero_page_file(T.SNAPPY, snappy_zeros(4 << 20), 65536, 8192),
            "runs": battery("err", count=8192, count_kind="eq", select="z", agg="sum(z) AS s", agg_value={"s": 0})}


@case("page_claims_2g_uncompressed_tiny_file", B3)
def _():
    return {"file": zero_page_file(T.ZSTD, zstd_rle_frame(64), 2**31 - 1, 8),
            "runs": battery("err", count=8, count_kind="eq", select="z", agg="sum(z) AS s", agg_value={"s": 0})}


@case("page_claims_2g_uncompressed_plain", B3)
def _():
    # UNCOMPRESSED codec: claims 2^31-1 uncompressed, 64 bytes compressed.
    return {"file": zero_page_file(T.UNCOMPRESSED, b"\x00" * 64, 2**31 - 1, 8),
            "runs": battery("err", count=8, count_kind="eq", select="z", agg="sum(z) AS s", agg_value={"s": 0})}


@case("page_negative_sizes", B3)
def _():
    pg = T.data_page_v1(b"\x00" * 64, 8)
    pg.header.set(T.PH_UNCOMP, -1, T.T_I32).set(T.PH_COMP, 64, T.T_I32)
    pg.pin_sizes = True
    schema = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED)]
    out, fmd = T.make_file(schema, [[[pg]]], rg_rows=[8], metas=[[T.column_meta(T.INT64, ["z"], 8)]])
    return {"file": T.finish(out, fmd), "runs": battery("err", count=8, count_kind="eq")}


@case("page_compressed_size_negative", B3)
def _():
    pg = T.data_page_v1(b"\x00" * 64, 8)
    pg.header.set(T.PH_COMP, -64, T.T_I32)
    pg.pin_sizes = True
    schema = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED)]
    out, fmd = T.make_file(schema, [[[pg]]], rg_rows=[8], metas=[[T.column_meta(T.INT64, ["z"], 8)]])
    return {"file": T.finish(out, fmd), "runs": battery("err", count=8, count_kind="eq")}


@case("page_num_values_2g_tiny", B3)
def _():
    return {"file": zero_page_file(T.UNCOMPRESSED, b"\x00" * 64, 64, 2**31 - 1),
            "runs": battery("err", count=2**31 - 1, count_kind="err|eq", write_kind="safe", select="z", agg="sum(z) AS s", agg_value={"s": 0})}


@case("dict_page_num_values_2g", B3)
def _():
    dp = T.dict_page(T.plain_i64([7]), 2**31 - 1)
    data = T.bitpack([0] * 8, 1)
    dpg = T.data_page_v1(bytes([1]) + data, 8, encoding=T.RLE_DICTIONARY)
    schema = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED)]
    m = T.column_meta(T.INT64, ["z"], 8, encodings=(T.PLAIN, T.RLE, T.RLE_DICTIONARY))
    out, fmd = T.make_file(schema, [[[dp, dpg]]], rg_rows=[8], metas=[[m]])
    return {"file": T.finish(out, fmd), "runs": battery("err", count=8, count_kind="eq",
                                                         select="z", agg="sum(z) AS s", agg_value={"s": 56})}


B4 = "1.4 Out-of-bounds column chunks"


def chunk_lie(mutate, kind="err", count_kind="err|eq", **kw):
    b = base()
    kw.setdefault("write_kind", "safe")  # a plain -o byte-copies chunks it does not decode
    return {"file": refooter(b, mutate), "runs": battery(kind, count=N, count_kind=count_kind, agg=AGG,
                                                         agg_value=AGG_TRUE, rows=TRUE_ROWS if "eq" in kind else None, select="a, s",
                                                         **kw)}


@case("chunk_data_offset_negative", B4)
def _():
    return chunk_lie(lambda f: cmd(f, 0, 0).set(T.CMD_DATA_OFF, -100, T.T_I64))


@case("chunk_data_offset_past_eof", B4)
def _():
    # The chunk has a dictionary page, whose offset zpq reads from: ignoring the lying data_page_offset is right.
    return chunk_lie(lambda f: cmd(f, 1, 1).set(T.CMD_DATA_OFF, 1 << 40, T.T_I64), kind="err|eq")


@case("chunk_data_offset_in_footer", B4)
def _():
    def m(f):
        b = base()
        cmd(f, 1, 0).set(T.CMD_DATA_OFF, len(b) - 100, T.T_I64)
    return chunk_lie(m, kind="err|eq")


def chunk_lie_nodict(mutate, kind="err"):
    b = base(use_dictionary=False)
    return {"file": refooter(b, mutate), "runs": battery(kind, count=N, count_kind="err|eq", agg=AGG,
                                                         agg_value=AGG_TRUE, select="a, s", write_kind="safe",
                                                         rows=TRUE_ROWS if "eq" in kind else None)}


@case("chunk_data_offset_past_eof_nodict", B4)
def _():
    return chunk_lie_nodict(lambda f: cmd(f, 1, 1).set(T.CMD_DATA_OFF, 1 << 40, T.T_I64))


@case("chunk_data_offset_into_other_column_nodict", B4)
def _():
    # a's data_page_offset (row group 1) points at s's first page in the same row group.
    return chunk_lie_nodict(lambda f: cmd(f, 1, 0).set(T.CMD_DATA_OFF, cmd(f, 1, 1).get(T.CMD_DATA_OFF), T.T_I64))


@case("chunk_data_offset_mid_page_nodict", B4)
def _():
    return chunk_lie_nodict(lambda f: cmd(f, 0, 0).set(T.CMD_DATA_OFF, cmd(f, 0, 0).get(T.CMD_DATA_OFF) + 3,
                                                       T.T_I64))


@case("chunk_dict_offset_negative", B4)
def _():
    return chunk_lie(lambda f: cmd(f, 0, 1).set(T.CMD_DICT_OFF, -5, T.T_I64))


@case("chunk_dict_offset_past_eof", B4)
def _():
    return chunk_lie(lambda f: cmd(f, 0, 1).set(T.CMD_DICT_OFF, 1 << 33, T.T_I64))


@case("chunk_dict_offset_after_data", B4)
def _():
    def m(f):
        c = cmd(f, 0, 1)
        c.set(T.CMD_DICT_OFF, c.get(T.CMD_DATA_OFF) + 7, T.T_I64)
    return chunk_lie(m)


@case("chunk_total_compressed_past_eof", B4)
def _():
    return chunk_lie(lambda f: cmd(f, 1, 1).set(T.CMD_TOTAL_COMP, 1 << 40, T.T_I64))


@case("chunk_total_compressed_negative", B4)
def _():
    return chunk_lie(lambda f: cmd(f, 0, 0).set(T.CMD_TOTAL_COMP, -1, T.T_I64))


@case("chunk_total_compressed_short", B4)
def _():
    # One byte short: the last page is cut.
    def m(f):
        c = cmd(f, 0, 0)
        c.set(T.CMD_TOTAL_COMP, c.get(T.CMD_TOTAL_COMP) - 1, T.T_I64)
    return chunk_lie(m)


@case("chunk_total_compressed_long_overlaps_next", B4)
def _():
    # Size reaches 50 bytes into the next column's chunk: pages of `s` would be read as `a` pages.
    def m(f):
        c = cmd(f, 0, 0)
        c.set(T.CMD_TOTAL_COMP, c.get(T.CMD_TOTAL_COMP) + 50, T.T_I64)
    return chunk_lie(m, kind="err|eq")


def two_int_cols(**kw):
    t = pa.table({"x": pa.array(A, pa.int64()), "y": pa.array([v * 10 for v in A], pa.int64())},
                 schema=pa.schema([pa.field("x", pa.int64(), nullable=False),
                                   pa.field("y", pa.int64(), nullable=False)]))
    b = io.BytesIO()
    pq.write_table(t, b, row_group_size=500, compression="snappy", **kw)
    return b.getvalue()


@case("chunk_overlap_same_type_consistent", B4)
def _():
    # Column y's chunk metadata copies column x's offsets and sizes exactly (stats dropped): a consistent lie. A reader
    # that does not check overlap answers y with x's values.
    def m(f):
        for rg in (0, 1):
            x, y = cmd(f, rg, 0), cmd(f, rg, 1)
            for fid in (T.CMD_DATA_OFF, T.CMD_DICT_OFF, T.CMD_TOTAL_COMP, T.CMD_TOTAL_UNCOMP):
                if x.has(fid):
                    y.set(fid, x.get(fid), T.T_I64)
            y.drop(T.CMD_STATS)
            x.drop(T.CMD_STATS)
            cc(f, rg, 1).set(T.CC_FILE_OFFSET, cc(f, rg, 0).get(T.CC_FILE_OFFSET), T.T_I64)
    b = refooter(two_int_cols(), m)
    agg = "sum(x) AS sx, sum(y) AS sy"
    return {"file": b, "runs": [
        run("agg", ["query", "{F}", "--aggregate", agg], E("err|eq", [{"sx": sum(A), "sy": sum(A)}])),
        run("rows", ["query", "{F}", "--limit", "3"], E("err|eq", [{"x": i, "y": i} for i in range(3)])),
        run("inspect", ["inspect", "{F}", "--detail", "--format", "json"], E("safe")),
    ], "note": "overlap accepted = answers y from x's bytes; the file is self-consistent so 'eq' is x's values"}


@case("chunk_overlap_cross_type", B4)
def _():
    # Column s (string) points at column a's (int64) chunk.
    def m(f):
        for rg in (0, 1):
            a, s = cmd(f, rg, 0), cmd(f, rg, 1)
            for fid in (T.CMD_DATA_OFF, T.CMD_TOTAL_COMP, T.CMD_TOTAL_UNCOMP):
                s.set(fid, a.get(fid), T.T_I64)
            s.drop(T.CMD_DICT_OFF).drop(T.CMD_STATS)
    return chunk_lie(m, kind="err", count_kind="eq")


@case("chunk_file_path_external", B4)
def _():
    # ColumnChunk.file_path names another file (here /etc/passwd). zpq never opens it: it reads the chunk from this
    # file's bytes (pyarrow does the same; DuckDB refuses). Whether to refuse is an open policy question (REPORT.md).
    return chunk_lie(lambda f: cc(f, 0, 0).set(T.CC_FILE_PATH, b"/etc/passwd", T.T_BIN), kind="err|eq")


@case("rowgroup_missing_column", B4)
def _():
    def m(f):
        rg = f.get(T.FMD_ROW_GROUPS).items[1]
        rg.get(T.RG_COLUMNS).items.pop()
    return chunk_lie(m, kind="err", count_kind="err|eq")


@case("rowgroup_extra_column", B4)
def _():
    def m(f):
        rg = f.get(T.FMD_ROW_GROUPS).items[0]
        cols = rg.get(T.RG_COLUMNS).items
        cols.append(cols[0])
    return chunk_lie(m, kind="err", count_kind="err|eq")


@case("rowgroup_columns_swapped", B4)
def _():
    # Row group 1 lists its column chunks in the other order (path_in_schema still right per chunk).
    def m(f):
        rg = f.get(T.FMD_ROW_GROUPS).items[1]
        cols = rg.get(T.RG_COLUMNS).items
        cols[0], cols[1] = cols[1], cols[0]
    return chunk_lie(m, kind="err|eq")


@case("chunk_type_mismatch_schema", B4)
def _():
    # ColumnMetaData.type says INT32 for the INT64 column a.
    return chunk_lie(lambda f: cmd(f, 0, 0).set(T.CMD_TYPE, T.INT32, T.T_I32), kind="err|eq")


@case("chunk_codec_unknown", B4)
def _():
    return chunk_lie(lambda f: cmd(f, 0, 0).set(T.CMD_CODEC, 99, T.T_I32), kind="err")


@case("chunk_codec_lzo", B4)
def _():
    return chunk_lie(lambda f: cmd(f, 0, 0).set(T.CMD_CODEC, T.LZO, T.T_I32), kind="err")


@case("chunk_codec_lies_zstd_for_snappy", B4)
def _():
    return chunk_lie(lambda f: cmd(f, 0, 0).set(T.CMD_CODEC, T.ZSTD, T.T_I32), kind="err")


@case("schema_num_children_too_many", B4)
def _():
    return chunk_lie(lambda f: f.get(T.FMD_SCHEMA).items[0].set(T.SE_NUM_CHILDREN, 5, T.T_I32), kind="err",
                     count_kind="err|eq")


@case("schema_num_children_negative", B4)
def _():
    return chunk_lie(lambda f: f.get(T.FMD_SCHEMA).items[0].set(T.SE_NUM_CHILDREN, -1, T.T_I32), kind="err",
                     count_kind="err|eq")


@case("schema_empty", B4)
def _():
    def m(f):
        f.get(T.FMD_SCHEMA).items.clear()
    return chunk_lie(m, kind="err", count_kind="err|eq")


@case("schema_leaf_without_type", B4)
def _():
    return chunk_lie(lambda f: f.get(T.FMD_SCHEMA).items[1].drop(T.SE_TYPE), kind="err", count_kind="err|eq")


@case("schema_group_child_count_cycle", B4)
def _():
    # Leaf `a` claims one child: the schema runs out of elements.
    return chunk_lie(lambda f: f.get(T.FMD_SCHEMA).items[2].set(T.SE_NUM_CHILDREN, 1, T.T_I32), kind="err",
                     count_kind="err|eq")


@case("schema_physical_type_unknown", B4)
def _():
    return chunk_lie(lambda f: f.get(T.FMD_SCHEMA).items[1].set(T.SE_TYPE, 42, T.T_I32), kind="err",
                     count_kind="err|eq")


@case("schema_repetition_unknown", B4)
def _():
    return chunk_lie(lambda f: f.get(T.FMD_SCHEMA).items[1].set(T.SE_REP, 7, T.T_I32), kind="err",
                     count_kind="err|eq")


@case("rowgroups_zero_num_rows_positive", B4)
def _():
    def m(f):
        f.get(T.FMD_ROW_GROUPS).items.clear()
    b = refooter(base(), m)
    return {"file": b, "runs": battery("err|eq", rows=[], count=0, count_kind="err|eq",
                                       agg="count(*) AS n, sum(a) AS s", agg_value={"n": 0, "s": None})}


# ================================================================ 2. Dremel level hallucinations

B21, B22, B23, B24 = ("2.1 orphaned repetition levels", "2.2 def level past max", "2.3 desynchronized streams",
                      "2.4 illegal def/rep transitions")

FLAT_SCHEMA = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.INT64, rep=T.OPTIONAL)]
STRUCT_SCHEMA = [T.schema_elem("schema", num_children=1), T.schema_elem("st", rep=T.OPTIONAL, num_children=1),
                 T.schema_elem("x", ty=T.INT64, rep=T.OPTIONAL)]
LIST_SCHEMA = [T.schema_elem("schema", num_children=1),
               T.schema_elem("l", rep=T.OPTIONAL, num_children=1, converted=T.CT_LIST),
               T.schema_elem("list", rep=T.REPEATED, num_children=1),
               T.schema_elem("element", ty=T.INT64, rep=T.OPTIONAL)]


def one_chunk_file(schema, path, pages, num_rows, num_values=None, codec=T.UNCOMPRESSED):
    m = T.column_meta(T.INT64, path, num_rows if num_values is None else num_values, codec=codec)
    out, fmd = T.make_file(schema, [[pages]], rg_rows=[num_rows], metas=[[m]])
    return T.finish(out, fmd)


def flat_page(defs_body, values, num_values):
    return T.data_page_v1(T.levels_v1(defs_body) + T.plain_i64(values), num_values)


def list_page(reps_body, defs_body, values, num_values):
    return T.data_page_v1(T.levels_v1(reps_body) + T.levels_v1(defs_body) + T.plain_i64(values), num_values)


def list_runs(count=None, count_kind="err|eq"):
    """Repeated leaves are decoded by the aggregate path before it refuses them, and copied by a plain -o."""
    return [run("count", ["query", "{F}", "--aggregate", "count(*) AS n"], E(count_kind, [{"n": count}])),
            run("agg-leaf", ["query", "{F}", "--aggregate", "count(l.list.element) AS c, sum(l.list.element) AS s"],
                E("err")),
            run("agg-leaf-scanall", ["query", "{F}", "--scan-all", "--aggregate", "max(l.list.element) AS m"],
                E("err")),
            run("rows", ["query", "{F}"], E("refuse")),
            run("write", ["query", "{F}", "-o", "{O}"], E("safe")),
            run("inspect", ["inspect", "{F}", "--detail", "--format", "json"], E("safe"))]


def flat_runs(kind, rows=None, agg_value=None, count=None, path="z"):
    return battery(kind, rows=rows, agg=f"count({path}) AS c, sum({path}) AS s, max({path}) AS m",
                   agg_value=agg_value, count=count, count_kind="eq", select=path, write_kind="safe")


@case("dremel_rep_orphan_chunk_start", B21)
def _():
    # 3 rows, but the chunk's first level says "continue the previous row's list".
    pg = list_page(T.bitpack([1, 0, 1, 0, 0], 1), T.rle_run(3, 5, 2), [1, 2, 3, 4, 5], 5)
    return {"file": one_chunk_file(LIST_SCHEMA, ["l", "list", "element"], [pg], 3, 5), "runs": list_runs(3)}


@case("dremel_rep_past_max", B21)
def _():
    pg = list_page(T.rle_run(0, 1, 1) + T.rle_run(2, 2, 1) + T.rle_run(0, 2, 1), T.rle_run(3, 5, 2), [1, 2, 3, 4, 5],
                   5)
    return {"file": one_chunk_file(LIST_SCHEMA, ["l", "list", "element"], [pg], 3, 5), "runs": list_runs(3)}


@case("dremel_rep_row_starts_disagree_num_rows", B21)
def _():
    # Five row starts (rep 0 everywhere) in a row group that says 3 rows.
    pg = list_page(T.rle_run(0, 5, 1), T.rle_run(3, 5, 2), [1, 2, 3, 4, 5], 5)
    return {"file": one_chunk_file(LIST_SCHEMA, ["l", "list", "element"], [pg], 3, 5), "runs": list_runs(3)}


@case("dremel_rep_v2_page_starts_mid_row", B21)
def _():
    # Two v2 pages; the second starts with rep 1 (v2 pages must start at a row boundary).
    def v2(reps, defs, vals, rows):
        r = T.rle_run(reps[0], len(reps), 1) if len(set(reps)) == 1 else T.bitpack(reps, 1)
        d = T.rle_run(3, len(defs), 2)
        return T.data_page_v2(r + d + T.plain_i64(vals), len(vals), 0, rows, len(d), len(r))
    p1 = v2([0, 1, 0], [3, 3, 3], [1, 2, 3], 2)
    p2 = v2([1, 0, 1], [3, 3, 3], [4, 5, 6], 1)
    return {"file": one_chunk_file(LIST_SCHEMA, ["l", "list", "element"], [p1, p2], 3, 6), "runs": list_runs(3)}


@case("dremel_def_past_max_rle_7", B22)
def _():
    # max_def 1 (bit width 1); an RLE run's value byte says 7.
    pg = flat_page(T.rle_run(1, 4, 1) + T.rle_run(7, 4, 1), list(range(8)), 8)
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 8), "runs": flat_runs("err", count=8)}


@case("dremel_def_past_max_bitpacked_struct", B22)
def _():
    # st OPTIONAL > x OPTIONAL: max_def 2, width 2; a bit-packed level of 3.
    pg = flat_page(T.bitpack([2, 2, 3, 2, 0, 1, 2, 2], 2), [10, 11, 12, 13, 14, 15], 8)
    return {"file": one_chunk_file(STRUCT_SCHEMA, ["st", "x"], [pg], 8),
            "runs": flat_runs("err", count=8, path="st.x")}


@case("dremel_def_past_max_v2", B22)
def _():
    d = T.rle_run(1, 2, 1) + T.rle_run(3, 1, 1) + T.rle_run(1, 5, 1)  # column max 1: an RLE value of 3
    pg = T.data_page_v2(d + T.plain_i64(range(8)), 8, 0, 8, len(d), 0)
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 8), "runs": flat_runs("err", count=8)}


@case("dremel_desync_1000_998_42", B23)
def _():
    # num_values 1000; def stream yields 998 present levels; only 42 values follow.
    pg = flat_page(T.rle_run(1, 998, 1), list(range(42)), 1000)
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 1000), "runs": flat_runs("err", count=1000)}


@case("dremel_desync_levels_ok_values_short", B23)
def _():
    pg = flat_page(T.rle_run(1, 1000, 1), list(range(42)), 1000)
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 1000), "runs": flat_runs("err", count=1000)}


@case("dremel_desync_values_short_by_one_byte", B23)
def _():
    pg = T.data_page_v1(T.levels_v1(T.rle_run(1, 100, 1)) + T.plain_i64(range(100))[:-1], 100)
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 100), "runs": flat_runs("err", count=100)}


@case("dremel_desync_level_prefix_past_page", B23)
def _():
    body = T.rle_run(1, 8, 1)
    pg = T.data_page_v1(struct.pack("<I", 4096) + body + T.plain_i64(range(8)), 8)
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 8), "runs": flat_runs("err", count=8)}


@case("dremel_desync_list_1000_998_42", B23)
def _():
    reps = T.rle_run(0, 1000, 1)
    defs = T.rle_run(3, 998, 2)
    pg = list_page(reps, defs, list(range(42)), 1000)
    return {"file": one_chunk_file(LIST_SCHEMA, ["l", "list", "element"], [pg], 1000), "runs": list_runs(1000)}


@case("dremel_desync_v2_def_len_past_page", B23)
def _():
    d = T.rle_run(1, 8, 1)
    pg = T.data_page_v2(d + T.plain_i64(range(8)), 8, 0, 8, 10_000, 0)
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 8), "runs": flat_runs("err", count=8)}


@case("dremel_desync_v2_rep_plus_def_past_page", B23)
def _():
    d = T.rle_run(1, 8, 1)
    pg = T.data_page_v2(d + T.plain_i64(range(8)), 8, 0, 8, len(d), 2**31 - 1)
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 8), "runs": flat_runs("err", count=8)}


@case("dremel_desync_v2_num_nulls_lies", B23)
def _():
    # 8 levels: 2 null, 6 values; num_nulls claims 5. The levels are the truth.
    d = T.bitpack([1, 0, 1, 1, 0, 1, 1, 1], 1)
    pg = T.data_page_v2(d + T.plain_i64([1, 2, 3, 4, 5, 6]), 8, 5, 8, len(d), 0)
    rows = [{"z": v} for v in [1, None, 2, 3, None, 4, 5, 6]]
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 8),
            "runs": flat_runs("err|eq", rows=rows, count=8, agg_value={"c": 6, "s": 21, "m": 6})}


@case("dremel_desync_v2_num_rows_lies_flat", B23)
def _():
    d = T.rle_run(1, 8, 1)
    pg = T.data_page_v2(d + T.plain_i64(range(8)), 8, 0, 3, len(d), 0)
    rows = [{"z": i} for i in range(8)]
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 8),
            "runs": flat_runs("err|eq", rows=rows, count=8, agg_value={"c": 8, "s": 28, "m": 7})}


@case("dremel_desync_v2_num_values_lies", B23)
def _():
    # Header says 5 values; levels and values hold 8.
    d = T.rle_run(1, 8, 1)
    pg = T.data_page_v2(d + T.plain_i64(range(8)), 5, 0, 5, len(d), 0)
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 8), "runs": flat_runs("err", count=8)}


@case("dremel_desync_pages_sum_short_of_chunk", B23)
def _():
    # Row group and chunk say 16 rows; the only page holds 8.
    pg = flat_page(T.rle_run(1, 8, 1), list(range(8)), 8)
    return {"file": one_chunk_file(FLAT_SCHEMA, ["z"], [pg], 16), "runs": flat_runs("err", count=16)}


@case("dremel_transition_rep1_def0", B24)
def _():
    # Row 0 = [1, <continuation of a NULL list>]: rep 1 with def 0 cannot happen.
    pg = list_page(T.bitpack([0, 1, 0, 0], 1), T.bitpack([3, 0, 3, 3], 2), [1, 3, 4], 4)
    return {"file": one_chunk_file(LIST_SCHEMA, ["l", "list", "element"], [pg], 3, 4), "runs": list_runs(3)}


@case("dremel_transition_rep1_def1_empty_continued", B24)
def _():
    pg = list_page(T.bitpack([0, 1, 0], 1), T.bitpack([1, 1, 3], 2), [4], 3)
    return {"file": one_chunk_file(LIST_SCHEMA, ["l", "list", "element"], [pg], 2, 3), "runs": list_runs(2)}


def struct2_file(x_defs, x_vals, y_defs, y_vals, n):
    schema = [T.schema_elem("schema", num_children=1), T.schema_elem("st", rep=T.OPTIONAL, num_children=2),
              T.schema_elem("x", ty=T.INT64, rep=T.OPTIONAL), T.schema_elem("y", ty=T.INT64, rep=T.OPTIONAL)]
    px = flat_page(T.bitpack(x_defs, 2), x_vals, n)
    py = flat_page(T.bitpack(y_defs, 2), y_vals, n)
    mx = T.column_meta(T.INT64, ["st", "x"], n)
    my = T.column_meta(T.INT64, ["st", "y"], n)
    out, fmd = T.make_file(schema, [[[px], [py]]], rg_rows=[n], metas=[[mx, my]])
    return T.finish(out, fmd)


@case("dremel_transition_siblings_disagree_on_struct", B24)
def _():
    # Row 1: x says st is NULL (def 0); y says st is present with y = 5 (def 2). Row 3 the other way round.
    b = struct2_file([2, 0, 2, 1], [1, 3], [2, 2, 2, 0], [4, 5, 6], 4)
    return {"file": b, "runs": [
        run("rows", ["query", "{F}"], E("safe")),
        run("agg", ["query", "{F}", "--aggregate", "count(st.x) AS cx, count(st.y) AS cy"], E("safe")),
        run("filter-write", ["query", "{F}", "--filter", "st.y IS NOT NULL", "-o", "{O}"], E("safe")),
        run("write", ["query", "{F}", "-o", "{O}"], E("safe")),
    ], "note": "inspect outputs by hand"}


# ================================================================ 3. encoding & dictionary sabotage

B31, B32, B33, B34, B35 = ("3.1 dict index OOB", "3.2 missing/duplicate dictionary", "3.3 bit-width lies",
                           "3.4 corrupt BYTE_STREAM_SPLIT", "3.5 DELTA_BINARY_PACKED overflow")

REQ_SCHEMA = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED)]
DICT12 = [100 + i for i in range(12)]


def dict_file(pages, n, schema=REQ_SCHEMA, ty=T.INT64, codec=T.UNCOMPRESSED):
    m = T.column_meta(ty, ["z"], n, codec=codec, encodings=(T.PLAIN, T.RLE, T.RLE_DICTIONARY))
    out, fmd = T.make_file(schema, [[pages]], rg_rows=[n], metas=[[m]])
    return T.finish(out, fmd)


def dpage(bw, body, n):
    return T.data_page_v1(bytes([bw]) + body, n, encoding=T.RLE_DICTIONARY)


def dict_runs(kind, n, rows=None, agg_value=None, group=None, filt_rows=None, filt="z = 105"):
    runs = battery(kind, rows=rows, agg="count(z) AS c, sum(z) AS s, min(z) AS mn, max(z) AS mx", agg_value=agg_value,
                   count=n, count_kind="eq", select="z", write_kind="safe", filt=filt, filt_rows=filt_rows)
    runs.append(run("group-by", ["query", "{F}", "--group-by", "z", "--aggregate", "count(*) AS n"], E(kind, group)))
    runs.append(run("sql-distinct", ["query", "--query", "SELECT count(DISTINCT z) AS d FROM '{F}'"],
                    E(kind, [{"d": len(group)}] if group else None)))
    runs.append(run("encoded-off", ["query", "{F}", "--aggregate", "sum(z) AS s"],
                    E(kind, [{"s": agg_value["s"]}] if agg_value else None), env=dict(os.environ, ZPQ_ENCODED="0")))
    return runs


@case("dict_index_oob_rle_500", B31)
def _():
    body = T.rle_run(3, 4, 9) + T.rle_run(500, 4, 9)
    return {"file": dict_file([T.dict_page(T.plain_i64(DICT12), 12), dpage(9, body, 8)], 8), "runs": dict_runs("err", 8)}


@case("dict_index_oob_bitpacked_13", B31)
def _():
    body = T.bitpack([0, 1, 2, 13, 4, 5, 15, 7], 4)
    return {"file": dict_file([T.dict_page(T.plain_i64(DICT12), 12), dpage(4, body, 8)], 8), "runs": dict_runs("err", 8)}


@case("dict_index_oob_optional_after_nulls", B31)
def _():
    sch = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.INT64, rep=T.OPTIONAL)]
    defs = T.levels_v1(T.bitpack([1, 0, 1, 0, 1, 0, 1, 1], 1))
    pg = T.data_page_v1(defs + bytes([4]) + T.bitpack([1, 2, 3, 12, 0], 4), 8, encoding=T.RLE_DICTIONARY)
    return {"file": dict_file([T.dict_page(T.plain_i64(DICT12), 12), pg], 8, schema=sch), "runs": dict_runs("err", 8)}


@case("dict_index_oob_string_dict", B31)
def _():
    sch = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.BYTE_ARRAY, rep=T.REQUIRED,
                                                                 converted=T.CT_UTF8)]
    dp = T.dict_page(T.plain_ba([b"a", b"b", b"c"]), 3)
    pg = dpage(2, T.bitpack([0, 1, 2, 3, 0, 1, 2, 3], 2), 8)
    return {"file": dict_file([dp, pg], 8, schema=sch, ty=T.BYTE_ARRAY),
            "runs": battery("err", agg="count(z) AS c, max(z) AS m", agg_value=None, count=8, count_kind="eq",
                            select="z", write_kind="safe", filt="z = 'b'") +
            [run("group-by", ["query", "{F}", "--group-by", "z", "--aggregate", "count(*) AS n"], E("err"))]}


@case("dict_missing", B32)
def _():
    body = T.bitpack([0, 1, 2, 3, 4, 5, 6, 7], 4)
    return {"file": dict_file([dpage(4, body, 8)], 8), "runs": dict_runs("err", 8)}


@case("dict_three_pages_mid_chunk", B32)
def _():
    d1 = T.dict_page(T.plain_i64(DICT12), 12)
    d2 = T.dict_page(T.plain_i64([7, 8]), 2)
    pages = [d1, dpage(4, T.bitpack([0] * 8, 4), 8), d2, dpage(4, T.bitpack([1] * 8, 4), 8), d2,
             dpage(4, T.bitpack([0] * 8, 4), 8)]
    return {"file": dict_file(pages, 24), "runs": dict_runs("err", 24)}


@case("dict_two_pages_at_start", B32)
def _():
    pages = [T.dict_page(T.plain_i64(DICT12), 12), T.dict_page(T.plain_i64([7, 8]), 2),
             dpage(4, T.bitpack([0, 1] * 4, 4), 8)]
    return {"file": dict_file(pages, 8), "runs": dict_runs("err", 8)}


@case("dict_page_after_plain_page", B32)
def _():
    pages = [T.data_page_v1(T.plain_i64(range(8)), 8), T.dict_page(T.plain_i64(DICT12), 12),
             dpage(4, T.bitpack([0] * 8, 4), 8)]
    return {"file": dict_file(pages, 16), "runs": dict_runs("err", 16)}


@case("bitwidth_33", B33)
def _():
    return {"file": dict_file([T.dict_page(T.plain_i64(DICT12), 12), dpage(33, T.rle_run(1, 8, 33), 8)], 8),
            "runs": dict_runs("err", 8)}


@case("bitwidth_255", B33)
def _():
    return {"file": dict_file([T.dict_page(T.plain_i64(DICT12), 12), dpage(255, b"\x10\x01" + b"\x00" * 64, 8)], 8),
            "runs": dict_runs("err", 8)}


@case("bitwidth_0_with_12_entries", B33)
def _():
    # Width 0 is legal: every index is 0. The answer is dict[0] eight times.
    rows = [{"z": 100}] * 8
    return {"file": dict_file([T.dict_page(T.plain_i64(DICT12), 12), dpage(0, T.rle_run(0, 8, 0), 8)], 8),
            "runs": dict_runs("err|eq", 8, rows=rows, agg_value={"c": 8, "s": 800, "mn": 100, "mx": 100},
                              group=[{"z": 100, "n": 8}], filt="z = 100", filt_rows=rows)}


@case("bitwidth_0_no_runs", B33)
def _():
    # Width 0 and no run at all: the indices are missing, but at width 0 every index can only be 0.
    rows = [{"z": 100}] * 8
    return {"file": dict_file([T.dict_page(T.plain_i64(DICT12), 12), dpage(0, b"", 8)], 8),
            "runs": dict_runs("err|eq", 8, rows=rows, agg_value={"c": 8, "s": 800, "mn": 100, "mx": 100},
                              group=[{"z": 100, "n": 8}], filt="z = 100", filt_rows=rows)}


@case("bitwidth_run_past_num_values", B33)
def _():
    # A run of 2^30 indices in a page of 8 values.
    return {"file": dict_file([T.dict_page(T.plain_i64(DICT12), 12), dpage(4, T.rle_run(1, 2**30, 4), 8)], 8),
            "runs": dict_runs("err|eq", 8, rows=[{"z": 101}] * 8,
                              agg_value={"c": 8, "s": 808, "mn": 101, "mx": 101}, group=[{"z": 101, "n": 8}],
                              filt="z = 101", filt_rows=[{"z": 101}] * 8)}


@case("bitwidth_bitpacked_groups_past_page", B33)
def _():
    # A bit-packed header claiming 2^20 groups of 8 with 4 bytes of data.
    return {"file": dict_file([T.dict_page(T.plain_i64(DICT12), 12), dpage(4, T.uvarint((2**20 << 1) | 1) + b"\x00" * 4,
                                                                            8)], 8),
            "runs": dict_runs("err|eq", 8, rows=[{"z": 100}] * 8, agg_value={"c": 8, "s": 800, "mn": 100, "mx": 100},
                              group=[{"z": 100, "n": 8}], filt="z = 100", filt_rows=[{"z": 100}] * 8)}


def float_file(ty, payload, n, encoding):
    sch = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=ty, rep=T.REQUIRED)]
    pg = T.data_page_v1(payload, n, encoding=encoding)
    m = T.column_meta(ty, ["z"], n, encodings=(encoding,))
    out, fmd = T.make_file(sch, [[[pg]]], rg_rows=[n], metas=[[m]])
    return T.finish(out, fmd)


def bss(raw, width):
    n = len(raw) // width
    return bytes(raw[i * width + b] for b in range(width) for i in range(n))


def float_runs(kind, n, rows=None, agg_value=None):
    return battery(kind, rows=rows, agg="count(z) AS c, sum(z) AS s, max(z) AS m", agg_value=agg_value, count=n,
                   count_kind="eq", select="z", write_kind="safe", filt="z > 1.5")


@case("bss_float_13_bytes", B34)
def _():
    return {"file": float_file(T.FLOAT, bss(T.plain_f32([1, 2, 3]), 4) + b"\x00", 3, T.BYTE_STREAM_SPLIT),
            "runs": float_runs("err", 3)}


@case("bss_double_7_bytes", B34)
def _():
    return {"file": float_file(T.DOUBLE, b"\x00" * 7, 1, T.BYTE_STREAM_SPLIT), "runs": float_runs("err", 1)}


@case("bss_short_of_num_values", B34)
def _():
    return {"file": float_file(T.FLOAT, bss(T.plain_f32([1, 2, 3, 4]), 4), 8, T.BYTE_STREAM_SPLIT),
            "runs": float_runs("err", 8)}


@case("bss_longer_than_num_values", B34)
def _():
    # 8 floats of payload for 4 values: the planes are 8 long, so values 0..3 are bytes 0..3 of each plane.
    raw = T.plain_f32([1, 2, 3, 4, 5, 6, 7, 8])
    return {"file": float_file(T.FLOAT, bss(raw, 4), 4, T.BYTE_STREAM_SPLIT), "runs": float_runs("safe", 4)}


@case("bss_empty_payload", B34)
def _():
    return {"file": float_file(T.DOUBLE, b"", 4, T.BYTE_STREAM_SPLIT), "runs": float_runs("err", 4)}


def dbp(block_size, miniblocks, total, first, blocks):
    """blocks: [(min_delta, [bit widths], payload bytes)]"""
    out = T.uvarint(block_size) + T.uvarint(miniblocks) + T.uvarint(total) + T.zz(first)
    for md, bws, data in blocks:
        out += T.zz(md) + bytes(bws) + data
    return out


def dbp_file(payload, n, ty=T.INT64):
    sch = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=ty, rep=T.REQUIRED)]
    pg = T.data_page_v1(payload, n, encoding=T.DELTA_BINARY_PACKED)
    m = T.column_meta(ty, ["z"], n, encodings=(T.DELTA_BINARY_PACKED,))
    out, fmd = T.make_file(sch, [[[pg]]], rg_rows=[n], metas=[[m]])
    return T.finish(out, fmd)


def wrap64(v):
    v &= (1 << 64) - 1
    return v - (1 << 64) if v >> 63 else v


def dbp_runs(kind, n, vals=None):
    rows = [{"z": v} for v in vals] if vals is not None else None
    agg = {"c": n, "s": sum(vals), "m": max(vals)} if vals is not None else None
    return battery(kind, rows=rows, agg="count(z) AS c, sum(z) AS s, max(z) AS m", agg_value=agg, count=n,
                   count_kind="eq", select="z", write_kind="safe", filt="z > 0",
                   filt_rows=[r for r in rows if r["z"] > 0] if rows else None)


@case("dbp_bitwidth_65", B35)
def _():
    return {"file": dbp_file(dbp(128, 4, 8, 0, [(1, [65, 0, 0, 0], b"\xff" * 300)]), 8), "runs": dbp_runs("err", 8)}


@case("dbp_bitwidth_33_int32", B35)
def _():
    return {"file": dbp_file(dbp(128, 4, 8, 0, [(1, [33, 0, 0, 0], b"\xff" * 200)]), 8, ty=T.INT32),
            "runs": dbp_runs("err", 8)}


@case("dbp_bitwidth_255", B35)
def _():
    return {"file": dbp_file(dbp(128, 4, 8, 0, [(1, [255, 255, 255, 255], b"\xff" * 4000)]), 8),
            "runs": dbp_runs("err", 8)}


@case("dbp_overflow_wraps", B35)
def _():
    # first = 2^63-1, min_delta = 2^63-1, every delta 0 at width 0: values wrap (spec: two's-complement wrapping).
    first = 2**63 - 1
    md = 2**63 - 1
    vals = [first]
    for _ in range(7):
        vals.append(wrap64(vals[-1] + md))
    return {"file": dbp_file(dbp(128, 4, 8, first, [(md, [0, 0, 0, 0], b"")]), 8), "runs": dbp_runs("err|eq", 8, vals)}


@case("dbp_overflow_min_delta_plus_packed", B35)
def _():
    # min_delta = -2^63, packed deltas of 2^63-1 at width 63: each step adds -1 after wrapping.
    first = 5
    md = -(2**63)
    packed = [2**63 - 1] * 32
    vals = [first]
    for d in packed[:7]:
        vals.append(wrap64(vals[-1] + md + d))
    data = T.bitpack(packed, 63)
    data = data[len(T.uvarint(((32 // 8) << 1) | 1)):]  # raw miniblock bits, no hybrid header
    return {"file": dbp_file(dbp(128, 4, 8, first, [(md, [63, 0, 0, 0], data)]), 8),
            "runs": dbp_runs("err|eq", 8, vals)}


@case("dbp_block_size_zero", B35)
def _():
    return {"file": dbp_file(dbp(0, 4, 8, 0, [(1, [0, 0, 0, 0], b"")]), 8), "runs": dbp_runs("err", 8)}


@case("dbp_miniblocks_zero", B35)
def _():
    return {"file": dbp_file(dbp(128, 0, 8, 0, [(1, [], b"")]), 8), "runs": dbp_runs("err", 8)}


@case("dbp_miniblocks_17", B35)
def _():
    return {"file": dbp_file(dbp(17 * 32, 17, 8, 0, [(1, [0] * 17, b"")]), 8), "runs": dbp_runs("err", 8)}


@case("dbp_block_size_not_multiple", B35)
def _():
    return {"file": dbp_file(dbp(100, 4, 8, 0, [(1, [0, 0, 0, 0], b"")]), 8), "runs": dbp_runs("err", 8)}


@case("dbp_block_size_huge", B35)
def _():
    return {"file": dbp_file(dbp(2**31 * 128, 4, 8, 0, [(1, [0, 0, 0, 0], b"")]), 8), "runs": dbp_runs("err", 8)}


@case("dbp_total_count_huge", B35)
def _():
    # Header claims 2^62 values; the page says 8 and the block is width 0, delta 1.
    return {"file": dbp_file(dbp(128, 4, 2**62, 0, [(1, [0, 0, 0, 0], b"")]), 8),
            "runs": dbp_runs("err|eq", 8, list(range(8)))}


@case("dbp_total_count_short", B35)
def _():
    return {"file": dbp_file(dbp(128, 4, 3, 0, [(1, [0, 0, 0, 0], b"")]), 8), "runs": dbp_runs("err", 8)}


@case("dbp_negative_block_size_varint", B35)
def _():
    # 10-byte varint with the top bit set: as i64, negative.
    neg = T.uvarint((1 << 64) - 128)
    return {"file": dbp_file(neg + T.uvarint(4) + T.uvarint(8) + T.zz(0) + T.zz(1) + b"\x00" * 4, 8),
            "runs": dbp_runs("err", 8)}


@case("dbp_truncated_mid_miniblock", B35)
def _():
    return {"file": dbp_file(dbp(128, 4, 8, 0, [(1, [8, 0, 0, 0], b"\x01\x02\x03")]), 8), "runs": dbp_runs("err", 8)}


def ba_file(payload, n, encoding):
    sch = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.BYTE_ARRAY, rep=T.REQUIRED,
                                                                 converted=T.CT_UTF8)]
    pg = T.data_page_v1(payload, n, encoding=encoding)
    m = T.column_meta(T.BYTE_ARRAY, ["z"], n, encodings=(encoding,))
    out, fmd = T.make_file(sch, [[[pg]]], rg_rows=[n], metas=[[m]])
    return T.finish(out, fmd)


def ba_runs(kind, n):
    return battery(kind, agg="count(z) AS c, max(z) AS m", count=n, count_kind="eq", select="z", write_kind="safe",
                   filt="z = 'a'")


@case("plain_ba_len_4g", B35)
def _():
    return {"file": ba_file(b"\x01\x00\x00\x00a" + b"\xff\xff\xff\xff" + b"b" * 16, 2, T.PLAIN), "runs": ba_runs("err", 2)}


@case("dlba_negative_length", B35)
def _():
    lengths = dbp(128, 4, 3, 1, [(-5, [0, 0, 0, 0], b"")])  # 1, -4, -9
    return {"file": ba_file(lengths + b"abc", 3, T.DELTA_LENGTH_BYTE_ARRAY), "runs": ba_runs("err", 3)}


@case("dlba_lengths_past_page", B35)
def _():
    lengths = dbp(128, 4, 3, 1000, [(0, [0, 0, 0, 0], b"")])
    return {"file": ba_file(lengths + b"abc", 3, T.DELTA_LENGTH_BYTE_ARRAY), "runs": ba_runs("err", 3)}


@case("dba_prefix_longer_than_previous", B35)
def _():
    prefixes = dbp(128, 4, 3, 0, [(5, [0, 0, 0, 0], b"")])  # 0, 5, 10 on values of length 1
    suffixes = dbp(128, 4, 3, 1, [(0, [0, 0, 0, 0], b"")])
    return {"file": ba_file(prefixes + suffixes + b"abc", 3, T.DELTA_BYTE_ARRAY), "runs": ba_runs("err", 3)}


@case("dba_negative_prefix", B35)
def _():
    prefixes = dbp(128, 4, 3, 0, [(-1, [0, 0, 0, 0], b"")])
    suffixes = dbp(128, 4, 3, 1, [(0, [0, 0, 0, 0], b"")])
    return {"file": ba_file(prefixes + suffixes + b"abc", 3, T.DELTA_BYTE_ARRAY), "runs": ba_runs("err", 3)}


def raw_pack(values, bw):
    """Plain LSB-first bit packing (DELTA_BINARY_PACKED miniblocks), no hybrid header."""
    acc = 0
    for i, v in enumerate(values):
        acc |= (v & ((1 << bw) - 1)) << (i * bw)
    return acc.to_bytes(len(values) * bw // 8, "little")


def dbp_from_deltas(first, deltas):
    """DELTA_BINARY_PACKED of `first` then `deltas`: blocks of 128 (4 miniblocks of 32)."""
    out = T.uvarint(128) + T.uvarint(4) + T.uvarint(len(deltas) + 1) + T.zz(first)
    for b in range(0, len(deltas), 128):
        blk = deltas[b:b + 128]
        md = min(blk)
        rel = [d - md for d in blk] + [0] * (128 - len(blk))
        bws = [max(rel[m * 32:(m + 1) * 32]).bit_length() for m in range(4)]
        out += T.zz(md) + bytes(bws) + b"".join(raw_pack(rel[m * 32:(m + 1) * 32], bws[m]) for m in range(4))
    return out


@case("dba_prefix_expansion_bomb", B35)
def _():
    # Value 0 is 64 KiB; every later value repeats all of the previous one and adds a byte: 2^16 values, 4 GiB decoded
    # from a ~70 KB page.
    n = 1 << 16
    prefixes = dbp_from_deltas(0, [65536] + [1] * (n - 2))
    suffixes = dbp_from_deltas(65536, [-65535] + [0] * (n - 2))
    return {"file": ba_file(prefixes + suffixes + b"x" * 65536 + b"y" * (n - 1), n, T.DELTA_BYTE_ARRAY),
            "runs": ba_runs("err|eq", n)}


@case("rle_bool_run_past_num_values", B33)
def _():
    sch = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.BOOLEAN, rep=T.REQUIRED)]
    pg = T.data_page_v1(T.levels_v1(T.rle_run(1, 2**31 - 1, 1)), 8, encoding=T.RLE)
    m = T.column_meta(T.BOOLEAN, ["z"], 8, encodings=(T.RLE,))
    out, fmd = T.make_file(sch, [[[pg]]], rg_rows=[8], metas=[[m]])
    return {"file": T.finish(out, fmd), "runs": battery("err|eq", rows=[{"z": True}] * 8, count=8, count_kind="eq",
                                                        select="z", write_kind="safe")}


@case("flba_type_length_2g", B35)
def _():
    sch = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.FLBA, rep=T.REQUIRED,
                                                                 type_length=2**31 - 1)]
    pg = T.data_page_v1(b"\x00" * 64, 4)
    m = T.column_meta(T.FLBA, ["z"], 4)
    out, fmd = T.make_file(sch, [[[pg]]], rg_rows=[4], metas=[[m]])
    return {"file": T.finish(out, fmd), "runs": battery("err", count=4, count_kind="err|eq", select="z",
                                                        write_kind="safe")}


@case("int96_truncated", B35)
def _():
    sch = [T.schema_elem("schema", num_children=1), T.schema_elem("z", ty=T.INT96, rep=T.REQUIRED)]
    pg = T.data_page_v1(b"\x00" * 30, 3)
    m = T.column_meta(T.INT96, ["z"], 3)
    out, fmd = T.make_file(sch, [[[pg]]], rg_rows=[3], metas=[[m]])
    return {"file": T.finish(out, fmd), "runs": battery("err", count=3, count_kind="eq", write_kind="safe",
                                                        agg="max(z) AS m")}


# ================================================================ 4. metadata contradictions
# Lead's rule (2026-10-20): by default stats are usable only when self-consistent (null_count = 0 needs bounds,
# null_count = num_rows needs no bounds; min > max, null_count > num_rows or counts that do not add up: decode).
# --trust-stats trusts stats as written, so a lie there giving a wrong answer is by design.

B41, B42, B43 = "4.1 row count disagreements", "4.2 poisoned stats / page index", "4.3 logical vs physical type"

SK = list(range(400))
SV = [None if 200 <= i < 300 else i for i in SK]
SD = [float(i) for i in SK]
SS = [f"{i:04d}" for i in SK]


def stats_base(page_index=True, **kw):
    """4 row groups × 100 rows: k = id, v = id but NULL on rows 200..299, d = float(k), s = '%04d' % k."""
    t = pa.table({"k": pa.array(SK, pa.int64()), "v": pa.array(SV, pa.int64()), "d": pa.array(SD, pa.float64()),
                  "s": pa.array(SS, pa.string())})
    b = io.BytesIO()
    pq.write_table(t, b, row_group_size=100, write_page_index=page_index, data_page_size=128, write_batch_size=25,
                   compression="none", **kw)
    return b.getvalue()


def chunk_stats(f, rg, col):
    return cmd(f, rg, col).get(T.CMD_STATS)


def count_where(pred):
    return [{"n": sum(1 for i in SK if pred(i))}]


def stats_case(mutate, filters, base=None):
    """`filters`: [(label, filter text, python predicate on row id)]: count, rows, write and --scan-all each."""
    b = refooter(base or stats_base(), mutate)
    runs = []
    for label, filt, pred in filters:
        want = count_where(pred)
        rows = [{"k": i} for i in SK if pred(i)]
        runs.append(run(f"{label}-count", ["query", "{F}", "--filter", filt, "--aggregate", "count(*) AS n"],
                        E("err|eq", want)))
        runs.append(run(f"{label}-rows", ["query", "{F}", "--filter", filt, "--columns", "k"], E("err|eq", rows)))
        runs.append(run(f"{label}-write", ["query", "{F}", "--filter", filt, "--columns", "k", "-o", "{O}"],
                        E("err|eq", rows)))
        runs.append(run(f"{label}-scanall", ["query", "{F}", "--scan-all", "--filter", filt, "--aggregate",
                                             "count(*) AS n"], E("eq", want)))
    return {"file": b, "runs": runs}


def v_null(i):
    return SV[i] is None


@case("rows_fmd_num_rows_10x", B41)
def _():
    # FileMetaData.num_rows says 10,000; the row groups say 400 and hold 400.
    b = refooter(stats_base(), lambda f: f.set(T.FMD_NUM_ROWS, 10_000, T.T_I64))
    return {"file": b, "runs": [
        run("count", ["query", "{F}", "--aggregate", "count(*) AS n"], E("err|eq", [{"n": 400}])),
        run("count-scanall", ["query", "{F}", "--scan-all", "--aggregate", "count(*) AS n, count(k) AS c"],
            E("err|eq", [{"n": 400, "c": 400}])),
        run("sql-count", ["query", "--query", "SELECT count(*) AS n FROM '{F}'"], E("err|eq", [{"n": 400}])),
        run("rows-limit", ["query", "{F}", "--columns", "k", "--limit", "1000"], E("err|eq", [{"k": i} for i in SK])),
        run("write", ["query", "{F}", "-o", "{O}"], E("safe")),
        run("inspect", ["inspect", "{F}", "--format", "json"], E("safe")),
    ]}


@case("rows_fmd_num_rows_short", B41)
def _():
    b = refooter(stats_base(), lambda f: f.set(T.FMD_NUM_ROWS, 5, T.T_I64))
    return {"file": b, "runs": [
        run("count", ["query", "{F}", "--aggregate", "count(*) AS n"], E("err|eq", [{"n": 400}])),
        run("rows-limit", ["query", "{F}", "--columns", "k", "--limit", "1000"], E("err|eq", [{"k": i} for i in SK])),
        run("filter-count", ["query", "{F}", "--filter", "k >= 0", "--aggregate", "count(*) AS n"],
            E("err|eq", [{"n": 400}])),
    ]}


@case("rows_rg_num_rows_half", B41)
def _():
    # RowGroup 0 says 50 rows; its chunks say and hold 100.
    b = refooter(stats_base(), lambda f: f.get(T.FMD_ROW_GROUPS).items[0].set(T.RG_NUM_ROWS, 50, T.T_I64))
    return {"file": b, "runs": [
        run("count", ["query", "{F}", "--aggregate", "count(*) AS n"], E("err|eq", [{"n": 400}])),
        run("agg", ["query", "{F}", "--aggregate", "count(k) AS c, sum(k) AS s"],
            E("err|eq", [{"c": 400, "s": sum(SK)}])),
        run("rows", ["query", "{F}", "--columns", "k"], E("err|eq", [{"k": i} for i in SK])),
    ]}


@case("rows_column_pages_one_short", B41)
def _():
    # Column v's chunk in row group 1 loses its last page (25 rows): chunk metadata still says 100.
    def mp(rg, col, pages):
        return pages[:-1] if (rg, col) == (1, 1) else pages
    raw = T.rebuild(stats_base(page_index=False), mutate_pages=mp)
    return {"file": raw, "runs": battery("err", count=400, count_kind="err|eq", agg="count(v) AS c, sum(v) AS s",
                                         select="k, v", write_kind="safe")}


@case("rows_columns_disagree_num_values", B41)
def _():
    # Column v's chunk says 99 values, column k's 100, row group 100 (flat columns).
    b = refooter(stats_base(), lambda f: cmd(f, 1, 1).set(T.CMD_NUM_VALUES, 99, T.T_I64))
    return {"file": b, "runs": battery("err", count=400, count_kind="err|eq", agg="count(v) AS c", select="k, v",
                                       write_kind="safe")}


@case("stats_min_gt_max_int", B42, known="T1")
def _():
    def m(f):
        s = chunk_stats(f, 1, 0)
        for fid, v in ((T.ST_MIN_VALUE, 150), (T.ST_MAX_VALUE, 120), (T.ST_MIN, 150), (T.ST_MAX, 120)):
            s.set(fid, struct.pack("<q", v), T.T_BIN)
    return stats_case(m, [("eq130", "k = 130", lambda i: i == 130),
                          ("range", "k >= 125 AND k <= 135", lambda i: 125 <= i <= 135),
                          ("ne", "k != 130", lambda i: i != 130)])


@case("stats_min_gt_max_double", B42, known="T1")
def _():
    def m(f):
        s = chunk_stats(f, 1, 2)
        s.set(T.ST_MIN_VALUE, struct.pack("<d", 150.0), T.T_BIN).set(T.ST_MAX_VALUE, struct.pack("<d", 120.0), T.T_BIN)
    return stats_case(m, [("eq130", "d = 130", lambda i: i == 130), ("lt", "d < 110", lambda i: i < 110)])


@case("stats_min_gt_max_string", B42, known="T1")
def _():
    def m(f):
        chunk_stats(f, 1, 3).set(T.ST_MIN_VALUE, b"0150", T.T_BIN).set(T.ST_MAX_VALUE, b"0120", T.T_BIN)
    return stats_case(m, [("eq130", "s = '0130'", lambda i: i == 130),
                          ("like", "s LIKE '013%'", lambda i: 130 <= i <= 139)])


@case("stats_nullcount_all_with_bounds", B42, known="T2")
def _():
    return stats_case(lambda f: chunk_stats(f, 1, 1).set(T.ST_NULLS, 100, T.T_I64),
                      [("isnull", "v IS NULL", v_null), ("notnull", "v IS NOT NULL", lambda i: not v_null(i))])


@case("stats_nullcount0_no_bounds", B42, known="T3")
def _():
    return stats_case(lambda f: chunk_stats(f, 2, 1).set(T.ST_NULLS, 0, T.T_I64),
                      [("isnull", "v IS NULL", v_null), ("notnull", "v IS NOT NULL", lambda i: not v_null(i)),
                       ("gt", "v > 250", lambda i: SV[i] is not None and SV[i] > 250)])


@case("stats_nullcount_gt_num_rows", B42, known="T2")
def _():
    return stats_case(lambda f: chunk_stats(f, 1, 1).set(T.ST_NULLS, 1000, T.T_I64),
                      [("isnull", "v IS NULL", v_null), ("notnull", "v IS NOT NULL", lambda i: not v_null(i)),
                       ("eq", "v = 150", lambda i: i == 150)])


@case("stats_nullcount_negative", B42)
def _():
    return stats_case(lambda f: chunk_stats(f, 1, 1).set(T.ST_NULLS, -5, T.T_I64),
                      [("isnull", "v IS NULL", v_null), ("notnull", "v IS NOT NULL", lambda i: not v_null(i))])


@case("stats_bounds_wrong_width", B42)
def _():
    # INT64 bounds of 3 bytes and 12 bytes.
    def m(f):
        chunk_stats(f, 1, 0).set(T.ST_MIN_VALUE, b"\x01\x02\x03", T.T_BIN).set(T.ST_MAX_VALUE, b"\xff" * 12, T.T_BIN)
    return stats_case(m, [("eq", "k = 150", lambda i: i == 150), ("gt", "k > 190", lambda i: i > 190)])


@case("stats_nan_count_gt_rows", B42)
def _():
    return stats_case(lambda f: chunk_stats(f, 1, 2).set(T.ST_NAN, 1000, T.T_I64),
                      [("gt", "d > 150", lambda i: i > 150), ("eq", "d = 150", lambda i: i == 150)])


@case("stats_nan_count_negative", B42)
def _():
    return stats_case(lambda f: chunk_stats(f, 1, 2).set(T.ST_NAN, -1, T.T_I64),
                      [("gt", "d > 150", lambda i: i > 150), ("lt", "d < 150", lambda i: i < 150)])


@case("stats_distinct_count_lies", B42)
def _():
    b = refooter(stats_base(), lambda f: [chunk_stats(f, r, 0).set(T.ST_DISTINCT, 1, T.T_I64) for r in range(4)])
    return {"file": b, "runs": [
        run("distinct", ["query", "--query", "SELECT count(DISTINCT k) AS d FROM '{F}'"], E("err|eq", [{"d": 400}]))]}


def ci_mutate(rg, col, fn, base=None):
    """Re-point column `col` of row group `rg` at a mutated copy of its ColumnIndex (appended before the footer)."""
    raw, fmd, fs = T.read_file(base or stats_base())
    c = cc(fmd, rg, col)
    ci, _ = T.decode(raw, c.get(T.CC_CI_OFF))
    fn(ci)
    e = T.encode(ci)
    out = bytearray(raw[:fs])
    c.set(T.CC_CI_OFF, len(out), T.T_I64).set(T.CC_CI_LEN, len(e), T.T_I32)
    out += e
    return T.finish(bytes(out), fmd)


def oi_mutate(rg, col, fn, base=None):
    raw, fmd, fs = T.read_file(base or stats_base())
    c = cc(fmd, rg, col)
    oi, _ = T.decode(raw, c.get(T.CC_OI_OFF))
    fn(oi)
    e = T.encode(oi)
    out = bytearray(raw[:fs])
    c.set(T.CC_OI_OFF, len(out), T.T_I64).set(T.CC_OI_LEN, len(e), T.T_I32)
    out += e
    return T.finish(bytes(out), fmd)


def pi_runs(b, filters):
    runs = []
    for label, filt, pred in filters:
        hit = [i for i in SK if pred(i)]
        runs.append(run(f"{label}-count", ["query", "{F}", "--filter", filt, "--aggregate", "count(*) AS n"],
                        E("err|eq", [{"n": len(hit)}])))
        runs.append(run(f"{label}-rows", ["query", "{F}", "--filter", filt, "--columns", "k"],
                        E("err|eq", [{"k": i} for i in hit])))
        runs.append(run(f"{label}-write", ["query", "{F}", "--filter", filt, "-o", "{O}"],
                        E("err|eq", [{"k": i, "v": SV[i], "d": SD[i], "s": SS[i]} for i in hit])))
        runs.append(run(f"{label}-sum", ["query", "{F}", "--filter", filt, "--aggregate", "sum(k) AS s"],
                        E("err|eq", [{"s": sum(hit) if hit else None}])))
    runs.append(run("inspect", ["inspect", "{F}", "--detail", "--format", "json"], E("safe")))
    return {"file": b, "runs": runs}


def swap_page(ci, p):
    mins, maxs = ci.get(T.CI_MINS).items, ci.get(T.CI_MAXS).items
    mins[p], maxs[p] = maxs[p], mins[p]


@case("pageindex_min_gt_max", B42, known="T6")
def _():
    # Row group 1, column k: page 1 (rows 125..149) bounds swapped -> min 149 > max 125.
    return pi_runs(ci_mutate(1, 0, lambda ci: swap_page(ci, 1)),
                   [("eq130", "k = 130", lambda i: i == 130),
                    ("range", "k BETWEEN 126 AND 128", lambda i: 126 <= i <= 128)])


@case("pageindex_min_gt_max_string", B42, known="T6")
def _():
    return pi_runs(ci_mutate(1, 3, lambda ci: swap_page(ci, 1)), [("eq130", "s = '0130'", lambda i: i == 130)])


@case("pageindex_null_page_with_values", B42)
def _():
    # Column v, row group 1: page 1 flagged null_pages = true (its bounds kept): contradiction.
    def fn(ci):
        ci.get(T.CI_NULL_PAGES).items[1] = True
    return pi_runs(ci_mutate(1, 1, fn), [("eq130", "v = 130", lambda i: i == 130),
                                          ("notnull", "v IS NOT NULL", lambda i: not v_null(i)),
                                          ("isnull", "v IS NULL", v_null)])


@case("pageindex_null_counts_zero_on_null_pages", B42)
def _():
    # Column v, row group 2 (all NULL): null_counts claim 0 on every (null) page.
    def fn(ci):
        nc = ci.get(T.CI_NULL_COUNTS)
        nc.items = [0] * len(nc.items)
    return pi_runs(ci_mutate(2, 1, fn), [("isnull", "v IS NULL", v_null),
                                          ("notnull", "v IS NOT NULL", lambda i: not v_null(i))])


@case("pageindex_null_counts_exceed_rows", B42)
def _():
    def fn(ci):
        nc = ci.get(T.CI_NULL_COUNTS)
        nc.items = [10_000] * len(nc.items)
    return pi_runs(ci_mutate(1, 1, fn), [("isnull", "v IS NULL", v_null), ("eq", "v = 130", lambda i: i == 130)])


@case("pageindex_bounds_shifted_consistent", B42)
def _():
    # Page bounds of k in row group 1 rotated by one page: each page carries another page's bounds (a consistent lie
    # only decoding can catch). By the lead's rule this is trusted; the case records what zpq does.
    def fn(ci):
        for fid in (T.CI_MINS, T.CI_MAXS):
            it = ci.get(fid).items
            it[:] = it[1:] + it[:1]
        ci.set(T.CI_BOUNDARY, 0, T.T_I32)
    spec = pi_runs(ci_mutate(1, 0, fn), [("eq130", "k = 130", lambda i: i == 130)])
    for r in spec["runs"]:
        r["expect"] = E("safe")  # undetectable without decoding: trusted by design (recorded in REPORT.md)
    return spec


@case("pageindex_boundary_order_lies", B42)
def _():
    return pi_runs(ci_mutate(1, 0, lambda ci: ci.set(T.CI_BOUNDARY, 2, T.T_I32)),
                   [("eq130", "k = 130", lambda i: i == 130), ("gt", "k > 190", lambda i: i > 190)])


@case("pageindex_lists_wrong_length", B42)
def _():
    return pi_runs(ci_mutate(1, 0, lambda ci: ci.get(T.CI_MINS).items.pop()),
                   [("eq130", "k = 130", lambda i: i == 130)])


@case("offsetindex_pages_swapped", B42, known="T7")
def _():
    # Row group 1, column k: the locations of pages 1 and 2 swapped (first_row_index kept).
    def fn(oi):
        locs = oi.get(T.OI_LOCATIONS).items
        for fid, ty in ((T.PL_OFFSET, T.T_I64), (T.PL_SIZE, T.T_I32)):
            a, b2 = locs[1].get(fid), locs[2].get(fid)
            locs[1].set(fid, b2, ty)
            locs[2].set(fid, a, ty)
    return pi_runs(oi_mutate(1, 0, fn), [("eq130", "k = 130", lambda i: i == 130),
                                          ("eq160", "k = 160", lambda i: i == 160)])


@case("offsetindex_first_row_shifted", B42)
def _():
    # first_row_index of page 1 says 30 instead of 25 (monotone, plausible).
    def fn(oi):
        loc = oi.get(T.OI_LOCATIONS).items[1]
        loc.set(T.PL_FIRST_ROW, loc.get(T.PL_FIRST_ROW) + 5, T.T_I64)
    return pi_runs(oi_mutate(1, 0, fn), [("eq127", "k = 127", lambda i: i == 127),
                                          ("eq152", "k = 152", lambda i: i == 152)])


@case("offsetindex_offset_into_other_column", B42)
def _():
    b = stats_base()
    raw, fmd, _ = T.read_file(b)
    voff = cmd(fmd, 1, 1).get(T.CMD_DICT_OFF) or cmd(fmd, 1, 1).get(T.CMD_DATA_OFF)

    def fn(oi):
        oi.get(T.OI_LOCATIONS).items[2].set(T.PL_OFFSET, voff, T.T_I64)
    return pi_runs(oi_mutate(1, 0, fn, base=b), [("eq160", "k = 160", lambda i: i == 160)])


@case("offsetindex_size_past_chunk", B42)
def _():
    def fn(oi):
        oi.get(T.OI_LOCATIONS).items[1].set(T.PL_SIZE, 2**31 - 1, T.T_I32)
    return pi_runs(oi_mutate(1, 0, fn), [("eq130", "k = 130", lambda i: i == 130)])


def logical_file(se, values_payload, n, ty, enc=T.PLAIN, stats=None):
    sch = [T.schema_elem("schema", num_children=1), se]
    pg = T.data_page_v1(values_payload, n, encoding=enc)
    m = T.column_meta(ty, ["z"], n, stats=stats)
    out, fmd = T.make_file(sch, [[[pg]]], rg_rows=[n], metas=[[m]])
    return T.finish(out, fmd)


def logical_runs(kind, rows=None, agg=None, agg_value=None, n=None):
    # A re-encode carries the input's (invalid) annotation into the output, which strict readers then refuse as they
    # refuse the input: recorded, not judged here (`select-write` is "safe").
    runs = battery(kind, rows=rows, agg=agg, agg_value=agg_value, count=n, count_kind="err|eq", write_kind="safe")
    runs.append(run("select-write", ["query", "{F}", "--select", "z", "-o", "{O}"], E("safe")))
    return runs


def int_logical(bits, signed):
    return T.Struct([[10, T.T_STRUCT, T.Struct([[1, T.T_I8, bits], [2, T.T_TRUE, signed]])]])


@case("logical_utf8_on_int32", B43)
def _():
    se = T.schema_elem("z", ty=T.INT32, rep=T.REQUIRED, converted=T.CT_UTF8)
    se.set(T.SE_LOGICAL, T.Struct([[1, T.T_STRUCT, T.Struct()]]), T.T_STRUCT)
    b = logical_file(se, T.plain_i32([1, 2, 3]), 3, T.INT32)
    return {"file": b, "runs": logical_runs("err|eq", rows=[{"z": 1}, {"z": 2}, {"z": 3}], n=3,
                                            agg="sum(z) AS s", agg_value={"s": 6})}


@case("logical_decimal_38_on_flba2", B43)
def _():
    # DECIMAL(38,10) on FIXED_LEN_BYTE_ARRAY(2): 38 digits cannot fit 2 bytes. 0x0102 = 258, 0xfffe = -2.
    se = T.schema_elem("z", ty=T.FLBA, rep=T.REQUIRED, type_length=2, converted=T.CT_DECIMAL, scale=10, precision=38)
    b = logical_file(se, b"\x01\x02\xff\xfe", 2, T.FLBA)
    return {"file": b, "runs": logical_runs("err|eq", n=2, agg="sum(z) AS s, max(z) AS m",
                                            agg_value={"s": 2.56e-08, "m": 2.58e-08})}


@case("logical_decimal_p4_on_int64_huge_values", B43)
def _():
    # DECIMAL(4,0) on INT64 holding 2^62 three times: an accumulator sized from the precision would overflow.
    se = T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED, converted=T.CT_DECIMAL, scale=0, precision=4)
    v = 2**62
    b = logical_file(se, T.plain_i64([v, v, v]), 3, T.INT64)
    return {"file": b, "runs": logical_runs("err|eq", n=3, agg="sum(z) AS s, max(z) AS m",
                                            agg_value={"s": 3 * v, "m": v}) + [
        run("sum-only", ["query", "{F}", "--aggregate", "sum(z) AS s"], E("err|eq", [{"s": 3 * v}])),
        run("arith", ["query", "{F}", "--select", "z + 1 AS y"], E("err|eq", [{"y": v + 1}] * 3)),
        run("filter", ["query", "{F}", "--filter", "z > 9999", "--aggregate", "count(*) AS n"],
            E("err|eq", [{"n": 3}]))]}


@case("logical_decimal_p2_on_int32_sum", B43)
def _():
    se = T.schema_elem("z", ty=T.INT32, rep=T.REQUIRED, converted=T.CT_DECIMAL, scale=0, precision=2)
    v = 2**31 - 1
    b = logical_file(se, T.plain_i32([v] * 64), 64, T.INT32)
    return {"file": b, "runs": [
        run("sum", ["query", "{F}", "--aggregate", "sum(z) AS s"], E("err|eq", [{"s": 64 * v}])),
        run("max", ["query", "{F}", "--aggregate", "max(z) AS m"], E("err|eq", [{"m": v}]))]}


@case("logical_decimal_scale_gt_precision", B43)
def _():
    se = T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED, converted=T.CT_DECIMAL, scale=10, precision=4)
    b = logical_file(se, T.plain_i64([12345]), 1, T.INT64)
    return {"file": b, "runs": logical_runs("err|eq", n=1, agg="sum(z) AS s", agg_value={"s": 1.2345e-06})}


@case("logical_decimal_negative_scale", B43, known="T13")
def _():
    se = T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED, converted=T.CT_DECIMAL, scale=-3, precision=10)
    b = logical_file(se, T.plain_i64([12345]), 1, T.INT64)
    return {"file": b, "runs": logical_runs("err", n=1, agg="sum(z) AS s")}


@case("logical_decimal_p1_on_bytearray_20_bytes", B43)
def _():
    se = T.schema_elem("z", ty=T.BYTE_ARRAY, rep=T.REQUIRED, converted=T.CT_DECIMAL, scale=0, precision=1)
    big = (10**40).to_bytes(20, "big", signed=True)
    b = logical_file(se, T.plain_ba([big, b"\x05"]), 2, T.BYTE_ARRAY)
    return {"file": b, "runs": logical_runs("err|eq", n=2, agg="sum(z) AS s", agg_value={"s": 10**40 + 5})}


@case("logical_int8_on_int64", B43)
def _():
    # INT(8, signed) on INT64 (invalid pairing) holding 1000: the INT32 range check does not apply.
    se = T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED)
    se.set(T.SE_LOGICAL, int_logical(8, True), T.T_STRUCT)
    b = logical_file(se, T.plain_i64([1000, -1000]), 2, T.INT64)
    return {"file": b, "runs": logical_runs("err|eq", rows=[{"z": 1000}, {"z": -1000}], n=2, agg="sum(z) AS s",
                                            agg_value={"s": 0})}


@case("logical_uint8_on_int32_out_of_range", B43)
def _():
    se = T.schema_elem("z", ty=T.INT32, rep=T.REQUIRED)
    se.set(T.SE_LOGICAL, int_logical(8, False), T.T_STRUCT)
    b = logical_file(se, T.plain_i32([255, 256, -1]), 3, T.INT32)
    return {"file": b, "runs": logical_runs("err", n=3, agg="sum(z) AS s")}


@case("logical_date_on_int64", B43)
def _():
    se = T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED, converted=6)  # DATE
    b = logical_file(se, T.plain_i64([2**40]), 1, T.INT64)
    return {"file": b, "runs": logical_runs("err|eq", n=1, rows=[{"z": 2**40}])}


@case("logical_timestamp_on_int32", B43)
def _():
    se = T.schema_elem("z", ty=T.INT32, rep=T.REQUIRED, converted=9)  # TIMESTAMP_MILLIS
    b = logical_file(se, T.plain_i32([5]), 1, T.INT32)
    return {"file": b, "runs": logical_runs("err|eq", n=1, rows=[{"z": 5}])}


@case("logical_string_on_flba", B43)
def _():
    se = T.schema_elem("z", ty=T.FLBA, rep=T.REQUIRED, type_length=3, converted=T.CT_UTF8)
    b = logical_file(se, b"abc\xff\xfe\xfd", 2, T.FLBA)
    return {"file": b, "runs": logical_runs("err|eq", n=2)}


@case("logical_timestamp_extremes", B43)
def _():
    # TIMESTAMP(MICROS, UTC) at INT64 extremes: text conversion must not overflow.
    se = T.schema_elem("z", ty=T.INT64, rep=T.REQUIRED)
    unit = T.Struct([[2, T.T_STRUCT, T.Struct()]])
    se.set(T.SE_LOGICAL, T.Struct([[8, T.T_STRUCT, T.Struct([[1, T.T_TRUE, True], [2, T.T_STRUCT, unit]])]]),
           T.T_STRUCT)
    b = logical_file(se, T.plain_i64([2**63 - 1, -(2**63)]), 2, T.INT64)
    return {"file": b, "runs": logical_runs("err|eq", n=2) + [
        run("csv", ["query", "{F}", "--format", "csv"], E("safe")),
        run("filter", ["query", "{F}", "--filter", "z > '2024-01-01'", "--aggregate", "count(*) AS n"],
            E("err|eq", [{"n": 1}]))]}
