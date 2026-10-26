"""decode_errors: corrupt multi-column, multi-page V1/V2 files whose failure every execution mode must report alike.

Each file damages chosen pages after PyArrow writes it: a level stream no RLE run can hold, or (late-value) an INT8
value out of range. tools/check/cases/decode_errors.json declares the error each must give, byte for byte the same
at every worker count, budget, decode window and I/O mode.
"""
import struct

import pyarrow as pa
import pyarrow.parquet as pq


def varint(data, pos):
    result = shift = 0
    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 127) << shift
        if byte < 128:
            return result, pos
        shift += 7


def fields(data, pos, spans=None, prefix=()):
    """A Thrift compact struct at `pos`: {field id: value}, with integer spans recorded when asked."""
    result = {}
    last = 0
    while True:
        head = data[pos]
        pos += 1
        if head == 0:
            return result, pos
        kind = head & 15
        delta = head >> 4
        if delta:
            fid = last + delta
        else:
            value, pos = varint(data, pos)
            fid = (value >> 1) ^ -(value & 1)
        last = fid
        if kind in (1, 2):
            result[fid] = kind == 1
        elif kind in (4, 5, 6):
            start = pos
            value, pos = varint(data, pos)
            result[fid] = (value >> 1) ^ -(value & 1)
            if spans is not None:
                spans[prefix + (fid,)] = (start, pos)
        elif kind == 12:
            result[fid], pos = fields(data, pos, spans, prefix + (fid,))
        elif kind == 8:
            size, pos = varint(data, pos)
            pos += size
        elif kind == 3:
            pos += 1
        elif kind == 7:
            pos += 8
        else:
            raise AssertionError(f"unexpected fixture field type {kind}")


def corrupt(data, meta, version, rg, ci, page, mode="levels"):
    pos = meta.row_group(rg).column(ci).data_page_offset
    for pi in range(page + 1):
        header, body = fields(data, pos)
        if pi == page:
            if mode == "levels":
                if version == "1.0":
                    data[body:body + 4] = b"\xff\xff\xff\xff"
                else:
                    # A level-1 stream cannot hold the RLE value 2. Validation must precede values.
                    data[body:body + 2] = b"\x02\x02"
            else:
                if version == "1.0":
                    value_start = body + 4 + struct.unpack_from("<I", data, body)[0]
                else:
                    value_start = body + header[8][5] + header[8][6]
                # INT8 annotation on physical INT32: a late value in the first page is out of range.
                struct.pack_into("<i", data, value_start + 110 * 4, 9999)
        pos = body + header[3]


CASES = {
    "columns": [(1, 0, 3), (1, 1, 1), (1, 2, 3)],
    "ties": [(1, 0, 0), (1, 1, 0)],
    "row-groups": [(0, 0, 3), (1, 1, 0), (2, 2, 0)],
    "late-value": [(1, 0, 0, "value"), (1, 1, 0)],
}


def decode_errors(out_dir):
    n = 65553 * 3
    table = pa.table({
        "a": pa.array([None if i % 11 == 0 else i % 71 for i in range(n)], type=pa.int8()),
        "b": pa.array([None if i % 13 == 0 else i % 503 for i in range(n)], type=pa.int32()),
        "c": pa.array([None if i % 17 == 0 else i * 0.01 for i in range(n)], type=pa.float64()),
        "g": ["abcd"[i % 4] for i in range(n)],
    })
    for version in ["1.0", "2.0"]:
        v = version[0]
        path = out_dir / f"v{v}-base.parquet"
        pq.write_table(table, path, data_page_version=version, row_group_size=65553, data_page_size=512,
                       write_batch_size=128, use_dictionary=["g"], compression=None, write_statistics=False)
        original = path.read_bytes()
        meta = pq.ParquetFile(path).metadata
        for label, changes in CASES.items():
            data = bytearray(original)
            for change in changes:
                corrupt(data, meta, version, *change)
            (out_dir / f"v{v}-{label}.parquet").write_bytes(data)
        # File order precedes the row-group index within either file.
        for name, change in [("a", (2, 0, 3)), ("b", (0, 1, 0))]:
            data = bytearray(original)
            corrupt(data, meta, version, *change)
            (out_dir / f"v{v}-parts-{name}.parquet").write_bytes(data)
        path.unlink()
        # A REQUIRED page overclaim used to disappear when sliced execution stopped at footer.num_values.
        required = pa.table({"a": list(range(128)), "b": list(range(128))}, schema=pa.schema([
            pa.field("a", pa.int32(), nullable=False), pa.field("b", pa.int32(), nullable=False),
        ]))
        path = out_dir / f"v{v}-required.parquet"
        pq.write_table(required, path, data_page_version=version, compression=None, use_dictionary=False,
                       write_statistics=False)
        md = pq.ParquetFile(path).metadata
        data = bytearray(path.read_bytes())
        spans = {}
        fields(data, md.row_group(0).column(0).data_page_offset, spans)
        start, end = spans[(5 if version == "1.0" else 8, 1)]
        assert end - start == 2
        data[start:end] = b"\x80\x04"  # zigzag(256), from the original 128; the footer still says 128.
        path.write_bytes(data)
        # Tight budgets must keep corruption diagnostics, reader-backed inputs included.
        tight = pa.table({"a": pa.array(range(4096), type=pa.int32()), "b": pa.array(range(4096), type=pa.int32())})
        path = out_dir / f"v{v}-tight.parquet"
        pq.write_table(tight, path, row_group_size=1024, data_page_size=128, write_batch_size=64,
                       use_dictionary=False, compression=None, data_page_version=version)
        meta = pq.ParquetFile(path).metadata
        data = bytearray(path.read_bytes())
        corrupt(data, meta, version, 0, 0, 3)
        corrupt(data, meta, version, 0, 1, 0)
        path.write_bytes(data)
        # A compressed byte dictionary survives all its data pages while every drained page is freed.
        dictionary = pa.table({"g": [("group-" + str(i % 16) + "x" * 128) for i in range(4096)],
                               "a": pa.array(range(4096), type=pa.int32())})
        path = out_dir / f"v{v}-dictionary.parquet"
        pq.write_table(dictionary, path, row_group_size=1024, data_page_size=128, write_batch_size=64,
                       use_dictionary=["g"], compression={"g": "zstd", "a": "NONE"}, data_page_version=version)
        meta = pq.ParquetFile(path).metadata
        data = bytearray(path.read_bytes())
        corrupt(data, meta, version, 0, 1, 3)
        path.write_bytes(data)
