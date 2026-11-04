"""A file whose later filter conjunct's page over rows [40000, 80000) is corrupt, and those rows are filtered out.

`p` is PLAIN, so the whole-row-group decode reads it after `sd >= '1994-01-01'`, gated by that conjunct's
survivors, and never opens the page. Shipdate on that span is 1993-06-01. The page's definition-level length is
replaced, as in prepare_q6gate.py. Every decode path, sliced or whole, must return the kept rows.

Arguments: the plain file, then the same rows (five times as many) with an offset index and a five-value string
key, so four or more workers split its one row group into row subgroups for a hash GROUP BY.
"""
import datetime
import decimal
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def varint(data, pos):
    result = shift = 0
    while True:
        b = data[pos]
        pos += 1
        result |= (b & 127) << shift
        if b < 128:
            return result, pos
        shift += 7


def fields(data, pos):
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
            v, pos = varint(data, pos)
            fid = (v >> 1) ^ -(v & 1)
        last = fid
        if kind in (1, 2):
            result[fid] = kind == 1
        elif kind in (4, 5, 6):
            v, pos = varint(data, pos)
            result[fid] = (v >> 1) ^ -(v & 1)
        elif kind == 12:
            result[fid], pos = fields(data, pos)
        elif kind == 8:
            s, pos = varint(data, pos)
            pos += s
        elif kind == 3:
            pos += 1
        elif kind == 7:
            pos += 8
        else:
            raise AssertionError(kind)


def pages(data, meta, rg, ci):
    col = meta.row_group(rg).column(ci)
    pos = col.data_page_offset
    end = (col.dictionary_page_offset or col.data_page_offset) + col.total_compressed_size
    row = 0
    out = []
    while pos < end:
        h, body = fields(data, pos)
        nv = h[5][1] if 5 in h else h[8][1]
        out.append((body, h, row, nv))
        row += nv
        pos = body + h[3]
    return out


def write(out, indexed):
    # The indexed file is large enough, and grouped finely enough, for four workers to split its one row group.
    n = 1_000_000 if indexed else 200_000
    D = decimal.Decimal
    sd = [datetime.date(1993, 6, 1) if 40000 <= i < 80000 else datetime.date(1994, 6, 1) for i in range(n)]
    d = [D(i % 11) / 100 for i in range(n)]
    p = [None if i % 7 == 3 else (D(90091 + (i * 7919) % 10404859) / 100).quantize(D("0.01")) for i in range(n)]
    f = [float(i % 101) for i in range(n)]
    dt = pa.decimal128(15, 2)
    cols = {
        "sd": pa.array(sd, type=pa.date32()),
        "d": pa.array(d, type=dt),
        "p": pa.array(p, type=dt),
        "f": pa.array(f, type=pa.float64()),
    }
    fields = [
        pa.field("sd", pa.date32(), nullable=False),
        pa.field("d", dt, nullable=False),
        pa.field("p", dt),
        pa.field("f", pa.float64(), nullable=False),
    ]
    if indexed:
        cols["g"] = pa.array([(i * 7919) % 100_003 for i in range(n)], type=pa.int64())
        fields.append(pa.field("g", pa.int64(), nullable=False))
        cols["k"] = pa.array([f"k{i % 5}" for i in range(n)], type=pa.string())
        fields.append(pa.field("k", pa.string(), nullable=False))
    pq.write_table(
        pa.table(cols, schema=pa.schema(fields)),
        out,
        row_group_size=n,
        use_dictionary=["d", "sd"],
        compression=None,
        data_page_size=64 * 1024,
        data_page_version="1.0",
        store_decimal_as_integer=True,
        write_statistics=False,
        write_page_index=indexed,
    )
    orig = out.read_bytes()
    meta = pq.ParquetFile(out).metadata
    pp = pages(orig, meta, 0, 2)
    k = next(i for i, x in enumerate(pp) if x[2] >= 40000 and x[2] + x[3] <= 80000)
    data = bytearray(orig)
    data[pp[k][0] : pp[k][0] + 4] = b"\xff\xff\xff\xff"
    out.write_bytes(data)


def main():
    for out, indexed in zip(sys.argv[1:], (False, True)):
        write(pathlib.Path(out), indexed)


if __name__ == "__main__":
    main()
