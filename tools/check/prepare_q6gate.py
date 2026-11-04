"""A q6-shaped file whose payload page over rows [40000, 60000) is filtered out and corrupt.

The page's definition-level length is replaced, the same way tools/datagen and the
q6-gate review fixture do. Shipdate on that span is 1993-06-01, so
`sd >= '1994-01-01'` keeps none of it. Every decode path must return the kept rows.
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


def main():
    out = pathlib.Path(sys.argv[1])
    n = 200_000
    D = decimal.Decimal
    sd = [datetime.date(1993, 6, 1) if 40000 <= i < 60000 else datetime.date(1994, 6, 1) for i in range(n)]
    d = [D("0.05")] * n
    q = [D(i % 30) for i in range(n)]
    p = [None if i % 7 == 3 else (D(90091 + (i * 7919) % 10404859) / 100).quantize(D("0.01")) for i in range(n)]
    dt = pa.decimal128(15, 2)
    t = pa.table(
        {
            "sd": pa.array(sd, type=pa.date32()),
            "d": pa.array(d, type=dt),
            "q": pa.array(q, type=dt),
            "p": pa.array(p, type=dt),
        },
        schema=pa.schema(
            [
                pa.field("sd", pa.date32(), nullable=False),
                pa.field("d", dt, nullable=False),
                pa.field("q", dt, nullable=False),
                pa.field("p", dt),
            ]
        ),
    )
    pq.write_table(
        t,
        out,
        row_group_size=n,
        use_dictionary=["d", "q", "sd"],
        compression=None,
        data_page_version="1.0",
        store_decimal_as_integer=True,
        write_statistics=False,
    )
    orig = out.read_bytes()
    meta = pq.ParquetFile(out).metadata
    pp = pages(orig, meta, 0, 3)
    k = next(i for i, x in enumerate(pp) if x[2] == 40000)
    data = bytearray(orig)
    data[pp[k][0] : pp[k][0] + 4] = b"\xff\xff\xff\xff"
    out.write_bytes(data)


if __name__ == "__main__":
    main()
