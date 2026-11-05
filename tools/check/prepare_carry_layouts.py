"""Layouts a write's local read plan must cover exactly, from the cold-write review.

    prepare_carry_layouts.py PARTIAL LENGTHLESS GAPPED REFUSED

- PARTIAL: ci/fixtures/parquet/bloom_keys.parquet with every Bloom filter's optional length removed and the last row
  group's `id` filter placed 32 bytes before a page boundary past the last chunk, so its header shares a page with
  planned bytes while its bitset runs into pages nothing else plans.
- LENGTHLESS: the same filters, lengthless, all after 128 KiB of padding: no planned page holds any of them.
- REFUSED: LENGTHLESS with every filter header's first byte made an invalid field type: no reader keeps such a
  filter, and a plan cannot bound what a parse of it would read, so the write must read through the mapping.
- GAPPED: two Snappy chunks, no dictionaries, with 128 KiB of nonzero padding between them, which a whole-row-group
  copy writes verbatim.

Chunk bytes and filter bitsets are kept; only filter placement, padding and the footer change. The compact-Thrift
codec checks it reproduces each footer exactly before editing it.
"""
import struct
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "ci/fixtures/parquet/bloom_keys.parquet"
PAD = b"\xa5" * (128 * 1024)


def varint(n):
    out = bytearray()
    while n > 127:
        out.append((n & 127) | 128)
        n >>= 7
    out.append(n)
    return bytes(out)


def zz(n):
    return varint((n << 1) ^ (n >> 63))


class Reader:
    def __init__(self, data):
        self.data, self.pos = data, 0

    def take(self, n):
        b = self.data[self.pos:self.pos + n]
        assert len(b) == n
        self.pos += n
        return b

    def byte(self):
        return self.take(1)[0]

    def integer(self):
        n, shift = 0, 0
        while True:
            b = self.byte()
            n |= (b & 127) << shift
            if not b & 128:
                return n
            shift += 7

    def signed(self):
        n = self.integer()
        return (n >> 1) ^ -(n & 1)

    def value(self, t):
        if t in (1, 2):
            return t == 1
        if t == 3:
            return self.take(1)
        if t in (4, 5, 6):
            return self.signed()
        if t == 7:
            return self.take(8)
        if t == 8:
            return self.take(self.integer())
        if t in (9, 10):
            h = self.byte()
            n = h >> 4
            if n == 15:
                n = self.integer()
            return (h & 15, [self.value(h & 15) for _ in range(n)])
        if t == 12:
            fields, last = [], 0
            while True:
                h = self.byte()
                if not h:
                    return fields
                f = last + (h >> 4) if h >> 4 else self.signed()
                fields.append([f, h & 15, self.value(h & 15)])
                last = f
        raise ValueError(t)


def encode(t, v):
    if t in (1, 2):
        return b""
    if t in (3, 7):
        return v
    if t in (4, 5, 6):
        return zz(v)
    if t == 8:
        return varint(len(v)) + v
    if t in (9, 10):
        et, items = v
        n = len(items)
        head = bytes([(min(n, 15) << 4) | et]) + (varint(n) if n >= 15 else b"")
        return head + b"".join(encode(et, x) for x in items)
    if t == 12:
        out, last = bytearray(), 0
        for f, ft, fv in v:
            diff = f - last
            out.extend(bytes([(diff << 4) | ft]) if 0 < diff < 16 else bytes([ft]) + zz(f))
            out.extend(encode(ft, fv))
            last = f
        return bytes(out) + b"\0"
    raise ValueError(t)


def field(obj, ident):
    return next(f for f in obj if f[0] == ident)


def load(path):
    data = path.read_bytes()
    n = struct.unpack("<I", data[-8:-4])[0]
    pos = len(data) - 8 - n
    r = Reader(data[pos:-8])
    meta = r.value(12)
    assert r.pos == n
    assert encode(12, meta) == data[pos:-8]
    return data[:pos], meta


def save(path, data, meta):
    footer = encode(12, meta)
    Path(path).write_bytes(data + footer + struct.pack("<I", len(footer)) + b"PAR1")


def chunks(meta):
    return [c for rg in field(meta, 4)[2][1] for c in field(rg, 1)[2][1]]


def place_filters(data, original, order):
    """Append each filtered chunk's filter to `data` in `order`, dropping its length; the filters' bytes as before."""
    for c in order:
        cm = field(c, 3)[2]
        if not any(f[0] == 14 for f in cm):
            continue
        off, length = field(cm, 14), field(cm, 15)
        payload = original[off[2]:off[2] + length[2]]
        off[2] = len(data)
        data += payload
        cm.remove(length)
    return data


def partial(path):
    original, meta = load(SOURCE)
    all_chunks = chunks(meta)
    last = field(all_chunks[-1], 3)[2]
    end = field(last, 9)[2] + field(last, 7)[2]
    start = ((end + 4095) // 4096) * 4096 - 32
    assert start >= end
    target = all_chunks[-4]  # the last row group's `id`
    data = place_filters(original[:end] + b"\xa5" * (start - end), original,
                         [target] + [c for c in all_chunks if c is not target])
    save(path, data, meta)


def lengthless(path):
    original, meta = load(SOURCE)
    save(path, place_filters(original + PAD, original, chunks(meta)), meta)


def refused(path):
    original, meta = load(SOURCE)
    data = bytearray(place_filters(original + PAD, original, chunks(meta)))
    for c in chunks(meta):
        cm = field(c, 3)[2]
        if any(f[0] == 14 for f in cm):
            data[field(cm, 14)[2]] = 0x1F  # field delta 1, type 15: no compact-Thrift type
    save(path, bytes(data), meta)


def gapped(path):
    tmp = Path(path).with_suffix(".src")
    pq.write_table(pa.table({"a": list(range(1000)), "b": list(range(1000, 2000))}), tmp,
                   use_dictionary=False, compression="snappy")
    data, meta = load(tmp)
    tmp.unlink()
    second = field(chunks(meta)[1], 3)[2]
    cut = field(second, 9)[2]
    for f in second:
        if f[0] in (9, 10, 11, 14) and f[2] >= cut:
            f[2] += len(PAD)
    save(path, data[:cut] + PAD + data[cut:], meta)


def main():
    partial(sys.argv[1])
    lengthless(sys.argv[2])
    gapped(sys.argv[3])
    refused(sys.argv[4])


if __name__ == "__main__":
    main()
