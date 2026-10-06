"""An independent split-block Bloom filter reader, written from parquet-format's BloomFilter.md alone.

Pure Python (XXH64 included), so CI needs nothing beyond pyarrow. It cross-checks both what writers hash and what zpq
reads: a chunk's filter must test every one of the chunk's stored values present.
"""
import struct

SALT = (0x47B6137B, 0x44974D91, 0x8824AD5B, 0xA2B7289D, 0x705495C7, 0x2DF1424B, 0x9EFC4947, 0x5C6BFB31)
M64 = (1 << 64) - 1
P1, P2, P3, P4, P5 = (0x9E3779B185EBCA87, 0xC2B2AE3D27D4EB4F, 0x165667B19E3779F9, 0x85EBCA77C2B2AE63,
                      0x27D4EB2F165667C5)


def _rotl(x, r):
    return ((x << r) | (x >> (64 - r))) & M64


def _round(acc, lane):
    return (_rotl((acc + lane * P2) & M64, 31) * P1) & M64


def _merge(acc, v):
    return ((acc ^ _round(0, v)) * P1 + P4) & M64


def xxh64(data: bytes, seed: int = 0) -> int:
    """XXH64, per the reference algorithm."""
    n = len(data)
    i = 0
    if n >= 32:
        v = [(seed + P1 + P2) & M64, (seed + P2) & M64, seed, (seed - P1) & M64]
        while i + 32 <= n:
            for k in range(4):
                v[k] = _round(v[k], struct.unpack_from("<Q", data, i + 8 * k)[0])
            i += 32
        h = (_rotl(v[0], 1) + _rotl(v[1], 7) + _rotl(v[2], 12) + _rotl(v[3], 18)) & M64
        for k in range(4):
            h = _merge(h, v[k])
    else:
        h = (seed + P5) & M64
    h = (h + n) & M64
    while i + 8 <= n:
        h = (_rotl(h ^ _round(0, struct.unpack_from("<Q", data, i)[0]), 27) * P1 + P4) & M64
        i += 8
    if i + 4 <= n:
        h = (_rotl(h ^ (struct.unpack_from("<I", data, i)[0] * P1 & M64), 23) * P2 + P3) & M64
        i += 4
    while i < n:
        h = (_rotl(h ^ (data[i] * P5 & M64), 11) * P1) & M64
        i += 1
    h ^= h >> 33
    h = (h * P2) & M64
    h ^= h >> 29
    h = (h * P3) & M64
    h ^= h >> 32
    return h


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


def check(bits, h):
    """The spec's block_check."""
    z = len(bits) // 32
    i = ((h >> 32) * z) >> 32
    x = h & 0xFFFFFFFF
    for k in range(8):
        w = struct.unpack_from("<I", bits, i * 32 + 4 * k)[0]
        if not (w >> (((x * SALT[k]) & 0xFFFFFFFF) >> 27)) & 1:
            return False
    return True


def plain(physical, v):
    """The PLAIN bytes a writer hashes for a stored value (BYTE_ARRAY without its length prefix)."""
    if physical == "INT32":
        return struct.pack("<I", v & 0xFFFFFFFF)
    if physical == "INT64":
        return struct.pack("<Q", v & M64)
    if physical == "FLOAT":
        return struct.pack("<f", v)
    if physical == "DOUBLE":
        return struct.pack("<d", v)
    if physical in ("BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"):
        return v if isinstance(v, bytes) else v.encode()
    raise ValueError(physical)


assert xxh64(b"") == 0xEF46DB3751D8E999
assert xxh64(b"parquet") == 0x3C9D29275C52E429
assert xxh64(b"0123456789abcdef0123456789abcdef!") == 0x8AFFF4DAAC4E677E
