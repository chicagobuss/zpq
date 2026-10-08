"""Dump every page body of a Parquet file (its compact-thrift page headers walked by hand) as page-NNNN.gz, for compare.c.
Usage: extract_pages.py FILE.parquet OUTDIR"""
import sys, struct, os
import pyarrow.parquet as pq
def varint(b, i):
    r = s = 0
    while True:
        x = b[i]; i += 1; r |= (x & 0x7f) << s; s += 7
        if x < 0x80: return r, i
def zz(n): return (n >> 1) ^ -(n & 1)
def skip(b, i, t):
    if t in (1, 2): return i            # bool true/false
    if t == 3: return i + 1             # byte
    if t in (4, 5, 6): return varint(b, i)[1]
    if t == 7: return i + 8             # double
    if t == 8: n, i = varint(b, i); return i + n
    if t in (9, 10):
        h = b[i]; i += 1; n = h >> 4; et = h & 0xf
        if n == 15: n, i = varint(b, i)
        for _ in range(n): i = skip(b, i, et)
        return i
    if t == 12: return struct_(b, i)[1]
    raise ValueError(t)
def struct_(b, i):
    fields = {}; last = 0
    while True:
        h = b[i]; i += 1
        if h == 0: return fields, i
        t = h & 0xf; d = h >> 4
        if d: fid = last + d
        else: fid, i = varint(b, i); fid = zz(fid)
        last = fid
        if t in (5, 6) and fid in (1, 2, 3):
            v, i = varint(b, i); fields[fid] = zz(v)
        else:
            i = skip(b, i, t)
def main(path, out):
    data = open(path, 'rb').read(); m = pq.ParquetFile(path).metadata; n = 0
    for g in range(m.num_row_groups):
        for c in range(m.num_columns):
            col = m.row_group(g).column(c)
            start = col.dictionary_page_offset or col.data_page_offset
            if col.dictionary_page_offset is not None and col.dictionary_page_offset > 0:
                start = min(col.dictionary_page_offset, col.data_page_offset)
            end = start + col.total_compressed_size; i = start
            while i < end:
                f, j = struct_(data, i); body = data[j:j + f[3]]
                open(f'{out}/page-{n:04d}.gz', 'wb').write(body); n += 1; i = j + f[3]
    print(n, 'pages')
main(sys.argv[1], sys.argv[2])
