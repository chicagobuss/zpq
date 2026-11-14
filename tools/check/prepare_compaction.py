"""Small real Parquet inputs for coalescing's ownership, DECIMAL and empty-chunk regressions."""
import struct
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from prepare_carry_layouts import load, save, field, chunks, encode


def set_field(obj, ident, kind, value):
    obj[:] = [f for f in obj if f[0] != ident]
    obj.append([ident, kind, value])
    obj.sort(key=lambda f: f[0])


def prepare(paths):
    decimal, mixed_a, mixed_b, empty, nan, short = map(Path, paths[:6])
    pq.write_table(pa.table({'d': [b'\xf6', b'\xff\x00', b'\x64']}), decimal,
                   compression='snappy', row_group_size=1, use_dictionary=False, write_page_index=True)
    data, meta = load(decimal)
    leaf = field(meta, 2)[2][1][1]
    set_field(leaf, 6, 5, 5)  # ConvertedType.DECIMAL
    set_field(leaf, 7, 5, 0)
    set_field(leaf, 8, 5, 9)
    set_field(leaf, 10, 12, [[5, 12, [[1, 5, 0], [2, 5, 9]]]])
    meta[:] = [f for f in meta if f[0] != 5]  # The embedded Arrow binary schema is no longer applicable.
    save(decimal, data, meta)
    if len(paths) > 6:
        released = Path(paths[6])
        pq.write_table(pa.table({'d': [b'\x01', b'\x01\x00', b'\x7f']}), released,
                       compression='snappy', use_dictionary=False, write_page_index=True)
        data, old_meta = load(released)
        field(old_meta, 2)[2][1][1] = leaf
        old_meta[:] = [f for f in old_meta if f[0] != 5]
        set_field(old_meta, 6, 8, b'zpq version 0.5.2')
        save(released, data, old_meta)
    for path, codec, start in ((mixed_a, 'NONE', 0), (mixed_b, 'snappy', 1000)):
        pq.write_table(pa.table({'a': range(start, start+1000), 'b': range(start, start+1000)}), path,
                       compression=codec, use_dictionary=False, write_page_index=True)
    pq.write_table(pa.table({'a': pa.array([], type=pa.int64()), 'b': pa.array([], type=pa.int64())}), empty)
    data, meta = load(empty)
    for chunk in chunks(meta):
        cm = field(chunk, 3)[2]
        cm[:] = [f for f in cm if f[0] not in (11, 12)]
        for ident in (6, 7):set_field(cm, ident, 6, 0)
        set_field(cm, 9, 6, 4)
    for rg in field(meta, 4)[2][1]:set_field(rg, 2, 6, 0)
    save(empty, b'PAR1', meta)
    for path, short_bounds in ((nan, False), (short, True)):
        pq.write_table(pa.table({'x': pa.array([float('nan'), float('nan')], type=pa.float64())}), path,
                       row_group_size=1, compression='snappy', use_dictionary=False, write_page_index=True)
        data, meta = load(path)
        data = bytearray(data)
        for chunk in chunks(meta):
            cm = field(chunk, 3)[2]
            if short_bounds:
                cm[:] = [f for f in cm if f[0] != 12]
                ci = encode(12, [[1, 9, (1, [False])], [2, 9, (8, [b''])],
                                 [3, 9, (8, [b''])], [4, 5, 1], [5, 9, (6, [0])]])
                set_field(chunk, 6, 6, len(data)); set_field(chunk, 7, 5, len(ci)); data.extend(ci)
            else:
                value = struct.pack('<d', float('nan'))
                set_field(cm, 12, 12, [[3, 6, 0], [5, 8, value], [6, 8, value]])
        save(path, bytes(data), meta)


if __name__ == '__main__':
    prepare(sys.argv[1:])
