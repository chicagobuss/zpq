"""Fixtures for the CLI contract cases (tools/check/cases/cli_*.json). Each recipe writes the bytes the contract
script it replaced wrote, so the cases' goldens pin outputs those scripts' assertions accepted."""
import datetime as dt
import decimal
import random
import struct
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[2]

# (column, precision, scale): s = 0 and s = p - 1 at p = 34, 35 and 38; DECIMAL(36,35), whose mean of values >= 1 the
# (p + 4, s + 4) rule capped at 38 could not hold; and a narrow everyday type.
DECIMALS = [("d9_2", 9, 2), ("d34_0", 34, 0), ("d34_33", 34, 33), ("d35_0", 35, 0), ("d35_34", 35, 34),
            ("d36_35", 36, 35), ("d38_0", 38, 0), ("d38_37", 38, 37), ("d38_38", 38, 38)]
NUMERIC = [("i32", pa.int32()), ("i64", pa.int64()), ("u64", pa.uint64()), ("f32", pa.float32()),
           ("f64", pa.float64())]
KEYS = ["b", "a", "z", "c"]  # first-appearance order; every value in group "z" is NULL



def avg_types(out_dir):
    """avg_types.parquet and avg_wide.parquet: every avg input type, NULL groups, and a DECIMAL(38, 0) mean whose sum
    passes 38 digits (tools/check/cases/cli_avg.json)."""
    with decimal.localcontext() as context:
        # 38-digit unscaled values: the default 28-digit context would round them.
        context.prec = 120
        _avg_types(out_dir)


def _avg_types(tmp):
    r = random.Random(0xA5C0)
    n = 96
    keys = [KEYS[i % len(KEYS)] for i in range(n)]
    cols = {"k": pa.array(keys, pa.string())}
    for name, ty in NUMERIC:
        vals = []
        for i, k in enumerate(keys):
            if k == "z" or i % 7 == 3:
                vals.append(None)
            elif name == "i32":
                vals.append(r.randint(-2**31, 2**31 - 1))
            elif name == "i64":
                vals.append(r.randint(-2**62, 2**62))
            elif name == "u64":
                vals.append(r.randint(2**63, 2**64 - 1) if i % 2 else r.randint(0, 2**40))
            elif name == "f32":
                vals.append(struct.unpack("<f", struct.pack("<f", r.uniform(-1e6, 1e6)))[0])
            else:
                vals.append(r.uniform(-1e3, 1e3) * 10 ** r.randint(-3, 12))
        cols[name] = pa.array(vals, ty)
    for name, p, s in DECIMALS:
        # Unscaled values under 10^min(p, 36): 96 of them sum under 10^38, so avg_state's DECIMAL(38, s) column holds
        # every sum. DECIMAL(36, 35) values are >= 1, so every mean is too.
        top = 10 ** min(p, 36) - 1
        vals = []
        for i, k in enumerate(keys):
            if k == "z" or i % 5 == 2:
                vals.append(None)
                continue
            u = r.randint(10 ** 35, top) if name == "d36_35" else r.randint(-top, top)
            vals.append(Decimal(u).scaleb(-s))
        cols[name] = pa.array(vals, pa.decimal128(p, s))
    cols["nulls"] = pa.array([None] * n, pa.int64())
    main = tmp / "avg_types.parquet"
    pq.write_table(pa.table(cols), main, row_group_size=8)
    # Values near 10^37 and 10^38 - 1 in DECIMAL(38, 0): their sum passes 38 digits, their mean does not.
    wide = tmp / "avg_wide.parquet"
    big = [10 ** 38 - 1, 10 ** 38 - 3, 10 ** 37 + 5, 10 ** 38 - 2]
    pq.write_table(pa.table({"w": pa.array([Decimal(v) for v in big], pa.decimal128(38, 0))}), wide, row_group_size=1)


def rewrite_footer(src, dst, old, new):
    """`src` with one byte sequence of its footer replaced: another writer's spelling of the same schema."""
    b = src.read_bytes()
    n = struct.unpack('<I', b[-8:-4])[0]
    footer = b[-8 - n:-8]
    assert footer.count(old) == 1, footer.hex()
    footer = footer.replace(old, new)
    dst.write_bytes(b[:-8 - n] + footer + struct.pack('<I', len(footer)) + b'PAR1')


def formats(out_dir):
    """Byte values, writers that spell one annotation differently, unsigned and nested columns
    (tools/check/cases/cli_formats.json)."""
    # Distinct values that U+FFFD replacement printed alike; text that spells the base64 of another value; a 3n+1
    # length for padding, and one past a 3072-byte chunk.
    pq.write_table(pa.table({'v': [b'\xff', b'\xfe', b'ASCII', b'', None, b'\x00', b'\xff', b'/w==', 'é'.encode(),
                                   bytes(range(256)) * 13]}), out_dir / 'blob.parquet')
    pq.write_table(pa.table({'v': pa.array([b'\xff' * 16, b'\xfe' * 16, None, bytes(range(16))],
                                           pa.binary(16)).cast(pa.uuid())}), out_dir / 'uuid.parquet')
    pq.write_table(pa.table({'v': pa.array([b'\xff\x00', b'\xfe\x00', None], pa.binary(2))}), out_dir / 'flba.parquet')
    bad = pa.Array.from_buffers(pa.string(), 2, [None, pa.py_buffer(struct.pack('<iii', 0, 1, 3)),
                                                 pa.py_buffer(b'\xff\xfe\x80')])
    pq.write_table(pa.table({'bad': bad}), out_dir / 'invalid_string.parquet')
    # Writers that spell one annotation differently: they must read together.
    ts = out_dir / 'ts.parquet'
    pq.write_table(pa.table({'v': pa.array([1, 2], pa.timestamp('ns'))}), ts)
    rewrite_footer(ts, out_dir / 'ts_reordered.parquet', bytes.fromhex('6c8c121c3c00000000'),
                   bytes.fromhex('6c8c2c3c000002020000'))  # TimestampType's fields in the other order
    text = out_dir / 'text.parquet'
    pq.write_table(pa.table({'v': ['abc', 'def']}), text)
    rewrite_footer(text, out_dir / 'text_long.parquet', b'\x4c\x1c\x00\x00', b'\x4c\x0c\x02\x00\x00')
    rewrite_footer(text, out_dir / 'text_legacy.parquet', b'\x4c\x1c\x00\x00', b'')  # only the legacy UTF8
    # One future logical type arm over different legacy fallbacks: text in one file, bytes in the other.
    rewrite_footer(text, out_dir / 'future_text.parquet', bytes.fromhex('4c1c0000'), bytes.fromhex('4c0cc8010000'))
    raw = out_dir / 'raw.parquet'
    pq.write_table(pa.table({'v': [b'\xff', b'\xfe']}), raw, store_schema=False)
    rewrite_footer(raw, out_dir / 'future_bytes.parquet', b'\x18\x01v\x00',
                   b'\x18\x01v' + bytes.fromhex('6c0cc8010000') + b'\x00')
    raw.unlink()
    # Genuinely different annotations.
    pq.write_table(pa.table({'v': pa.array([1, 2], pa.timestamp('us'))}), out_dir / 'ts_micros.parquet')
    pq.write_table(pa.table({'v': [b'abc', b'def']}), out_dir / 'blob_text.parquet')
    pq.write_table(pa.table({'flba': pa.array([Decimal('1.50'), None, Decimal('-2.25')], pa.decimal128(25, 2))}),
                   out_dir / 'answer_flba.parquet', store_decimal_as_integer=False)
    top = {8: 255, 16: 65535, 32: (1 << 32) - 1, 64: (1 << 64) - 1}
    pq.write_table(pa.table({**{f'u{w}': pa.array([1, top[w], None, 1 << (w - 1)], getattr(pa, f'uint{w}')())
                                for w in top}, 'k': pa.array([1, 1, 2, 2], pa.int32())}), out_dir / 'unsigned.parquet')
    pq.write_table(pa.table({
        'dt': pa.array([1, None, -1], pa.date32()), 'i8': pa.array([-5, None, 7], pa.int8()),
        'i16': pa.array([-300, 2, None], pa.int16()), 'u8': pa.array([250, None, 1], pa.uint8()),
        'u32': pa.array([(1 << 32) - 1, 2, None], pa.uint32()), 'u64': pa.array([(1 << 64) - 1, None, 3], pa.uint64()),
        'f': pa.array([1.5, None, -2.25], pa.float32()), 'h': pa.array([1.5, None, -2.0], pa.float16()),
        'b': pa.array([True, None, False], pa.bool_()), 's': pa.array(['x', None, 'z']),
        'ts': pa.array([1, None, 3], pa.timestamp('ms')), 'tm': pa.array([1000, None, 3], pa.time32('ms')),
        'd': pa.array([Decimal('1.50'), None, Decimal('-2.25')], pa.decimal128(9, 2)),
        'n': pa.array([1, 2, 3], pa.int32()),
        'st': pa.array([{'a': 1, 'b': None}, None, {'a': None, 'b': 'q'}],
                       pa.struct([pa.field('a', pa.int16()), pa.field('b', pa.string())])),
    }), out_dir / 'alias.parquet')
    pq.write_table(pa.table({'id': [1, 2, 3], 'st': [{'a': 1, 'b': 'x'}, None, {'a': 3, 'b': None}],
                             'l': [[1, 2], [], None]}), out_dir / 'nested_by_name.parquet')
    pq.write_table(pa.table({'k': ['a', 'b', 'a']}), out_dir / 'alias_spaces.parquet')


def contract(out_dir):
    """The CLI channel and destination fixtures (tools/check/cases/cli_contract.json)."""
    import datetime

    src = out_dir / 'input.parquet'
    pq.write_table(pa.table({'k': [1, 2, 1, 3], 'v': [3, None, 5, 7], 's': ['a', 'b', None, 'd']}), src,
                   row_group_size=2)
    n = 600_003
    pq.write_table(pa.table({'k': range(n), 'v': [i * .03125 for i in range(n)],
                             's': [f'name-{i % 997}' for i in range(n)]}), out_dir / 'parallel.parquet',
                   row_group_size=16_384)
    # LIMIT 0 must bind against real footers while avoiding corrupt data pages.
    b = bytearray(src.read_bytes())
    footer = len(b) - 8 - struct.unpack_from('<I', b, len(b) - 8)[0]
    b[4:footer] = bytes(footer - 4)
    (out_dir / 'poisoned.parquet').write_bytes(b)
    pq.write_table(pa.table({
        'wide': pa.array([2**63 - 1, 2**63 - 2, None], pa.int64()),
        'd': pa.array([Decimal('1.10'), None, Decimal('-2.20')], pa.decimal128(12, 2)),
        'big': pa.array([Decimal('12345678901234567890.123456789'), None, Decimal('0.000000001')],
                        pa.decimal128(38, 9)),
        'h': pa.array([1.5, None, -2.0], pa.float16()),
    }), out_dir / 'typed.parquet', row_group_size=2)
    pq.write_table(pa.table({'a': pa.array([{'v': 1}, None, {'v': 3}], pa.struct([('v', pa.int64())])),
                             'b': pa.array([{'v': 10}, {'v': None}, {'v': 30}], pa.struct([('v', pa.int64())]))}),
                   out_dir / 'nested.parquet')
    pq.write_table(pa.table({'t': pa.array([datetime.datetime(2020, 1, 1), None, datetime.datetime(1999, 12, 31)],
                                         pa.timestamp('ns'))}), out_dir / 'int96.parquet',
                   use_deprecated_int96_timestamps=True)
    pq.write_table(pa.table({'id': [1, 2, 3], 'b': [True, False, True],
                             'u': pa.array([1, 2**64 - 1, 2**63], pa.uint64()),
                             'st': [{'a': 1}, {'a': 2}, {'a': 3}], 'l': [[1, 2], None, []]}),
                   out_dir / 'contracts.parquet')


# ---- Bloom filters (tools/check/cases/bloom.json): seeded tables of every bloomable type

EPOCH = __import__("datetime").datetime(2020, 1, 1)
BLOOM_SEED = 20261006
BLOOM_ROWS, BLOOM_RG = 12_000, 2000


def alnum(rnd, k):
    return "".join(rnd.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(k))


# name -> (arrow type, value for row key i, zpq literal for a value). Keys are distinct per row unless the column is
# low-cardinality, and are shuffled across row groups so statistics cannot prune equality.
def column_kinds(rnd):
    def ts_lit(unit):
        def f(v):
            if unit == "ns":
                return str(v)  # bare ticks: the binder's back-compat form
            return "'" + v.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
        return f

    return {
        "i8": (pa.int8(), lambda i: (i % 251) - 125, str),
        "i32": (pa.int32(), lambda i: i * 7919 - 400_000_000, str),
        "i64": (pa.int64(), lambda i: i * 1_000_000_007 - 2**52, str),
        "u16": (pa.uint16(), lambda i: (i * 7) % 65536, str),
        "u32": (pa.uint32(), lambda i: (i * 45007) % 2**32, str),
        "u64": (pa.uint64(), lambda i: 2**63 + i * 3 if i % 2 else i, str),
        "f32": (pa.float32(), lambda i: -0.0 if i == 7 else ((i % 4001) - 2000) / 4.0, repr),
        "f64": (pa.float64(), lambda i: (i * 37 % 100003) / 8.0 - 3000.0, repr),
        "s": (pa.string(), lambda i: f"k{i:07d}{alnum(rnd, 4)}", lambda v: f"'{v}'"),
        "bin": (pa.binary(), lambda i: f"b{i}".encode(), lambda v: f"'{v.decode()}'"),
        "fsb": (pa.binary(12), lambda i: f"{i:012d}".encode(), lambda v: f"'{v.decode()}'"),
        "d": (pa.date32(), lambda i: dt.date(2000, 1, 1) + dt.timedelta(days=i % 9000),
              lambda v: f"'{v.isoformat()}'"),
        "ts_us": (pa.timestamp("us"), lambda i: EPOCH + dt.timedelta(microseconds=i * 1_000_003), ts_lit("us")),
        "ts_ms": (pa.timestamp("ms"), lambda i: EPOCH + dt.timedelta(milliseconds=i * 1009), ts_lit("ms")),
        "ts_ns": (pa.timestamp("ns"), lambda i: 1_600_000_000_000_000_000 + i * 1_000_000_007, ts_lit("ns")),
        "t_us": (pa.time64("us"), lambda i: dt.time(i % 24, i % 60, (i // 7) % 60, (i * 13) % 1_000_000),
                 lambda v: f"'{v.isoformat()}'"),
        "n_i64": (pa.int64(), lambda i: None if i % 19 == 3 else i * 13, str),
        "n_s": (pa.string(), lambda i: None if i % 23 == 5 else f"n{i}", lambda v: f"'{v}'"),
        "low": (pa.string(), lambda i: f"cat{i % 37}", lambda v: f"'{v}'"),
    }


def build_table(rnd, n, names):
    kinds = column_kinds(rnd)
    keys = list(range(n))
    rnd.shuffle(keys)
    cols = {}
    for name in names:
        t, gen, _ = kinds[name]
        cols[name] = pa.array([gen(i) for i in keys], t)
    return pa.table(cols)




def bloom_rounds():
    """The fixed table, then four fuzz rounds: (name, table, row group size, codec, fpp, re-encode?)."""
    rnd = random.Random(BLOOM_SEED)
    names = list(column_kinds(rnd))
    rounds = [("fixed", build_table(rnd, BLOOM_ROWS, names), BLOOM_RG, "snappy", None, None)]
    pool = list(column_kinds(rnd))
    for k in range(4):
        r = random.Random(BLOOM_SEED + k + 1)
        cols = list(dict.fromkeys(r.sample(pool, r.randint(2, 6)) + ["f64"]))
        n = r.randint(50, 6000)
        table = build_table(r, n, cols)
        rounds.append((f"fz{k}", table, r.choice([97, 500, 2048, n]), r.choice(["snappy", "zstd", "none"]),
                       r.choice([0.001, 0.01, 0.1, 0.4]), r.random() < 0.5))
    return rounds


def bloom_filters(out_dir):
    for name, table, rg, codec, _, _ in bloom_rounds():
        pq.write_table(table, out_dir / f"{name}.parquet", row_group_size=rg, compression=codec)
        if name == "fixed":
            pq.write_table(table, out_dir / "pyarrow_filters.parquet", row_group_size=rg, compression=codec,
                           bloom_filter_options={n: {"ndv": rg, "fpp": 0.01} for n in table.column_names})
    # Automatic filters: every value even, so `low = 1` is in range and absent from every chunk.
    n = 4000
    low = [(i % 64) * 2 for i in range(n)]
    table = pa.table({
        "low": pa.array(low, type=pa.int32()),
        "i64": pa.array(low, type=pa.int64()),
        "f32": pa.array(low, type=pa.float32()),
        "f64": pa.array(low, type=pa.float64()),
        "s": pa.array([f"k{v:04d}" for v in low]),
        "fixed": pa.array([f"k{v:04d}".encode() for v in low], type=pa.binary(5)),
        "high": pa.array(range(n), type=pa.int64()),
        "b": pa.array([i % 2 == 0 for i in range(n)]),
    })
    for dictionary in [False, True]:
        pq.write_table(table, out_dir / f"auto_{'dict' if dictionary else 'plain'}.parquet", use_dictionary=dictionary,
                       row_group_size=1000)
    pq.write_table(pa.table({"xs": pa.array([[i % 7, None, i % 11] for i in range(1000)])}),
                   out_dir / "auto_nested.parquet", use_dictionary=True)
    epoch = dt.datetime(2020, 1, 1)
    times = pa.array([epoch + dt.timedelta(seconds=(i % 64) * 2) for i in range(1000)], type=pa.timestamp("ns"))
    pq.write_table(pa.table({"t": times}), out_dir / "auto_int96.parquet", use_dictionary=True,
                   use_deprecated_int96_timestamps=True)
    # A dictionary that falls back to PLAIN: those values are outside it, so no dictionary filter is sound.
    pq.write_table(pa.table({"s": [f"long value {i:08d}" for i in range(5000)]}), out_dir / "auto_fallback.parquet",
                   use_dictionary=True, dictionary_pagesize_limit=128, write_batch_size=64)


def order(out_dir):
    """Two files of many small row groups, keys first appearing out of byte order, floats of mixed magnitudes; and
    ~1M rows whose row groups each start with a huge value and continue with small ones, which a running sum drops
    and a fresh one keeps, so a scan on several workers could move where partial float sums meet."""
    rnd = random.Random(11)
    keys = ["zz", "m", "a", "q", "b", "k", "c"]
    for f in range(2):
        n = 3000
        ks = [rnd.choice(keys[: 2 + (i * len(keys)) // n]) for i in range(n)]
        t = pa.table({
            "id": pa.array([f * 10_000 + i for i in range(n)], pa.int64()),
            "k": ks,
            "v": pa.array([rnd.choice([1e15, 1.0, 1e-4, 3e7]) * (rnd.random() - 0.5) for _ in range(n)]),
            "s": [f"r{f}-{i}" for i in range(n)],
            "n": pa.array([None if i % 13 == 0 else i % 97 for i in range(n)], pa.int32()),
        })
        pq.write_table(t, out_dir / f"order{f}.parquet", row_group_size=137)
    rnd = random.Random(3)
    keys = ["zz", "m", "a", "q"]
    n, rg = 1 << 20, 1 << 14
    ks = [keys[min(len(keys) - 1, (i // rg) % 7)] if i % rg else keys[(i // rg) % len(keys)] for i in range(n)]
    vs = [(1e16 if (i // rg) % 2 else -1e16) if i % rg == 0 else rnd.random() for i in range(n)]
    pq.write_table(pa.table({"k": ks, "v": vs}), out_dir / "order_groups.parquet", row_group_size=rg)


def planner(out_dir):
    """Inputs whose planner choices tools/check/cases/planner.json pins: unknown NDV over long shared-prefix keys; a
    small dictionary domain whose data pages are all zeroed (statistics must never need one); keys recurring in
    every row group; raw fixed-width keys; and short strings whose composite with a second key is almost unique."""
    n = 80_000
    keys = [None if i % 97 == 0 else "https://example.org/" + "shared-prefix/" * 8 + str(i % 12_001)
            for i in range(n)]
    pq.write_table(pa.table({"k": keys, "v": [(i % 17) - 8 for i in range(n)], "pos": range(n)}),
                   out_dir / "planner.parquet", row_group_size=10_000, use_dictionary=False)
    bounded = out_dir / "planner_bounded.parquet"
    pq.write_table(pa.table({"k": [i % 100 for i in range(n)], "v": range(n)}), bounded, row_group_size=10_000)
    metadata = pq.read_metadata(bounded)
    b = bytearray(bounded.read_bytes())
    footer = len(b) - 8 - struct.unpack_from("<I", b, len(b) - 8)[0]
    for rg in range(metadata.num_row_groups):
        for ci in range(metadata.num_columns):
            col = metadata.row_group(rg).column(ci)
            end = min(footer, col.dictionary_page_offset + col.total_compressed_size)
            b[col.data_page_offset:end] = bytes(end - col.data_page_offset)
    bounded.write_bytes(b)
    m = 400_000
    pq.write_table(pa.table({"k": [i % 25_000 for i in range(m)], "v": [(i % 201) - 100 for i in range(m)],
                             "pos": range(m)}), out_dir / "planner_census.parquet", row_group_size=25_000)
    pq.write_table(pa.table({"k": [i % 25_000 for i in range(m)], "f": [i // 25_000 for i in range(m)],
                             "pos": range(m)}), out_dir / "planner_census_raw.parquet", row_group_size=25_000,
                   use_dictionary=False)
    indices = [*range(1_100_000), *range(20)]
    pq.write_table(pa.table({"k": [None if i == 0 else "k" + str(i % 400_000) for i in indices],
                             "f": [i // 400_000 for i in indices], "pos": range(len(indices))}),
                   out_dir / "planner_strings.parquet", row_group_size=100_000, use_dictionary=False)


def writer_defaults(out_dir):
    """Numeric columns at 80, 100 and 101 distinct values in 1000 present rows (the dictionary cap is 10% of present
    rows), dense and with trailing NULLs, PLAIN and dictionary-coded; and a column per statistics-exactness case."""
    types = [pa.int32(), pa.int64(), pa.float32(), pa.float64()]
    for nullable in [False, True]:
        n = 1250 if nullable else 1000
        table = pa.table({f"v{ti}_{card}": pa.array([i % card if i < 1000 else None for i in range(n)], type=dtype)
                          for ti, dtype in enumerate(types) for card in [80, 100, 101]})
        for dictionary in [False, True]:
            name = f"card_{'null' if nullable else 'dense'}_{'dict' if dictionary else 'plain'}.parquet"
            pq.write_table(table, out_dir / name, use_dictionary=dictionary)
    n = 512
    table = pa.table({
        "i32": pa.array(range(n), type=pa.int32()),
        "u32": pa.array([3_000_000_000 + i for i in range(n)], type=pa.uint32()),
        "i64": pa.array(range(n), type=pa.int64()),
        "u64": pa.array([2**63 + i for i in range(n)], type=pa.uint64()),
        "f32": pa.array([i / 4 for i in range(n)], type=pa.float32()),
        "f64": pa.array([i / 4 for i in range(n)], type=pa.float64()),
        "b": pa.array([i % 2 == 0 for i in range(n)]),
        "s": pa.array(["short", "text"] * (n // 2)),
        "cut": pa.array(["a" * 200, "z" * 200] * (n // 2)),
        "mixed": pa.array(["a", "z" * 200] * (n // 2)),
        "overflow": pa.array([b"\xff" * 200] * n, type=pa.binary()),
        "fixed": pa.array([b"x" * 200] * n, type=pa.binary(200)),
        "d32": pa.array([Decimal(i) / 100 for i in range(n)], type=pa.decimal128(9, 2)),
        "d64": pa.array([Decimal(i) / 100 for i in range(n)], type=pa.decimal128(18, 2)),
        "d128": pa.array([Decimal(i) / 100 for i in range(n)], type=pa.decimal128(30, 2)),
        "nulls": pa.array([None] * n, type=pa.int32()),
        "nans": pa.array([float("nan")] * n),
    })
    for dictionary in [False, True]:
        pq.write_table(table, out_dir / f"exact_{'dict' if dictionary else 'plain'}.parquet", use_dictionary=dictionary,
                       store_decimal_as_integer=True)


def layout(out_dir):
    """Re-encoding writes' inputs: one row group of 100,000 equal short strings then 100,000 unique 1 KiB ones (the
    T9/T10 repros); 16, 32 and 48 copies of one string column (one-row slices must amortize per-piece overhead);
    and 230,001 rows in row groups of 33,333 that output targets cut across."""
    rng = random.Random(5)
    vals = ["same-value"] * 100_000 + ["".join(rng.choice("0123456789abcdef") for _ in range(32)) * 32
                                       for _ in range(100_000)]
    pq.write_table(pa.table({"id": pa.array(range(len(vals)), pa.int64()), "s": pa.array(vals)}),
                   out_dir / "layout_t9.parquet", row_group_size=len(vals), compression="zstd")
    wide_values = pa.array([f"v-{i:08x}" for i in range(100_003)])
    for width in (16, 32, 48):
        wide = pa.Table.from_arrays([wide_values] * width, names=[f"c{i}" for i in range(width)])
        pq.write_table(wide, out_dir / f"layout_wide{width}.parquet", row_group_size=wide.num_rows, compression="zstd")
    n = 230_001
    r2 = random.Random(7)
    pq.write_table(pa.table({
        "k": pa.array(range(n), pa.int64()),
        "v": pa.array([r2.random() if i % 13 else None for i in range(n)], pa.float64()),
        "s": pa.array([f"name-{i % 5000}" if i % 7 else None for i in range(n)], pa.string()),
        "d": pa.array([Decimal(r2.randrange(-10**9, 10**9)).scaleb(-2) for _ in range(n)], pa.decimal128(12, 2)),
    }), out_dir / "layout_targets.parquet", row_group_size=33_333)


def private_decode(out_dir):
    """Four row groups, each a reducing head of 1000 keys x 7 over 100,000 rows before a 900,000-row unique dictionary
    tail: the decoder-floor cancellation regression (reducing samples must return their private funding)."""
    path = out_dir / "reducing_head.parquet"
    writer = None
    try:
        for rg in range(4):
            head = [f"head-{i % 1000:04d}-" + "h" * 70 for i in range(100_000)]
            tail = [f"uniq-{rg:02d}-{i:07d}-" + "u" * 70 for i in range(900_000)]
            table = pa.table({"k": head + tail, "f": pa.array([i % 7 for i in range(1_000_000)], pa.int32())})
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema, use_dictionary=["k"], dictionary_pagesize_limit=1 << 30,
                                          compression="snappy")
            writer.write_table(table, row_group_size=1_000_000)
    finally:
        if writer is not None:
            writer.close()


def smoke(out_dir):
    """The CLI front door's inputs: 50k mixed rows with NULLs every 5th row, and that file with every page zeroed (an
    explanation reads the footer alone); top-level names containing dots or quotes beside nested fields; a codec zpq
    cannot decompress; INT96 timestamps; small V2 pages with dictionaries and a page index; and a ZSTD file."""
    n = 50_000
    t = pa.table({
        "i32": pa.array([i - n // 2 for i in range(n)], type=pa.int32()),
        "i64": pa.array([i * 7 for i in range(n)], type=pa.int64()),
        "f64": pa.array([i * 0.5 for i in range(n)], type=pa.float64()),
        "name": pa.array([f"row_{i:05d}" for i in range(n)], type=pa.string()),
        "i32_null": pa.array([None if i % 5 == 0 else i for i in range(n)], type=pa.int32()),
        "name_null": pa.array([None if i % 5 == 0 else f"v_{i}" for i in range(n)], type=pa.string()),
    })
    pq.write_table(t, out_dir / "smoke_in.parquet", compression="snappy")
    b = bytearray((out_dir / "smoke_in.parquet").read_bytes())
    footer = len(b) - 8 - struct.unpack_from("<I", b, len(b) - 8)[0]
    b[4:footer] = bytes(footer - 4)
    (out_dir / "smoke_poisoned.parquet").write_bytes(b)
    pq.write_table(pa.table({"a.b": pa.array([1, None, 3], type=pa.int32()),
                             "k": pa.array([7, 8, 9], type=pa.int64())}), out_dir / "dotted.parquet")
    ab = pa.struct([("b", pa.int64())])
    pq.write_table(pa.table({"a.b": pa.array([1, None, 3], type=pa.int64()),
                             "a": pa.array([{"b": 10}, {"b": 20}, {"b": 30}], type=ab)}),
                   out_dir / "dotted_twin_nn.parquet")
    pq.write_table(pa.table({"a.b": [1], '"a"."b"': [2], "a": pa.array([{"b": 3}], type=ab)}),
                   out_dir / "dotted_triplet.parquet")
    pq.write_table(pa.table({"r": pa.array([{"v": 1}, {"v": 2}, {"v": 1}], type=pa.struct([("v", pa.int64())]))}),
                   out_dir / "plain_nested.parquet")
    pq.write_table(pa.table({'"x"': [1, 2], '"a"."b"': [3, 4], "x": [5, 6],
                             "a": pa.array([{"b": 7}, {"b": 8}], type=ab),
                             "r": pa.array([{"v": None}, {"v": 9}], type=pa.struct([("v", pa.int64())]))}),
                   out_dir / "odd_names.parquet")
    pq.write_table(pa.table({"x": pa.array([1, 2, 3], type=pa.int64())}), out_dir / "brotli.parquet",
                   compression="brotli")
    ts = pa.array([dt.datetime(2020, 1, i) for i in range(1, 4)], type=pa.timestamp("ns"))
    pq.write_table(pa.table({"id": pa.array([1, 2, 3], type=pa.int32()), "ts": ts}), out_dir / "smoke_int96.parquet",
                   use_deprecated_int96_timestamps=True)
    m = 30_000
    t = pa.table({
        "i32": pa.array(range(m), type=pa.int32()),
        "cat": pa.array([f"c{i % 17}" for i in range(m)], type=pa.string()),
        "opt": pa.array([None if i % 3 == 0 else i * 0.25 for i in range(m)], type=pa.float64()),
    })
    # Recompression has to rewrite many page headers, leave levels alone and remap every page location.
    pq.write_table(t, out_dir / "pages.parquet", compression="snappy", data_page_version="2.0", data_page_size=4096,
                   write_page_index=True, row_group_size=10_000)
    pq.write_table(t, out_dir / "zstd_src.parquet", compression="zstd")


def unknown_encodings(out_dir):
    """alp_extended with ALP (id 10) renamed to encoding ids no reader knows (31, -17), and decimal_exact's d38 data
    pages given those ids: inspect must name them, and decoding must refuse naming them."""
    from .parquet import Reader, T_STRUCT, field, rewrite_footer, write_value

    fixtures = REPO / "ci/fixtures/parquet"
    for encoding_id in [31, -17]:
        unknown = out_dir / f"unknown_alp{encoding_id}.parquet"
        unknown.write_bytes((fixtures / "alp_extended.zstd.parquet").read_bytes())

        def edit(meta, body):
            for rg in field(meta, 4)[2][1]:
                for column, chunk in enumerate(field(rg, 1)[2][1]):
                    cm = field(chunk, 3)[2]
                    encodings = field(cm, 2)[2][1]
                    encodings[:] = [encoding_id if e == 10 else e for e in encodings]
                    start = field(cm, 9)[2]
                    parser = Reader(body[start:])
                    header = parser.value(T_STRUCT)
                    data_header = field(header, 5) or field(header, 8)
                    enc = field(data_header[2], 2 if field(header, 5) else 4)
                    if enc[2] == 10:
                        enc[2] = encoding_id
                    if column == 0:
                        field(data_header[2], 3)[2] = encoding_id
                    encoded = write_value(T_STRUCT, header)
                    assert len(encoded) == parser.pos
                    body[start:start + parser.pos] = encoded
        rewrite_footer(unknown, edit)
        unknown = out_dir / f"unknown_decimal{encoding_id}.parquet"
        unknown.write_bytes((fixtures / "decimal_exact.parquet").read_bytes())

        def edit(meta, body):
            patched = 0
            for rg in field(meta, 4)[2][1]:
                for chunk in field(rg, 1)[2][1]:
                    cm = field(chunk, 3)[2]
                    if field(cm, 3)[2][1] != [b"d38"]:
                        continue
                    field(cm, 2)[2][1].append(encoding_id)
                    start = field(cm, 9)[2]
                    parser = Reader(body[start:])
                    header = parser.value(T_STRUCT)
                    assert field(header, 1)[2] == 0  # V1 data page, after its dictionary.
                    field(field(header, 5)[2], 2)[2] = encoding_id
                    encoded = write_value(T_STRUCT, header)
                    assert len(encoded) == parser.pos
                    body[start:start + parser.pos] = encoded
                    patched += 1
            assert patched == 3
        rewrite_footer(unknown, edit)


def skspill(out_dir):
    """4M rows of 16-byte dictionary strings, most distinct, in 16 row groups: string-key GROUP BY spill refuses a
    500MB budget at -j8 that the no-spill path answers (planner review P1-1), pinned as the declared refusal."""
    import numpy as np

    rng = np.random.default_rng(20261016)
    keys = np.char.add("key-", np.char.zfill(rng.integers(0, 3_500_000, 4_000_000).astype(str), 12))
    pq.write_table(pa.table({"s": pa.array(keys.astype(object), pa.string())}), out_dir / "skspill.parquet",
                   row_group_size=250_000, compression="snappy")
