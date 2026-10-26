#!/usr/bin/env python3
"""Format regressions with independent readers; invoked by cli_smoke.sh."""
import base64
from decimal import Decimal
import json
import math
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ZPQ = str(Path(sys.argv[1]).resolve())
FIXTURES = Path(__file__).resolve().parent.parent / 'ci/fixtures/parquet'


def query(src, *args, ok=True):
    p = subprocess.run([ZPQ, 'query', str(src), '-j', '1', *args], capture_output=True, text=True, timeout=30)
    if ok:
        assert p.returncode == 0, (p.returncode, p.stderr)
    else:
        assert p.returncode > 0, (p.returncode, p.stdout, p.stderr)
    return p


def sql_query(sql, *args):
    p = subprocess.run([ZPQ, 'query', '--query', sql, '-j', '1', *args], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, (p.returncode, p.stderr)
    return p


def logical_annotations(tmp):
    for name in ['geometry', 'unknown', 'variant']:
        src = FIXTURES / f'format_{name}.parquet'
        expected = pq.read_table(src)
        for mode, args in [('copy', []), ('project', ['--columns', expected.column_names[-1]]),
                           ('rewrite', ['--limit', '2'])]:
            if name == 'variant' and mode == 'rewrite':
                query(src, *args, '-o', str(tmp / 'refused.parquet'), ok=False)
                continue
            out = tmp / f'{name}-{mode}.parquet'
            query(src, *args, '-o', str(out))
            want = expected.select([expected.column_names[-1]]) if mode == 'project' else expected
            if mode == 'rewrite':
                want = want.slice(0, 2)
            got = pq.read_table(out)
            assert got.equals(want), (name, mode, want, got)
            with duckdb.connect(config={'threads': '1', 'memory_limit': '512MB'}) as d:
                projection = '*' if mode != 'project' else '"' + expected.column_names[-1] + '"'
                sql = f'SELECT {projection} FROM read_parquet(?)' + (' LIMIT 2' if mode == 'rewrite' else '')
                before = d.execute(sql, [str(src)]).fetchall()
                after = d.execute('SELECT * FROM read_parquet(?)', [str(out)]).fetchall()
                assert before == after, (name, mode, before, after)
        if name == 'geometry':
            out = tmp / 'geometry-renamed.parquet'
            query(src, '--select', 'v AS renamed', '-o', str(out))
            got = pq.read_table(out)
            assert got.equals(expected.rename_columns(['renamed'])), got


def raw_values(src):
    """Column `v`'s values as the bytes stored, whatever type the reader maps the annotation to."""
    col = pq.read_table(src).column('v').combine_chunks()
    return (col.storage if isinstance(col, pa.ExtensionArray) else col).to_pylist()


def as_text(value, kind):
    if value is None:
        return None
    if kind == 'uuid':
        h = value.hex()
        return f'{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}'
    if kind == 'interval':
        return dict(zip(['months', 'days', 'millis'], struct.unpack('<III', value)))
    if kind == 'bytes':
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            pass
    return {'base64': base64.b64encode(value).decode('ascii')}


def binary_text(tmp):
    blob = tmp / 'blob.parquet'
    # Distinct values that U+FFFD replacement printed alike; text that spells the base64 of another value; a 3n+1
    # length for padding, and one past a 3072-byte chunk.
    pq.write_table(pa.table({'v': [b'\xff', b'\xfe', b'ASCII', b'', None, b'\x00', b'\xff', b'/w==', 'é'.encode(),
                                   bytes(range(256)) * 13]}), blob)
    uuids = tmp / 'uuid.parquet'
    pq.write_table(pa.table({'v': pa.array([b'\xff' * 16, b'\xfe' * 16, None, bytes(range(16))],
                                           pa.binary(16)).cast(pa.uuid())}), uuids)
    flba = tmp / 'flba.parquet'
    pq.write_table(pa.table({'v': pa.array([b'\xff\x00', b'\xfe\x00', None], pa.binary(2))}), flba)
    cases = [(blob, 'bytes'), (uuids, 'uuid'), (flba, 'bytes'), (FIXTURES / 'format_geometry.parquet', 'base64'),
             (FIXTURES / 'format_bson.parquet', 'base64'), (FIXTURES / 'format_interval.parquet', 'interval')]
    lib = None
    if os.environ.get('ZPQ_LIB'):
        sys.path.insert(0, str(FIXTURES.parents[2] / 'python'))
        import zpq
        lib = zpq.Zpq(max_memory='256MB')
    for src, kind in cases:
        values = raw_values(src)
        rows = [{'v': as_text(v, kind)} for v in values]
        assert len({json.dumps(r, sort_keys=True) for r in rows}) == len(set(values)), src
        for args, want in [([], rows), (['--limit', '2'], rows[:2]),
                           (['--filter', 'v IS NOT NULL'], [r for r in rows if r['v'] is not None])]:
            for fmt in ['json', 'ndjson']:
                out = query(src, *args, '--format', fmt).stdout
                got = json.loads(out) if fmt == 'json' else [json.loads(line) for line in out.splitlines()]
                assert got == want, (src, args, fmt, want, got)
        # A bare alias keeps the column's NULLs.
        renamed = json.loads(query(src, '--select', 'v AS renamed', '--format', 'json').stdout)
        assert renamed == [{'renamed': r['v']} for r in rows], (src, renamed)
        sql = json.loads(sql_query(f"SELECT v FROM '{src}' LIMIT 2 OFFSET 1", '--format', 'json').stdout)
        assert sql == rows[1:3], (src, sql)
        present = [v for v in values if v is not None]
        for args in [[], ['--scan-all']]:
            got = json.loads(query(src, '--aggregate', 'min(v) AS lo,max(v) AS hi', *args, '--format', 'json').stdout)
            assert got == [{'lo': as_text(min(present), kind), 'hi': as_text(max(present), kind)}], (src, got)
        counts = {}
        for v in values:
            counts[v] = counts.get(v, 0) + 1
        want = sorted(json.dumps({'v': as_text(v, kind), 'n': n}) for v, n in counts.items())
        for args in [[], ['--max-memory', '64MB']]:
            got = json.loads(query(src, '--group-by', 'v', '--aggregate', 'count(*) AS n', *args, '--format',
                                   'json').stdout)
            assert sorted(json.dumps(r) for r in got) == want, (src, got)
        # An explanation names columns and counts; it never prints a value.
        json.loads(query(src, '--aggregate', 'min(v) AS lo', '--explain').stdout)
        if lib:
            for sql in [f"SELECT * FROM '{src}'", f"SELECT min(v) AS lo, max(v) AS hi FROM '{src}'",
                        f"SELECT v, count(*) AS n FROM '{src}' GROUP BY v"]:
                assert lib.query_raw(sql, parallelism=1) == sql_query(sql).stdout, sql
    invalid = tmp / 'invalid-string.parquet'
    bad = pa.Array.from_buffers(pa.string(), 2, [None, pa.py_buffer(struct.pack('<iii', 0, 1, 3)),
                                                 pa.py_buffer(b'\xff\xfe\x80')])
    pq.write_table(pa.table({'bad': bad}), invalid)
    for args in [[], ['--limit', '1'], ['--aggregate', 'min(bad) AS lo'],
                 ['--aggregate', 'max(bad) AS hi', '--scan-all'], ['--group-by', 'bad', '--aggregate', 'count(*) AS n']]:
        p = query(invalid, *args, ok=False)
        assert 'not valid UTF-8' in p.stderr and '`bad`' in p.stderr, (args, p.stderr)
    if lib:
        for sql in [f"SELECT * FROM '{invalid}'", f"SELECT max(bad) AS hi FROM '{invalid}'"]:
            response = lib.run({'sql': sql}, check=False)
            assert response.get('reason') == 'InvalidUtf8' and response.get('column') == 'bad', response
        lib.close()


def repeated_fields(tmp):
    # Legacy REPEATED fields with no LIST/MAP annotation hold a list per row; printing each leaf value as a row was
    # incorrect, and a filter answered from statistics skipped the decode that refuses them.
    for filename, column in [('format_repeated.parquet', 'Int32_list'),
                             ('format_repeated_group.parquet', 'phoneNumbers.phone.number')]:
        src = FIXTURES / filename
        for args in [[], ['--limit', '1'], ['--limit', '0'], ['--columns', column], ['--select', column + ' + 1 AS x'],
                     ['--format', 'json']]:
            p = query(src, *args, ok=False)
            assert p.returncode == 2 and 'repeated' in p.stderr and column in p.stderr, (args, p.stderr)
            assert not p.stdout, p.stdout
        p = subprocess.run([ZPQ, 'query', '--query', f"SELECT * FROM '{src}'"], capture_output=True, text=True,
                           timeout=30)
        assert p.returncode == 2 and 'repeated' in p.stderr and not p.stdout, p.stderr
        for predicate in [column + ' IS NULL', column + ' IS NOT NULL', column + ' > 999999999999']:
            for args in [['--aggregate', 'count(*) AS n'], ['--aggregate', 'count(*) AS n', '--scan-all'],
                         ['-o', str(tmp / 'refused.parquet')]]:
                p = query(src, '--filter', predicate, *args, ok=False)
                assert p.returncode == 2 and 'repeated' in p.stderr, (predicate, args, p.stderr)
        assert not (tmp / 'refused.parquet').exists()
        out = tmp / filename
        query(src, '-o', str(out))
        assert pq.read_table(src).equals(pq.read_table(out))
        with duckdb.connect(config={'threads': '1', 'memory_limit': '512MB'}) as d:
            sql = 'SELECT * FROM read_parquet(?)'
            assert d.execute(sql, [str(src)]).fetchall() == d.execute(sql, [str(out)]).fetchall()
    # A flat column beside a repeated group still prints and filters.
    src = FIXTURES / 'format_repeated_group.parquet'
    want = [{'id': i} for i in pq.read_table(src).column('id').to_pylist() if i > 2]
    got = json.loads(query(src, '--filter', 'id > 2', '--columns', 'id', '--format', 'json').stdout)
    assert got == want, got

def encrypted_inputs(tmp):
    # zpq has no keys. A copy carried encrypted pages under a new footer without their crypto metadata: unreadable.
    for name in ['format_encrypted_footer.parquet', 'format_encrypted_columns.parquet',
                 'format_encrypted_plaintext.parquet']:
        src = FIXTURES / name
        out = tmp / 'encrypted-copy.parquet'
        for args in [['--format', 'json'], ['--aggregate', 'count(*) AS n'], ['--limit', '1', '-o', str(out)],
                     ['-o', str(out)], ['--explain', '--aggregate', 'count(*) AS n']]:
            p = query(src, *args, ok=False)
            assert p.returncode == 1 and 'encrypted Parquet' in p.stderr, (name, args, p.stderr)
            assert not p.stdout and not out.exists(), (name, args)
        p = subprocess.run([ZPQ, 'inspect', str(src), '--format', 'json'], capture_output=True, text=True, timeout=30)
        if name == 'format_encrypted_footer.parquet':
            assert p.returncode != 0 and 'EncryptedFile' in p.stdout, p.stdout
        else:
            # A plaintext footer is still inspectable metadata; its chunks say they are encrypted.
            assert p.returncode == 0 and '"encrypted":true' in p.stdout, p.stdout

def rewrite_footer(src, dst, old, new):
    """`src` with one byte sequence of its footer replaced: another writer's spelling of the same schema."""
    b = src.read_bytes()
    n = struct.unpack('<I', b[-8:-4])[0]
    footer = b[-8 - n:-8]
    assert footer.count(old) == 1, footer.hex()
    footer = footer.replace(old, new)
    dst.write_bytes(b[:-8 - n] + footer + struct.pack('<I', len(footer)) + b'PAR1')


def mixed_writers(tmp):
    # Inputs whose footers spell one annotation differently must still read together; only a different type refuses.
    ts = tmp / 'ts.parquet'
    pq.write_table(pa.table({'v': pa.array([1, 2], pa.timestamp('ns'))}), ts)
    ts_reordered = tmp / 'ts-reordered.parquet'  # TimestampType's fields in the other order
    rewrite_footer(ts, ts_reordered, bytes.fromhex('6c8c121c3c00000000'), bytes.fromhex('6c8c2c3c000002020000'))
    text = tmp / 'text.parquet'
    pq.write_table(pa.table({'v': ['abc', 'def']}), text)
    text_long = tmp / 'text-long-header.parquet'  # STRING's field header in the long form
    rewrite_footer(text, text_long, b'\x4c\x1c\x00\x00', b'\x4c\x0c\x02\x00\x00')
    text_legacy = tmp / 'text-legacy.parquet'  # only the legacy UTF8 ConvertedType
    rewrite_footer(text, text_legacy, b'\x4c\x1c\x00\x00', b'')
    assert pq.ParquetFile(text_legacy).schema.column(0).logical_type.type in ('NONE', 'STRING')
    for first, second in [(ts, ts_reordered), (text, text_long), (text, text_legacy), (text_legacy, text)]:
        want = pa.concat_tables([pq.read_table(first), pq.read_table(second)])
        assert pq.read_table(second).equals(pq.read_table(first)), (first, second)
        out = tmp / 'mixed.parquet'
        for args in [[], ['--limit', '3'], ['--filter', 'v IS NOT NULL']]:
            if out.exists():
                out.unlink()
            p = subprocess.run([ZPQ, 'query', str(first), str(second), '-j', '1', *args, '-o', str(out)],
                               capture_output=True, text=True, timeout=30)
            assert p.returncode == 0, (first.name, second.name, args, p.stderr)
            expected = want.slice(0, 3) if '--limit' in args else want
            assert pq.read_table(out).equals(expected), (first.name, second.name, args)
            with duckdb.connect(config={'threads': '1', 'memory_limit': '512MB'}) as d:
                got = d.execute('SELECT * FROM read_parquet(?)', [str(out)]).fetchall()
                both = d.execute('SELECT * FROM read_parquet(?)', [[str(first), str(second)]]).fetchall()
                assert got == (both[:3] if '--limit' in args else both), (first.name, second.name, args)
        p = subprocess.run([ZPQ, 'query', str(first), str(second), '-j', '1', '--format', 'json'],
                           capture_output=True, text=True, timeout=30)
        assert p.returncode == 0 and len(json.loads(p.stdout)) == want.num_rows, p.stderr
    # One future logical type arm over different legacy fallbacks: text (UTF8) in one file, bytes in the other. Read
    # alone each is fine; one output footer over both would label the bytes UTF8.
    future_text, future_bytes = tmp / 'future-text.parquet', tmp / 'future-bytes.parquet'
    rewrite_footer(text, future_text, bytes.fromhex('4c1c0000'), bytes.fromhex('4c0cc8010000'))
    raw = tmp / 'raw.parquet'
    pq.write_table(pa.table({'v': [b'\xff', b'\xfe']}), raw, store_schema=False)
    rewrite_footer(raw, future_bytes, b'\x18\x01v\x00', b'\x18\x01v' + bytes.fromhex('6c0cc8010000') + b'\x00')
    assert json.loads(query(future_text, '--format', 'json').stdout) == [{'v': 'abc'}, {'v': 'def'}]
    assert json.loads(query(future_bytes, '--format', 'json').stdout) == [{'v': {'base64': '/w=='}},
                                                                          {'v': {'base64': '/g=='}}]
    for args in [['-o', str(tmp / 'refused.parquet')], ['--limit', '4', '-o', str(tmp / 'refused.parquet')],
                 ['--filter', 'v IS NOT NULL', '-o', str(tmp / 'refused.parquet')], ['--format', 'json']]:
        for pair in [(future_text, future_bytes), (future_bytes, future_text)]:
            p = subprocess.run([ZPQ, 'query', *map(str, pair), '-j', '1', *args], capture_output=True, text=True,
                               timeout=30)
            assert p.returncode != 0 and 'schema mismatch' in p.stderr and not p.stdout, (args, p.stderr)
    assert not (tmp / 'refused.parquet').exists()
    # Genuinely different annotations still refuse a shared output footer.
    micros = tmp / 'ts-micros.parquet'
    pq.write_table(pa.table({'v': pa.array([1, 2], pa.timestamp('us'))}), micros)
    blob = tmp / 'blob.parquet'
    pq.write_table(pa.table({'v': [b'abc', b'def']}), blob)
    for first, second in [(ts, micros), (text, blob)]:
        p = subprocess.run([ZPQ, 'query', str(first), str(second), '-j', '1', '-o', str(tmp / 'refused.parquet')],
                           capture_output=True, text=True, timeout=30)
        assert p.returncode != 0 and 'schema mismatch' in p.stderr, (first.name, second.name, p.stderr)


def answer_values(tmp):
    """A GROUP BY key and min/max of a column print as its rows print, per type: the answer's lane (BOOLEAN's 0/1,
    an unsigned value's bits, DECIMAL's unscaled integer, a UUID's bytes) is never what reaches the JSON."""
    src = tmp / 'answer_types.parquet'
    duckdb.connect().execute(f"""COPY (SELECT * FROM (VALUES
      (true, DATE '2020-01-02', TIMESTAMP '2020-01-02 03:04:05.123456', TIMESTAMP_NS '2020-01-02 03:04:05.123456789',
       TIME '01:02:03.5', 12.34::DECIMAL(9,2), 123456789012.34::DECIMAL(18,2), 1234567890123456789012.345::DECIMAL(38,3),
       'b6d5f6a0-0000-4000-8000-000000000001'::UUID, 250::UTINYINT, 4000000000::UINTEGER, 18446744073709551615::UBIGINT,
       -5::TINYINT, 1.5::FLOAT, 'x', '\\xFF\\x00'::BLOB, INTERVAL 3 DAY),
      (false, DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59', TIMESTAMP_NS '1970-01-01 00:00:00.000000001',
       TIME '23:59:59', -0.05::DECIMAL(9,2), -1.00::DECIMAL(18,2), -0.001::DECIMAL(38,3),
       '00000000-0000-4000-8000-000000000002'::UUID, 1::UTINYINT, 3::UINTEGER, 9223372036854775808::UBIGINT,
       5::TINYINT, -1.5::FLOAT, 'y', 'ab'::BLOB, INTERVAL 1 MONTH),
      (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    ) t(b, dt, ts, tsns, tm, d9, d18, d38, u, u8, u32, u64, i8, f, s, bl, iv)) TO '{src}' (FORMAT PARQUET)""")
    flba = tmp / 'answer_flba.parquet'
    pq.write_table(pa.table({'flba': pa.array([Decimal('1.50'), None, Decimal('-2.25')], pa.decimal128(25, 2))}),
                   flba, store_decimal_as_integer=False)
    for path, names in ((src, pq.read_schema(src).names), (flba, ['flba'])):
        for c in names:
            texts = lambda sql: sorted(json.dumps(v) for line in sql_query(sql).stdout.splitlines()
                                       for v in json.loads(line).values())
            rows = texts(f"SELECT {c} FROM '{path}'")
            keys = texts(f"SELECT {c} FROM '{path}' GROUP BY {c}")
            assert keys == rows, (c, keys, rows)
            extremes = texts(f"SELECT min({c}) AS mn, max({c}) AS mx FROM '{path}'")
            assert extremes == [v for v in rows if v != 'null'], (c, extremes, rows)
    # A BOOLEAN-typed expression (an aliased column, a CASE over one) is BOOLEAN as rows, keys and min/max, printed
    # true/false and written BOOLEAN; its 0/1 lane never shows.
    # (An aliased column's rows are taken where it is present: a NULL in a renamed column is refused as yet.)
    for e, where in (('b', ' WHERE b IS NOT NULL'), ('CASE WHEN i8 > 0 THEN b END', ''),
                     ('CASE WHEN i8 > 0 THEN b WHEN i8 < 0 THEN b END', '')):
        values = lambda sql: [v for line in sql_query(sql).stdout.splitlines() for v in json.loads(line).values()]
        rows = values(f"SELECT {e} AS k FROM '{src}'{where}")
        assert all(v is None or type(v) is bool for v in rows) and any(v is not None for v in rows), (e, rows)
        keys = values(f"SELECT {e} AS k FROM '{src}' GROUP BY k")
        assert set(map(json.dumps, rows)) <= set(map(json.dumps, keys)) and all(
            v is None or type(v) is bool for v in keys), (e, keys, rows)
        extremes = values(f"SELECT min({e}) AS mn, max({e}) AS mx FROM '{src}'")
        assert all(type(v) is bool for v in extremes), (e, extremes)
        for sql in (f"SELECT {e} AS k FROM '{src}'{where}", f"SELECT {e} AS k, count(*) AS n FROM '{src}' GROUP BY k",
                    f"SELECT max({e}) AS k FROM '{src}'"):
            out = tmp / 'bool_expr.parquet'
            out.unlink(missing_ok=True)
            sql_query(sql, '-o', str(out))
            got = pq.read_table(out)
            assert got.schema.field('k').type == pa.bool_(), (sql, got.schema)
            printed = [json.loads(line)['k'] for line in sql_query(sql).stdout.splitlines()]
            assert sorted(map(json.dumps, got.column('k').to_pylist())) == sorted(
                map(json.dumps, printed)), (sql, got.to_pylist())


def unsigned_answers(tmp):
    """min/max keep an unsigned column's type in a write, at every width and past 2^63: scalar, grouped and as keys."""
    src = tmp / 'unsigned.parquet'
    top = {8: 255, 16: 65535, 32: (1 << 32) - 1, 64: (1 << 64) - 1}
    pq.write_table(pa.table({**{f'u{w}': pa.array([1, top[w], None, 1 << (w - 1)], getattr(pa, f'uint{w}')())
                                for w in top}, 'k': pa.array([1, 1, 2, 2], pa.int32())}), src)
    for w in top:
        c = f'u{w}'
        want = pa.field('x', getattr(pa, f'uint{w}')()).type
        for sql, rows in ((f"SELECT min({c}) AS lo, max({c}) AS x FROM '{src}'", [{'lo': 1, 'x': top[w]}]),
                          (f"SELECT k, max({c}) AS x FROM '{src}' GROUP BY k ORDER BY k",
                           [{'k': 1, 'x': top[w]}, {'k': 2, 'x': 1 << (w - 1)}]),
                          (f"SELECT {c} AS x, count(*) AS n FROM '{src}' WHERE {c} IS NOT NULL GROUP BY x ORDER BY x",
                           [{'x': v, 'n': 1} for v in sorted((1, top[w], 1 << (w - 1)))])):
            out = tmp / 'unsigned_out.parquet'
            out.unlink(missing_ok=True)
            printed = [json.loads(line) for line in sql_query(sql).stdout.splitlines()]
            assert printed == rows, (sql, printed)
            sql_query(sql, '-o', str(out))
            got = pq.read_table(out)
            assert got.schema.field('x').type == want, (sql, got.schema)
            assert sorted(got.to_pylist(), key=json.dumps) == sorted(rows, key=json.dumps), (sql, got.to_pylist())


def alias_is_rename(tmp):
    """`SELECT c AS k` writes and prints the column `SELECT c` does, renamed: the same type, annotation and NULLs. A
    nested leaf's dotted label is the same leaf, flat and NULL wherever it or its struct is."""
    src = tmp / 'alias.parquet'
    t = pa.table({
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
    })
    pq.write_table(t, src)
    def written(sql):
        out = tmp / 'alias_out.parquet'
        out.unlink(missing_ok=True)
        sql_query(sql, '-o', str(out))
        return pq.read_table(out)
    def printed(sql):
        return [list(json.loads(line).values())[0] for line in sql_query(sql).stdout.splitlines()]
    for c in [n for n in t.schema.names if n != 'st']:
        bare, alias = written(f"SELECT {c} FROM '{src}'"), written(f"SELECT {c} AS k FROM '{src}'")
        fb, fa = bare.schema.field(c), alias.schema.field('k')
        assert (fa.type, fa.nullable) == (fb.type, fb.nullable), (c, fb, fa)
        assert alias.column('k').to_pylist() == bare.column(c).to_pylist(), c
        assert printed(f"SELECT {c} AS k FROM '{src}'") == printed(f"SELECT {c} FROM '{src}'"), c
    # Nested leaves by label and by alias: the leaf's own type, NULL where the struct is NULL.
    want = {'st.a': (pa.int16(), [1, None, None]), 'st.b': (pa.string(), [None, None, 'q'])}
    for leaf, (typ, values) in want.items():
        for sql, name in ((f"SELECT {leaf} FROM '{src}'", leaf), (f"SELECT {leaf} AS k FROM '{src}'", 'k')):
            got = written(sql)
            assert got.schema.names == [name] and got.schema.field(name).type == typ, (sql, got.schema)
            assert got.schema.field(name).nullable and got.column(name).to_pylist() == values, (sql, got.to_pylist())
            assert printed(sql) == values, (sql, printed(sql))


def nested_by_name(tmp):
    """`SELECT st` / `SELECT l` on a STRUCT or LIST: refused in rows with a message naming a leaf (exit 2); written
    whole with -o when the list is bare columns in the file's order; refused clearly in any other order."""
    src = tmp / 'nested_by_name.parquet'
    t = pa.table({'id': [1, 2, 3], 'st': [{'a': 1, 'b': 'x'}, None, {'a': 3, 'b': None}], 'l': [[1, 2], [], None]})
    pq.write_table(t, src)
    def cli(*args):
        return subprocess.run([ZPQ, 'query', '--query', *args], capture_output=True, text=True, timeout=30)
    for col, words in (('st', ['`st` is nested (a STRUCT)', '`st.a`']),
                       ('l', ['`l` is repeated (a LIST or MAP)', '`l.list.element`'])):
        p = cli(f"SELECT {col} FROM '{src}'")
        assert p.returncode == 2 and not p.stdout and all(w in p.stderr for w in words), (col, p.returncode, p.stderr)
    out = tmp / 'nested_by_name_out.parquet'
    for sql, names in ((f"SELECT st FROM '{src}'", ['st']), (f"SELECT id, st, l FROM '{src}'", ['id', 'st', 'l']),
                       (f"SELECT id, st FROM '{src}' WHERE id >= 2", ['id', 'st'])):
        out.unlink(missing_ok=True)
        p = cli(sql, '-o', str(out))
        assert p.returncode == 0, (sql, p.stderr)
        got = pq.read_table(out)
        want = duckdb.connect().execute(sql).to_arrow_table()
        assert got.schema.names == names and got.to_pylist() == want.to_pylist(), (sql, got.to_pylist())
        assert all(got.schema.field(n).type == t.schema.field(n).type for n in names), (sql, got.schema)
    # A filtered LIST is refused as `--columns l --filter` refuses it: its rows are not re-encoded yet.
    out.unlink(missing_ok=True)
    p = cli(f"SELECT id, l FROM '{src}' WHERE id >= 2", '-o', str(out))
    q = query(src, '--columns', 'id,l', '--filter', 'id >= 2', '-o', str(out), ok=False)
    assert p.returncode == q.returncode == 2 and p.stderr == q.stderr and not out.exists(), (p.stderr, q.stderr)
    p = cli(f"SELECT st, id FROM '{src}'", '-o', str(out))
    assert p.returncode == 2 and "file's order" in p.stderr and not out.exists(), p.stderr


def alias_spaces(tmp):
    """A quoted alias keeps its surrounding spaces in every output, for a grouped key and an aggregate; two names that
    differ only in their spaces are two names, as DuckDB has them."""
    src = tmp / 'alias_spaces.parquet'
    pq.write_table(pa.table({'k': ['a', 'b', 'a']}), src)
    out = tmp / 'alias_spaces.parquet.out'
    for sql in (f"SELECT k AS \" trailing \", count(*) AS n FROM '{src}' GROUP BY k ORDER BY k",
                f"SELECT k, count(*) AS \" n \" FROM '{src}' GROUP BY k ORDER BY k",
                f"SELECT k AS x, count(*) AS \" x \" FROM '{src}' GROUP BY k ORDER BY k",
                f"SELECT count(*) AS \" n \", min(k) AS n FROM '{src}'",
                f"SELECT k AS \" k \" FROM '{src}'"):
        names = [d[0] for d in duckdb.connect().execute(sql).description]
        p = sql_query(sql)
        assert list(json.loads(p.stdout.splitlines()[0])) == names, (sql, p.stdout)
        p = sql_query(sql, '--format', 'csv')
        assert p.stdout.splitlines()[0] == ",".join(names), (sql, p.stdout)
        out.unlink(missing_ok=True)
        sql_query(sql, '-o', str(out))
        assert pq.read_table(out).column_names == names, (sql, pq.read_table(out).column_names)


def int96_signed_days(tmp):
    src = FIXTURES / 'int96_from_spark.parquet'
    exact = -9357363680509551616000
    rows = [json.loads(line)['a'] for line in query(src).stdout.splitlines()]
    assert rows[-1] == exact, rows
    # Both readers overflow microseconds on the last row. The other rows remain independent oracles.
    arrow = pq.read_table(src, coerce_int96_timestamp_unit='us')['a'].cast(pa.int64()).to_pylist()
    duck = duckdb.connect().execute('SELECT epoch_us(a) FROM read_parquet(?)', [str(src)]).fetchall()
    for got, a, (d,) in zip(rows[:-1], arrow[:-1], duck[:-1]):
        assert got == (a * 1000 if a is not None else None) == (d * 1000 if d is not None else None)
    for flags in [[], ['--trust-stats'], ['--scan-all']]:
        got = query(src, '--filter', f'a = {exact}', *flags)
        assert [json.loads(line)['a'] for line in got.stdout.splitlines()] == [exact], got
        got = query(src, '--aggregate', 'min(a) AS lo, max(a) AS hi', *flags)
        assert json.loads(got.stdout) == {'lo': exact, 'hi': 253402225200000000000}, got.stdout
    # Decimal integer literals are exact; an unrepresentable i128 or calendar literal must refuse.
    for literal in [str(-(2**127) - 1), "'-999999999999-01-01'"]:
        query(src, '--filter', f'a = {literal}', ok=False)
    p = query(src, '--filter', f'a = {exact}', '--format', 'csv')
    assert p.stdout.splitlines()[-1] == '-294554-12-13 14:58:10.448384000', p.stdout
    out = tmp / 'int96-too-wide.parquet'
    p = query(src, '--group-by', 'a', '--aggregate', 'count(*) AS n', '-o', str(out), ok=False)
    assert 'Int96OutOfRange' in p.stderr or '64-bit nanoseconds' in p.stderr, p.stderr
    p = query(src, '--filter', 'a < 0', '-o', str(out), ok=False)
    assert 'INT96 re-encoding' in p.stderr, p.stderr
    out.unlink(missing_ok=True)
    query(src, '--filter', "a >= '2000-01-01' AND a < '2100-01-01'", '--group-by', 'a',
          '--aggregate', 'count(*) AS n', '-o', str(out))
    got = pq.read_table(out)
    expected = sorted((v, 1) for v in rows if v is not None and 0 < v < 2**63)
    assert sorted(zip(got['a'].cast(pa.int64()).to_pylist(), got['n'].to_pylist())) == expected
    readback = duckdb.connect().execute('SELECT epoch_ns(a), n FROM read_parquet(?)', [str(out)]).fetchall()
    assert sorted(readback) == expected, readback


def unsupported_encodings(tmp):
    src = FIXTURES / 'alp_extended.zstd.parquet'
    for detail in [[], ['--detail']]:
        p = subprocess.run([ZPQ, 'inspect', str(src), '--format', 'json', *detail],
                           capture_output=True, text=True, timeout=30)
        assert p.returncode == 0 and 'ALP' in p.stdout, (p.returncode, p.stderr, p.stdout)
        json.loads(p.stdout)
    # The PLAIN columns of the same file remain queryable; every written result has two strict readbacks.
    cols = ['float_plain', 'double_plain']
    expected = pq.read_table(src, columns=cols)
    out = tmp / 'alp-plain.parquet'
    query(src, '--columns', ','.join(cols), '-o', str(out))
    def values(table):
        return {k: ['nan' if v is not None and math.isnan(v) else v for v in vals]
                for k, vals in table.to_pydict().items()}
    assert pq.read_table(out).schema == expected.schema
    assert values(pq.read_table(out)) == values(expected)
    got = duckdb.connect().execute('SELECT * FROM read_parquet(?)', [str(out)]).to_arrow_table()
    assert values(got) == values(expected), got
    for column in ['float_alp_1024', 'double_alp_4096']:
        p = query(src, '--columns', column, ok=False)
        assert 'unsupported encoding ALP (id 10)' in p.stderr and 'UnsupportedEncoding' in p.stderr, p.stderr
        assert 'BadMetadata' not in p.stderr, p.stderr
    # Future enum values must be inspectable and reach the same refusal, including with profiling enabled.
    sys.path.insert(0, str(FIXTURES.parents[2] / 'tools'))
    from datagen.parquet import Reader, T_STRUCT, field, rewrite_footer, write_value
    for encoding_id in [31, -17]:
        unknown = tmp / f'unknown-encoding-{encoding_id}.parquet'
        unknown.write_bytes(src.read_bytes())
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
        p = subprocess.run([ZPQ, 'inspect', str(unknown), '--detail', '--format', 'json'],
                           capture_output=True, text=True, timeout=30)
        assert p.returncode == 0 and f'UNKNOWN({encoding_id})' in p.stdout, (p.returncode, p.stderr, p.stdout)
        p = query(unknown, '--columns', 'float_alp_1024', '--profile', str(tmp / 'unknown.trace.json'), ok=False)
        assert f'unsupported encoding UNKNOWN (id {encoding_id})' in p.stderr, p.stderr
        p = query(unknown, '--columns', 'float_plain', ok=False)
        assert f'unsupported encoding UNKNOWN (id {encoding_id})' in p.stderr, p.stderr

with tempfile.TemporaryDirectory(prefix='zpq-formats-') as directory:
    int96_signed_days(Path(directory))
    unsupported_encodings(Path(directory))
    logical_annotations(Path(directory))
    binary_text(Path(directory))
    repeated_fields(Path(directory))
    encrypted_inputs(Path(directory))
    mixed_writers(Path(directory))
    answer_values(Path(directory))
    unsigned_answers(Path(directory))
    alias_is_rename(Path(directory))
    nested_by_name(Path(directory))
    alias_spaces(Path(directory))
print('Format correctness regressions passed')
