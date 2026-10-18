#!/usr/bin/env python3
"""Format regressions with independent readers; invoked by cli_smoke.sh."""
import base64
import json
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
        # A bare alias of a nullable column is refused (`NullableNotSupported`) before any value is printed.
        if None not in values:
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
    # Genuinely different annotations still refuse a shared output footer.
    micros = tmp / 'ts-micros.parquet'
    pq.write_table(pa.table({'v': pa.array([1, 2], pa.timestamp('us'))}), micros)
    blob = tmp / 'blob.parquet'
    pq.write_table(pa.table({'v': [b'abc', b'def']}), blob)
    for first, second in [(ts, micros), (text, blob)]:
        p = subprocess.run([ZPQ, 'query', str(first), str(second), '-j', '1', '-o', str(tmp / 'refused.parquet')],
                           capture_output=True, text=True, timeout=30)
        assert p.returncode != 0 and 'schema mismatch' in p.stderr, (first.name, second.name, p.stderr)


with tempfile.TemporaryDirectory(prefix='zpq-formats-') as directory:
    logical_annotations(Path(directory))
    binary_text(Path(directory))
    repeated_fields(Path(directory))
    encrypted_inputs(Path(directory))
    mixed_writers(Path(directory))
print('Format correctness regressions passed')
