#!/usr/bin/env python3
"""CLI channels, destinations, limits, and publication checked with independent readers."""
import io
import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import pty
import struct
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

binary = str(Path(sys.argv[1] if len(sys.argv) > 1 else 'zig-out/bin/zpq').resolve())
checks = 0


def run(src, *args, code=0, **kw):
    global checks
    p = subprocess.run([binary, 'query', str(src), *args], capture_output=True, **kw)
    assert p.returncode == code, (args, p.returncode, p.stderr)
    checks += 1
    return p


def ndjson(body):
    return [json.loads(line) for line in body.splitlines()]


with tempfile.TemporaryDirectory(prefix='zpq-contract-') as d:
    root = Path(d)
    src = root / 'input.parquet'
    table = pa.table({'k': [1, 2, 1, 3], 'v': [3, None, 5, 7], 's': ['a', 'b', None, 'd']})
    pq.write_table(table, src, row_group_size=2)
    scenarios = [([], table.to_pylist()),
                 (['--filter', 'k = 1', '--columns', 'k,v'], [{'k': 1, 'v': 3}, {'k': 1, 'v': 5}]),
                 (['--aggregate', 'count(*) AS n, sum(v) AS s'], [{'n': 4, 's': 15}]),
                 (['--group-by', 'k', '--aggregate', 'sum(v) AS s'],
                  [{'k': 1, 's': 8}, {'k': 2, 's': None}, {'k': 3, 's': 7}])]
    con = duckdb.connect()
    for args, wanted in scenarios:
        p = run(src, *args)
        assert ndjson(p.stdout) == wanted and not p.stderr, (args, p.stdout, p.stderr)
        assert ndjson(run(src, *args, '--format', 'jsonl').stdout) == wanted
        assert ndjson(run(src, *args, '-o', '-').stdout) == wanted
        assert json.loads(run(src, *args, '--format', 'json').stdout) == wanted
        p = run(src, *args, '--summary=json')
        summary = json.loads(p.stderr)
        assert summary['type'] == 'summary' and summary['rows'] == len(wanted)
        assert summary['output_us'] >= 0 and summary['bytes_out'] == len(p.stdout)
        for fmt in ('ndjson', 'json', 'parquet'):
            dst = root / 'answer.blob'
            p = run(src, *args, '-o', str(dst), '--format', fmt, '--summary=json')
            assert not p.stdout and json.loads(p.stderr)['rows'] == len(wanted)
            if fmt == 'parquet':
                got = pq.read_table(dst).to_pylist()
                cursor = con.execute(f"SELECT * FROM read_parquet('{dst}')")
                names = [c[0] for c in cursor.description]
                assert [dict(zip(names, r)) for r in cursor.fetchall()] == wanted
                assert got == wanted
            else:
                got = json.loads(dst.read_bytes()) if fmt == 'json' else ndjson(dst.read_bytes())
                assert got == wanted
        # Named defaults remain Parquet regardless of suffix; stdout needs an explicit binary format.
        dst = root / 'answer.json'
        assert not run(src, *args, '-o', str(dst)).stdout
        p = run(src, *args, '--format', 'parquet')
        assert pq.read_table(io.BytesIO(p.stdout)).to_pylist() == wanted
        for limit in (0, 1, 2):
            for fmt in ('ndjson', 'json', 'parquet'):
                p = run(src, *args, '--limit', str(limit), '--format', fmt)
                wanted_cut = wanted[:limit]
                if fmt == 'parquet':
                    t = pq.read_table(io.BytesIO(p.stdout))
                    assert t.to_pylist() == wanted_cut and t.num_columns == len(wanted[0])
                    dst.write_bytes(p.stdout)
                    assert con.execute(f"SELECT count(*) FROM read_parquet('{dst}')").fetchone()[0] == len(wanted_cut)
                else:
                    got = json.loads(p.stdout) if fmt == 'json' else ndjson(p.stdout)
                    assert got == wanted_cut
    for fmt in ('ndjson', 'json', 'parquet'):
        p = run(src, '--filter', 'k < 0', '--format', fmt)
        got = pq.read_table(io.BytesIO(p.stdout)).to_pylist() if fmt == 'parquet' else (
            json.loads(p.stdout) if fmt == 'json' else ndjson(p.stdout))
        assert got == []
    sql = f"SELECT k, v FROM '{src}' LIMIT 2 OFFSET 1"
    assert ndjson(run('--query', sql).stdout) == [{'k': 2, 'v': None}, {'k': 1, 'v': 5}]
    run('--query', sql, '--limit', '1', code=2)
    for sql in (f"SELECT count(*) AS n FROM '{src}' LIMIT 0",
                f"SELECT count(*) AS n FROM '{src}' LIMIT 1 OFFSET 1"):
        assert not run('--query', sql).stdout
    for args in (['--format', 'csv'], ['--format', 'wat'], ['--limit', '1', '--limit', '2'],
                 ['--aggregate', 'sum(missing)'], ['--query', 'SELECT broken(']):
        p = run(src, *args, '--summary=json', code=2) if '--query' not in args else run(
            '--query', 'SELECT broken(', '--summary=json', code=2)
        error = json.loads(p.stderr)
        assert error['type'] == 'error' and not error['ok'] and not p.stdout
    p = run(src, '--aggregate', 'count(*)', '--explain', '-o', str(root / 'never.parquet'))
    assert json.loads(p.stdout)['explain'] and not (root / 'never.parquet').exists()
    p = run(src, '--aggregate', 'count(*)', '--summary=json', '--profile-summary=json')
    records = ndjson(p.stderr)
    assert {r['type'] for r in records} == {'profile', 'summary'}
    run(src, '--summary=json', '--profile-summary', code=2)
    # LIMIT 0 must bind against real footers while avoiding corrupt data pages.
    poisoned = root / 'poisoned.parquet'
    b = bytearray(src.read_bytes())
    footer = len(b) - 8 - struct.unpack_from('<I', b, len(b) - 8)[0]
    b[4:footer] = bytes(footer - 4)
    poisoned.write_bytes(b)
    for args in ([], ['--aggregate', 'sum(v) AS s'], ['--group-by', 'k', '--aggregate', 'sum(v) AS s']):
        for fmt in ('ndjson', 'json', 'parquet'):
            p = run(poisoned, *args, '--limit', '0', '--format', fmt)
            if fmt == 'parquet':
                assert pq.read_table(io.BytesIO(p.stdout)).num_rows == 0
    # Exact numerics, widened sums, Float16 coercion, and nulls across every sink.
    typed = root / 'typed.parquet'
    typed_table = pa.table({
        'wide': pa.array([2**63 - 1, 2**63 - 2, None], pa.int64()),
        'd': pa.array([Decimal('1.10'), None, Decimal('-2.20')], pa.decimal128(12, 2)),
        'big': pa.array([Decimal('12345678901234567890.123456789'), None, Decimal('0.000000001')],
                        pa.decimal128(38, 9)),
        'h': pa.array([1.5, None, -2.0], pa.float16()),
    })
    pq.write_table(typed_table, typed, row_group_size=2)

    def exact_rows(body, fmt):
        return json.loads(body, parse_float=Decimal) if fmt == 'json' else [
            json.loads(line, parse_float=Decimal) for line in body.splitlines()]

    def destinations(source, args, wanted, formats=('ndjson', 'json', 'parquet')):
        for fmt in formats:
            for dest in (None, '-', str(root / 'typed-answer')):
                flags = [] if dest is None else ['-o', dest]
                p = run(source, *args, '--format', fmt, *flags, '--summary=json')
                body = p.stdout if dest in (None, '-') else Path(dest).read_bytes()
                assert dest in (None, '-') or not p.stdout
                summary = json.loads(p.stderr)
                assert summary['codec'] == ('SNAPPY' if fmt == 'parquet' else None)
                if fmt == 'parquet':
                    got = pq.read_table(io.BytesIO(body)).to_pylist()
                    strict = root / 'strict.parquet'
                    strict.write_bytes(body)
                    cur = con.execute(f"SELECT * FROM read_parquet('{strict}')")
                    assert [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()] == wanted
                else:
                    got = exact_rows(body, fmt)
                assert got == wanted, (source, args, fmt, dest, got, wanted)

    for args, wanted in (([], typed_table.to_pylist()), (['--limit', '2'], typed_table.to_pylist()[:2]),
                         (['--limit', '0'], [])):
        destinations(typed, args, wanted)
    destinations('--query', [f"SELECT * FROM '{typed}' LIMIT 2 OFFSET 1"], typed_table.to_pylist()[1:])
    wide_sum = 2**64 - 3
    scalar = [{'s': wide_sum, 'd': Decimal('-1.10')}]
    for args, wanted in (([], scalar), (['--limit', '1'], scalar), (['--limit', '0'], [])):
        destinations(typed, ['--aggregate', 'sum(wide) AS s, sum(d) AS d', *args], wanted, ('ndjson', 'json'))
    destinations('--query', [f"SELECT sum(wide) AS s, sum(d) AS d FROM '{typed}' LIMIT 1 OFFSET 1"], [])
    for args, wanted in (([], [{'d': Decimal('-1.10')}]), (['--limit', '1'], [{'d': Decimal('-1.10')}]),
                         (['--limit', '0'], [])):
        destinations(typed, ['--aggregate', 'sum(d) AS d', *args], wanted)
    for args in ([], ['--limit', '1']):
        for dest in (None, '-', str(root / 'wide-rejected')):
            flags = [] if dest is None else ['-o', dest]
            if dest not in (None, '-'):
                Path(dest).write_bytes(b'earlier')
            p = run(typed, '--aggregate', 'sum(wide) AS s', *args, '--format', 'parquet', *flags, code=2)
            assert b'AggIntTooWide' in p.stderr and not p.stdout
            assert dest in (None, '-') or Path(dest).read_bytes() == b'earlier'


    # Unlimited passthrough retains nested trees and INT96. The limited row encoder
    # must refuse those representations without replacing an earlier file.
    nested, int96 = root / 'nested.parquet', root / 'int96.parquet'
    nested_table = pa.table({'a': pa.array([{'v': 1}, None, {'v': 3}], pa.struct([('v', pa.int64())])),
                            'b': pa.array([{'v': 10}, {'v': None}, {'v': 30}], pa.struct([('v', pa.int64())]))})
    pq.write_table(nested_table, nested)
    ts_table = pa.table({'t': pa.array([datetime.datetime(2020, 1, 1), None, datetime.datetime(1999, 12, 31)],
                                     pa.timestamp('ns'))})
    pq.write_table(ts_table, int96, use_deprecated_int96_timestamps=True)
    for source, table_, reason in ((nested, nested_table, b'nested re-encoding'),
                                   (int96, ts_table, b'INT96 re-encoding')):
        text_rows = ([{'a.v': 1, 'b.v': 10}, {'a.v': None, 'b.v': None}, {'a.v': 3, 'b.v': 30}]
                     if source == nested else [{'t': 1577836800000000000}, {'t': None}, {'t': 946598400000000000}])
        for args, wanted in (([], text_rows), (['--limit', '2'], text_rows[:2]), (['--limit', '0'], [])):
            destinations(source, args, wanted, ('ndjson', 'json'))
        destinations('--query', [f"SELECT * FROM '{source}' LIMIT 2 OFFSET 1"], text_rows[1:], ('ndjson', 'json'))
        for dest in (None, '-', str(root / 'passthrough')):
            flags = [] if dest is None else ['-o', dest]
            p = run(source, '--format', 'parquet', *flags)
            body = p.stdout if dest in (None, '-') else Path(dest).read_bytes()
            assert pq.read_table(io.BytesIO(body)).to_pylist() == table_.to_pylist()
            strict = root / 'strict.parquet'
            strict.write_bytes(body)
            assert con.execute(f"SELECT count(*) FROM read_parquet('{strict}')").fetchone()[0] == 3
        for args in (['--limit', '2'], ['--limit', '0']):
            for dest in (None, '-', str(root / 'rejected')):
                flags = [] if dest is None else ['-o', dest]
                if dest not in (None, '-'):
                    Path(dest).write_bytes(b'earlier')
                p = run(source, *args, '--format', 'parquet', *flags, code=2)
                assert reason in p.stderr and not p.stdout
                assert dest in (None, '-') or Path(dest).read_bytes() == b'earlier'
        for dest in (None, '-', str(root / 'rejected')):
            flags = [] if dest is None else ['-o', dest]
            p = run('--query', f"SELECT * FROM '{source}' LIMIT 2 OFFSET 1", '--format', 'parquet',
                    *flags, code=2)
            assert reason in p.stderr and not p.stdout

    for args in (['--format', 'ndjson'], ['--format', 'json'], ['--limit', '1']):
        p = run(src, '-o', str(root / 'bloom'), '--bloom-filter', 'k', *args, code=2)
        assert b'--bloom-filter' in p.stderr
    for sql in (f"SELECT * FROM '{src}'; SELECT * FROM '{src}'",
                f"WITH x AS (SELECT * FROM '{src}') SELECT * FROM x",
                f"SELECT * FROM '{src}' WHERE count(*) > 0"):
        p = run('--query', sql, '--summary=json', code=2)
        assert json.loads(p.stderr)['type'] == 'error'
    p = subprocess.run([binary, 'schema', '--summary=json'], capture_output=True)
    assert p.returncode != 0 and p.stderr
    p = run('--query', '--summary=json', code=2)
    assert p.stderr and not p.stderr.startswith(b'{"type":')
    master, slave = pty.openpty()
    try:
        p = subprocess.run([binary, 'query', str(src), '--format', 'parquet'], stdout=slave,
                           stderr=subprocess.PIPE)
        assert p.returncode == 2 and b'terminal' in p.stderr
    finally:
        os.close(master)
        os.close(slave)
print(f'CLI contract checks passed ({checks} invocations; pyarrow + DuckDB validation)')
