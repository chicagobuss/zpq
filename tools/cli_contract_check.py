#!/usr/bin/env python3
"""CLI channels, destinations, limits, and publication checked with independent readers."""
import io
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
    master, slave = pty.openpty()
    try:
        p = subprocess.run([binary, 'query', str(src), '--format', 'parquet'], stdout=slave,
                           stderr=subprocess.PIPE)
        assert p.returncode == 2 and b'terminal' in p.stderr
    finally:
        os.close(master)
        os.close(slave)
print(f'CLI contract checks passed ({checks} invocations; pyarrow + DuckDB validation)')
