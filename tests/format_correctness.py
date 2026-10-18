#!/usr/bin/env python3
"""Format regressions with independent readers; invoked by cli_smoke.sh."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile

import duckdb
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


with tempfile.TemporaryDirectory(prefix='zpq-formats-') as directory:
    logical_annotations(Path(directory))
print('Format correctness regressions passed')
