#!/usr/bin/env python3
"""Local libzpq output uses exclusive staging and preserves old files on failure."""
import os
from pathlib import Path
import stat
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'python'))
import zpq

with tempfile.TemporaryDirectory(prefix='zpq-lib-publication-') as d:
    root = Path(d)
    src, bad_src, dst = (root / n for n in ('input.parquet', 'bad.parquet', 'out"quoted.parquet'))
    table = pa.table({'k': [1, 2, 1], 'v': [3, 4, 5]})
    pq.write_table(table, src)
    bad = pa.Array.from_buffers(pa.decimal128(20, 0), 10_000,
                               [None, pa.py_buffer((10**21).to_bytes(16, 'little', signed=True) * 10_000)])
    pq.write_table(pa.table({'d': bad}), bad_src, compression='NONE', use_dictionary=False)
    con = duckdb.connect()
    with zpq.Zpq() as z:
        for j in (1, 16):
            for old in (None, b'preserve this result'):
                for request in ({'inputs': [str(bad_src)], 'filter': 'd IS NOT NULL'},
                                {'inputs': [str(src)], 'select': 'missing'}):
                    if old is None:
                        dst.unlink(missing_ok=True)
                    if old is not None:
                        dst.write_bytes(old)
                        dst.chmod(0o640)
                    try:
                        z.run(dict(request, output_url=str(dst), parallelism=j))
                        raise AssertionError('expected write failure')
                    except zpq.ZpqError:
                        pass
                    assert dst.read_bytes() == old if old is not None else not dst.exists()
                    if old is not None:
                        assert stat.S_IMODE(dst.stat().st_mode) == 0o640
                        dst.unlink()
                    assert not list(root.glob('*.zpq-tmp'))
                    assert z.run({'inputs': [str(src)], 'aggregate': 'count(*) AS n'})['agg']['n'] == 3
            for mode in (0o600, 0o444, 0o754):
                dst.chmod(0o644) if dst.exists() else None
                dst.write_bytes(b'earlier')
                dst.chmod(mode)
                response = z.run({'inputs': [str(src)], 'output_url': str(dst), 'parallelism': j})
                assert response['output'] == str(dst)
                assert stat.S_IMODE(dst.stat().st_mode) == mode
                assert pq.read_table(dst).equals(table)
                assert con.execute(f"SELECT * FROM '{dst}'").fetchall() == [(1, 3), (2, 4), (1, 5)]
                assert not list(root.glob('*.zpq-tmp'))
        alias = root / 'alias.parquet'
        os.link(src, alias)
        for path in (src, alias):
            try:
                z.run({'inputs': [str(src)], 'output_url': str(path)})
                raise AssertionError('expected alias refusal')
            except zpq.ZpqError:
                pass
            assert pq.read_table(src).equals(table)
print('libzpq publication checks passed (failure preservation, reuse, permissions, aliases, strict readers)')
