#!/usr/bin/env python3
"""Bounded writer failure probes: exit status, atomic publication and error cleanup."""
from pathlib import Path
import resource
import signal
import stat
import struct
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

binary = str(Path(sys.argv[1] if len(sys.argv) > 1 else 'zig-out/bin/zpq').resolve())
checks = 0


def run(args, *, code=0, limit=None, stdout=subprocess.PIPE):
    global checks

    def constrain():
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))

    p = subprocess.run([binary, 'query', *args], stdout=stdout, stderr=subprocess.PIPE,
                       preexec_fn=constrain if limit is not None else None, timeout=20)
    assert p.returncode == code, (args, limit, p.returncode, p.stderr)
    checks += 1
    return p


with tempfile.TemporaryDirectory(prefix='zpq-write-failure-') as d:
    root = Path(d)
    src = root / 'input.parquet'
    n = 100_000
    table = pa.table({'group': pa.array([i % 1000 for i in range(n)], type=pa.int32()),
                      'value': [9900.01 + i / 1000 for i in range(n)]})
    pq.write_table(table, src, compression='zstd', row_group_size=25_000)
    base = [str(src), '--filter', 'value > 9900', '--select',
            'group,value,value*1.19 AS gross,value*value AS squared', '--codec', 'zstd',
            '--row-group-size', '8192']
    con = duckdb.connect(config={'threads': 2, 'memory_limit': '128MB'})
    before = b'old result must remain'
    for memory in ([], ['--max-memory', '64MB']):
        for j in ('1', '4', '16'):
            args = [*base, *memory, '-j', j]
            good = root / 'good.parquet'
            run([*args, '-o', str(good)])
            assert pq.read_table(good).num_rows == n
            assert con.execute(f"SELECT count(*) FROM read_parquet('{good}')").fetchone()[0] == n
            body = good.read_bytes()
            footer = len(body) - 8 - struct.unpack('<I', body[-8:-4])[0]
            # Fail the first page, an early chunk, a later row group and the footer itself.
            for offset in (8, 32768, len(body) // 2, footer + 1):
                for existing in (False, True):
                    dst = root / 'failed.parquet'
                    if existing:
                        dst.write_bytes(before)
                        dst.chmod(0o640)
                    p = run([*args, '-o', str(dst)], code=1, limit=offset)
                    assert not p.stdout and p.stderr, (offset, p.stderr)
                    assert dst.read_bytes() == before if existing else not dst.exists()
                    if existing:
                        assert stat.S_IMODE(dst.stat().st_mode) == 0o640
                        dst.unlink()
                    assert not list(root.glob('*.zpq-tmp'))
            if Path('/dev/full').exists():
                with open('/dev/full', 'wb', buffering=0) as full:
                    p = run([*args, '-o', '-', '--format', 'parquet'], code=1, stdout=full)
                    assert p.stderr
                # Byte-copy failures use the same publication contract as filtered encodes.
                with open('/dev/full', 'wb', buffering=0) as full:
                    p = run([str(src), '-j', j, '-o', '-', '--format', 'parquet'], code=1, stdout=full)
                    assert p.stderr
    con.close()

    # A deliberately invalid decimal reaches the encoder's precision guard. Its physical 9-byte value fits,
    # but its magnitude exceeds DECIMAL(20,0); this exercises an encode error after the pipeline starts.
    bad = pa.Array.from_buffers(pa.decimal128(20, 0), n,
                               [None, pa.py_buffer((10**21).to_bytes(16, 'little', signed=True) * n)])
    bad_src = root / 'invalid-decimal.parquet'
    pq.write_table(pa.table({'d': bad}), bad_src, compression='NONE', use_dictionary=False,
                   row_group_size=8192)
    for j in ('1', '4', '16'):
        for existing in (False, True):
            dst = root / 'bad-decimal.parquet'
            if existing:
                dst.write_bytes(before)
            p = run([str(bad_src), '--scan-all', '--filter', 'd IS NOT NULL', '--codec', 'zstd',
                     '--row-group-size', '1024', '-j', j, '-o', str(dst)], code=1)
            assert b'exceeding its declared precision' in p.stderr, p.stderr
            assert dst.read_bytes() == before if existing else not dst.exists()
            if existing:
                dst.unlink()
            assert not list(root.glob('*.zpq-tmp'))
print(f'write failure contract: {checks} bounded invocations passed')
