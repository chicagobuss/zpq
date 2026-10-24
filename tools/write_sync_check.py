#!/usr/bin/env python3
"""Inject sync failures to check publication, library recovery and CLI polarities (Linux/strace)."""
from pathlib import Path
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


def library_worker(root):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'python'))
    import zpq

    src, dst = root / 'source.parquet', root / 'lib.parquet'
    with zpq.Zpq() as z:
        # False must not consume the injected sync fault.
        z.run({'inputs': [str(src)], 'output_url': str(dst), 'fsync': False})
        dst.write_bytes(b'old library answer')
        try:
            z.run({'inputs': [str(src)], 'output_url': str(dst), 'fsync': True})
            raise AssertionError('injected fdatasync error was missed')
        except zpq.ZpqError as e:
            assert e.response['reason'] == 'InputOutput', e.response
        assert dst.read_bytes() == b'old library answer'
        assert not list(root.glob('*.zpq-tmp'))
        z.run({'inputs': [str(src)], 'output_url': str(dst), 'fsync': True})
        assert z.run({'inputs': [str(dst)], 'aggregate': 'count(*) AS n'})['agg']['n'] == 3
        assert not list(root.glob('*.zpq-tmp'))


def main(binary):
    with tempfile.TemporaryDirectory(prefix='zpq-sync-') as d:
        root = Path(d)
        src, dst, trace = root / 'source.parquet', root / 'answer.parquet', root / 'trace'
        pq.write_table(pa.table({'k': [1, 2, 1], 'v': [3, 4, 5]}), src)
        inject = ['strace', '-f', '-qq', '-e', 'inject=fdatasync:error=EIO:when=1', '-o', str(trace)]
        for fmt in ('parquet', 'json', 'ndjson'):
            for old in (None, b'old result'):
                if old is None:
                    dst.unlink(missing_ok=True)
                else:
                    dst.write_bytes(old)
                p = subprocess.run([*inject, binary, 'query', str(src), '--select', 'v + 1 AS v',
                                    '--format', fmt, '--fsync', '-o', str(dst)], capture_output=True, timeout=20)
                assert p.returncode == 1 and b'InputOutput' in p.stderr and not p.stdout, (p.returncode, p.stderr)
                assert dst.read_bytes() == old if old is not None else not dst.exists()
                assert not list(root.glob('*.zpq-tmp'))
        for flags in (['--no-fsync'], ['--fsync', '--no-fsync']):
            p = subprocess.run([*inject, binary, 'query', str(src), '-o', str(dst), *flags],
                               capture_output=True, timeout=20)
            assert p.returncode == 0, p.stderr
            assert pq.read_table(dst).num_rows == 3
            assert 'fdatasync(' not in trace.read_text()
        # EINTR must retry while retaining the descriptor and staging file.
        p = subprocess.run(['strace', '-f', '-qq', '-e', 'inject=fdatasync:error=EINTR:when=1',
                            '-o', str(trace), binary, 'query', str(src), '-o', str(dst), '--fsync'],
                           capture_output=True, timeout=20)
        assert p.returncode == 0 and pq.read_table(dst).num_rows == 3, p.stderr
        p = subprocess.run([*inject, binary, 'query', str(src), '-o', '-', '--format', 'parquet', '--fsync'],
                           capture_output=True, timeout=20)
        assert p.returncode == 0, p.stderr
        assert pq.read_table(pa.BufferReader(p.stdout)).num_rows == 3
        assert 'fdatasync(' not in trace.read_text()
        p = subprocess.run(['strace', '-f', '-qq', '-e', 'trace=fdatasync,close,renameat', '-o', str(trace),
                            binary, 'query', str(src), '-o', str(dst), '--fsync'], capture_output=True, timeout=20)
        assert p.returncode == 0, p.stderr
        lines = trace.read_text()
        assert lines.index('fdatasync(') < lines.index('renameat('), lines
        p = subprocess.run([*inject, sys.executable, str(Path(__file__).resolve()), '--lib-worker', str(root)],
                           capture_output=True, timeout=20)
        assert p.returncode == 0, (p.stdout, p.stderr)
    print('Sync checks passed: injected CLI/lib EIO, preservation, cleanup/reuse, polarities, syscall order')


if __name__ == '__main__':
    if sys.argv[1:2] == ['--lib-worker']:
        library_worker(Path(sys.argv[2]))
    else:
        main(str(Path(sys.argv[1] if len(sys.argv) > 1 else 'zig-out/bin/zpq').resolve()))
