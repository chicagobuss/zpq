#!/usr/bin/env python3
"""Fail the C ABI's final response allocation: output must stay unpublished (glibc Linux)."""
from pathlib import Path
import os
import platform
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


def worker(root, existing):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'python'))
    import zpq

    src, dst = root / 'input.parquet', root / 'out"quoted.parquet'
    if existing:
        dst.write_bytes(b'earlier answer')
    with zpq.Zpq() as z:
        request = {'inputs': [str(src)], 'output_url': str(dst), 'fsync': True, 'profile': True}
        try:
            z.run(request)
            raise AssertionError('expected final ABI allocation to fail')
        except MemoryError:
            pass
        assert dst.read_bytes() == b'earlier answer' if existing else not dst.exists()
        assert not list(root.glob('*.zpq-tmp'))
        z.run(request)
        assert pq.read_table(dst).num_rows == 3
        assert z.run({'inputs': [str(src)], 'aggregate': 'count(*) AS n'})['agg']['n'] == 3
        assert not list(root.glob('*.zpq-tmp'))


def main():
    if sys.platform != 'linux' or platform.libc_ver()[0] != 'glibc':
        print('Final allocation fault check requires glibc Linux; skipped')
        return
    with tempfile.TemporaryDirectory(prefix='zpq-finalize-') as d:
        root = Path(d)
        shim = root / 'fail_finalize.so'
        source = Path(__file__).with_name('fail_finalize.c')
        subprocess.run([os.environ.get('CC', 'cc'), '-shared', '-fPIC', '-O1', '-o', str(shim),
                        str(source), '-ldl'], check=True)
        pq.write_table(pa.table({'k': [1, 2, 1]}), root / 'input.parquet')
        env = {**os.environ, 'LD_PRELOAD': str(shim)}
        for existing in (False, True):
            (root / 'out"quoted.parquet').unlink(missing_ok=True)
            p = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker', str(root), str(int(existing))],
                               env=env, capture_output=True, timeout=30)
            assert p.returncode == 0, (p.stdout, p.stderr)
    print('Final ABI allocation failure preserves absent/existing targets, cleans staging, and permits reuse')


if __name__ == '__main__':
    if sys.argv[1:2] == ['--worker']:
        worker(Path(sys.argv[2]), bool(int(sys.argv[3])))
    else:
        main()
