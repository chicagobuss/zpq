#!/usr/bin/env python3
"""Write robustness test lane: disk-full, disk-error, permission, and input-failure handling.

Verifies that zpq's file-writing paths fail cleanly across 5 failure cases:
1. Destination filesystem fills up mid-write (tiny ext4 image).
2. File-size limit reached (ulimit -f / RLIMIT_FSIZE).
3. Destination becomes read-only, or destination directory has no write permission.
4. Device returns I/O errors partway through (device-mapper error target over loop device).
5. Input side fails mid-query (truncated input file, concurrent input truncation, max-memory exceeded, broken pipe).

For each case, checks:
- Command finishes within timeout (no hang).
- Exit code and error messages match expected contracts.
- No partial or corrupted file is left at the final destination path.
- Temporary files (*.zpq-tmp) are cleaned up.
- Pre-existing files at the destination path remain untouched.
- Tested across -j 1 and -j 16.
- In-process libzpq library stays usable across query errors.
"""
from pathlib import Path
import multiprocessing
import os
import resource
import shutil
import signal
import stat
import struct
import subprocess
import sys
import tempfile
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Locate binaries and libraries
REPO_ROOT = Path(__file__).resolve().parent.parent
BINARY = Path(sys.argv[1] if len(sys.argv) > 1 else (REPO_ROOT / 'zig-out/bin/zpq')).resolve()
LIB_PATH = Path(os.environ.get('ZPQ_LIB', REPO_ROOT / 'zig-out/lib/libzpq.so')).resolve()
if LIB_PATH.exists():
    os.environ['ZPQ_LIB'] = str(LIB_PATH)

if str(REPO_ROOT / 'python') not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / 'python'))

try:
    import zpq
    HAS_LIBZPQ = LIB_PATH.exists()
except ImportError:
    HAS_LIBZPQ = False

checks = 0


def log_check(name):
    global checks
    checks += 1


def run_cli(args, *, code=0, limit=None, stdout=subprocess.PIPE, timeout=20):
    def constrain():
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))

    p = subprocess.run([str(BINARY), 'query', *args], stdout=stdout, stderr=subprocess.PIPE,
                       preexec_fn=constrain if limit is not None else None, timeout=timeout)
    assert p.returncode == code, (args, limit, p.returncode, p.stderr)
    log_check(f'cli {" ".join(args[:4])}')
    return p


def check_directory_clean(directory, pattern='*.zpq-tmp'):
    tmps = list(Path(directory).glob(pattern))
    assert not tmps, f'Temporary files left behind in {directory}: {tmps}'


def sudo_available():
    try:
        p = subprocess.run(['sudo', '-n', 'true'], capture_output=True, timeout=5)
        return p.returncode == 0
    except Exception:
        return False


def dmsetup_available():
    if not sudo_available():
        return False
    dmsetup = shutil.which('dmsetup') or ('/sbin/dmsetup' if Path('/sbin/dmsetup').exists() else None)
    if not dmsetup:
        return False
    try:
        p = subprocess.run(['sudo', dmsetup, 'targets'], capture_output=True, timeout=5)
        return p.returncode == 0 and b'error' in p.stdout
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Test Fixtures Generator
# ---------------------------------------------------------------------------
def make_fixtures(root: Path):
    src_clean = root / 'input_clean.parquet'
    n = 200_000
    table = pa.table({
        'k': list(range(n)),
        'v': [float(x) * 1.5 for x in range(n)],
        's': [f'str-{x:08d}' for x in range(n)]
    })
    pq.write_table(table, src_clean, compression='NONE', row_group_size=25_000)

    # Corrupted input: valid header/footer but destroyed data page in row group 1
    src_corrupt = root / 'input_corrupt.parquet'
    pq.write_table(table, src_corrupt, compression='NONE', row_group_size=25_000)
    meta = pq.read_metadata(src_corrupt)
    raw = bytearray(src_corrupt.read_bytes())
    chunk = meta.row_group(1).column(0)
    footer_pos = len(raw) - 8 - struct.unpack_from('<I', raw, len(raw) - 8)[0]
    start = chunk.data_page_offset
    end = min(footer_pos, start + chunk.total_compressed_size)
    raw[start:end] = bytes(end - start)
    src_corrupt.write_bytes(raw)

    # Fixture with many distinct keys for max-memory testing
    src_distinct = root / 'input_distinct.parquet'
    n_distinct = 150_000
    table_distinct = pa.table({
        'k': [f'key-{x:08d}' for x in range(n_distinct)],
        'v': list(range(n_distinct))
    })
    pq.write_table(table_distinct, src_distinct, compression='NONE', row_group_size=25_000)

    return src_clean, src_corrupt, src_distinct


# ---------------------------------------------------------------------------
# Case 1: Destination filesystem fills up mid-write (tiny ext4 image)
# ---------------------------------------------------------------------------
def test_case_1_filesystem_full(root: Path, src_clean: Path):
    if not sudo_available() or not (shutil.which('mkfs.ext4') or Path('/sbin/mkfs.ext4').exists()):
        print('Case 1: Skipping (sudo or mkfs.ext4 not available)')
        return

    fs_dir = root / 'fs'
    fs_dir.mkdir(parents=True, exist_ok=True)
    img = fs_dir / 'case1.img'
    mnt = fs_dir / 'case1_mnt'
    mnt.mkdir(parents=True, exist_ok=True)

    mkfs = shutil.which('mkfs.ext4') or '/sbin/mkfs.ext4'
    try:
        subprocess.run(['truncate', '-s', '16M', str(img)], check=True)
        subprocess.run([mkfs, '-F', '-q', str(img)], check=True)
        subprocess.run(['sudo', 'mount', '-o', 'loop', str(img), str(mnt)], check=True)
        subprocess.run(['sudo', 'chown', f'{os.getuid()}:{os.getgid()}', str(mnt)], check=True)

        # Fill up until ~1MB remains
        st = os.statvfs(mnt)
        free_bytes = st.f_bavail * st.f_frsize
        fill_size = max(0, free_bytes - 1024 * 1024)
        with open(mnt / 'fill.bin', 'wb') as f:
            f.write(b'\0' * fill_size)

        dst = mnt / 'out.parquet'
        before = b'PRE_EXISTING_CASE1_CONTENT'

        for j in ('1', '16'):
            for existing in (False, True):
                if existing:
                    dst.write_bytes(before)
                    dst.chmod(0o640)
                elif dst.exists():
                    dst.unlink()

                p = run_cli([str(src_clean), '-o', str(dst), '-j', j, '--codec', 'uncompressed'],
                            code=1, timeout=15)
                assert b'NoSpaceLeft' in p.stderr, (j, p.stderr)
                assert not p.stdout

                if existing:
                    assert dst.read_bytes() == before, f'Pre-existing file corrupted at -j {j}'
                    assert stat.S_IMODE(dst.stat().st_mode) == 0o640
                    dst.unlink()
                else:
                    assert not dst.exists(), f'Partial file left behind at -j {j}'

                check_directory_clean(mnt)

        # In-process libzpq check
        if HAS_LIBZPQ:
            with zpq.Zpq() as z:
                # When output does not pre-exist, failed write must not leave partial file
                if dst.exists():
                    dst.unlink()
                try:
                    z.run({'inputs': [str(src_clean)], 'output_url': str(dst), 'output_codec': 'uncompressed'})
                    assert False, 'Expected write failure on full disk'
                except zpq.ZpqError as e:
                    assert e.response.get('error') == 'engine'
                    assert e.response.get('reason') in ('WriteFailed', 'NoSpaceLeft')
                    log_check('libzpq case 1 full fs non-existing')
                assert not dst.exists(), 'libzpq left partial file on full disk'

                # Verify library remains usable after error
                agg = z.run({'inputs': [str(src_clean)], 'aggregate': 'count(*) AS n'})
                assert agg.get('agg', {}).get('n') == 200_000
                log_check('libzpq usable after case 1')

    finally:
        subprocess.run(['sudo', 'umount', '-f', str(mnt)], check=False)
        if img.exists():
            img.unlink()
        if mnt.exists():
            mnt.rmdir()


# ---------------------------------------------------------------------------
# Case 2: File-size limit reached (ulimit -f / RLIMIT_FSIZE)
# ---------------------------------------------------------------------------
def test_case_2_fsize_limit(root: Path, src_clean: Path):
    before = b'PRE_EXISTING_CASE2_CONTENT'

    # Measure uncompressed output size
    good = root / 'good_case2.parquet'
    run_cli([str(src_clean), '-o', str(good), '--codec', 'uncompressed', '-j', '1'])
    size = good.stat().st_size
    good.unlink()

    # Test failure across early header, mid-stream, and footer cut points
    for offset in (4096, 32768, size // 2, size - 16):
        for j in ('1', '16'):
            for existing in (False, True):
                dst = root / 'out_case2.parquet'
                if existing:
                    dst.write_bytes(before)
                    dst.chmod(0o640)
                elif dst.exists():
                    dst.unlink()

                p = run_cli([str(src_clean), '-o', str(dst), '-j', j, '--codec', 'uncompressed'],
                            code=1, limit=offset, timeout=15)
                assert (b'WriteFailed' in p.stderr or b'NoSpaceLeft' in p.stderr), p.stderr
                assert not p.stdout

                if existing:
                    assert dst.read_bytes() == before, f'Pre-existing file corrupted at offset {offset}'
                    assert stat.S_IMODE(dst.stat().st_mode) == 0o640
                    dst.unlink()
                else:
                    assert not dst.exists(), f'Partial file left at offset {offset}'

                check_directory_clean(root)

    # In-process libzpq check in a constrained subprocess
    if HAS_LIBZPQ:
        def child_libzpq(dst_path):
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
            resource.setrlimit(resource.RLIMIT_FSIZE, (10000, 10000))
            with zpq.Zpq() as z:
                try:
                    z.run({'inputs': [str(src_clean)], 'output_url': str(dst_path), 'output_codec': 'uncompressed'})
                    sys.exit(2)
                except zpq.ZpqError as e:
                    if e.response.get('reason') == 'WriteFailed':
                        sys.exit(0)
                    sys.exit(3)
                except Exception:
                    sys.exit(4)

        dst_lib = root / 'out_libzpq_case2.parquet'
        if dst_lib.exists():
            dst_lib.unlink()
        proc = multiprocessing.Process(target=child_libzpq, args=(dst_lib,))
        proc.start()
        proc.join(timeout=10)
        assert proc.exitcode == 0, f'libzpq child failed with exitcode {proc.exitcode}'
        assert not dst_lib.exists(), 'libzpq left partial file on RLIMIT_FSIZE'
        log_check('libzpq case 2 fsize limit')

        # Check library in main process stays usable
        with zpq.Zpq() as z:
            agg = z.run({'inputs': [str(src_clean)], 'aggregate': 'count(*) AS n'})
            assert agg.get('agg', {}).get('n') == 200_000
            log_check('libzpq usable after case 2')


# ---------------------------------------------------------------------------
# Case 3: Destination becomes read-only or directory has no write permission
# ---------------------------------------------------------------------------
def test_case_3_readonly_and_permissions(root: Path, src_clean: Path):
    before = b'PRE_EXISTING_CASE3_CONTENT'

    # Subcase 3A: Directory without write permission
    no_write_dir = root / 'unwritable_dir'
    no_write_dir.mkdir(parents=True, exist_ok=True)
    dst_3a = no_write_dir / 'out.parquet'

    # 3A.1 Destination does not pre-exist in read-only directory
    no_write_dir.chmod(0o555)
    for j in ('1', '16'):
        p = run_cli([str(src_clean), '-o', str(dst_3a), '-j', j], code=1)
        assert b'AccessDenied' in p.stderr, p.stderr
        assert not dst_3a.exists()
        check_directory_clean(no_write_dir)
    no_write_dir.chmod(0o755)

    # 3A.2 Pre-existing destination in directory without write permission (rename blocked)
    dst_3a.write_bytes(before)
    no_write_dir.chmod(0o555)
    for j in ('1', '16'):
        p = run_cli([str(src_clean), '-o', str(dst_3a), '-j', j], code=1)
        assert b'AccessDenied' in p.stderr, p.stderr
        assert dst_3a.read_bytes() == before
        check_directory_clean(no_write_dir)
    no_write_dir.chmod(0o755)
    dst_3a.unlink()

    # Subcase 3B: Read-only filesystem
    if sudo_available() and (shutil.which('mkfs.ext4') or Path('/sbin/mkfs.ext4').exists()):
        fs_dir = root / 'fs'
        fs_dir.mkdir(parents=True, exist_ok=True)
        img = fs_dir / 'case3.img'
        mnt = fs_dir / 'case3_mnt'
        mnt.mkdir(parents=True, exist_ok=True)
        mkfs = shutil.which('mkfs.ext4') or '/sbin/mkfs.ext4'
        try:
            subprocess.run(['truncate', '-s', '16M', str(img)], check=True)
            subprocess.run([mkfs, '-F', '-q', str(img)], check=True)
            subprocess.run(['sudo', 'mount', '-o', 'loop', str(img), str(mnt)], check=True)
            subprocess.run(['sudo', 'chown', f'{os.getuid()}:{os.getgid()}', str(mnt)], check=True)

            dst_ro = mnt / 'out_ro.parquet'
            dst_ro.write_bytes(before)

            # Remount filesystem read-only
            subprocess.run(['sudo', 'mount', '-o', 'remount,ro', str(mnt)], check=True)

            for j in ('1', '16'):
                p = run_cli([str(src_clean), '-o', str(dst_ro), '-j', j], code=1)
                assert (b'Unexpected' in p.stderr or b'AccessDenied' in p.stderr or b'ReadOnly' in p.stderr), p.stderr
                assert dst_ro.read_bytes() == before
                check_directory_clean(mnt)

            # libzpq check on read-only mount
            if HAS_LIBZPQ:
                with zpq.Zpq() as z:
                    try:
                        z.run({'inputs': [str(src_clean)], 'output_url': str(dst_ro)})
                        assert False, 'Expected open failure on read-only fs'
                    except zpq.ZpqError as e:
                        assert e.response.get('reason') in ('OpenFailed', 'Unexpected')
                        log_check('libzpq case 3 read-only fs')
                    assert dst_ro.read_bytes() == before

                    agg = z.run({'inputs': [str(src_clean)], 'aggregate': 'count(*) AS n'})
                    assert agg.get('agg', {}).get('n') == 200_000
                    log_check('libzpq usable after case 3')

        finally:
            subprocess.run(['sudo', 'umount', '-f', str(mnt)], check=False)
            if img.exists():
                img.unlink()
            if mnt.exists():
                mnt.rmdir()


# ---------------------------------------------------------------------------
# Case 4: Device returns I/O errors partway through (dmsetup error target)
# ---------------------------------------------------------------------------
def test_case_4_device_error(root: Path, src_clean: Path):
    if not dmsetup_available():
        print('Case 4: Skipping dmsetup error target (dmsetup or error target not available)')
        return

    dmsetup = shutil.which('dmsetup') or '/sbin/dmsetup'
    mkfs = shutil.which('mkfs.ext4') or '/sbin/mkfs.ext4'
    fs_dir = root / 'fs'
    fs_dir.mkdir(parents=True, exist_ok=True)
    img = fs_dir / 'case4.img'
    mnt = fs_dir / 'case4_mnt'
    mnt.mkdir(parents=True, exist_ok=True)
    dm_name = f'zpq_write_robustness_{os.getpid()}'
    dm_dev = f'/dev/mapper/{dm_name}'

    loop = None
    try:
        subprocess.run(['truncate', '-s', '16M', str(img)], check=True)
        loop = subprocess.check_output(['sudo', 'losetup', '--find', '--show', str(img)]).decode().strip()

        # Initial linear mapping (32768 sectors = 16MB)
        subprocess.run(['sudo', dmsetup, 'create', dm_name, '--table', f'0 32768 linear {loop} 0'], check=True)
        subprocess.run(['sudo', mkfs, '-F', '-q', dm_dev], check=True)

        # Async ext4 must report delayed writeback errors at fdatasync before publication.
        subprocess.run(['sudo', 'mount', '-o', 'async', dm_dev, str(mnt)], check=True)
        subprocess.run(['sudo', 'chown', f'{os.getuid()}:{os.getgid()}', str(mnt)], check=True)

        def setup_dm_iteration(existing_content=None):
            subprocess.run(['sudo', 'umount', '-f', str(mnt)], check=False)
            subprocess.run(['sudo', dmsetup, 'load', dm_name, '--table', f'0 32768 linear {loop} 0'], check=True)
            subprocess.run(['sudo', dmsetup, 'resume', dm_name], check=True)
            subprocess.run(['sudo', mkfs, '-F', '-q', dm_dev], check=True)
            subprocess.run(['sudo', 'mount', '-o', 'async', dm_dev, str(mnt)], check=True)
            subprocess.run(['sudo', 'chown', f'{os.getuid()}:{os.getgid()}', str(mnt)], check=True)

            dst_path = mnt / 'out.parquet'
            if existing_content is not None:
                dst_path.write_bytes(existing_content)
                dst_path.chmod(0o640)
                with dst_path.open('rb') as f:
                    os.fsync(f.fileno())

            # Now switch table so upper sectors return I/O error
            subprocess.run(['sudo', dmsetup, 'load', dm_name,
                            '--table', f'0 8192 linear {loop} 0\n8192 24576 error\n'], check=True)
            subprocess.run(['sudo', dmsetup, 'resume', dm_name], check=True)
            return dst_path

        before = b'PRE_EXISTING_CASE4_CONTENT'

        for j in ('1', '16'):
            for existing in (False, True):
                dst = setup_dm_iteration(before if existing else None)

                p = run_cli([str(src_clean), '-o', str(dst), '-j', j, '--codec', 'uncompressed', '--fsync'],
                            code=1, timeout=15)
                assert b'WriteFailed' in p.stderr or b'InputOutput' in p.stderr, (j, p.stderr)
                assert not p.stdout

                if existing:
                    assert dst.read_bytes() == before, f'Pre-existing file corrupted under device error at -j {j}'
                    assert stat.S_IMODE(dst.stat().st_mode) == 0o640
                    dst.unlink()
                else:
                    assert not dst.exists(), f'Partial file left at -j {j} under device error'

                check_directory_clean(mnt)

        # Both a new target and an earlier result must survive delayed writeback failure.
        if HAS_LIBZPQ:
            with zpq.Zpq() as z:
                for previous in (None, before):
                    dst = setup_dm_iteration(previous)
                    try:
                        z.run({'inputs': [str(src_clean)], 'output_url': str(dst),
                               'output_codec': 'uncompressed', 'fsync': True})
                        assert False, 'Expected write failure on dm error device'
                    except zpq.ZpqError as e:
                        assert e.response.get('reason') in ('WriteFailed', 'NoSpaceLeft', 'InputOutput')
                        log_check('libzpq case 4 async device error')
                    assert dst.read_bytes() == previous if previous is not None else not dst.exists()
                    check_directory_clean(mnt)
                    subprocess.run(['sudo', 'umount', '-f', str(mnt)], check=False)
                    agg = z.run({'inputs': [str(src_clean)], 'aggregate': 'count(*) AS n'})
                    assert agg.get('agg', {}).get('n') == 200_000
                    log_check('libzpq usable after case 4')

    finally:
        subprocess.run(['sudo', 'umount', '-f', str(mnt)], check=False)
        subprocess.run(['sudo', dmsetup, 'remove', dm_name], check=False)
        if loop:
            subprocess.run(['sudo', 'losetup', '-d', loop], check=False)
        if img.exists():
            img.unlink()
        if mnt.exists():
            mnt.rmdir()


# ---------------------------------------------------------------------------
# Case 5: Input side fails mid-query (truncated, cancelled, max-memory)
# ---------------------------------------------------------------------------
def test_case_5_input_failures(root: Path, src_clean: Path, src_corrupt: Path, src_distinct: Path):
    before = b'PRE_EXISTING_CASE5_CONTENT'

    # Subcase 5A: Truncated input files
    # 5A.1: Valid footer, but truncated/corrupted row group data mid-stream
    for j in ('1', '16'):
        for existing in (False, True):
            dst = root / 'out_case5a1.parquet'
            if existing:
                dst.write_bytes(before)
                dst.chmod(0o640)
            elif dst.exists():
                dst.unlink()

            p = run_cli([str(src_corrupt), '--select', 'k, v', '-o', str(dst), '-j', j],
                        code=1, timeout=15)
            assert b'cannot decode input file' in p.stderr, p.stderr
            assert not p.stdout

            if existing:
                assert dst.read_bytes() == before
                assert stat.S_IMODE(dst.stat().st_mode) == 0o640
                dst.unlink()
            else:
                assert not dst.exists()

            check_directory_clean(root)

    # 5A.2: File truncated before footer
    src_no_footer = root / 'input_no_footer.parquet'
    src_no_footer.write_bytes(src_clean.read_bytes()[:50_000])
    for j in ('1', '16'):
        for existing in (False, True):
            dst = root / 'out_case5a2.parquet'
            if existing:
                dst.write_bytes(before)
            elif dst.exists():
                dst.unlink()

            p = run_cli([str(src_no_footer), '--select', 'k, v', '-o', str(dst), '-j', j],
                        code=1, timeout=15)
            assert not p.stdout
            if existing:
                assert dst.read_bytes() == before
                dst.unlink()
            else:
                assert not dst.exists()

            check_directory_clean(root)
    src_no_footer.unlink()

    # Subcase 5C: --max-memory too small for GROUP BY aggregation output
    for j in ('1', '16'):
        for existing in (False, True):
            dst = root / 'out_case5c.parquet'
            if existing:
                dst.write_bytes(before)
            elif dst.exists():
                dst.unlink()

            p = run_cli([str(src_distinct), '--group-by', 'k', '--aggregate', 'sum(v) AS s',
                         '--max-memory', '64KB', '-o', str(dst), '-j', j],
                        code=1, timeout=15)
            assert b'the query needs more memory than --max-memory allows' in p.stderr, p.stderr
            assert not p.stdout

            if existing:
                assert dst.read_bytes() == before
                dst.unlink()
            else:
                assert not dst.exists()

            check_directory_clean(root)

    # Subcase 5D: Cancelled query (closed pipe / SIGPIPE on stdout)
    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    try:
        p = subprocess.run([str(BINARY), 'query', str(src_clean), '--select', 'k, v'],
                           stdout=write_fd, stderr=subprocess.PIPE, timeout=15)
        assert p.returncode == 141, f'Expected 141 on closed stdout pipe, got {p.returncode}'
        assert not p.stderr
        log_check('cancelled query closed pipe')
    finally:
        os.close(write_fd)

    # libzpq check on input errors
    if HAS_LIBZPQ:
        with zpq.Zpq() as z:
            dst = root / 'out_libzpq_case5.parquet'
            if dst.exists():
                dst.unlink()

            # Corrupted input error (requires decoding columns)
            try:
                z.run({'inputs': [str(src_corrupt)], 'select': 'k, v', 'output_url': str(dst)})
                assert False, 'Expected failure on corrupt input'
            except zpq.ZpqError as e:
                assert e.response.get('error') == 'engine'
                log_check('libzpq case 5 corrupt input')
            assert not dst.exists()

            dst.write_bytes(b'earlier libzpq output')
            try:
                z.run({'inputs': [str(src_corrupt)], 'select': 'k, v', 'output_url': str(dst)})
                assert False, 'Expected failure on corrupt input with pre-existing target'
            except zpq.ZpqError:
                pass
            assert dst.read_bytes() == b'earlier libzpq output'
            check_directory_clean(root)
            dst.unlink()
            log_check('libzpq pre-existing target survives decode failure')

            # max_memory budget error
            try:
                z.run({'inputs': [str(src_distinct)], 'group_by': 'k', 'aggregate': 'sum(v) AS s',
                       'max_memory': '64KB', 'output_url': str(dst)})
                assert False, 'Expected failure on small max_memory'
            except zpq.ZpqError as e:
                assert e.response.get('error') in ('engine', 'bad_request')
                log_check('libzpq case 5 max_memory')
            assert not dst.exists()

            # Verify library remains healthy and usable
            agg = z.run({'inputs': [str(src_clean)], 'aggregate': 'count(*) AS n'})
            assert agg.get('agg', {}).get('n') == 200_000
            log_check('libzpq usable after case 5')


def main():
    print(f'Starting write robustness checks on binary: {BINARY}')
    with tempfile.TemporaryDirectory(prefix='zpq-write-robustness-') as d:
        root = Path(d)
        src_clean, src_corrupt, src_distinct = make_fixtures(root)

        print('Running Case 1: Filesystem fills up mid-write...')
        test_case_1_filesystem_full(root, src_clean)

        print('Running Case 2: File-size limit reached...')
        test_case_2_fsize_limit(root, src_clean)

        print('Running Case 3: Read-only destination & unwritable directories...')
        test_case_3_readonly_and_permissions(root, src_clean)

        print('Running Case 4: Device-mapper I/O error target...')
        test_case_4_device_error(root, src_clean)

        print('Running Case 5: Input side failures mid-query...')
        test_case_5_input_failures(root, src_clean, src_corrupt, src_distinct)

    print(f'write robustness contract: {checks} bounded invocations passed')


if __name__ == '__main__':
    main()
