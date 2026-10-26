#!/usr/bin/env python3
"""OS-level contracts of zpq's CLI and libzpq: publication, permissions, terminals and pipes, S3 uploads, sync and
write faults. Everything a query answers lives in tools/check/cases; this file holds only what needs the operating
system staged around the process.

  contracts.py ZPQ_BIN [--lib LIBZPQ_SO] [--only NAME] [--privileged]

Each contract is a function named for what it pins. A contract may skip only for a named need in NEEDS; any other
skip, and any failure, exits 1. Runs in the full tier. The filesystem-fault contracts mount loop devices and
device-mapper targets, a change to the host: they run only with --privileged, on a dedicated host with passwordless
sudo, and otherwise skip by name.
"""
import argparse
import multiprocessing
import os
from pathlib import Path
import pty
import re
import resource
import shutil
import signal
import stat
import struct
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[2]
NEEDS = {
    "privileged": "loop-mounted filesystems and device-mapper error targets change the host: run with --privileged",
    "sudo": "--privileged needs passwordless sudo",
    "strace": "syscall tracing and fault injection need strace",
    "libzpq": "library recovery needs libzpq.so (zig build libzpq)",
}
EARLIER = b"earlier result must remain"
CONTRACTS = []
ZPQ = LIB = None
PRIVILEGED = False


class Skip(Exception):
    def __init__(self, need):
        assert need in NEEDS, f"unnamed skip: {need}"
        super().__init__(need)


def contract(fn):
    CONTRACTS.append(fn)
    return fn


def run(*args, code=0, limit=None, stdout=subprocess.PIPE, env=None, cmd="query", timeout=30):
    def constrain():
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))

    p = subprocess.run([ZPQ, cmd, *map(str, args)], stdout=stdout, stderr=subprocess.PIPE, env=env, timeout=timeout,
                       preexec_fn=constrain if limit is not None else None)
    assert p.returncode == code, (args, p.returncode, p.stderr[-2000:])
    return p


def no_staging(directory):
    left = list(Path(directory).glob("*.zpq-tmp"))
    assert not left, f"staging files left behind: {left}"


def refused_write(dst, *args, code=1, mode=None, needle=None, **kw):
    """A failed write leaves no new file, or the earlier one byte for byte (and its mode), and no staging file."""
    for existing in (False, True):
        if existing:
            dst.write_bytes(EARLIER)
            if mode is not None:
                dst.chmod(mode)
        elif dst.exists():
            dst.unlink()
        p = run(*args, "-o", dst, code=code, **kw)
        assert not p.stdout, p.stdout[:200]
        if needle is not None:
            assert needle in p.stderr, p.stderr[-2000:]
        if existing:
            assert dst.read_bytes() == EARLIER, f"{args}: earlier target replaced"
            if mode is not None:
                assert stat.S_IMODE(dst.stat().st_mode) == mode
            dst.unlink()
        else:
            assert not dst.exists(), f"{args}: partial output left"
        no_staging(dst.parent)


def library():
    if LIB is None:
        raise Skip("libzpq")
    os.environ["ZPQ_LIB"] = str(LIB)
    sys.path.insert(0, str(REPO / "python"))
    import zpq
    return zpq


def fixtures(root):
    n = 200_000
    table = pa.table({"k": list(range(n)), "v": [x * 1.5 for x in range(n)], "s": [f"str-{x:08d}" for x in range(n)]})
    clean = root / "clean.parquet"
    pq.write_table(table, clean, compression="NONE", row_group_size=25_000)
    corrupt = root / "corrupt.parquet"
    shutil.copy(clean, corrupt)
    meta = pq.read_metadata(corrupt)
    raw = bytearray(corrupt.read_bytes())
    chunk = meta.row_group(1).column(0)
    footer = len(raw) - 8 - struct.unpack_from("<I", raw, len(raw) - 8)[0]
    end = min(footer, chunk.data_page_offset + chunk.total_compressed_size)
    raw[chunk.data_page_offset:end] = bytes(end - chunk.data_page_offset)
    corrupt.write_bytes(raw)
    distinct = root / "distinct.parquet"
    pq.write_table(pa.table({"k": [f"key-{x:08d}" for x in range(150_000)], "v": list(range(150_000))}), distinct,
                   compression="NONE", row_group_size=25_000)
    small = root / "small.parquet"
    pq.write_table(pa.table({"k": [1, 2, 1], "v": [3, 4, 5]}), small)
    return clean, corrupt, distinct, small


@contract
def refused_queries_keep_the_earlier_target(root, clean, corrupt, distinct, small):
    dst = root / "out.parquet"
    for args in (["--aggregate", "sum(missing)"], ["--select", "missing"],
                 ["--group-by", "missing", "--aggregate", "count(*)"]):
        refused_write(dst, small, *args, code=2)
    refused_write(dst, corrupt, "--select", "k, v", needle=b"cannot decode input file", mode=0o640)
    truncated = root / "no_footer.parquet"
    truncated.write_bytes(clean.read_bytes()[:50_000])
    for j in ("1", "16"):
        refused_write(dst, truncated, "--select", "k, v", "-j", j)
        refused_write(dst, distinct, "--group-by", "k", "--aggregate", "sum(v) AS s", "--max-memory", "64KB", "-j", j,
                      needle=b"the query needs more memory than --max-memory allows")
    for fmt in ("ndjson", "json", "parquet"):
        p = run(corrupt, "--select", "v + 0 AS v", "-o", dst, "--format", fmt, "--summary=json", code=1)
        assert b'"type":"error"' in p.stderr and b'"type":"summary"' not in p.stderr
    # An encode error after the pipeline starts: a 9-byte value past DECIMAL(20, 0)'s precision.
    n = 100_000
    bad = root / "bad_decimal.parquet"
    pq.write_table(pa.table({"d": pa.Array.from_buffers(pa.decimal128(20, 0), n, [None, pa.py_buffer(
        (10**21).to_bytes(16, "little", signed=True) * n)])}), bad, compression="NONE", use_dictionary=False,
        row_group_size=8192)
    for j in ("1", "4", "16"):
        refused_write(dst, bad, "--scan-all", "--filter", "d IS NOT NULL", "--codec", "zstd", "--row-group-size",
                      "1024", "-j", j, needle=b"exceeding its declared precision")
    # A wide sum and a limited row encoder refuse before replacing anything.
    typed = root / "typed.parquet"
    pq.write_table(pa.table({"wide": pa.array([2**63 - 1, 2**63 - 2, None], pa.int64())}), typed)
    for args in ([], ["--limit", "1"]):
        refused_write(dst, typed, "--aggregate", "sum(wide) AS s", *args, "--format", "parquet",
                      needle=b"AggIntTooWide")
    int96 = root / "int96.parquet"
    pq.write_table(pa.table({"t": pa.array([1, None, 3], pa.timestamp("ns"))}), int96,
                   use_deprecated_int96_timestamps=True)
    nested = root / "nested.parquet"
    pq.write_table(pa.table({"a": pa.array([{"v": 1}, None], pa.struct([("v", pa.int64())]))}), nested)
    for src, reason in ((nested, b"nested re-encoding"), (int96, b"INT96 re-encoding")):
        for limit in ("2", "0"):
            refused_write(dst, src, "--limit", limit, "--format", "parquet", code=2, needle=reason)


@contract
def an_input_is_never_its_own_output(root, clean, corrupt, distinct, small):
    copy = root / "self.parquet"
    shutil.copy(small, copy)
    run(copy, "-o", copy, code=2)
    assert pq.read_table(copy).num_rows == 3
    alias = root / "alias.parquet"
    os.link(copy, alias)
    assert not run(copy, "-o", alias, code=2).stdout
    assert b"input and output" in run("s3://bucket/input", "-o", "s3://bucket/input", code=2).stderr


@contract
def publication_keeps_the_mode_and_replaces_a_symlink(root, clean, corrupt, distinct, small):
    dst = root / "out.parquet"
    for mode in (0o600, 0o444, 0o754):
        dst.chmod(0o644) if dst.exists() else None
        dst.write_bytes(EARLIER)
        dst.chmod(mode)
        assert not run(small, "-o", dst).stdout
        assert stat.S_IMODE(dst.stat().st_mode) == mode and pq.read_table(dst).num_rows == 3
    referent = root / "referent"
    referent.write_bytes(b"referent unchanged")
    referent.chmod(0o600)
    link = root / "link.parquet"
    link.symlink_to(referent)
    assert not run(small, "-o", link).stdout and not link.is_symlink()
    assert referent.read_bytes() == b"referent unchanged" and stat.S_IMODE(link.stat().st_mode) == 0o600
    assert pq.read_table(link).num_rows == 3


@contract
def a_file_size_limit_fails_the_write_at_every_point(root, clean, corrupt, distinct, small):
    n = 100_000
    src = root / "groups.parquet"
    pq.write_table(pa.table({"group": pa.array([i % 1000 for i in range(n)], type=pa.int32()),
                             "value": [9900.01 + i / 1000 for i in range(n)]}), src, compression="zstd",
                   row_group_size=25_000)
    base = [src, "--filter", "value > 9900", "--select", "group,value,value*1.19 AS gross,value*value AS squared",
            "--codec", "zstd", "--row-group-size", "8192"]
    con = duckdb.connect(config={"threads": 2, "memory_limit": "128MB"})
    for memory in ([], ["--max-memory", "64MB"]):
        for j in ("1", "4", "16"):
            good = root / "good.parquet"
            run(*base, *memory, "-j", j, "-o", good)
            assert con.execute(f"SELECT count(*) FROM read_parquet('{good}')").fetchone()[0] == n
            body = good.read_bytes()
            footer = len(body) - 8 - struct.unpack("<I", body[-8:-4])[0]
            # The first page, an early chunk, a later row group and the footer itself.
            for offset in (8, 32768, len(body) // 2, footer + 1):
                refused_write(root / "failed.parquet", *base, *memory, "-j", j, limit=offset, mode=0o640)
    con.close()
    # The byte copy path, unfiltered and uncompressed, under the same limit.
    whole = root / "whole.parquet"
    run(clean, "-o", whole, "--codec", "uncompressed", "-j", "1")
    size = whole.stat().st_size
    for offset in (4096, size // 2, size - 16):
        for j in ("1", "16"):
            refused_write(root / "copy.parquet", clean, "-j", j, "--codec", "uncompressed", limit=offset,
                          mode=0o640)


@contract
def library_keeps_no_partial_file_under_a_size_limit(root, clean, corrupt, distinct, small):
    zpq = library()
    dst = root / "lib.parquet"

    def child():
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        resource.setrlimit(resource.RLIMIT_FSIZE, (10000, 10000))
        with zpq.Zpq() as z:
            try:
                z.run({"inputs": [str(clean)], "output_url": str(dst), "output_codec": "uncompressed"})
            except zpq.ZpqError as e:
                sys.exit(0 if e.response.get("reason") == "WriteFailed" else 3)
            sys.exit(2)

    proc = multiprocessing.Process(target=child)
    proc.start()
    proc.join(timeout=20)
    assert proc.exitcode == 0 and not dst.exists(), proc.exitcode
    with zpq.Zpq() as z:
        for previous in (None, EARLIER):
            if previous:
                dst.write_bytes(previous)
            try:
                z.run({"inputs": [str(corrupt)], "select": "k, v", "output_url": str(dst)})
                raise AssertionError("a corrupt input wrote an answer")
            except zpq.ZpqError as e:
                assert e.response.get("error") == "engine", e.response
            assert dst.read_bytes() == previous if previous else not dst.exists()
            dst.unlink(missing_ok=True)
        try:
            z.run({"inputs": [str(distinct)], "group_by": "k", "aggregate": "sum(v) AS s", "max_memory": "64KB",
                   "output_url": str(dst)})
            raise AssertionError("a 64KB budget answered")
        except zpq.ZpqError as e:
            assert e.response.get("error") in ("engine", "bad_request"), e.response
        assert not dst.exists()
        assert z.run({"inputs": [str(clean)], "aggregate": "count(*) AS n"})["agg"]["n"] == 200_000
    no_staging(root)


def strace(*args):
    if not shutil.which("strace"):
        raise Skip("strace")
    return ["strace", "-f", "-qq", *args]


@contract
def publication_stages_exclusively_and_renames_last(root, clean, corrupt, distinct, small):
    trace, dst = root / "trace", root / "out.parquet"
    dst.write_bytes(EARLIER)
    p = subprocess.run([*strace("-e", "trace=openat,close,fchmod,renameat", "-o", trace), ZPQ, "query", small,
                        "-o", dst], capture_output=True, timeout=30)
    assert p.returncode == 0 and not p.stdout, p.stderr
    calls = [re.sub(r"^\d+ +", "", line) for line in trace.read_text().splitlines()]  # -f prefixes thread ids
    opens = [line for line in calls if line.startswith("openat(") and '.zpq-tmp"' in line]
    assert len(opens) == 1 and "O_EXCL" in opens[0] and "O_TRUNC" not in opens[0], opens
    fd = re.search(r"= (\d+)$", opens[0]).group(1)
    assert any(line.startswith(f"fchmod({fd},") for line in calls)
    assert pq.read_table(dst).num_rows == 3
    dst.write_bytes(EARLIER)
    p = subprocess.run([*strace("-e", "inject=renameat:error=EIO:when=1", "-o", trace), ZPQ, "query", small,
                        "-o", dst], capture_output=True, timeout=30)
    assert p.returncode == 1 and not p.stdout and dst.read_bytes() == EARLIER, p.stderr
    no_staging(root)


@contract
def a_failed_sync_is_an_error_and_publishes_nothing(root, clean, corrupt, distinct, small):
    trace, dst = root / "trace", root / "sync.parquet"
    inject = strace("-e", "inject=fdatasync:error=EIO:when=1", "-o", trace)
    for fmt in ("parquet", "json", "ndjson"):
        for old in (None, EARLIER):
            dst.write_bytes(old) if old else dst.unlink(missing_ok=True)
            p = subprocess.run([*inject, ZPQ, "query", small, "--select", "v + 1 AS v", "--format", fmt, "-o", dst],
                               capture_output=True, timeout=30)
            assert p.returncode == 1 and b"InputOutput" in p.stderr and not p.stdout, p.stderr
            assert dst.read_bytes() == old if old else not dst.exists()
            no_staging(root)
    for flags in (["--no-fsync"], ["--fsync", "--no-fsync"]):
        assert subprocess.run([*inject, ZPQ, "query", small, "-o", dst, *flags], timeout=30).returncode == 0
        assert pq.read_table(dst).num_rows == 3 and "fdatasync(" not in trace.read_text()
    # EINTR retries with the descriptor and staging file kept.
    p = subprocess.run([*strace("-e", "inject=fdatasync:error=EINTR:when=1", "-o", trace), ZPQ, "query", small,
                        "-o", dst, "--fsync"], capture_output=True, timeout=30)
    assert p.returncode == 0 and pq.read_table(dst).num_rows == 3, p.stderr
    p = subprocess.run([*inject, ZPQ, "query", small, "-o", "-", "--format", "parquet", "--fsync"],
                       capture_output=True, timeout=30)
    assert p.returncode == 0 and pq.read_table(pa.BufferReader(p.stdout)).num_rows == 3
    assert "fdatasync(" not in trace.read_text()
    subprocess.run([*strace("-e", "trace=fdatasync,close,renameat", "-o", trace), ZPQ, "query", small, "-o", dst,
                    "--fsync"], check=True, capture_output=True, timeout=30)
    lines = trace.read_text()
    assert lines.index("fdatasync(") < lines.index("renameat("), lines


@contract
def library_recovers_after_a_failed_sync(root, clean, corrupt, distinct, small):
    library()
    p = subprocess.run([*strace("-e", "inject=fdatasync:error=EIO:when=1", "-o", root / "trace"), sys.executable,
                        __file__, "--lib-sync-worker", root, "--lib", LIB], capture_output=True, timeout=60)
    assert p.returncode == 0, (p.stdout, p.stderr)


def lib_sync_worker(root):
    zpq = library()
    src, dst = root / "small.parquet", root / "lib.parquet"
    with zpq.Zpq() as z:
        z.run({"inputs": [str(src)], "output_url": str(dst), "fsync": False})  # must not consume the fault
        dst.write_bytes(b"old library answer")
        try:
            z.run({"inputs": [str(src)], "output_url": str(dst)})
            raise AssertionError("injected fdatasync error was missed")
        except zpq.ZpqError as e:
            assert e.response["reason"] == "InputOutput", e.response
        assert dst.read_bytes() == b"old library answer"
        no_staging(root)
        z.run({"inputs": [str(src)], "output_url": str(dst), "fsync": True})
        assert z.run({"inputs": [str(dst)], "aggregate": "count(*) AS n"})["agg"]["n"] == 3


@contract
def an_s3_write_puts_one_object(root, clean, corrupt, distinct, small):
    uploaded = []

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):
            uploaded.append((self.path, self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env = {**os.environ, "S3_NO_SIGN_REQUEST": "1", "AWS_REGION": "us-east-1",
               "S3_ENDPOINT_URL": f"http://127.0.0.1:{server.server_port}"}
        assert not run(small, "--select", "v + 1 AS v", "-o", "s3://test/output.parquet", env=env).stdout
        assert len(uploaded) == 1 and uploaded[0][0] == "/test/output.parquet", uploaded
        (root / "remote.parquet").write_bytes(uploaded[0][1])
        assert pq.read_table(root / "remote.parquet").column("v").to_pylist() == [4, 5, 6]
        no_staging(root)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@contract
def a_full_device_fails_and_a_closed_pipe_exits_quietly(root, clean, corrupt, distinct, small):
    for args in ([small, "--aggregate", "count(*)"], [clean, "-o", "-", "--format", "parquet"],
                 [clean, "--select", "k, v * 2 AS w", "-o", "-", "--format", "parquet"]):
        with open("/dev/full", "wb", buffering=0) as full:
            p = run(*args, code=1, stdout=full)
        assert b"NoSpaceLeft" in p.stderr if args[0] == small else p.stderr, (args, p.stderr)
    for args in ([small, "--aggregate", "count(*)"], [clean, "--select", "k, v"]):
        read_fd, write_fd = os.pipe()
        os.close(read_fd)
        try:
            p = run(*args, code=141, stdout=write_fd)
        finally:
            os.close(write_fd)
        assert not p.stderr, p.stderr


@contract
def binary_output_refuses_a_terminal(root, clean, corrupt, distinct, small):
    master, slave = pty.openpty()
    try:
        p = run(small, "--format", "parquet", code=2, stdout=slave)
        assert b"terminal" in p.stderr
    finally:
        os.close(master)
        os.close(slave)


@contract
def an_unwritable_directory_is_access_denied(root, clean, corrupt, distinct, small):
    locked = root / "locked"
    locked.mkdir()
    dst = locked / "out.parquet"
    for existing in (False, True):
        locked.chmod(0o755)
        if existing:
            dst.write_bytes(EARLIER)
        locked.chmod(0o555)
        try:
            for j in ("1", "16"):
                assert b"AccessDenied" in run(clean, "-o", dst, "-j", j, code=1).stderr
                assert dst.read_bytes() == EARLIER if existing else not dst.exists()
                no_staging(locked)
        finally:
            locked.chmod(0o755)


@contract
def a_profile_trace_publishes_only_when_complete(root, clean, corrupt, distinct, small):
    """The answer precedes the summary; the trace path is checked before the query runs; a trace replaces what was at
    its path only once complete; a failed query leaves no trace and an earlier one untouched."""
    trace = root / "trace.json"
    trace.write_text("earlier\n")
    p = subprocess.run([ZPQ, "query", small, "--aggregate", "count(*) AS n", "--profile", trace, "--profile-summary"],
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
    both = p.stdout.decode()
    assert 0 <= both.find('{"n":') < both.find("zpq profile:") and "rss peak" in both, both
    body = __import__("json").loads(trace.read_text())
    assert body["traceEvents"] and body["zpq"]["omitted_events"] == 0, body["zpq"]
    never = root / "prof_never.parquet"
    p = run(small, "-o", never, "--profile", root / "no/such/dir/t.json", code=2)
    assert not never.exists() and not p.stdout and b"cannot write profile" in p.stderr, p
    kept = root / "prof_kept.json"
    kept.write_text("earlier\n")
    for target in (root / "prof_failed.json", kept):
        run(small, "--aggregate", "sum(nope) AS n", "--profile", target, code=2)
    assert not (root / "prof_failed.json").exists() and kept.read_text() == "earlier\n"
    (root / "prof_dir").mkdir()
    out = root / "prof_dir_out.parquet"
    out.write_text("earlier\n")
    assert not run(small, "-o", out, "--profile", root / "prof_dir", code=2).stdout and out.read_text() == "earlier\n"
    run(small, "-o", root / "prof_dev_out.parquet", "--profile", "/dev/null", code=2)
    assert not (root / "prof_dev_out.parquet").exists()
    no_staging(root)


def sudo(*args):
    if not PRIVILEGED:
        raise Skip("privileged")
    if subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode != 0:
        raise Skip("sudo")
    subprocess.run(["sudo", *map(str, args)], check=True, capture_output=True)


class Loop:
    """A 16 MiB ext4 image mounted at `mnt`, optionally behind a device-mapper table whose upper sectors fail."""

    def __init__(self, root, name, error_target=False):
        self.img, self.mnt = root / f"{name}.img", root / f"{name}_mnt"
        self.dm, self.loop = (f"zpq_contract_{os.getpid()}_{name}" if error_target else None), None

    def __enter__(self):
        sudo("true")  # skips by name before anything touches the host
        self.mnt.mkdir()
        subprocess.run(["truncate", "-s", "16M", self.img], check=True)
        if self.dm:
            self.loop = subprocess.check_output(["sudo", "losetup", "--find", "--show", self.img]).decode().strip()
            sudo("dmsetup", "create", self.dm, "--table", f"0 32768 linear {self.loop} 0")
            self.dev = f"/dev/mapper/{self.dm}"
            sudo("mkfs.ext4", "-F", "-q", self.dev)
            sudo("mount", "-o", "async", self.dev, self.mnt)
        else:
            subprocess.run(["mkfs.ext4", "-F", "-q", self.img], check=True, capture_output=True)
            sudo("mount", "-o", "loop", self.img, self.mnt)
        sudo("chown", f"{os.getuid()}:{os.getgid()}", self.mnt)
        return self

    def fail_upper_sectors(self):
        sudo("dmsetup", "load", self.dm, "--table", f"0 8192 linear {self.loop} 0\n8192 24576 error\n")
        sudo("dmsetup", "resume", self.dm)

    def __exit__(self, *_):
        subprocess.run(["sudo", "umount", "-f", self.mnt], capture_output=True)
        if self.dm:
            subprocess.run(["sudo", "dmsetup", "remove", self.dm], capture_output=True)
        if self.loop:
            subprocess.run(["sudo", "losetup", "-d", self.loop], capture_output=True)
        self.img.unlink(missing_ok=True)
        self.mnt.rmdir()


@contract
def a_full_filesystem_keeps_the_earlier_target(root, clean, corrupt, distinct, small):
    with Loop(root, "full") as fs:
        st = os.statvfs(fs.mnt)
        (fs.mnt / "fill.bin").write_bytes(b"\0" * max(0, st.f_bavail * st.f_frsize - (1 << 20)))
        for j in ("1", "16"):
            refused_write(fs.mnt / "out.parquet", clean, "-j", j, "--codec", "uncompressed", needle=b"NoSpaceLeft",
                          mode=0o640)


@contract
def a_read_only_filesystem_keeps_the_earlier_target(root, clean, corrupt, distinct, small):
    with Loop(root, "readonly") as fs:
        dst = fs.mnt / "out.parquet"
        dst.write_bytes(EARLIER)
        sudo("mount", "-o", "remount,ro", fs.mnt)
        for j in ("1", "16"):
            p = run(clean, "-o", dst, "-j", j, code=1)
            assert any(w in p.stderr for w in (b"Unexpected", b"AccessDenied", b"ReadOnly")), p.stderr
            assert dst.read_bytes() == EARLIER


@contract
def a_failing_device_keeps_the_earlier_target(root, clean, corrupt, distinct, small):
    with Loop(root, "device", error_target=True) as fs:
        for j in ("1", "16"):
            for previous in (None, EARLIER):
                dst = fs.mnt / f"out-{j}-{bool(previous)}.parquet"
                if previous:
                    dst.write_bytes(previous)
                    with dst.open("rb") as f:
                        os.fsync(f.fileno())
                fs.fail_upper_sectors()
                p = run(clean, "-o", dst, "-j", j, "--codec", "uncompressed", "--fsync", code=1)
                assert b"WriteFailed" in p.stderr or b"InputOutput" in p.stderr, p.stderr
                assert dst.read_bytes() == previous if previous else not dst.exists()
                no_staging(fs.mnt)
                sudo("dmsetup", "load", fs.dm, "--table", f"0 32768 linear {fs.loop} 0")
                sudo("dmsetup", "resume", fs.dm)


def main():
    global ZPQ, LIB, PRIVILEGED
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("zpq", nargs="?")
    ap.add_argument("--lib")
    ap.add_argument("--only")
    ap.add_argument("--lib-sync-worker", type=Path)
    ap.add_argument("--privileged", action="store_true", help="also run the contracts that mount loop devices")
    args = ap.parse_args()
    LIB = Path(args.lib).resolve() if args.lib else None
    PRIVILEGED = args.privileged
    if args.lib_sync_worker:
        return lib_sync_worker(args.lib_sync_worker)
    ZPQ = str(Path(args.zpq).resolve())
    passed, skipped, failed = 0, [], []
    with tempfile.TemporaryDirectory(prefix="zpq-contracts-") as d:
        base = Path(d)
        shared = fixtures(base)
        for fn in CONTRACTS:
            if args.only and fn.__name__ != args.only:
                continue
            root = base / fn.__name__
            root.mkdir()
            shutil.copy(shared[3], root / "small.parquet")
            try:
                fn(root, *shared)
                passed += 1
                print(f"PASS {fn.__name__}", flush=True)
            except Skip as s:
                skipped.append((fn.__name__, str(s)))
                print(f"SKIP {fn.__name__}: {NEEDS[str(s)]}", flush=True)
            except Exception as e:  # noqa: BLE001 - every failure is reported, then the run fails
                failed.append(fn.__name__)
                print(f"FAIL {fn.__name__}: {e!r}"[:2000], flush=True)
    print(f"contracts: {passed} passed, {len(skipped)} skipped by named need, {len(failed)} failed")
    if failed or passed == 0 or args.only and not passed + len(skipped):
        sys.exit(1)


if __name__ == "__main__":
    main()
