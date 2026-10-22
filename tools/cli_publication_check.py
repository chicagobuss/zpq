#!/usr/bin/env python3
"""Failed CLI writes preserve old targets; output errors cannot report success."""
import os
import pathlib
import subprocess
import sys
import struct
import tempfile
import re
import shutil
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow as pa
import pyarrow.parquet as pq

binary = str(pathlib.Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory(prefix="zpq-publication-") as d:
    root = pathlib.Path(d)
    src, dst = root / "in.parquet", root / "out.parquet"
    pq.write_table(pa.table({"k": [1, 2, 1], "v": [3, 4, 5]}), src)
    for args in (["--aggregate", "sum(missing)"], ["--select", "missing"],
                 ["--group-by", "missing", "--aggregate", "count(*)"]):
        dst.write_bytes(b"earlier")
        p = subprocess.run([binary, "query", str(src), "-o", str(dst), *args], capture_output=True)
        assert p.returncode == 2, (args, p.returncode, p.stderr)
        assert not p.stdout
        assert dst.read_bytes() == b"earlier"
        assert not list(root.glob("*.zpq-tmp"))
    p = subprocess.run([binary, "query", str(src), "-o", str(src)], capture_output=True)
    assert p.returncode == 2, p.stderr
    assert pq.read_table(src).num_rows == 3
    alias = root / "alias.parquet"
    os.link(src, alias)
    p = subprocess.run([binary, "query", str(src), "-o", str(alias)], capture_output=True)
    assert p.returncode == 2 and not p.stdout
    for mode in (0o600, 0o444, 0o754):
        dst.chmod(0o644)
        dst.write_bytes(b"earlier")
        dst.chmod(mode)
        p = subprocess.run([binary, "query", str(src), "-o", str(dst)], capture_output=True)
        assert p.returncode == 0 and not p.stdout, p.stderr
        assert dst.stat().st_mode & 0o7777 == mode
        assert pq.read_table(dst).num_rows == 3
    referent = root / "referent"
    referent.write_bytes(b"referent unchanged")
    referent.chmod(0o600)
    symlink = root / "link.parquet"
    symlink.symlink_to(referent)
    p = subprocess.run([binary, "query", str(src), "-o", str(symlink)], capture_output=True)
    assert p.returncode == 0 and not p.stdout and not symlink.is_symlink(), p.stderr
    assert referent.read_bytes() == b"referent unchanged"
    assert symlink.stat().st_mode & 0o7777 == 0o600
    assert pq.read_table(symlink).num_rows == 3
    p = subprocess.run([binary, "query", "s3://bucket/input", "-o", "s3://bucket/input"], capture_output=True)
    assert p.returncode == 2 and b"input and output" in p.stderr
    damaged = root / "damaged.parquet"
    pq.write_table(pa.table({"v": list(range(10_000))}), damaged, row_group_size=5_000,
                   use_dictionary=False)
    meta = pq.read_metadata(damaged)
    data = bytearray(damaged.read_bytes())
    chunk = meta.row_group(1).column(0)
    footer = len(data) - 8 - struct.unpack_from("<I", data, len(data) - 8)[0]
    start = chunk.data_page_offset
    end = min(footer, start + chunk.total_compressed_size)
    data[start:end] = bytes(end - start)
    damaged.write_bytes(data)
    for fmt in ("ndjson", "json", "parquet"):
        dst.write_bytes(b"earlier")
        p = subprocess.run([binary, "query", str(damaged), "--select", "v + 0 AS v", "-o", str(dst),
                            "--format", fmt, "--summary=json"], capture_output=True)
        assert p.returncode == 1, (fmt, p.returncode, p.stderr)
        assert not p.stdout and dst.read_bytes() == b"earlier"
        assert b'"type":"error"' in p.stderr and b'"type":"summary"' not in p.stderr
        assert not list(root.glob("*.zpq-tmp"))
    if os.name == "posix":
        import resource
        import signal

        def limit_file():
            resource.setrlimit(resource.RLIMIT_FSIZE, (4096, 4096))
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)

        limited = root / "limited.parquet"
        pq.write_table(pa.table({"v": [i * 104729 % 1_000_003 for i in range(10_000)]}),
                       limited, row_group_size=5_000,
                       use_dictionary=False)
        for previous in (None, b"earlier result"):
            failed = root / ("new-failed.parquet" if previous is None else "old-failed.parquet")
            if previous is not None:
                failed.write_bytes(previous)
            p = subprocess.run(
                [binary, "query", str(limited), "--select", "v + 1 AS v", "--row-group-size", "1024",
                 "--codec", "uncompressed", "-j", "4", "-o", str(failed)],
                capture_output=True, preexec_fn=limit_file, timeout=15,
            )
            assert p.returncode == 1 and not p.stdout, (p.returncode, p.stderr)
            assert b"WriteFailed" in p.stderr, p.stderr
            assert not failed.exists() if previous is None else failed.read_bytes() == previous
            assert not list(root.glob("*.zpq-tmp"))
    if sys.platform.startswith("linux") and shutil.which("strace"):
        trace = root / "publication.strace"
        dst.write_bytes(b"earlier")
        p = subprocess.run(
            ["strace", "-qq", "-e", "trace=openat,close,fchmod,renameat", "-o", str(trace),
             binary, "query", str(src), "-o", str(dst)], capture_output=True, timeout=15,
        )
        assert p.returncode == 0 and not p.stdout, (p.returncode, p.stderr)
        opens = [line for line in trace.read_text().splitlines() if line.startswith("openat(")
                 and ".zpq-tmp\"" in line]
        assert len(opens) == 1 and "O_EXCL" in opens[0] and "O_TRUNC" not in opens[0], opens
        fd = int(re.search(r"= (\d+)$", opens[0]).group(1))
        assert f"fchmod({fd}," in trace.read_text()
        assert pq.read_table(dst).num_rows == 3
        dst.write_bytes(b"earlier")
        p = subprocess.run(
            ["strace", "-qq", "-e", "inject=renameat:error=EIO:when=1", "-o", str(trace),
             binary, "query", str(src), "-o", str(dst)], capture_output=True, timeout=15,
        )
        assert p.returncode == 1 and not p.stdout and dst.read_bytes() == b"earlier", (p.returncode, p.stderr)
        assert not list(root.glob("*.zpq-tmp"))
    uploaded = []

    class UploadHandler(BaseHTTPRequestHandler):
        def do_PUT(self):
            uploaded.append((self.path, self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), UploadHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env = {**os.environ, "S3_NO_SIGN_REQUEST": "1", "AWS_REGION": "us-east-1",
               "S3_ENDPOINT_URL": f"http://127.0.0.1:{server.server_port}"}
        p = subprocess.run([binary, "query", str(src), "--select", "v + 1 AS v",
                            "-o", "s3://test/output.parquet"], capture_output=True, env=env, timeout=15)
        assert p.returncode == 0 and not p.stdout, (p.returncode, p.stderr)
        assert len(uploaded) == 1 and uploaded[0][0] == "/test/output.parquet", uploaded
        remote = root / "remote.parquet"
        remote.write_bytes(uploaded[0][1])
        assert pq.read_table(remote).column("v").to_pylist() == [4, 5, 6]
        assert not list(root.glob("*.zpq-tmp"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    with open("/dev/full", "wb") as full:
        p = subprocess.run([binary, "query", str(src), "--aggregate", "count(*)"],
                           stdout=full, stderr=subprocess.PIPE)
    assert p.returncode == 1 and b"NoSpaceLeft" in p.stderr, (p.returncode, p.stderr)
    # A small scalar still exercises the final buffered flush after the reader closes.
    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    try:
        p = subprocess.run([binary, "query", str(src), "--aggregate", "count(*)"],
                           stdout=write_fd, stderr=subprocess.PIPE)
    finally:
        os.close(write_fd)
    assert p.returncode == 141, (p.returncode, p.stderr)
    assert not p.stderr
print("CLI publication/error checks passed")
