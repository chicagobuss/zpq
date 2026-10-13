#!/usr/bin/env python3
"""Failed CLI writes preserve old targets; output errors cannot report success."""
import os
import pathlib
import subprocess
import sys
import tempfile

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
    with open("/dev/full", "wb") as full:
        p = subprocess.run([binary, "query", str(src), "--aggregate", "count(*)"],
                           stdout=full, stderr=subprocess.PIPE)
    assert p.returncode == 1, (p.returncode, p.stderr)
    # A small scalar still exercises the final buffered flush after the reader closes.
    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    try:
        p = subprocess.run([binary, "query", str(src), "--aggregate", "count(*)"],
                           stdout=write_fd, stderr=subprocess.PIPE)
    finally:
        os.close(write_fd)
    assert p.returncode in (141, -13), (p.returncode, p.stderr)
    assert not p.stderr
print("CLI publication/error checks passed")
