#!/usr/bin/env python3
"""Failed CLI writes preserve old targets; output errors cannot report success."""
import os
import pathlib
import subprocess
import sys
import struct
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
    alias = root / "alias.parquet"
    os.link(src, alias)
    p = subprocess.run([binary, "query", str(src), "-o", str(alias)], capture_output=True)
    assert p.returncode == 2 and not p.stdout
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
