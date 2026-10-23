#!/usr/bin/env python3
"""Corrupt multi-column/page V1/V2 files must report identical errors in every execution mode.

Usage: python tools/decode_error_check.py CANDIDATE
Requires pyarrow. All temporary fixtures are removed on exit.
"""
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


def varint(data, pos):
    result = shift = 0
    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 127) << shift
        if byte < 128:
            return result, pos
        shift += 7


def fields(data, pos, spans=None, prefix=()):
    result = {}
    last = 0
    while True:
        head = data[pos]
        pos += 1
        if head == 0:
            return result, pos
        kind = head & 15
        delta = head >> 4
        if delta:
            fid = last + delta
        else:
            value, pos = varint(data, pos)
            fid = (value >> 1) ^ -(value & 1)
        last = fid
        if kind in (1, 2):
            result[fid] = kind == 1
        elif kind in (4, 5, 6):
            start = pos
            value, pos = varint(data, pos)
            result[fid] = (value >> 1) ^ -(value & 1)
            if spans is not None:
                spans[prefix + (fid,)] = (start, pos)
        elif kind == 12:
            result[fid], pos = fields(data, pos, spans, prefix + (fid,))
        elif kind == 8:
            size, pos = varint(data, pos)
            pos += size
        elif kind == 3:
            pos += 1
        elif kind == 7:
            pos += 8
        else:
            raise AssertionError(f"unexpected fixture field type {kind}")


def corrupt(data, meta, version, rg, ci, page, mode="levels"):
    pos = meta.row_group(rg).column(ci).data_page_offset
    for pi in range(page + 1):
        header, body = fields(data, pos)
        if pi == page:
            if mode == "levels":
                if version == "1.0":
                    data[body:body + 4] = b"\xff\xff\xff\xff"
                else:
                    # A level-1 stream cannot hold the RLE value 2. Validation must precede values.
                    data[body:body + 2] = b"\x02\x02"
            else:
                if version == "1.0":
                    value_start = body + 4 + struct.unpack_from("<I", data, body)[0]
                else:
                    value_start = body + header[8][5] + header[8][6]
                # INT8 annotation on physical INT32: a late value in the first page is out of range.
                struct.pack_into("<i", data, value_start + 110 * 4, 9999)
        pos = body + header[3]


def main():
    binary = str(Path(sys.argv[1]).resolve())
    n = 65553 * 3
    table = pa.table({
        "a": pa.array([None if i % 11 == 0 else i % 71 for i in range(n)], type=pa.int8()),
        "b": pa.array([None if i % 13 == 0 else i % 503 for i in range(n)], type=pa.int32()),
        "c": pa.array([None if i % 17 == 0 else i * 0.01 for i in range(n)], type=pa.float64()),
        "g": ["abcd"[i % 4] for i in range(n)],
    })
    env = dict(os.environ, ZPQ_IO_PROFILE="mmap")
    env.pop("ZPQ_DECODE_BATCH_ROWS", None)
    checks = 0
    with tempfile.TemporaryDirectory(prefix="zpq-decode-order-") as tmp:
        path = Path(tmp) / "corrupt.parquet"
        for version in ["1.0", "2.0"]:
            pq.write_table(table, path, data_page_version=version, row_group_size=65553,
                           data_page_size=512, write_batch_size=128, use_dictionary=["g"],
                           compression=None, write_statistics=False)
            original = path.read_bytes()
            meta = pq.ParquetFile(path).metadata
            cases = [
                ("columns", [(1, 0, 3), (1, 1, 1), (1, 2, 3)], "b", 1),
                ("ties", [(1, 0, 0), (1, 1, 0)], None, 1),
                ("row-groups", [(0, 0, 3), (1, 1, 0), (2, 2, 0)], "a", 0),
                ("late-value", [(1, 0, 0, "value"), (1, 1, 0)], None, 1),
            ]
            for label, changes, winner, failing_rg in cases:
                data = bytearray(original)
                for change in changes:
                    corrupt(data, meta, version, *change)
                path.write_bytes(data)
                for reverse, grouped in [(False, False), (True, False), (False, True)]:
                    args = "sum(b) AS sb, sum(a) AS sa, sum(c) AS sc" if reverse else \
                        "sum(a) AS sa, sum(b) AS sb, sum(c) AS sc"
                    sql = f"SELECT {'g, ' if grouped else ''}{args} FROM '{path}'"
                    if grouped:
                        sql += " GROUP BY g"
                    want = None
                    expected_column = winner or ("b" if reverse else "a")
                    for memory, batch in [(None, "0"), ("1M", "0"), (None, "64"), ("1M", "256")]:
                        for j in range(1, 17):
                            cmd = [binary, "query", "--query", sql, "--scan-all", "-j", str(j)]
                            if memory:
                                cmd += ["--max-memory", memory]
                            proc = subprocess.run(cmd, env=dict(env, ZPQ_DECODE_BATCH_ROWS=batch),
                                                  capture_output=True, timeout=60)
                            assert proc.returncode == 1 and proc.stdout == b"", \
                                (version, label, memory, batch, j, proc.returncode, proc.stdout, proc.stderr)
                            if want is None:
                                want = proc.stderr
                                expected = f"row group {failing_rg}, column `{expected_column}`".encode()
                                assert expected in want, (version, label, reverse, grouped, want, expected)
                                expected_page = 1 if label == "columns" else 3 if label == "row-groups" else 0
                                assert f"page {expected_page}:".encode() in want, want
                                kind = "IntOutOfRange" if label == "late-value" and not reverse else "UnexpectedPage"
                                assert f"({kind})".encode() in want, want
                            assert proc.stderr == want, \
                                (version, label, reverse, grouped, memory, batch, j, proc.stderr, want)
                            checks += 1
                print(f"{version} {label}: whole/sliced/cache, j1..16 identical", flush=True)
            # File order precedes the row-group index within either file.
            parts = Path(tmp) / f"parts-{version}"
            parts.mkdir()
            for name, change in [("a.parquet", (2, 0, 3)), ("b.parquet", (0, 1, 0))]:
                data = bytearray(original)
                corrupt(data, meta, version, *change)
                (parts / name).write_bytes(data)
            sql = f"SELECT sum(a) AS sa, sum(b) AS sb, sum(c) AS sc FROM '{parts}/*.parquet'"
            want = None
            for memory, batch in [(None, "0"), ("1M", "0"), (None, "64"), ("1M", "256")]:
                for j in range(1, 17):
                    cmd = [binary, "query", "--query", sql, "--scan-all", "-j", str(j)]
                    if memory:
                        cmd += ["--max-memory", memory]
                    proc = subprocess.run(cmd, env=dict(env, ZPQ_DECODE_BATCH_ROWS=batch),
                                          capture_output=True, timeout=60)
                    assert proc.returncode == 1 and proc.stdout == b"", (version, memory, batch, j, proc.stderr)
                    if want is None:
                        want = proc.stderr
                        assert b"a.parquet" in want and b"row group 2, column `a`, page 3:" in want, want
                    assert proc.stderr == want, (version, memory, batch, j, proc.stderr, want)
                    checks += 1
            print(f"{version} input order: whole/sliced/cache, j1..16 identical", flush=True)
            # A REQUIRED page overclaim used to disappear when sliced execution stopped at footer.num_values.
            required = pa.table({"a": list(range(128)), "b": list(range(128))}, schema=pa.schema([
                pa.field("a", pa.int32(), nullable=False), pa.field("b", pa.int32(), nullable=False),
            ]))
            path = Path(tmp) / f"required-{version}.parquet"
            pq.write_table(required, path, data_page_version=version, compression=None,
                           use_dictionary=False, write_statistics=False)
            md = pq.ParquetFile(path).metadata
            data = bytearray(path.read_bytes())
            spans = {}
            fields(data, md.row_group(0).column(0).data_page_offset, spans)
            start, end = spans[(5 if version == "1.0" else 8, 1)]
            assert end - start == 2
            data[start:end] = b"\x80\x04"  # zigzag(256), from the original 128; footer still says 128.
            path.write_bytes(data)
            sql = f"SELECT sum(a) AS sa, sum(b) AS sb FROM '{path}'"
            want = None
            for memory, batch in [(None, "0"), ("1M", "0"), (None, "64"), ("1M", "256")]:
                for j in range(1, 17):
                    cmd = [binary, "query", "--query", sql, "--scan-all", "-j", str(j)]
                    if memory:
                        cmd += ["--max-memory", memory]
                    proc = subprocess.run(cmd, env=dict(env, ZPQ_DECODE_BATCH_ROWS=batch),
                                          capture_output=True, timeout=60)
                    assert proc.returncode == 1 and proc.stdout == b"", (version, memory, batch, j, proc.stdout)
                    if want is None:
                        want = proc.stderr
                        assert b"column `a`, page 0:" in want and b"PageValueCountExceedsChunk" in want, want
                    assert proc.stderr == want, (version, memory, batch, j, proc.stderr, want)
                    checks += 1
            print(f"{version} required page count: whole/sliced/cache, j1..16 identical", flush=True)
    print(f"canonical decode errors: {checks} byte-identical failures")


if __name__ == "__main__":
    main()
