"""A valid Parquet body with a bounded sparse gap before its footer."""
from __future__ import annotations

import argparse
import os
import struct
import subprocess
from pathlib import Path


def parse_size(value: str) -> int:
    units = {
        "k": 1024,
        "m": 1024**2,
        "g": 1024**3,
        "t": 1024**4,
    }
    v = value.strip().lower()
    if not v:
        raise argparse.ArgumentTypeError("empty size")
    if v[-1] in units:
        return int(float(v[:-1]) * units[v[-1]])
    return int(v)


def allocated_bytes(path: Path) -> int:
    st = path.stat()
    return st.st_blocks * 512


def make_base(base: Path, rows: int, duckdb: str) -> None:
    base.parent.mkdir(parents=True, exist_ok=True)
    sql = f"""
COPY (
    SELECT
        i::BIGINT AS id,
        (i % 100)::INTEGER AS group_id,
        CASE WHEN i % 10 = 0 THEN NULL ELSE i::INTEGER END AS metric,
        repeat('payload-', 8) || (i % 1000)::VARCHAR AS payload
    FROM range({rows}) AS t(i)
) TO '{base.as_posix()}'
  (FORMAT PARQUET, COMPRESSION 'snappy', ROW_GROUP_SIZE 100000);
"""
    subprocess.run([duckdb, "-c", sql], check=True)


def inflate_with_gap(base: Path, out: Path, target_size: int) -> int:
    raw = base.read_bytes()
    if len(raw) < 12 or raw[:4] != b"PAR1" or raw[-4:] != b"PAR1":
        raise ValueError(f"{base} does not look like a parquet file")
    footer_len = struct.unpack("<I", raw[-8:-4])[0]
    footer_start = len(raw) - footer_len - 8
    if footer_start < 4:
        raise ValueError("invalid parquet footer length")
    body = raw[:footer_start]
    trailer = raw[footer_start:]
    if target_size < len(raw):
        raise ValueError(f"target size {target_size} is smaller than base size {len(raw)}")
    gap = target_size - len(raw)

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("wb") as f:
        f.write(body)
        if gap:
            f.seek(gap, os.SEEK_CUR)
        f.write(trailer)
    return gap
