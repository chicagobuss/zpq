#!/usr/bin/env python3
"""Create a valid Parquet file with a large sparse gap before the footer.

The resulting file keeps the column chunk offsets from a normal DuckDB-written
Parquet file, then moves the footer to the end of a much larger logical object.
Readers locate the footer from the trailer and still find the original chunks by
absolute offset, while S3 Content-Range reports the inflated object size.
"""

from __future__ import annotations

import argparse
import os
import shutil
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="/mnt/b/Work/side-projects/parquet-testing/zpq-sparse-oom")
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--target-size", type=parse_size, default=parse_size("4g"))
    ap.add_argument("--duckdb", default=shutil.which("duckdb") or "duckdb")
    args = ap.parse_args()

    out_dir = Path(args.dir)
    base = out_dir / "base.parquet"
    sparse = out_dir / f"sparse_{args.target_size // (1024 * 1024)}m.parquet"

    make_base(base, args.rows, args.duckdb)
    gap = inflate_with_gap(base, sparse, args.target_size)

    for label, path in [("base", base), ("sparse", sparse)]:
        st = path.stat()
        print(
            f"{label}: path={path} logical_bytes={st.st_size} "
            f"allocated_bytes={allocated_bytes(path)}"
        )
    print(f"inserted_gap_bytes={gap}")


if __name__ == "__main__":
    main()
