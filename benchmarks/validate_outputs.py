#!/usr/bin/env python3
"""Validate S3 parquet outputs from a bench run with pyarrow + duckdb.

Reads `label\ts3://...` lines from stdin (or argv as one-shot),
downloads each via boto3, and prints OK/FAIL alongside row count and
schema sanity. Exit code is non-zero if any output fails to parse.

The whole point of this script existing: "bytes_out > 0" is not the
same as "valid parquet." Today's bench runs validate every output
before claiming the wallclock numbers mean something.
"""
import argparse
import io
import os
import re
import shutil
import subprocess
import sys
from urllib.parse import urlparse

import boto3
import pyarrow.parquet as pq
import duckdb


# Hardwood is an optional third strict reader (Java parquet engine,
# native CLI binary). When available we cross-validate; missing
# hardwood is not an error — the script reports "skipped" for that
# column. Looks first at $HARDWOOD env var, then $PATH, then the
# vendored copy under tools/hardwood/.
def find_hardwood() -> str | None:
    if env := os.environ.get("HARDWOOD"):
        return env if os.path.exists(env) else None
    if path_hw := shutil.which("hardwood"):
        return path_hw
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    vendored = os.path.join(repo_root, "tools", "hardwood", "hardwood-cli-1.0.0.Beta2-linux-x86_64", "bin", "hardwood")
    return vendored if os.path.exists(vendored) else None


HARDWOOD_BIN = find_hardwood()


def hardwood_check(path: str) -> tuple[bool, str]:
    """Run `hardwood info -f <path>` and parse Total Rows. Returns
    (ok, detail). On failure, detail carries the error excerpt."""
    if HARDWOOD_BIN is None:
        return True, "skipped"
    try:
        proc = subprocess.run(
            [HARDWOOD_BIN, "info", "-f", path],
            capture_output=True, text=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        return False, "hardwood_timeout"
    if proc.returncode != 0:
        # First line of stderr is usually the most informative.
        snippet = (proc.stderr.strip().splitlines() or ["?"])[0][:120]
        return False, f"hardwood_exit_{proc.returncode}: {snippet}"
    m = re.search(r"Total Rows:\s+(\d+)", proc.stdout)
    if not m:
        return False, f"hardwood_no_row_count: {proc.stdout[:120]}"
    return True, m.group(1)


def parse_s3(url: str):
    p = urlparse(url)
    if p.scheme != "s3":
        raise ValueError(f"not an s3 URL: {url}")
    return p.netloc, p.path.lstrip("/")


def validate(label: str, url: str, s3) -> tuple[bool, str]:
    try:
        bucket, key = parse_s3(url)
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as e:
        return False, f"s3_get_failed: {type(e).__name__}: {e}"

    # pyarrow: opens the footer + schema, strict thrift validation.
    try:
        md = pq.read_metadata(io.BytesIO(body))
        sch = pq.read_schema(io.BytesIO(body))
    except Exception as e:
        return False, f"pyarrow_open_failed: {type(e).__name__}: {e}"

    py_rows = md.num_rows
    py_cols = len(list(sch))
    py_nullable = sum(1 for f in sch if f.nullable)

    # duckdb + hardwood: both want a path on disk.
    tmp = f"/tmp/_validate_{abs(hash(url))}.parquet"
    try:
        with open(tmp, "wb") as f:
            f.write(body)
        try:
            n = duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
        except Exception as e:
            return False, f"duckdb_read_failed: {type(e).__name__}: {e}"

        hw_ok, hw_detail = hardwood_check(tmp)
        if not hw_ok:
            return False, f"hardwood_failed: {hw_detail}"
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass

    if n != py_rows:
        return False, f"row_count_disagreement: pyarrow={py_rows} duckdb={n}"

    hw_note = "hw=skip" if hw_detail == "skipped" else f"hw_rows={hw_detail}"
    return True, f"rows={py_rows} cols={py_cols} nullable={py_nullable} bytes={len(body):,} ({hw_note})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-stdin", action="store_true",
                    help="read 'label<TAB>url' lines from stdin")
    ap.add_argument("pairs", nargs="*", help="alternating label url pairs")
    args = ap.parse_args()

    s3 = boto3.client("s3")
    pairs = []
    if args.from_stdin:
        for line in sys.stdin:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            pairs.append(tuple(parts))
    else:
        if len(args.pairs) % 2 != 0:
            print("usage: validate_outputs.py LABEL URL [LABEL URL ...]", file=sys.stderr)
            sys.exit(2)
        for i in range(0, len(args.pairs), 2):
            pairs.append((args.pairs[i], args.pairs[i + 1]))

    if not pairs:
        print("no inputs to validate", file=sys.stderr)
        sys.exit(2)

    any_fail = False
    print(f"{'label':<30}  status  detail")
    print("-" * 80)
    for label, url in pairs:
        ok, detail = validate(label, url, s3)
        marker = "OK  " if ok else "FAIL"
        print(f"{label:<30}  {marker}    {detail}")
        if not ok:
            any_fail = True

    if any_fail:
        print()
        print("FAILED — at least one output is not valid parquet for pyarrow/duckdb.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
