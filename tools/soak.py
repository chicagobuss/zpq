#!/usr/bin/env python3
"""Longer-tail correctness checks kept out of Tier 1/Tier 2.

These are narrow regressions for edge cases that need external or synthetic
fixtures but are not worth adding to the critical path test suite.
"""

import json
import os
import subprocess
import sys
import tempfile
from decimal import Decimal

try:
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("Missing dependencies: please run via .venv/bin/python (needs pyarrow and duckdb)")
    sys.exit(2)


ZPQ = "zig-out/bin/zpq"


def norm(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


def assert_close(got, want, label):
    if got is None or want is None:
        if got != want:
            raise AssertionError(f"{label}: got {got!r}, want {want!r}")
        return
    if abs(float(got) - float(want)) > 1e-4 * max(1.0, abs(float(want))):
        raise AssertionError(f"{label}: got {got!r}, want {want!r}")


def check_nullable_flba_decimal_aggregates(tmpdir):
    path = os.path.join(tmpdir, "nullable_flba_decimal.parquet")
    price_type = pa.decimal128(20, 2)
    table = pa.table(
        {
            "price": pa.array(
                [
                    Decimal("100.00"),
                    None,
                    Decimal("200.50"),
                    None,
                    Decimal("50.25"),
                ],
                type=price_type,
            ),
        }
    )
    pq.write_table(table, path)

    cmd = [
        ZPQ,
        "query",
        path,
        "--aggregate",
        "sum(price) AS s, count(price) AS c, min(price) AS lo, max(price) AS hi",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ZPQ failed: {(proc.stderr or proc.stdout).strip()}")

    got = json.loads(proc.stdout)["agg"]
    con = duckdb.connect()
    want_row = con.execute(
        f"SELECT sum(price) AS s, count(price) AS c, min(price) AS lo, max(price) AS hi FROM '{path}'"
    ).fetchone()
    want = dict(zip(["s", "c", "lo", "hi"], [norm(v) for v in want_row]))

    for key in ["s", "lo", "hi"]:
        assert_close(got[key], want[key], f"nullable FLBA DECIMAL {key}")
    if got["c"] != want["c"]:
        raise AssertionError(f"nullable FLBA DECIMAL count: got {got['c']!r}, want {want['c']!r}")

    print("  OK    nullable FLBA DECIMAL aggregates skip null rows")


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        check_nullable_flba_decimal_aggregates(tmpdir)
    print("soak: all checks passed")


if __name__ == "__main__":
    main()
