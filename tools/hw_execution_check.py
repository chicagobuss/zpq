#!/usr/bin/env python3
"""Cache size may change execution policy, never canonical aggregate output bytes.

Usage: python tools/hw_execution_check.py CANDIDATE [BASE]
Requires pyarrow; creates and cleans its own temporary fixture.
"""
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    candidate = str(Path(sys.argv[1]).resolve())
    base = str(Path(sys.argv[2]).resolve()) if len(sys.argv) > 2 else candidate
    with tempfile.TemporaryDirectory(prefix="zpq-hw-") as tmp:
        path = Path(tmp) / "lineitem.parquet"
        n = 65553
        pq.write_table(pa.table({
            "g": ["abcd"[i % 4] for i in range(n)],
            "price": [(1e16, 1.0, -1e16, 0.1, -0.0)[i % 5] for i in range(n)],
            "discount": [(i % 11) * 0.01 for i in range(n)],
            "tax": [(i % 9) * 0.01 for i in range(n)],
            "f": pa.array([i % 1000 for i in range(n)], type=pa.int32()),
        }), path, use_dictionary=["g"], row_group_size=32768, compression="zstd")
        aggs = ("sum(price*(1-discount)) AS s, sum(price*(1-discount)*(1+tax)) AS c, "
                "avg(price*(1-discount)) AS a, count(*) AS n")
        queries = [f"SELECT {aggs} FROM '{path}' WHERE f < 300",
                   f"SELECT g, {aggs} FROM '{path}' WHERE f < 300 GROUP BY g ORDER BY g"]
        env = {k: v for k, v in os.environ.items() if k not in {
            "ZPQ_EXPR_BATCH_ROWS", "ZPQ_DECODE_BATCH_ROWS", "ZPQ_HW_L2_BYTES", "ZPQ_HW_L3_BYTES"}}

        def run(binary, sql, j, size, l3, decode):
            options = dict(env, ZPQ_HW_L2_BYTES=size, ZPQ_HW_L3_BYTES=l3)
            if decode is not None:
                options["ZPQ_DECODE_BATCH_ROWS"] = decode
            p = subprocess.run([binary, "query", "--query", sql, "-j", str(j)],
                               env=options, capture_output=True, timeout=60)
            if p.returncode:
                raise AssertionError((binary, j, size, l3, decode, p.stderr.decode()))
            return p.stdout

        checks = 0
        for sql in queries:
            want = run(base, sql, 1, "1M", "32M", "0")
            for l3 in ["256K", "32M"]:
                for size in ["256K", "1M", "4M"]:
                    for j in [1, 3, 8, 16]:
                        for decode in [None, "0", "64", "256"]:
                            got = run(candidate, sql, j, size, l3, decode)
                            assert got == want, (size, l3, j, decode, got, want)
                            checks += 1
        print(f"hardware execution: {checks} byte-identical scalar/GROUP BY answers across "
              "L2 256K/1M/4M, L3 256K/32M, decode auto/0/64/256 and j1/3/8/16")


if __name__ == "__main__":
    main()
