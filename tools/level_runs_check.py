#!/usr/bin/env python3
"""Compare encoded level runs with legacy expansion, including V1/V2 and sliced decode.

Usage: python tools/level_runs_check.py CANDIDATE [BASE]
Requires pyarrow. Fixtures and outputs live only in an isolated temporary directory.
"""
import datetime
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
    with tempfile.TemporaryDirectory(prefix="zpq-level-runs-") as tmp:
        n = 32781
        table = pa.table({
            "g": ["abcd"[i % 4] for i in range(n)],
            "x": pa.array([None if i % 4096 >= 2048 else (1e16, 1.0, -1e16, 0.1)[i % 4]
                           for i in range(n)], type=pa.float64()),
            "d": pa.array([None if 3072 <= i % 6144 < 4096 else (i % 11) * 0.01
                           for i in range(n)], type=pa.float32()),
            "i": pa.array([None if i % 4096 >= 2048 else i % 127 for i in range(n)], type=pa.int32()),
            "b": [None if i % 3072 >= 2048 else i % 3 == 0 for i in range(n)],
            "day": pa.array([None if i % 4096 >= 2048 else datetime.date(1998, 1, 1)
                             for i in range(n)], type=pa.date32()),
        })
        checks = 0
        env = {k: v for k, v in os.environ.items() if k not in {"ZPQ_EXPR_BATCH_ROWS", "ZPQ_LEVEL_RUNS"}}

        def run(binary, query, j, memory, levels):
            cmd = [binary, "query", "--query", query, "--scan-all", "-j", str(j)]
            if memory:
                cmd += ["--max-memory", memory]
            overrides = dict(env)
            if levels is not None:
                overrides["ZPQ_LEVEL_RUNS"] = levels
            proc = subprocess.run(cmd, env=overrides, capture_output=True, timeout=60)
            if proc.returncode:
                raise AssertionError((binary, j, memory, levels, proc.stderr.decode()))
            return proc.stdout

        for version in ["1.0", "2.0"]:
            path = Path(tmp) / f"levels-{version}.parquet"
            pq.write_table(table, path, use_dictionary=["g", "i"], row_group_size=16384,
                           data_page_size=8192, data_page_version=version, compression="zstd")
            queries = [
                f"SELECT g, sum(x*(1-d)) AS s, avg(x*(1-d)) AS a, count(*) AS n "
                f"FROM '{path}' WHERE day <= '1998-09-02' GROUP BY g ORDER BY g",
                f"SELECT sum(i) AS s, count(i) AS n, count(b) AS nb, count(day) AS nd FROM '{path}'",
                f"SELECT g, count(*) AS n, sum(i) AS s FROM '{path}' GROUP BY g ORDER BY g",
                f"SELECT count(*) AS n, sum(i) AS s FROM '{path}' WHERE b = true",
            ]
            for query in queries:
                want = run(base, query, 1, None, "0")
                for j in [1, 4, 16]:
                    for memory in [None, "1M"]:
                        for levels in [None, "0", "1"]:
                            got = run(candidate, query, j, memory, levels)
                            assert got == want, (version, j, memory, levels, got, want)
                            checks += 1
        print(f"level runs: {checks} byte-identical V1/V2 nullable scalar/GROUP BY/BOOLEAN answers; "
              "auto/legacy/runs, j1/4/16, default/1M")


if __name__ == "__main__":
    main()
