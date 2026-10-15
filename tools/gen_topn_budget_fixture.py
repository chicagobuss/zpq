#!/usr/bin/env python3
"""Generate the fixtures for the parallel plain-row Top-N's memory budget (src/engine.zig), after a review repro.

Each is one dictionary-encoded row group, so a LIMIT's winners sit apart in it and the gather reads many short runs:
  topn_budget_80.parquet   80 rows: id, s ('' every seventh row, else 'z' + five digits), payload 'row:<i>'
  topn_budget_12k.parquet  12000 rows: id, s alternating 'z' and 'a', payload 'p'

Usage:  .venv/bin/python tools/gen_topn_budget_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

DIR = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet"


def main() -> None:
    n = 80
    pq.write_table(pa.table({"id": range(n), "s": ["" if i % 7 == 0 else f"z{i:05d}" for i in range(n)],
                             "payload": [f"row:{i}" for i in range(n)]}),
                   DIR / "topn_budget_80.parquet", use_dictionary=True, row_group_size=n)
    n = 12000
    pq.write_table(pa.table({"id": range(n), "s": ["a" if i % 2 else "z" for i in range(n)],
                             "payload": ["p" for _ in range(n)]}),
                   DIR / "topn_budget_12k.parquet", use_dictionary=True, row_group_size=n)


if __name__ == "__main__":
    main()
