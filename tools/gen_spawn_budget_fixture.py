#!/usr/bin/env python3
"""Generate the fixture pair for the spawn-failure GROUP BY budget test.

The test in src/core/scan.zig needs a very specific shape, and none of the
corpus files provide it while also being small enough to commit:

  * enough estimated fetch volume that worker sizing picks more than one
    worker. Sizing counts max(uncompressed_size, num_rows * 4) per fetched
    column and wants 2 MiB per worker, so 12 columns x 50k rows x 2 files
    gives 4.8 MB -- just over the 4 MiB needed for two workers. Constant
    columns still count via the num_rows * 4 floor, which is what keeps the
    files tiny while the estimate stays high.
  * group keys DISJOINT between the two files, so the worker that ends up
    draining the whole queue really does hold twice the groups. With shared
    keys, one worker holding the union costs no more than its own share and
    the bug under test is invisible.
  * small on disk, because it is tracked in git (ci/fixtures/parquet is explicitly un-ignored) rather than the
    ignored data/ directory (a skipped test is not a regression test).

Every column except the key is constant, so dictionary + snappy squeeze the
files to a few KB while `num_rows * 4` keeps the estimated fetch volume high
enough to trigger multi-worker scheduling.

Usage:  .venv/bin/python tools/gen_spawn_budget_fixture.py
"""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 50_000
GROUPS_PER_FILE = 1_000
PAD_COLUMNS = 11
OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet"


def build(key_base: int) -> pa.Table:
    # Disjoint key ranges per file: file A owns [0, 1000), file B [1000, 2000).
    keys = [key_base + (i % GROUPS_PER_FILE) for i in range(ROWS)]
    data = {"group_key": pa.array(keys, type=pa.int64())}
    for c in range(PAD_COLUMNS):
        # Constant, so it costs ~nothing on disk while still counting toward
        # the fetched-bytes estimate that drives worker sizing.
        data[f"pad{c}"] = pa.array([1.0] * ROWS, type=pa.float64())
    return pa.table(data)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, key_base in (("spawn_budget_a", 0), ("spawn_budget_b", GROUPS_PER_FILE)):
        path = OUT_DIR / f"{name}.parquet"
        # One row group per file: each file must be exactly one work item, so
        # two files give two items and the scan can pick two workers.
        pq.write_table(build(key_base), path, compression="snappy", row_group_size=ROWS)
        print(f"wrote {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
