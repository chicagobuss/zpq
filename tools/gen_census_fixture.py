#!/usr/bin/env python3
"""Generate the near-unique GROUP BY fixture for the census top-k tests in src/core/scan.zig.

8 row groups of 1000 rows, in a shuffled order, so that the first keys to appear are not the least. The cells of a
sketched top-k cannot separate keys this even, so these cuts reach the census.

Columns:
  u  int64, unique but for three keys seen twice: a cut of 10 ties at 1
  t  int64, four keys seen five times and twelve seen three times, the rest once: a cut of 10 ties at 3
  s  utf8, u spelled as a 20-byte string (past the 16 bytes an unframed sketch key needs)
  n  int64, NULL through the first row group and 1 after: count(n) gives the first keys to appear 0

Also `census_sliced.parquet`: key `k` = 0..39, then 3..9 six more times, in row groups of 20. Keys 3-9 count 7 and
the ninth to appear is 8, so `LIMIT 1 OFFSET 8` under a decode budget that slices row groups ends inside a later
slice.

Also `census_heavy.parquet`: key `h`, three keys seen 1500 times each and the other 3500 rows unique, shuffled, in
8 row groups of 1000. A cut of 10 ties at 1 behind the heavy keys, and the prefix that must hold k rows beyond every
heavy row is most of the file; the census first tries a far shorter one. `g` is `h` with the heavy rows first, so the
short prefix holds no unique key and the census must widen it.

Also `census_widefilter.parquet`: a plain int64 key and a 200-byte string filter column.

Usage:  .venv/bin/python tools/gen_census_fixture.py
"""

import pathlib
import random

import pyarrow as pa
import pyarrow.parquet as pq

ROWS_PER_GROUP = 1000
GROUPS = 8
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "census_topk.parquet"


def main() -> None:
    rng = random.Random(20261012)
    n = ROWS_PER_GROUP * GROUPS
    u = rng.sample(range(1, 10**12), n - 3)
    u += u[:3]  # three keys seen twice
    rng.shuffle(u)
    t = []
    for i in range(4):
        t += [10**9 + i] * 5
    for i in range(12):
        t += [2 * 10**9 + i] * 3
    t += rng.sample(range(1, 10**8), n - len(t))
    rng.shuffle(t)
    s = [f"key-{v:015d}" for v in u]
    nulls = [None] * ROWS_PER_GROUP + [1] * (n - ROWS_PER_GROUP)
    table = pa.table({
        "u": pa.array(u, pa.int64()),
        "t": pa.array(t, pa.int64()),
        "s": pa.array(s, pa.string()),
        "n": pa.array(nulls, pa.int64()),
    })
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP, use_dictionary=False, compression="snappy")
    print(f"wrote {OUT}")
    sliced = OUT.with_name("census_sliced.parquet")
    k = list(range(40)) + list(range(3, 10)) * 6
    pq.write_table(pa.table({"k": pa.array(k, pa.int64())}), sliced, row_group_size=20, use_dictionary=False)
    print(f"wrote {sliced}")
    # A plain-width integer key beside a wide filter column: the plain-width guess may fuse a census but must not make
    # the sketch decode an expensive filter twice.
    wide = OUT.with_name("census_widefilter.parquet")
    nk = 12000
    pq.write_table(pa.table({
        "k": pa.array(range(nk), pa.int64()),
        "p": pa.array([("yes" if i % 2 else "no") + "x" * 200 for i in range(nk)], pa.string()),
    }), wide, use_dictionary=False, compression="NONE", write_statistics=False)
    print(f"wrote {wide}")
    heavy = OUT.with_name("census_heavy.parquet")
    h = [7, 8, 9] * 1500 + rng.sample(range(10**3, 10**12), n - 4500)
    rng.shuffle(h)
    g = sorted(h, key=lambda x: x >= 10**3)
    pq.write_table(pa.table({"h": pa.array(h, pa.int64()), "g": pa.array(g, pa.int64()),
                             "v": pa.array(range(n), pa.int64())}), heavy,
                   row_group_size=ROWS_PER_GROUP, use_dictionary=False, compression="snappy")
    print(f"wrote {heavy}")
    dict_fixture = OUT.with_name("census_dictionary_review.parquet")
    dict_table = table.set_column(1, "t", pa.array([i % 7 for i in range(table.num_rows)], pa.int64()))
    pq.write_table(dict_table, dict_fixture, row_group_size=ROWS_PER_GROUP, use_dictionary=True, compression="snappy")
    print(f"wrote {dict_fixture}")


if __name__ == "__main__":
    main()
