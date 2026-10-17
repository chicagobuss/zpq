#!/usr/bin/env python3
"""ci/fixtures/parquet/skew_filter.parquet: 24 row groups of 1,500 rows. `k`: ten heavy keys (a count cut certifies
them) among near-unique ones (a plain grouping holds them all). `s` spans 0..999 in every row group, so its bounds
put `s = 5` at a thousandth of the rows, but nine rows in ten hold 5. `f` is 0 in the first row group only and 1..1000
elsewhere, so the bounds prove `f = 0` passes a 24th of the rows at most.
Usage: .venv/bin/python tools/gen_skew_filter_fixture.py"""
import random

import pyarrow as pa
import pyarrow.parquet as pq

N, RGS = 1500, 24
rnd = random.Random(11)
k, s, f = [], [], []
for i in range(N * RGS):
    k.append(rnd.randrange(10) if rnd.random() < 0.3 else 1000 + rnd.randrange(1 << 40))
    s.append(5 if rnd.random() < 0.9 else rnd.randrange(1000))
    f.append(0 if i < N else 1 + rnd.randrange(1000))
for rg in range(RGS):
    s[rg * N], s[rg * N + 1] = 0, 999
table = pa.table({"k": pa.array(k, pa.int64()), "s": pa.array(s, pa.int32()), "f": pa.array(f, pa.int32())})
pq.write_table(table, "ci/fixtures/parquet/skew_filter.parquet", row_group_size=N, compression="snappy")
