#!/usr/bin/env python3
"""Two dictionary-coded files and one PLAIN string-key file for the dense product checks.

Paths are positional: coded-0, coded-1, plain. Any older coded-* sibling is removed so the glob is exactly those two.
"""
import glob
import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

coded0, coded1, plain = sys.argv[1:4]
parent = os.path.dirname(os.path.abspath(coded0))
prefix = os.path.basename(coded0)[: -len("coded-0.parquet")]
keep = {os.path.abspath(p) for p in (coded0, coded1)}
for path in glob.glob(os.path.join(parent, prefix + "coded-*.parquet")):
    if os.path.abspath(path) not in keep:
        os.remove(path)

rng = np.random.default_rng(20261024)
n = 262144
table = None
for path in (coded0, coded1):
    codes = rng.integers(0, 4, n)
    names = np.array(["", "a\x00b", "region", "z"], dtype=object)[codes]
    table = pa.table({
        "a": pa.array(rng.integers(-8, 9, n).astype(np.int32), mask=rng.random(n) < 0.03),
        "b": pa.array(names, pa.string(), mask=rng.random(n) < 0.02),
        "f": pa.array(rng.integers(0, 100, n).astype(np.int32)),
    })
    pq.write_table(table, path, row_group_size=8192)
pq.write_table(table, plain, row_group_size=8192, use_dictionary=["a", "f"])
