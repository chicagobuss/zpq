#!/usr/bin/env python3
"""ci/fixtures/parquet/scalar_strings.parquet: four row groups of one 1 MiB string each, the largest first, so every
row group's min(s) is a new owned winner of its own until the merge (the memory ledger's scalar winner charge).

usage: gen_scalar_strings_fixture.py [OUT]
"""
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1] if len(sys.argv) > 1 else "ci/fixtures/parquet/scalar_strings.parquet"
values = [chr(ord("d") - i) * (1 << 20) for i in range(4)]
pq.write_table(pa.table({"s": pa.array(values, pa.string())}), out, row_group_size=1, use_dictionary=False,
               compression="snappy")
