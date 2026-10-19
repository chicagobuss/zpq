#!/usr/bin/env python3
"""ci/fixtures/parquet/topn_descending.parquet: one row group of 100000 required INT32 values, descending, no
statistics, so an ascending Top-N's every row improves on the last and evicts the row before it (the serial Top-N's
kept-key memory under its budget). tools/memory_gate_fixtures.py writes the same shape at 4M rows.

usage: gen_topn_descending_fixture.py [OUT] [ROWS]
"""
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1] if len(sys.argv) > 1 else "ci/fixtures/parquet/topn_descending.parquet"
rows = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000
values = pa.array(range(rows - 1, -1, -1), pa.int32())
table = pa.Table.from_arrays([values], schema=pa.schema([pa.field("k", pa.int32(), nullable=False)]))
pq.write_table(table, out, row_group_size=rows, compression="NONE", use_dictionary=False, write_statistics=False)
