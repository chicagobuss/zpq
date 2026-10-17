#!/usr/bin/env python3
"""ci/fixtures/parquet/repeat_keys.parquet: 8 row groups of 8,000 rows, every key 0..7999 in every row group, so each
of several workers' GROUP BY tables holds every key whichever row groups it claims; one table holds each once.
Usage: .venv/bin/python tools/gen_repeat_keys_fixture.py"""
import pyarrow as pa
import pyarrow.parquet as pq

N, RGS = 8000, 8
k = [i % N for i in range(N * RGS)]
v = [(i * 7) % 1000 for i in range(N * RGS)]
table = pa.table({"k": pa.array(k, pa.int64()), "v": pa.array(v, pa.int64())})
pq.write_table(table, "ci/fixtures/parquet/repeat_keys.parquet", row_group_size=N, compression="snappy")
