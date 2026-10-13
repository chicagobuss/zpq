#!/usr/bin/env python3
"""Generate the plain single-row-group fixture for the shared partition budget regression."""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

path = Path(__file__).resolve().parent.parent / "ci/fixtures/parquet/partagg_budget.parquet"
pq.write_table(pa.table({"k": list(range(1000)), "v": list(range(1000))}),
               path, use_dictionary=False, row_group_size=1000)
