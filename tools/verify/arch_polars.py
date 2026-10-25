"""The architecture runner's only Python process: the pinned Polars rival."""

from datetime import date, datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import sys

import polars as pl

assert pl.__version__ == "2.0.0", pl.__version__
pl.Config.set_engine_affinity("streaming")
spec = json.loads(Path(sys.argv[1]).read_text())
case, source, output = spec["case"], spec["input"], Path(sys.argv[2])
parquet = case.get("output") == "parquet"
if case.get("pdsh"):
    # The pinned scorecard's publisher expressions, not Polars' SQL decimal coercions.
    li = pl.scan_parquet(source)
    if case["pdsh"] == "1":
        result = (li.filter(pl.col("l_shipdate") <= date(1998, 9, 2))
                  .group_by("l_returnflag", "l_linestatus")
                  .agg(pl.sum("l_quantity").alias("sum_qty"), pl.sum("l_extendedprice").alias("sum_base_price"),
                       (pl.col("l_extendedprice") * (1.0 - pl.col("l_discount"))).sum().alias("sum_disc_price"),
                       (pl.col("l_extendedprice") * (1.0 - pl.col("l_discount")) * (1.0 + pl.col("l_tax")))
                       .sum().alias("sum_charge"), pl.mean("l_quantity").alias("avg_qty"),
                       pl.mean("l_extendedprice").alias("avg_price"), pl.mean("l_discount").alias("avg_disc"),
                       pl.len().alias("count_order"))
                  .sort("l_returnflag", "l_linestatus").collect())
    else:
        result = (li.filter(pl.col("l_shipdate").is_between(date(1994, 1, 1), date(1995, 1, 1), closed="left"))
                  .filter(pl.col("l_discount").is_between(0.05, 0.07)).filter(pl.col("l_quantity") < 24)
                  .with_columns((pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue"))
                  .select(pl.sum("revenue")).collect())
elif case.get("polars_expr"):
    hits = pl.scan_parquet(source).with_columns(
        (pl.col("EventTime") * int(1e6)).cast(pl.Datetime(time_unit="us")), pl.col("EventDate").cast(pl.Date))
    result = eval(compile(case["polars_expr"], "<pinned scorecard>", "eval"), dict(hits=hits, pl=pl, date=date))
else:
    ctx = pl.SQLContext(t=pl.scan_parquet(source))
    result = ctx.execute(case["sql"].replace("{T}", "t"))
    if parquet:
        codec = case.get("codec", "snappy")
        result.sink_parquet(output, compression=codec, **({"compression_level": 3} if codec == "zstd" else {}))
    else:
        result = result.collect()
if parquet:
    with output.open("rb") as handle:
        os.fsync(handle.fileno())
    fd = os.open(output.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
else:
    rows = result.rows() if hasattr(result, "rows") else [list(result)] if isinstance(result, tuple) else [[result]]
    def encode(value):
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, bytes):
            return value.decode("utf-8")
        raise TypeError(type(value).__name__)

    output.write_text(json.dumps(rows, default=encode) + "\n")
