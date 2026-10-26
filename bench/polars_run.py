"""Run one bench query under Polars and write its answer; standalone, for the Polars interpreter.

    polars_run.py REQUEST_JSON

`runs` executions in one process (1 for the fresh-process lanes; warm-up plus N for the engine lane), each timed
from planning through `collect()` or the sink; answers are written after the timer. Prints {"engine_ns": [...]}.
"""

import datetime
import decimal
import json
import math
import sys
import time
from pathlib import Path


def pdsh(pl, path, query_id):
    li = pl.scan_parquet(path)
    if query_id == "1":
        price = pl.col("l_extendedprice") * (1.0 - pl.col("l_discount"))
        return (
            li.filter(pl.col("l_shipdate") <= datetime.date(1998, 9, 2))
            .group_by("l_returnflag", "l_linestatus")
            .agg(pl.sum("l_quantity").alias("sum_qty"), pl.sum("l_extendedprice").alias("sum_base_price"),
                 price.sum().alias("sum_disc_price"), (price * (1.0 + pl.col("l_tax"))).sum().alias("sum_charge"),
                 pl.mean("l_quantity").alias("avg_qty"), pl.mean("l_extendedprice").alias("avg_price"),
                 pl.mean("l_discount").alias("avg_disc"), pl.len().alias("count_order"))
            .sort("l_returnflag", "l_linestatus")
        )
    return (
        li.filter(pl.col("l_shipdate").is_between(datetime.date(1994, 1, 1), datetime.date(1995, 1, 1),
                                                  closed="left"))
        .filter(pl.col("l_discount").is_between(0.05, 0.07))
        .filter(pl.col("l_quantity") < 24)
        .select((pl.col("l_extendedprice") * pl.col("l_discount")).sum().alias("revenue"))
    )


def plan(pl, req):
    """The lazy query, or a collected frame for ClickBench's published expressions (they end in collect())."""
    if req["form"] == "clickbench":
        hits = pl.scan_parquet(req["inputs"]).with_columns(
            (pl.col("EventTime") * 1_000_000).cast(pl.Datetime(time_unit="us")), pl.col("EventDate").cast(pl.Date))
        return eval(req["expr"], {"hits": hits, "pl": pl, "date": datetime.date})  # noqa: S307 - pinned query text
    if req["form"] == "pdsh":
        return pdsh(pl, req["inputs"][0], req["query_id"])
    return pl.SQLContext(t=pl.scan_parquet(req["inputs"])).execute(req["sql"])


def cell(v):
    if isinstance(v, float) and not math.isfinite(v):
        return "NaN" if v != v else "Infinity" if v > 0 else "-Infinity"
    if isinstance(v, (datetime.date, datetime.time, datetime.timedelta)):
        return str(v)  # DuckDB's text: "2013-07-15 12:34:56", never ISO's "T"
    if isinstance(v, bytes):
        return v.decode("utf-8")
    if isinstance(v, list):
        return [cell(x) for x in v]
    if isinstance(v, dict):
        return {k: cell(x) for k, x in v.items()}
    return v


def encode(v):
    """JSON text with a Decimal's digits verbatim: the number grammar carries them, float() would round them."""
    if isinstance(v, decimal.Decimal):
        return str(v) if v.is_finite() else json.dumps(str(v))
    if isinstance(v, list):
        return "[" + ",".join(map(encode, v)) + "]"
    if isinstance(v, dict):
        return "{" + ",".join(json.dumps(k) + ":" + encode(x) for k, x in v.items()) + "}"
    return json.dumps(v, allow_nan=False)


def answer(pl, frame, req):
    if "EventTime" in frame.columns and isinstance(frame.schema["EventTime"], pl.Datetime):
        # The scan above made epoch seconds a Datetime for the expressions; SQL engines answer in seconds. A derived
        # column that kept the name (q18's minute) is not a timestamp and stays as it is.
        frame = frame.with_columns(pl.col("EventTime").dt.epoch("s"))
    if req.get("columns"):
        frame = frame.select(req["columns"])
    return encode([{k: cell(v) for k, v in row.items()} for row in frame.to_dicts()])


def main():
    req = json.loads(sys.argv[1])
    import polars as pl

    pl.Config.set_engine_affinity("streaming")
    times = []
    for i in range(req["runs"]):
        sink = req.get("sink")
        start = time.monotonic_ns()
        query = plan(pl, req)
        if sink:
            if req.get("timestamp_units"):
                query = query.with_columns(pl.col(n).dt.cast_time_unit(u) for n, u in req["timestamp_units"].items())
            level = {"compression_level": req["level"]} if req.get("level") else {}
            query.sink_parquet(sink[i], compression=req.get("codec") or "snappy", **level)
            frame = None
        else:
            # Some published ClickBench forms end in `.rows()[0]`: a plain tuple is one positional row.
            frame = (query if isinstance(query, pl.DataFrame) else
                     pl.DataFrame([list(query)], orient="row") if isinstance(query, tuple) else query.collect())
        times.append(time.monotonic_ns() - start)
        if frame is not None:
            Path(req["answers"][i]).write_text(answer(pl, frame, req))
    print(json.dumps({"engine_ns": times}))


if __name__ == "__main__":
    main()
