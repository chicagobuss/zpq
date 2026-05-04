"""Python comparable Lambda for the ZPQ writer benchmark.

Modes:
  - boto3_copy   : raw S3 GET + PUT (no Parquet awareness; closest
                   thing to a `cp` baseline). Filter not supported.
  - polars_copy  : pl.scan_parquet(...).filter(...).sink_parquet(...)
                   uses Polars's lazy frame with predicate pushdown.
  - duckdb_copy  : DuckDB SQL "COPY (SELECT * FROM ... WHERE ...) TO ...".
                   Uses the httpfs extension for S3.

All modes accept:
  s3_url:      s3://bucket/key  (input)
  output_url:  s3://bucket/key  (output)
  filter_sql:  optional SQL WHERE-clause body, e.g. "int8 > 9999"

Returns a JSON envelope with timings + bytes_out so we can compare
against ZPQ's response shape directly.
"""

import time
import json
import os
from urllib.parse import urlparse

import boto3


def _parse_s3(url: str):
    p = urlparse(url)
    if p.scheme != "s3":
        raise ValueError(f"not an s3 url: {url}")
    return p.netloc, p.path.lstrip("/")


def _now_ns() -> int:
    return time.monotonic_ns()


def _ms(start: int, end: int) -> int:
    return (end - start) // 1_000_000


def _head_size(bucket: str, key: str) -> int:
    s3 = boto3.client("s3")
    return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]


def _boto3_copy(event: dict) -> dict:
    """GET + PUT — no Parquet awareness. Filter not supported."""
    if event.get("filter_sql"):
        return {"error": "boto3_copy doesn't support filter_sql"}
    s3 = boto3.client("s3")
    src_b, src_k = _parse_s3(event["s3_url"])
    dst_b, dst_k = _parse_s3(event["output_url"])

    t0 = _now_ns()
    body = s3.get_object(Bucket=src_b, Key=src_k)["Body"].read()
    t1 = _now_ns()
    s3.put_object(Bucket=dst_b, Key=dst_k, Body=body)
    t2 = _now_ns()

    return {
        "mode": "boto3_copy",
        "bytes_in": len(body),
        "bytes_out": len(body),
        "fetch_ms": _ms(t0, t1),
        "put_ms": _ms(t1, t2),
        "total_ms": _ms(t0, t2),
    }


def _polars_copy(event: dict) -> dict:
    import polars as pl

    src = event["s3_url"]
    dst = event["output_url"]
    flt = event.get("filter_sql")

    t0 = _now_ns()
    lf = pl.scan_parquet(src)
    if flt:
        # Use SQL context so the filter syntax matches ZPQ + DuckDB calls.
        ctx = pl.SQLContext(register_globals=False, eager=False)
        ctx.register("t", lf)
        lf = ctx.execute(f"SELECT * FROM t WHERE {flt}")
    lf.sink_parquet(dst)
    t1 = _now_ns()

    # bytes_out via head — Polars doesn't return the size.
    dst_b, dst_k = _parse_s3(dst)
    bytes_out = _head_size(dst_b, dst_k)

    return {
        "mode": "polars_copy",
        "filter_sql": flt,
        "bytes_out": bytes_out,
        "total_ms": _ms(t0, t1),
    }


def _polars_project(event: dict) -> dict:
    """Polars with column-projection pushdown. `columns` is a list of
    column names to keep; everything else is dropped."""
    import polars as pl

    src = event["s3_url"]
    dst = event["output_url"]
    cols = event["columns"]  # required for projection
    flt = event.get("filter_sql")

    t0 = _now_ns()
    lf = pl.scan_parquet(src).select(cols)
    if flt:
        ctx = pl.SQLContext(register_globals=False, eager=False)
        ctx.register("t", lf)
        lf = ctx.execute(f"SELECT * FROM t WHERE {flt}")
    lf.sink_parquet(dst)
    t1 = _now_ns()

    dst_b, dst_k = _parse_s3(dst)
    bytes_out = _head_size(dst_b, dst_k)

    return {
        "mode": "polars_project",
        "columns": cols,
        "filter_sql": flt,
        "bytes_out": bytes_out,
        "total_ms": _ms(t0, t1),
    }


def _duckdb_copy(event: dict) -> dict:
    import duckdb

    src = event["s3_url"]
    dst = event["output_url"]
    flt = event.get("filter_sql")

    # DuckDB tries to write extension state to ~/.duckdb; in a Lambda
    # the HOME env var isn't set by default, so point it at /tmp before
    # opening the connection.
    os.environ.setdefault("HOME", "/tmp")
    con = duckdb.connect(":memory:")
    # httpfs ships in the duckdb wheel; LOAD is a no-op if INSTALL ran
    # at build time. Force-install at first use to be safe.
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    # Pick up Lambda exec-role creds from env. DuckDB reads
    # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN /
    # AWS_REGION via the secret/credential chain when this is set.
    region = os.environ.get("AWS_REGION", "us-west-2")
    con.execute(f"SET s3_region='{region}';")
    con.execute("CREATE SECRET (TYPE S3, PROVIDER credential_chain);")

    where = f" WHERE {flt}" if flt else ""
    sql = f"COPY (SELECT * FROM read_parquet('{src}'){where}) TO '{dst}' (FORMAT 'parquet');"

    t0 = _now_ns()
    con.execute(sql)
    t1 = _now_ns()

    dst_b, dst_k = _parse_s3(dst)
    bytes_out = _head_size(dst_b, dst_k)

    return {
        "mode": "duckdb_copy",
        "filter_sql": flt,
        "bytes_out": bytes_out,
        "total_ms": _ms(t0, t1),
    }


def _polars_multi(event: dict) -> dict:
    """Multi-file scan via Polars. inputs is a list of s3 URIs;
    columns is the projection; filter_sql is optional."""
    import polars as pl

    inputs = event["inputs"]  # list of s3:// URIs
    dst = event["output_url"]
    cols = event.get("columns")
    flt = event.get("filter_sql")

    t0 = _now_ns()
    lf = pl.scan_parquet(inputs)
    if cols:
        lf = lf.select(cols)
    if flt:
        ctx = pl.SQLContext(register_globals=False, eager=False)
        ctx.register("t", lf)
        lf = ctx.execute(f"SELECT * FROM t WHERE {flt}")
    lf.sink_parquet(dst)
    t1 = _now_ns()

    dst_b, dst_k = _parse_s3(dst)
    bytes_out = _head_size(dst_b, dst_k)

    return {
        "mode": "polars_multi",
        "input_count": len(inputs),
        "columns": cols,
        "filter_sql": flt,
        "bytes_out": bytes_out,
        "total_ms": _ms(t0, t1),
    }


def _duckdb_multi(event: dict) -> dict:
    """Multi-file scan via DuckDB read_parquet([list])."""
    import duckdb

    inputs = event["inputs"]
    dst = event["output_url"]
    cols = event.get("columns")
    flt = event.get("filter_sql")

    os.environ.setdefault("HOME", "/tmp")
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    region = os.environ.get("AWS_REGION", "us-west-2")
    con.execute(f"SET s3_region='{region}';")
    con.execute("CREATE SECRET (TYPE S3, PROVIDER credential_chain);")

    select_cols = ", ".join(cols) if cols else "*"
    where = f" WHERE {flt}" if flt else ""
    files_lit = "[" + ", ".join(f"'{u}'" for u in inputs) + "]"
    sql = f"COPY (SELECT {select_cols} FROM read_parquet({files_lit}){where}) TO '{dst}' (FORMAT 'parquet');"

    t0 = _now_ns()
    con.execute(sql)
    t1 = _now_ns()

    dst_b, dst_k = _parse_s3(dst)
    bytes_out = _head_size(dst_b, dst_k)

    return {
        "mode": "duckdb_multi",
        "input_count": len(inputs),
        "columns": cols,
        "filter_sql": flt,
        "bytes_out": bytes_out,
        "total_ms": _ms(t0, t1),
    }


def _duckdb_project(event: dict) -> dict:
    import duckdb

    src = event["s3_url"]
    dst = event["output_url"]
    cols = event["columns"]
    flt = event.get("filter_sql")

    os.environ.setdefault("HOME", "/tmp")
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    region = os.environ.get("AWS_REGION", "us-west-2")
    con.execute(f"SET s3_region='{region}';")
    con.execute("CREATE SECRET (TYPE S3, PROVIDER credential_chain);")

    select_cols = ", ".join(cols)
    where = f" WHERE {flt}" if flt else ""
    sql = f"COPY (SELECT {select_cols} FROM read_parquet('{src}'){where}) TO '{dst}' (FORMAT 'parquet');"

    t0 = _now_ns()
    con.execute(sql)
    t1 = _now_ns()

    dst_b, dst_k = _parse_s3(dst)
    bytes_out = _head_size(dst_b, dst_k)

    return {
        "mode": "duckdb_project",
        "columns": cols,
        "filter_sql": flt,
        "bytes_out": bytes_out,
        "total_ms": _ms(t0, t1),
    }


_DISPATCH = {
    "boto3_copy": _boto3_copy,
    "polars_copy": _polars_copy,
    "polars_project": _polars_project,
    "polars_multi": _polars_multi,
    "duckdb_copy": _duckdb_copy,
    "duckdb_project": _duckdb_project,
    "duckdb_multi": _duckdb_multi,
}


def handler(event, _ctx):
    mode = event.get("mode")
    fn = _DISPATCH.get(mode)
    if fn is None:
        return {"error": f"unknown mode: {mode}", "modes": list(_DISPATCH)}
    try:
        return fn(event)
    except Exception as e:  # noqa: BLE001
        return {"error": type(e).__name__, "reason": str(e)}
