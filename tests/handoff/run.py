#!/usr/bin/env python3
"""The Python hand-off gate: zpq called from Python writes Parquet that DuckDB and PyArrow read with the same answer.

For each source file and query cell it checks, against the same SQL run by DuckDB on the source:
  py-rows    libzpq SQL through python/zpq.py (`query_raw`), its NDJSON values and types
  cli=py     the CLI's stdout for that SQL is byte-identical to `query_raw`
  py-write   a structured request with `output_url` (python/zpq.py `run`): the file read whole by PyArrow and by
             DuckDB, values and logical types; for aggregates also the JSON `agg` the call returns
  cli-write  `zpq query --query SQL -o FILE`, read the same two ways
  duck>zpq   the source rewritten by DuckDB, read back by zpq (CLI and Python), against DuckDB on that file
  arrow>zpq  the source rewritten by PyArrow, likewise

    .venv/bin/python -I tests/handoff/run.py [--zpq zig-out/bin/zpq] [--lib zig-out/lib/libzpq.so] [--report FILE]

Cells a finding is filed against are listed in tests/handoff/known.json and reported KNOWN, not FAIL; one of them that
passes is reported FIXED, so the list can be trimmed. Exit 1 on any FAIL. Study files (large synthetic tables) are read
from $ZPQ_HANDOFF_STUDY (default ~/bench/zpq-general/data) and skipped when absent.
"""

import argparse
import base64
import json
import math
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import traceback
import uuid
from decimal import Decimal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "python"))
import zpq  # noqa: E402

PT = os.path.join(ROOT, "data", "parquet-testing", "data")
STUDY = os.environ.get("ZPQ_HANDOFF_STUDY", os.path.expanduser("~/bench/zpq-general/data"))
UNIT_NS = {"s": 10**9, "ms": 10**6, "us": 10**3, "ns": 1}
# Rows of a large source that the reverse checks rewrite: enough for several row groups' worth of values.
REWRITE_ROWS = 200_000
# Checks that print rows: where a source has a repeated column, a refusal is their documented answer.
ROWS = ("py-rows", "cli=py", "duck>zpq", "arrow>zpq")


# ---------------------------------------------------------------------------------------------------------------------
# Generated sources: types the corpus files lack, written by each of the two readers we hand off to.

def gen_duck_types(path):
    con = duckdb.connect()
    con.execute(f"""
        COPY (
          SELECT i AS id,
                 CASE WHEN i % 7 = 0 THEN NULL ELSE uuid_from_i(i) END AS u,
                 to_months(i % 13) + to_days(i % 31) + to_milliseconds(i * 1001) AS iv,
                 (TIMESTAMP '2024-01-01 00:00:00' + to_milliseconds(i * 3600123))::TIMESTAMP_MS AS ts_ms,
                 TIMESTAMP '1969-12-31 23:00:00' + to_microseconds(i * 3600123457) AS ts_us,
                 (TIMESTAMP_NS '2262-01-01 00:00:00' - to_microseconds(i * 86400000001))::TIMESTAMP_NS AS ts_ns,
                 TIMESTAMPTZ '2024-03-10 01:30:00+00' + to_minutes(i * 17) AS tstz,
                 TIME '00:00:00' + to_microseconds((i * 1234567891) % 86400000000) AS t,
                 CASE WHEN i % 5 = 0 THEN NULL ELSE (i * 1234567890123.0123456789)::DECIMAL(38,10) END AS d38,
                 (i * 3.07)::DECIMAL(9,2) AS d9,
                 (i % 256)::UTINYINT AS u8, ((i * 40000) % 65535)::USMALLINT AS u16,
                 (4000000000 + i)::UINTEGER AS u32, (18446744073709550000 + i)::UBIGINT AS u64,
                 ((i % 256) - 128)::TINYINT AS i8,
                 CASE WHEN i % 3 = 0 THEN '\\xff\\x00'::BLOB WHEN i % 3 = 1 THEN 'plain'::BLOB ELSE NULL END AS blob,
                 CASE i % 4 WHEN 0 THEN 'naïve' WHEN 1 THEN '日本語' WHEN 2 THEN '' ELSE NULL END AS s,
                 i % 2 = 0 AS b,
                 {{'a': i, 'b': 'x' || i}} AS st,
                 [i, i + 1] AS l
          FROM (SELECT range AS i FROM range(1, 1001)) )
        TO '{path}' (FORMAT parquet, ROW_GROUP_SIZE 300)
    """.replace("uuid_from_i(i)", "('00000000-0000-4000-8000-' || lpad(printf('%x', i * 7919), 12, '0'))::UUID"))
    con.close()


def gen_arrow_types(path):
    n = 1000
    ids = list(range(n))
    tbl = pa.table({
        "id": pa.array(ids, pa.int32()),
        "fsb": pa.array([struct.pack(">I", (i * 2654435761) & 0xFFFFFFFF) if i % 9 else None for i in ids],
                        pa.binary(4)),
        "u": pa.array([uuid.UUID(int=(i * 0x9E3779B97F4A7C15) % (1 << 128)).bytes for i in ids], pa.uuid()),
        "h": pa.array([None if i % 11 == 0 else (i - 500) / 8 for i in ids], pa.float64()).cast(pa.float16()),
        "ts_ms_tz": pa.array([1_700_000_000_000 + i * 86_400_123 for i in ids], pa.timestamp("ms", tz="UTC")),
        "ts_ns": pa.array([-1_000_000_000_000_000_000 + i * 1_000_000_000_007 for i in ids], pa.timestamp("ns")),
        "i8": pa.array([(i % 256) - 128 for i in ids], pa.int8()),
        "u64": pa.array([(1 << 64) - 1 - i for i in ids], pa.uint64()),
        "ls": pa.array([None if i % 13 == 0 else "ζ" * (i % 5) + str(i) for i in ids], pa.large_string()),
        "bin": pa.array([bytes([i % 256, 255 - i % 256]) for i in ids], pa.binary()),
        "dict": pa.array(["red", "green", "blue", None][i % 4] for i in ids).dictionary_encode(),
        "d": pa.array([i * 37 - 20000 for i in ids], pa.int32()).cast(pa.date32()),
        "t": pa.array([(i * 86_399) % 86_400_000 for i in ids], pa.int32()).cast(pa.time32("ms")),
        "f32": pa.array([float("nan") if i == 500 else i / 3 for i in ids], pa.float32()),
    })
    pq.write_table(tbl, path, row_group_size=256, use_dictionary=["dict", "ls"], store_schema=False)


def gen_one_row(path):
    pq.write_table(pa.table({"k": pa.array(["only"]), "v": pa.array([Decimal("12345678901234567890.12")],
                                                                    pa.decimal128(38, 2))}), path)


def gen_empty(path):
    pq.write_table(pa.table({"k": pa.array([], pa.string()), "v": pa.array([], pa.int64())}), path)


# ---------------------------------------------------------------------------------------------------------------------
# The matrix. A cell is a SQL text over `{src}` that zpq and DuckDB both run, and optionally the structured request
# (filter / columns / select / aggregate / group_by) that writes the same table from Python.

def cell(cid, sql, req=None, order=False, passthrough=False, approx=(), units=None, refuse=(), widened=(), renames=None):
    """`refuse`: the checks where a refusal is the documented answer (rows of repeated columns). `widened`: computed
    outputs of a FLOAT16 column, which a write widens to DOUBLE by design (as `WIDENED` allows a passthrough).
    `renames`: output name -> source column for aliases, which must keep the source's type exactly, as a passthrough."""
    return {"id": cid, "sql": sql, "req": req, "order": order, "passthrough": passthrough, "approx": set(approx),
            "units": units or {}, "refuse": set(refuse), "widened": set(widened), "renames": renames or {}}


def sources(work):
    out = []

    def src(name, path, desc, cells, big=False, gen=None):
        out.append({"name": name, "path": path, "desc": desc, "cells": cells, "big": big, "gen": gen})

    src("alltypes_plain", f"{PT}/alltypes_plain.parquet", "INT96, BOOLEAN, FLOAT/DOUBLE, unannotated BYTE_ARRAY", [
        cell("proj", "SELECT id, bool_col, tinyint_col, bigint_col, float_col, double_col, date_string_col, string_col "
             "FROM '{src}' WHERE id >= 2 ORDER BY id", order=True, passthrough=True,
             req={"filter": "id >= 2", "columns": ["id", "bool_col", "tinyint_col", "bigint_col", "float_col",
                                                   "double_col", "date_string_col", "string_col"]}),
        cell("int96", "SELECT id, timestamp_col FROM '{src}' ORDER BY id", order=True, passthrough=True,
             req={"filter": "id >= 0", "columns": ["id", "timestamp_col"]}),
        cell("agg", "SELECT count(*) AS n, sum(bigint_col) AS s, min(float_col) AS mn, max(double_col) AS mx "
             "FROM '{src}'", req={"aggregate": "count(*) AS n, sum(bigint_col) AS s, min(float_col) AS mn, "
                                                "max(double_col) AS mx"}),
        cell("grp_bool", "SELECT bool_col, count(*) AS n, sum(int_col) AS s FROM '{src}' GROUP BY bool_col",
             req={"group_by": "bool_col", "aggregate": "count(*) AS n, sum(int_col) AS s"}),
    ])
    src("alltypes_dictionary", f"{PT}/alltypes_dictionary.parquet", "dictionary-encoded columns", [
        cell("proj", "SELECT id, string_col, double_col FROM '{src}'", passthrough=True,
             req={"columns": ["id", "string_col", "double_col"]}),
        cell("grp_str", "SELECT string_col, count(*) AS n, max(id) AS m FROM '{src}' GROUP BY string_col",
             req={"group_by": "string_col", "aggregate": "count(*) AS n, max(id) AS m"}),
    ])
    src("flba_decimal", f"{PT}/fixed_length_decimal.parquet", "DECIMAL(25,2) on FIXED_LEN_BYTE_ARRAY", [
        cell("proj", "SELECT value FROM '{src}' WHERE value > 10.00", passthrough=True,
             req={"filter": "value > 10.00", "columns": ["value"]}),
        cell("agg", "SELECT sum(value) AS s, min(value) AS mn, max(value) AS mx, count(value) AS n FROM '{src}'",
             req={"aggregate": "sum(value) AS s, min(value) AS mn, max(value) AS mx, count(value) AS n"}),
        cell("avg", "SELECT avg(value) AS a FROM '{src}'", req={"aggregate": "avg(value) AS a"}, approx=["a"]),
    ])
    src("bytearray_decimal", f"{PT}/byte_array_decimal.parquet", "DECIMAL(4,2) on BYTE_ARRAY", [
        cell("proj", "SELECT value FROM '{src}' WHERE value >= 5.00 ORDER BY value", order=True,
             req={"filter": "value >= 5.00", "columns": ["value"]}),
        cell("agg", "SELECT sum(value) AS s, max(value) AS mx FROM '{src}'",
             req={"aggregate": "sum(value) AS s, max(value) AS mx"}),
    ])
    src("null_pages", f"{PT}/int32_with_null_pages.parquet", "INT32 with all-null pages", [
        cell("proj", "SELECT int32_field FROM '{src}' WHERE int32_field IS NOT NULL", passthrough=True,
             req={"filter": "int32_field IS NOT NULL", "columns": ["int32_field"]}),
        cell("agg", "SELECT count(*) AS n, count(int32_field) AS c, sum(int32_field) AS s, min(int32_field) AS mn "
             "FROM '{src}'", req={"aggregate": "count(*) AS n, count(int32_field) AS c, sum(int32_field) AS s, "
                                                "min(int32_field) AS mn"}),
        cell("empty", "SELECT int32_field FROM '{src}' WHERE int32_field > 2147483646",
             req={"filter": "int32_field > 2147483646", "columns": ["int32_field"]}, passthrough=True),
    ])
    src("list_columns", f"{PT}/list_columns.parquet", "LIST<INT64>, LIST<STRING>", [
        cell("rows_refused", "SELECT int64_list FROM '{src}'", refuse=ROWS),
        cell("copy", "SELECT * FROM '{src}'", req={}, passthrough=True, refuse=ROWS),
    ])
    src("delta_strings", f"{PT}/delta_byte_array.parquet", "DELTA_BYTE_ARRAY strings", [
        cell("proj", "SELECT c_customer_id, c_first_name, c_birth_country FROM '{src}' WHERE c_birth_country = 'CHILE'",
             passthrough=True, req={"filter": "c_birth_country = 'CHILE'",
                                    "columns": ["c_customer_id", "c_first_name", "c_birth_country"]}),
        cell("topn", "SELECT c_last_name, c_customer_id FROM '{src}' ORDER BY c_last_name DESC, c_customer_id "
             "LIMIT 5", order=True),
        cell("cdist", "SELECT count(DISTINCT c_birth_country) AS d FROM '{src}'"),
    ])
    src("bench_types", os.path.join(ROOT, "data", "bench_types.parquet"),
        "DECIMAL(18,2) INT64, DATE, TIMESTAMP(us), STRING, BOOLEAN; 5M rows, 41 row groups", [
            cell("proj", "SELECT id, amt, price, name, d, ts, flag FROM '{src}' WHERE id BETWEEN 1999990 AND 2000010",
                 passthrough=True, req={"filter": "id BETWEEN 1999990 AND 2000010",
                                        "columns": ["id", "amt", "price", "name", "d", "ts", "flag"]}),
            cell("agg", "SELECT count(*) AS n, sum(price) AS s, min(d) AS md, max(ts) AS mt, sum(amt) AS sa "
                 "FROM '{src}' WHERE flag = true", approx=["sa"],
                 req={"filter": "flag = true", "aggregate": "count(*) AS n, sum(price) AS s, min(d) AS md, "
                                                            "max(ts) AS mt, sum(amt) AS sa"}),
            cell("grp_bool", "SELECT flag, count(*) AS n, sum(price) AS s FROM '{src}' GROUP BY flag",
                 req={"group_by": "flag", "aggregate": "count(*) AS n, sum(price) AS s"}),
            cell("bare_bool", "SELECT count(*) AS n FROM '{src}' WHERE flag AND NOT (id < 10)",
                 req={"filter": "flag AND NOT (id < 10)", "aggregate": "count(*) AS n"}),
            cell("grp_date", "SELECT d, count(*) AS n FROM '{src}' WHERE id < 100000 GROUP BY d ORDER BY d LIMIT 3",
                 order=True),
            cell("one", "SELECT max(price) AS m FROM '{src}'", req={"aggregate": "max(price) AS m"}),
        ], big=True)
    if os.path.exists(os.path.join(STUDY, "nullszipf.parquet")):
        src("study_nullszipf", os.path.join(STUDY, "nullszipf.parquet"), "INT64 keys with NULLs, Zipf; 30M rows", [
            cell("grp", "SELECT k, count(*) AS n, sum(v) AS s, count(x) AS c FROM '{src}' GROUP BY k "
                 "ORDER BY n DESC, k LIMIT 20", order=True),
            cell("grp_write", "SELECT f, count(*) AS n, min(v) AS mn FROM '{src}' WHERE f < 3 GROUP BY f",
                 req={"filter": "f < 3", "group_by": "f", "aggregate": "count(*) AS n, min(v) AS mn"}),
        ], big=True)
    if os.path.exists(os.path.join(STUDY, "sstr100k.parquet")):
        src("study_sstr100k", os.path.join(STUDY, "sstr100k.parquet"), "STRING keys, dictionary; 30M rows", [
            cell("topk", "SELECT k, count(*) AS n FROM '{src}' GROUP BY k ORDER BY n DESC, k LIMIT 10", order=True),
            cell("proj", "SELECT k, v, f FROM '{src}' WHERE x = 12345", passthrough=True,
                 req={"filter": "x = 12345", "columns": ["k", "v", "f"]}),
        ], big=True)
    src("gen_duck_types", os.path.join(work, "gen_duck_types.parquet"),
        "DuckDB-written: UUID, INTERVAL, TIMESTAMP ms/us/ns/tz, TIME, DECIMAL(38,10), unsigned, BLOB, STRUCT, LIST", [
            cell("proj", "SELECT id, u, iv, ts_ms, ts_us, ts_ns, tstz, t, d38, d9, u8, u16, u32, u64, i8, blob, s, b "
                 "FROM '{src}' WHERE id IN (3, 53, 103, 153, 203, 253, 303, 353, 403, 453, 503, 553, 603, 653, 703, "
                 "753, 803, 853, 903, 953)", passthrough=True,
                 req={"filter": "id IN (3, 53, 103, 153, 203, 253, 303, 353, 403, 453, 503, 553, 603, 653, 703, "
                                "753, 803, 853, 903, 953)",
                      "columns": ["id", "u", "iv", "ts_ms", "ts_us", "ts_ns", "tstz", "t", "d38", "d9", "u8",
                                  "u16", "u32", "u64", "i8", "blob", "s", "b"]}),
            cell("agg", "SELECT sum(d38) AS s38, min(ts_ns) AS mns, max(tstz) AS mtz, max(u64) AS mu, "
                 "min(blob) AS mb, count(u) AS cu FROM '{src}'",
                 req={"aggregate": "sum(d38) AS s38, min(ts_ns) AS mns, max(tstz) AS mtz, max(u64) AS mu, "
                                   "min(blob) AS mb, count(u) AS cu"}),
            cell("grp_uuid", "SELECT u, count(*) AS n FROM '{src}' WHERE id <= 30 GROUP BY u",
                 req={"filter": "id <= 30", "group_by": "u", "aggregate": "count(*) AS n"}),
            cell("grp_bool", "SELECT b, sum(d9) AS s FROM '{src}' GROUP BY b",
                 req={"group_by": "b", "aggregate": "sum(d9) AS s"}),
            cell("struct", "SELECT id, st.a AS sa, st.b AS sb FROM '{src}' WHERE id < 4",
                 req={"filter": "id < 4", "select": "id, st.a AS sa, st.b AS sb"}),
            cell("struct_whole", "SELECT id, st FROM '{src}' WHERE id < 4", passthrough=True,
                 req={"filter": "id < 4", "columns": ["id", "st"]}),
            cell("bool_minmax", "SELECT min(b) AS mn, max(b) AS mx FROM '{src}'",
                 req={"aggregate": "min(b) AS mn, max(b) AS mx"}),
            # BOOLEAN-typed expressions: an aliased column, a CASE over one, as rows, keys and min/max.
            cell("alias_types", "SELECT id AS k, u8 AS a8, u16 AS a16, u32 AS a32, i8 AS ai8, d38 AS ad, u AS au, "
                 "tstz AS az, st.a AS sa FROM '{src}' WHERE id < 40",
                 req={"filter": "id < 40", "select": "id AS k, u8 AS a8, u16 AS a16, u32 AS a32, i8 AS ai8, d38 AS ad, "
                                                     "u AS au, tstz AS az, st.a AS sa"},
                 renames={"k": "id", "a8": "u8", "a16": "u16", "a32": "u32", "ai8": "i8", "ad": "d38", "au": "u",
                          "az": "tstz", "sa": "st.a"}),
            cell("bool_alias", "SELECT id, b AS k FROM '{src}' WHERE id < 40",
                 req={"filter": "id < 40", "select": "id, b AS k"}),
            cell("bool_case", "SELECT id, CASE WHEN id < 20 THEN b END AS k FROM '{src}' WHERE id < 40",
                 req={"filter": "id < 40", "select": "id, CASE WHEN id < 20 THEN b END AS k"}),
            cell("bool_case_key", "SELECT CASE WHEN id < 500 THEN b END AS k, count(*) AS n FROM '{src}' GROUP BY k"),
            cell("bool_case_minmax", "SELECT min(CASE WHEN id < 500 THEN b END) AS mn, "
                 "max(CASE WHEN id > 500 THEN b END) AS mx, max(CASE WHEN id < 0 THEN b END) AS none FROM '{src}'",
                 req={"aggregate": "min(CASE WHEN id < 500 THEN b END) AS mn, max(CASE WHEN id > 500 THEN b END) AS mx, "
                                   "max(CASE WHEN id < 0 THEN b END) AS none"}),
            cell("copy", "SELECT * FROM '{src}'", req={}, passthrough=True, refuse=ROWS),
        ], gen=gen_duck_types)
    src("gen_arrow_types", os.path.join(work, "gen_arrow_types.parquet"),
        "PyArrow-written: FLBA(4), UUID, FLOAT16, TIMESTAMP ms tz / ns, INT8, UINT64, dictionary, DATE, TIME(ms)", [
            cell("proj", "SELECT id, fsb, u, h, ts_ms_tz, ts_ns, i8, u64, ls, bin, dict, d, t, f32 FROM '{src}' "
                 "WHERE id < 40", passthrough=True,
                 req={"filter": "id < 40", "columns": ["id", "fsb", "u", "h", "ts_ms_tz", "ts_ns", "i8", "u64",
                                                       "ls", "bin", "dict", "d", "t", "f32"]}),
            cell("agg", "SELECT max(u64) AS mu, min(ts_ns) AS mt, max(h) AS mh, min(d) AS md, max(f32) AS mf, "
                 "count(ls) AS c FROM '{src}'",
                 req={"aggregate": "max(u64) AS mu, min(ts_ns) AS mt, max(h) AS mh, min(d) AS md, max(f32) AS mf, "
                                   "count(ls) AS c"}, widened=("mh",)),
            # An alias is a rename: each keeps its column's type exactly, NULLs included.
            cell("alias_types", "SELECT id AS k, i8 AS a8, d AS ad, u64 AS au, h AS ah, t AS at, f32 AS af, ts_ns AS an "
                 "FROM '{src}' WHERE id < 40",
                 req={"filter": "id < 40", "select": "id AS k, i8 AS a8, d AS ad, u64 AS au, h AS ah, t AS at, f32 AS af, "
                                                     "ts_ns AS an"},
                 renames={"k": "id", "a8": "i8", "ad": "d", "au": "u64", "ah": "h", "at": "t", "af": "f32", "an": "ts_ns"}),
            cell("grp_dict", "SELECT dict, count(*) AS n, sum(i8) AS s FROM '{src}' GROUP BY dict",
                 req={"group_by": "dict", "aggregate": "count(*) AS n, sum(i8) AS s"}),
            cell("topn_ts", "SELECT id, ts_ms_tz FROM '{src}' ORDER BY ts_ms_tz DESC LIMIT 3", order=True,
                 passthrough=True),
        ], gen=gen_arrow_types)
    src("gen_one_row", os.path.join(work, "gen_one_row.parquet"), "one row, DECIMAL(38,2) past 2^53", [
        cell("rows", "SELECT k, v FROM '{src}'", passthrough=True, req={"columns": ["k", "v"]}),
        cell("agg", "SELECT sum(v) AS s, count(*) AS n FROM '{src}'", req={"aggregate": "sum(v) AS s, count(*) AS n"}),
    ], gen=gen_one_row)
    src("gen_empty", os.path.join(work, "gen_empty.parquet"), "zero rows", [
        cell("rows", "SELECT k, v FROM '{src}'", passthrough=True, req={"columns": ["k", "v"]}),
        cell("agg", "SELECT count(*) AS n, sum(v) AS s, min(k) AS mk FROM '{src}'",
             req={"aggregate": "count(*) AS n, sum(v) AS s, min(k) AS mk"}),
        cell("grp", "SELECT k, count(*) AS n FROM '{src}' GROUP BY k",
             req={"group_by": "k", "aggregate": "count(*) AS n"}),
    ], gen=gen_empty)
    return out


# ---------------------------------------------------------------------------------------------------------------------
# Values. Every reader's answer becomes a list of row tuples of canonical Python values, typed by the oracle's Arrow
# schema: numbers exact (Decimal or int) or float, temporal values as integer nanoseconds (days for DATE), bytes,
# UUIDs as canonical text, intervals as (months, days, nanoseconds), structs flattened to dotted names.

class Mismatch(Exception):
    pass


def flat_fields(schema, prefix=""):
    """(dotted name, type) for every column, structs flattened as zpq names their leaves."""
    out = []
    for f in schema:
        name = prefix + f.name
        if pa.types.is_struct(f.type):
            out.extend(flat_fields(f.type, name + "."))
        else:
            out.append((name, f.type))
    return out


def flat_table(t):
    while any(pa.types.is_struct(f.type) for f in t.schema):
        t = t.flatten()
    return t


def is_uuid(t):
    return getattr(t, "extension_name", None) == "arrow.uuid"


def arrow_values(col, t):
    """Canonical values of one Arrow column."""
    if isinstance(t, pa.DictionaryType):
        col = col.cast(t.value_type)
        t = t.value_type
    if is_uuid(t):
        return [None if v is None else str(uuid.UUID(bytes=v)) for v in col.cast(t.storage_type).to_pylist()]
    if pa.types.is_timestamp(t):
        return [None if v is None else v * UNIT_NS[t.unit] for v in col.cast(pa.int64()).to_pylist()]
    if pa.types.is_time(t):
        ints = col.cast(pa.int32() if pa.types.is_time32(t) else pa.int64())
        return [None if v is None else v * UNIT_NS[t.unit] for v in ints.to_pylist()]
    if pa.types.is_date32(t):
        return col.cast(pa.int32()).to_pylist()
    if pa.types.is_interval(t):
        return [None if v is None else (v.months, v.days, v.nanoseconds) for v in col.to_pylist()]
    if pa.types.is_float16(t):
        return [None if v is None else float(v) for v in col.to_pylist()]
    return col.to_pylist()


def json_value(v, t, unit=None):
    """Canonical value of one zpq JSON value whose oracle type is `t`."""
    if v is None:
        return None
    if isinstance(t, pa.DictionaryType):
        t = t.value_type
    if is_uuid(t):
        if not isinstance(v, str):
            raise Mismatch(f"UUID printed as {v!r}")
        return str(uuid.UUID(v))
    if pa.types.is_boolean(t):
        if not isinstance(v, bool):
            raise Mismatch(f"BOOLEAN printed as {v!r}")
        return v
    if pa.types.is_timestamp(t) or pa.types.is_time(t):
        if not isinstance(v, int):
            raise Mismatch(f"temporal value printed as {v!r}")
        return v * UNIT_NS[unit or t.unit]
    if pa.types.is_date32(t):
        return v
    if pa.types.is_interval(t):
        return (v["months"], v["days"], v["millis"] * 10**6)
    if pa.types.is_floating(t):
        return v if isinstance(v, dict) else float(v)  # a dict is avg's {"sum", "count"}; "NaN" is a string
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        if isinstance(v, dict):
            return base64.b64decode(v["base64"])
        return v.encode()
    if pa.types.is_integer(t) or pa.types.is_decimal(t):
        if isinstance(v, (int, Decimal)):
            return v
        raise Mismatch(f"number printed as {v!r}")
    return v


def eq(a, b, approx=False, f32=False):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, dict) or isinstance(b, dict):  # avg pair against a float average
        d, f = (a, b) if isinstance(a, dict) else (b, a)
        if d.get("count") in (0, None):
            return f is None
        avg = Decimal(str(d["avg"])) if "avg" in d else Decimal(str(d["sum"])) / Decimal(d["count"])
        return math.isclose(float(avg), float(f), rel_tol=1e-12)
    if isinstance(a, float) or isinstance(b, float):
        a, b = float(a), float(b)
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        if f32:
            return struct.pack("<f", a) == struct.pack("<f", b)
        if approx:
            return math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-300)
        return a == b and math.copysign(1, a) == math.copysign(1, b)
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, (int, Decimal)) and isinstance(b, (int, Decimal)):
        return Decimal(a) == Decimal(b)
    return a == b


def sort_key(row):
    def k(v):
        if v is None:
            return (0, "")
        if isinstance(v, bool):
            return (1, str(v))
        if isinstance(v, (int, Decimal)):
            return (2, format(Decimal(v).normalize(), "f"))
        if isinstance(v, float):
            return (3, repr(v))
        if isinstance(v, dict):
            return (4, json.dumps(v, sort_keys=True, default=str))
        return (5, repr(v))
    return [k(v) for v in row]


def compare_rows(got, want, names, order, approx, f32cols):
    if len(got) != len(want):
        raise Mismatch(f"{len(got)} rows, expected {len(want)}")
    if not order:
        got, want = sorted(got, key=sort_key), sorted(want, key=sort_key)
    for i, (g, w) in enumerate(zip(got, want)):
        for j, name in enumerate(names):
            if not eq(g[j], w[j], approx=name in approx, f32=name in f32cols):
                raise Mismatch(f"row {i} column {name}: got {g[j]!r}, expected {w[j]!r}")


def table_rows(t):
    """Canonical row tuples of an Arrow table, typed by its own schema; avg split columns rejoined."""
    t = flat_table(t)
    cols = [arrow_values(t.column(i), t.schema.field(i).type) for i in range(t.num_columns)]
    names = t.schema.names
    # `-o` writes an avg as `<name>__sum` and `<name>__count`.
    joined, rows_cols = [], []
    skip = set()
    for i, n in enumerate(names):
        if n in skip:
            continue
        if n.endswith("__sum") and n[:-5] + "__count" in names:
            j = names.index(n[:-5] + "__count")
            skip.add(names[j])
            joined.append(n[:-5])
            rows_cols.append([None if s is None and c in (0, None) else {"sum": s, "count": c}
                              for s, c in zip(cols[i], cols[j])])
            continue
        joined.append(n)
        rows_cols.append(cols[i])
    return joined, [tuple(r) for r in zip(*rows_cols)] if rows_cols else []


# ---------------------------------------------------------------------------------------------------------------------
# Types of written files.

def family(t):
    if isinstance(t, pa.DictionaryType):
        t = t.value_type
    if is_uuid(t):
        return ("uuid",)
    if pa.types.is_boolean(t):
        return ("bool",)
    if pa.types.is_integer(t):
        return ("dec", 0)
    if pa.types.is_decimal(t):
        return ("dec", t.scale)
    if pa.types.is_float16(t):
        return ("float", 16)
    if pa.types.is_floating(t):
        return ("float", t.bit_width)
    if pa.types.is_timestamp(t):  # the unit is checked where a column passes through; DuckDB answers in us
        return ("ts", t.tz is not None)
    if pa.types.is_time(t):
        return ("time", t.unit)
    if pa.types.is_date(t):
        return ("date",)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return ("str",)
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return ("bin",)
    if pa.types.is_fixed_size_binary(t):
        return ("fsb", t.byte_width)
    if pa.types.is_interval(t):
        return ("interval",)
    return (str(t),)


# ---------------------------------------------------------------------------------------------------------------------
# Runners.

class Ctx:
    def __init__(self, args, work):
        self.zpq_bin = args.zpq
        self.work = work
        self.z = zpq.Zpq(max_memory=args.max_memory, library=args.lib)
        self.duck = duckdb.connect()
        self.duck.execute(f"SET threads = {args.duck_threads}")
        self.duck.execute(f"SET memory_limit = '{args.duck_memory}'")
        self.n = 0

    def tmp(self, stem):
        self.n += 1
        return os.path.join(self.work, f"{self.n:04d}-{stem}.parquet")

    def oracle(self, sql):
        return self.duck.execute(sql).to_arrow_table()

    def zpq_rows(self, sql, oracle, units):
        """zpq's NDJSON through Python (`query_raw`): the text, and canonical rows typed by the oracle."""
        raw = self.z.query_raw(sql)
        names = [n for n, _ in flat_fields(oracle.schema)]
        objs = [json.loads(line, parse_float=Decimal) for line in raw.split("\n") if line]
        return raw, names, objs

    def typed(self, objs, names, oracle, units):
        return json_rows(objs, names, dict(flat_fields(oracle.schema)), units)

    def cli(self, *args):
        return subprocess.run([self.zpq_bin, *args], capture_output=True, timeout=600)


def json_rows(objs, names, types, units):
    rows = []
    for obj in objs:
        if list(obj) != names:
            raise Mismatch(f"columns {list(obj)}, expected {names}")
        rows.append(tuple(json_value(obj[n], types[n], units.get(n)) for n in names))
    return rows


def zpq_units(path, overrides, renames=None):
    """The unit zpq prints each temporal column in: the file's own, by name; aliases from the cell."""
    units = {n: t.unit for n, t in flat_fields(pq.read_schema(path))
             if pa.types.is_timestamp(t) or pa.types.is_time(t)}
    # An alias prints in its source column's unit.
    units.update({a: units[c] for a, c in (renames or {}).items() if c in units})
    units.update(overrides)
    return units


def float_cols(schema):
    return {n for n, t in flat_fields(schema) if pa.types.is_float32(t) or pa.types.is_float16(t)}


def message(e):
    r = e.response
    return r.get("message") or r.get("reason") or json.dumps(r)


def run_cell(ctx, s, c, src_path, results, notes):
    sql = c["sql"].format(src=src_path)
    try:
        oracle = ctx.oracle(sql)
    except Exception as e:  # the oracle itself cannot run it: nothing to compare
        for k in ("py-rows", "cli=py", "py-write", "cli-write"):
            results[k] = "n/a"
        notes.append(f"oracle failed: {e}")
        return
    want_names, want_rows = table_rows(oracle)
    f32 = float_cols(oracle.schema)
    source_schema = pq.read_schema(src_path)
    units = zpq_units(src_path, c["units"], c["renames"])

    raw = None
    try:
        raw, names, objs = ctx.zpq_rows(sql, oracle, units)
        got = ctx.typed(objs, names, oracle, units)
        if "py-rows" in c["refuse"]:
            notes.append("py-rows: answered where a refusal was expected")
        compare_rows(got, want_rows, names, c["order"], c["approx"], f32)
        results["py-rows"] = "PASS"
    except zpq.ZpqError as e:
        results["py-rows"] = "REFUSED" if "py-rows" in c["refuse"] else "FAIL"
        notes.append(f"py-rows: {message(e)}")
    except Mismatch as e:
        results["py-rows"] = "FAIL"
        notes.append(f"py-rows: {e}")
    p = ctx.cli("query", "--query", sql)
    if raw is None:
        results["cli=py"] = "PASS" if p.returncode != 0 else "FAIL"
        if p.returncode == 0:
            notes.append("cli=py: the CLI answered where Python was refused")
    elif p.returncode == 0 and p.stdout.decode() == raw:
        results["cli=py"] = "PASS"
    else:
        results["cli=py"] = "FAIL"
        notes.append(f"cli=py: rc {p.returncode}, stdout differs: {p.stdout[:200]!r} {p.stderr[:200]!r}")

    if c["req"] is not None:
        out = ctx.tmp(f"{s['name']}-{c['id']}-py")
        req = {"inputs": [src_path], "output_url": out, **c["req"]}
        try:
            resp = json.loads(ctx.z.run_raw(json.dumps(req)), parse_float=Decimal)
            if "error" in resp:
                raise zpq.ZpqError(resp)
            if "agg" in resp:  # the JSON the call returns beside the file
                agg = resp["agg"] if isinstance(resp["agg"], list) else [resp["agg"]]
                try:
                    got = json_rows(agg, want_names, dict(flat_fields(oracle.schema)), units)
                    compare_rows(got, want_rows, want_names, False, c["approx"], f32)
                except Mismatch as e:
                    raise Mismatch(f"agg JSON: {e}")
            # A structured request's `columns` keeps the file's column order (`column_order` sets another).
            check_written(ctx, out, oracle, source_schema, want_names, want_rows, c, f32, by_name=True)
            results["py-write"] = "PASS"
        except zpq.ZpqError as e:
            results["py-write"] = "REFUSED" if "py-write" in c["refuse"] else "FAIL"
            notes.append(f"py-write: {message(e)}")
        except Mismatch as e:
            results["py-write"] = "FAIL"
            notes.append(f"py-write: {e}")
    else:
        results["py-write"] = "-"

    out = ctx.tmp(f"{s['name']}-{c['id']}-cli")
    p = ctx.cli("query", "--query", sql, "-o", out)
    if p.returncode != 0:
        results["cli-write"] = "REFUSED" if "cli-write" in c["refuse"] else "FAIL"
        notes.append(f"cli-write: rc {p.returncode}: {p.stderr.decode()[:240]}")
    else:
        try:
            check_written(ctx, out, oracle, source_schema, want_names, want_rows, c, f32)
            results["cli-write"] = "PASS"
        except Mismatch as e:
            results["cli-write"] = "FAIL"
            notes.append(f"cli-write: {e}")


# Type changes a write makes by design (README "CLI output contract"), as (source type, written type).
WIDENED = {(pa.float16(), pa.float64())}
# DuckDB hands a UUID to Arrow as text.
SAME_FAMILY = {(("uuid",), ("str",))}


def check_types(out_schema, oracle_schema, source_schema, passthrough, widened=frozenset(), renames=None):
    """A column passed through keeps the source's type exactly; any other column the oracle's type family."""
    def plain(t):
        return t.value_type if isinstance(t, pa.DictionaryType) else t
    src = dict(flat_fields(source_schema))
    orc = dict(flat_fields(oracle_schema))
    for name, t in flat_fields(out_schema):
        if renames and name in renames:
            s_t = src[renames[name]]
            if plain(t) != plain(s_t) and (plain(s_t), plain(t)) not in WIDENED:
                raise Mismatch(f"column {name} (alias of {renames[name]}) written as {t}, source is {s_t}")
        elif passthrough and name in src:
            if plain(t) != plain(src[name]) and (plain(src[name]), plain(t)) not in WIDENED:
                raise Mismatch(f"column {name} written as {t}, source is {src[name]}")
        elif name in widened and pa.types.is_float64(t):
            continue
        elif name in orc and family(t) != family(orc[name]) and (family(t), family(orc[name])) not in SAME_FAMILY:
            raise Mismatch(f"column {name} written as {t}, oracle type {orc[name]}")


def as_oracle(names, rows, out_schema, oracle_schema):
    """PyArrow has no INTERVAL type and reads one as its 12 bytes: decode them as the oracle's interval."""
    out_t, orc_t = dict(flat_fields(out_schema)), dict(flat_fields(oracle_schema))
    idx = [i for i, n in enumerate(names)
           if n in orc_t and pa.types.is_interval(orc_t[n]) and pa.types.is_fixed_size_binary(out_t.get(n, pa.null()))]
    if not idx:
        return rows
    def conv(v):
        if v is None:
            return None
        m, d, ms = struct.unpack("<III", v)
        return (m, d, ms * 10**6)
    return [tuple(conv(v) if i in idx else v for i, v in enumerate(r)) for r in rows]


def check_written(ctx, path, oracle, source_schema, want_names, want_rows, c, f32, by_name=False):
    if not os.path.exists(path):
        raise Mismatch("no file written")
    try:
        t = pq.read_table(path)
        t.validate(full=True)
    except Exception as e:
        raise Mismatch(f"PyArrow cannot read the file: {type(e).__name__}: {e}")
    if by_name and sorted(t.schema.names) == sorted(oracle.schema.names):
        t = t.select(oracle.schema.names)
    check_types(t.schema, oracle.schema, source_schema, c["passthrough"], c["widened"], c["renames"])
    names, rows = table_rows(t)
    if names != want_names:
        raise Mismatch(f"PyArrow columns {names}, expected {want_names}")
    try:
        compare_rows(as_oracle(names, rows, t.schema, oracle.schema), want_rows, names, False, c["approx"], f32)
    except Mismatch as e:
        raise Mismatch(f"PyArrow: {e}")
    try:
        d = ctx.duck.execute(f"SELECT * FROM '{path}'").to_arrow_table()
    except Exception as e:
        raise Mismatch(f"DuckDB cannot read the file: {e}")
    if by_name and sorted(d.schema.names) == sorted(oracle.schema.names):
        d = d.select(oracle.schema.names)
    names, rows = table_rows(d)
    try:
        compare_rows(rows, want_rows, names, False, c["approx"], f32)
    except Mismatch as e:
        raise Mismatch(f"DuckDB: {e}")


def run_reverse(ctx, s, c, copy_path, label, results, notes):
    """zpq reads a file DuckDB or PyArrow wrote: Python and the CLI agree byte for byte, and with DuckDB's answer."""
    sql = c["sql"].format(src=copy_path)
    try:
        oracle = ctx.oracle(sql)
    except Exception as e:
        results[label] = "n/a"
        notes.append(f"{label}: oracle failed: {e}")
        return
    _, want_rows = table_rows(oracle)
    try:
        units = zpq_units(copy_path, c["units"], c["renames"])
        raw, names, objs = ctx.zpq_rows(sql, oracle, units)
        got = ctx.typed(objs, names, oracle, units)
        compare_rows(got, want_rows, names, c["order"], c["approx"], float_cols(oracle.schema))
        p = ctx.cli("query", "--query", sql)
        if p.returncode != 0 or p.stdout.decode() != raw:
            raise Mismatch(f"CLI rc {p.returncode} differs from Python: {p.stderr[:200]!r}")
        results[label] = "PASS"
    except zpq.ZpqError as e:
        results[label] = "REFUSED" if label in c["refuse"] else "FAIL"
        notes.append(f"{label}: {message(e)}")
    except Mismatch as e:
        results[label] = "FAIL"
        notes.append(f"{label}: {e}")


def make_copies(ctx, s):
    """The source rewritten by DuckDB and by PyArrow (its first REWRITE_ROWS rows when large)."""
    lim = f" LIMIT {REWRITE_ROWS}" if s["big"] else ""
    duck = os.path.join(ctx.work, f"{s['name']}.duck.parquet")
    arrow = os.path.join(ctx.work, f"{s['name']}.arrow.parquet")
    ctx.duck.execute(f"COPY (SELECT * FROM '{s['path']}'{lim}) TO '{duck}' (FORMAT parquet)")
    pf = pq.ParquetFile(s["path"])
    if s["big"]:
        batches = pf.iter_batches(batch_size=REWRITE_ROWS)
        first = next(batches)
        pq.write_table(pa.Table.from_batches([first]), arrow)
    else:
        pq.write_table(pf.read(), arrow)
    return duck, arrow


COLS = ["py-rows", "cli=py", "py-write", "cli-write", "duck>zpq", "arrow>zpq"]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--zpq", default=os.path.join(ROOT, "zig-out", "bin", "zpq"))
    ap.add_argument("--lib", default=os.environ.get("ZPQ_LIB", os.path.join(ROOT, "zig-out", "lib", "libzpq.so")))
    ap.add_argument("--max-memory", default="2GB", help="libzpq context ceiling")
    ap.add_argument("--duck-threads", type=int, default=4)
    ap.add_argument("--duck-memory", default="1GB")
    ap.add_argument("--work", help="scratch directory (default: a new temporary one, removed at the end)")
    ap.add_argument("--keep", action="store_true", help="keep the scratch directory")
    ap.add_argument("--report", help="also write the report (markdown) here")
    ap.add_argument("--known", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "known.json"))
    ap.add_argument("--only", help="run only sources whose name contains this")
    args = ap.parse_args()

    work = args.work or tempfile.mkdtemp(prefix="zpq-handoff-")
    os.makedirs(work, exist_ok=True)
    known = json.load(open(args.known)) if os.path.exists(args.known) else {}
    ctx = Ctx(args, work)
    table, details = [], []
    try:
        for s in sources(work):
            if args.only and args.only not in s["name"]:
                continue
            if s["gen"]:
                s["gen"](s["path"])
            try:
                duck_copy, arrow_copy = make_copies(ctx, s)
            except Exception as e:
                duck_copy = arrow_copy = None
                details.append(f"{s['name']}: could not rewrite the source: {e}")
            for c in s["cells"]:
                cid = f"{s['name']}/{c['id']}"
                results, notes = {}, []
                try:
                    run_cell(ctx, s, c, s["path"], results, notes)
                    for label, copy in (("duck>zpq", duck_copy), ("arrow>zpq", arrow_copy)):
                        if copy is None:
                            results[label] = "n/a"
                        else:
                            run_reverse(ctx, s, c, copy, label, results, notes)
                except Exception as e:
                    notes.append("harness error: " + "".join(traceback.format_exception_only(type(e), e)).strip())
                    for k in COLS:
                        results.setdefault(k, "ERROR")
                for k in COLS:
                    v = results.get(k, "-")
                    finding = known.get(f"{cid}:{k}")
                    if finding and v in ("FAIL", "ERROR"):
                        results[k] = f"KNOWN {finding.split()[0]}"
                    elif finding and v == "PASS":
                        results[k] = f"FIXED? {finding.split()[0]}"
                table.append((cid, s["desc"] if c is s["cells"][0] else "", results))
                for n in notes:
                    details.append(f"{cid}: {n}")
                line = " ".join(f"{k}={results.get(k, '-')}" for k in COLS)
                print(f"{cid}: {line}", file=sys.stderr, flush=True)
    finally:
        ctx.z.close()
        if not args.keep and not args.work:
            shutil.rmtree(work, ignore_errors=True)

    lines = ["| cell | source | " + " | ".join(COLS) + " |", "|---|---|" + "---|" * len(COLS)]
    for cid, desc, r in table:
        lines.append(f"| {cid} | {desc} | " + " | ".join(r.get(k, "-") for k in COLS) + " |")
    fails = sum(1 for _, _, r in table for k in COLS if r.get(k) in ("FAIL", "ERROR"))
    count = {v: sum(1 for _, _, r in table for k in COLS if r.get(k, "").split(" ")[0] == v)
             for v in ("PASS", "FAIL", "ERROR", "REFUSED", "KNOWN", "FIXED?", "n/a")}
    lines.append("")
    lines.append("Totals: " + ", ".join(f"{k} {v}" for k, v in count.items() if v))
    lines.append("")
    lines.append("REFUSED: zpq refused where a refusal is the documented behaviour (repeated columns in rows).")
    for finding in sorted(set(known.values())):
        lines.append(f"KNOWN {finding}")
    if details:
        lines.append("")
        lines.append("Notes:")
        lines.extend(f"- {d}" for d in details)
    report = "\n".join(lines) + "\n"
    print(report)
    if args.report:
        with open(args.report, "w") as f:
            f.write(report)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
