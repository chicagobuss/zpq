#!/usr/bin/env python3
"""Arrow/NDJSON parity and strict PyArrow type checks. Output must be an explicitly assigned scratch directory."""
import argparse
import base64
from decimal import Decimal
import json
import math
import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from zpq import Zpq


def canonical(value, ty):
    if value is None:
        return None
    if pa.types.is_floating(ty) and isinstance(value, (int, float)):
        value = float(value)
        if pa.types.is_float32(ty):
            value = struct.unpack("f", struct.pack("f", value))[0]
    if pa.types.is_binary(ty) or pa.types.is_fixed_size_binary(ty):
        if isinstance(value, dict):
            return base64.b64decode(value["base64"])
        return value.encode() if isinstance(value, str) else value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
    return value


def physical(table):
    arrays = []
    for col, field in zip(table.columns, table.schema):
        ty = field.type
        if pa.types.is_date32(ty) or pa.types.is_time32(ty):
            col = pc.cast(col, pa.int32())
        elif pa.types.is_timestamp(ty) or pa.types.is_time64(ty):
            col = pc.cast(col, pa.int64())
        arrays.append(col)
    return pa.Table.from_arrays(arrays, names=table.column_names)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--clickbench", required=True, type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    n = 150000
    fields = {
        "id": pa.array(range(n), type=pa.int64()),
        "i8": pa.array([-128, 0, None, 127] * (n // 4), type=pa.int8()),
        "u8": pa.array([0, 255, None, 127] * (n // 4), type=pa.uint8()),
        "i16": pa.array([-32768, 32767, None, 0] * (n // 4), type=pa.int16()),
        "u16": pa.array([65535, 0, None, 1] * (n // 4), type=pa.uint16()),
        "i32": pa.array([-2147483648, 2147483647, None, 0] * (n // 4), type=pa.int32()),
        "u32": pa.array([4294967295, 0, None, 1] * (n // 4), type=pa.uint32()),
        "u64": pa.array([2**64 - 1, 2**63, None, 0] * (n // 4), type=pa.uint64()),
        "f32": pa.array([1.25, -0.5, None, 100.125] * (n // 4), type=pa.float32()),
        "f64": pa.array([1.25, -1e100, None, 1e-20] * (n // 4), type=pa.float64()),
        "flag": pa.array([True, False, None, True] * (n // 4)),
        "text": pa.array(["héllo", "", None, "line\n🦆"] * (n // 4), type=pa.string()),
        "blob": pa.array([b"\xff\x00", b"", None, b"valid text"] * (n // 4), type=pa.binary()),
        "dec": pa.array([Decimal("-9999999.99"), Decimal("1.20"), None, Decimal("0.00")] * (n // 4), type=pa.decimal128(9, 2)),
        "dec38": pa.array([Decimal("1234567890123456789012345678.1234567890"), None, Decimal("0"), Decimal("-1")] * (n // 4), type=pa.decimal128(38, 10)),
        "day": pa.array([-1, 0, None, 20000] * (n // 4), type=pa.date32()),
    }
    for unit in ("ms", "us", "ns"):
        for zone in (None, "UTC"):
            fields[f"ts_{unit}_{zone or 'local'}"] = pa.array([-12345, 0, None, 123456789] * (n // 4), type=pa.timestamp(unit, zone))
    fixture = args.out / "all_types.parquet"
    expected = pa.table(fields)
    pq.write_table(expected, fixture, row_group_size=32768, compression="zstd", version="2.6")
    checks = 0
    with Zpq(max_memory="4GB") as z:
        def check(sql, want=None):
            nonlocal checks
            with z.arrow({"sql": sql, "max_memory": "1GB"}) as reader:
                got = reader.read_all()
            got.validate(full=True)
            if want is not None:
                assert got.schema.equals(want.schema, check_metadata=False), (sql, got.schema, want.schema)
                assert got.equals(want), sql
            rows = z.query(sql, max_memory="1GB")
            actual = physical(got).to_pylist()
            assert len(rows) == len(actual), (sql, len(rows), len(actual))
            for i, (left, right) in enumerate(zip(actual, rows)):
                for field in got.schema:
                    a, b = canonical(left[field.name], field.type), canonical(right[field.name], field.type)
                    assert a == b, (sql, i, field.name, a, b)
            checks += 1
            print(f"PASS {checks}: {got.num_rows} rows: {sql}", flush=True)

        check(f"SELECT * FROM '{fixture}'", expected)
        # Consumers may retain batches, close early, and use a batch after stream release.
        with z.arrow({"inputs": [str(fixture)], "columns": ["id"], "max_memory": "1GB"}) as reader:
            first = reader.read_next_batch()
            second = reader.read_next_batch()
        assert first.column(0)[0].as_py() == 0 and second.column(0)[0].as_py() == 32768
        first.validate(full=True)
        checks += 1
        print(f"PASS {checks}: retained batches after early release", flush=True)
        for name in ("nullable_mix", "decimal_exact", "typed_root", "unsigned_order", "scalar_strings", "lambda_int8"):
            path = Path("ci/fixtures/parquet") / f"{name}.parquet"
            check(f"SELECT * FROM '{path}'", pq.read_table(path))
        check(f"SELECT id, dec, day FROM '{fixture}' WHERE id >= 32760 ORDER BY id DESC LIMIT 17 OFFSET 3")
        check(f"SELECT day, count(*) AS n, sum(dec) AS total, min(dec) AS lo, max(dec) AS hi FROM '{fixture}' GROUP BY day")
        check(f"SELECT count(*) AS n, sum(id) AS total FROM '{fixture}'")
        check(f"SELECT id FROM '{fixture}' LIMIT 0", expected.select(["id"]).slice(0, 0))
        for name, message in (("list_int64", "Nested"), ("decimal_precision40", "UnsupportedDecimalPrecision")):
            try:
                with z.arrow({"inputs": [f"ci/fixtures/parquet/{name}.parquet"]}) as reader:
                    reader.read_all()
            except OSError as error:
                assert message in str(error), str(error)
            else:
                raise AssertionError(f"{name} must be refused clearly")
            checks += 1
            print(f"PASS {checks}: clear refusal: {name}", flush=True)
        for name in ("hits_0.parquet", "hits_1.parquet", "hits_2.parquet", "hits_3.parquet"):
            path = args.clickbench / name
            check(f"SELECT RegionID, UserID FROM '{path}'", pq.read_table(path, columns=["RegionID", "UserID"]))
        path = args.clickbench / "hits_0.parquet"
        check(f"SELECT AdvEngineID, count(*) AS n FROM '{path}' WHERE AdvEngineID <> 0 GROUP BY AdvEngineID")
        check(f"SELECT WatchID, URL, ResolutionWidth FROM '{path}' WHERE UserID = 1508127196834704092")
    # Exported batches survive context destruction as well.
    assert first.column(0)[100].as_py() == 100
    print(f"Arrow checks: {checks} passed, 0 failed")


if __name__ == "__main__":
    main()
