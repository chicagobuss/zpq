#!/usr/bin/env python3
"""Writer defaults: numeric dictionary boundaries, verified with PyArrow and DuckDB.

Usage: writer_defaults_check.py ZPQ
"""

import pathlib
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from datagen.parquet import Reader, T_STRUCT, field


def write(binary, source, output, names):
    command = [binary, "query", str(source), "--select", ",".join(names), "-o", str(output)]
    result = subprocess.run(command, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr.decode()


def check_readers(source, output):
    expected = pq.read_table(source)
    actual = pq.read_table(output)
    for i in range(expected.num_columns):
        a, b = actual.column(i), expected.column(i)
        assert a.is_null().equals(b.is_null())
        if pa.types.is_floating(a.type):
            assert pc.fill_null(pc.is_nan(a), False).equals(pc.fill_null(pc.is_nan(b), False))
            a, b = (pc.if_else(pc.is_nan(c), None, c) for c in [a, b])
        assert a.equals(b), expected.column_names[i]
    con = duckdb.connect()
    for left, right in [(source, output), (output, source)]:
        sql = f"SELECT count(*) FROM (SELECT * FROM '{left}' EXCEPT ALL SELECT * FROM '{right}')"
        count = con.execute(sql).fetchone()[0]
        assert count == 0, (left, right, count)
    con.close()


def footer_stats(data):
    length = int.from_bytes(data[-8:-4], "little")
    footer = Reader(data[-8 - length:-8]).value(T_STRUCT)
    return [[field(field(c, 3)[2], 12)[2] for c in field(rg, 1)[2][1]] for rg in field(footer, 4)[2][1]]


def check_exactness(binary, root):
    from decimal import Decimal
    n = 512
    columns = {
        "i32": pa.array(range(n), type=pa.int32()),
        "u32": pa.array([3_000_000_000 + i for i in range(n)], type=pa.uint32()),
        "i64": pa.array(range(n), type=pa.int64()),
        "u64": pa.array([2**63 + i for i in range(n)], type=pa.uint64()),
        "f32": pa.array([i / 4 for i in range(n)], type=pa.float32()),
        "f64": pa.array([i / 4 for i in range(n)], type=pa.float64()),
        "b": pa.array([i % 2 == 0 for i in range(n)]),
        "s": pa.array(["short", "text"] * (n // 2)),
        "cut": pa.array(["a" * 200, "z" * 200] * (n // 2)),
        "mixed": pa.array(["a", "z" * 200] * (n // 2)),
        "overflow": pa.array([b"\xff" * 200] * n, type=pa.binary()),
        "fixed": pa.array([b"x" * 200] * n, type=pa.binary(200)),
        "d32": pa.array([Decimal(i) / 100 for i in range(n)], type=pa.decimal128(9, 2)),
        "d64": pa.array([Decimal(i) / 100 for i in range(n)], type=pa.decimal128(18, 2)),
        "d128": pa.array([Decimal(i) / 100 for i in range(n)], type=pa.decimal128(30, 2)),
        "nulls": pa.array([None] * n, type=pa.int32()),
        "nans": pa.array([float("nan")] * n),
    }
    source, output = root / "stats-source.parquet", root / "stats-output.parquet"
    table = pa.table(columns)
    for dictionary in [False, True]:
        pq.write_table(table, source, use_dictionary=dictionary, store_decimal_as_integer=True)
        result = subprocess.run([binary, "query", str(source), "--select", ",".join(columns),
                                 "-o", str(output)], capture_output=True, timeout=60)
        assert result.returncode == 0, result.stderr.decode()
        check_readers(source, output)
        metadata = pq.read_metadata(output)
        # PyArrow 25 has no Python exactness properties. Read and reserialize through its C++ metadata API;
        # inspect that result to verify PyArrow actually retained both flags, including false and absent flags.
        sink = pa.BufferOutputStream()
        metadata.write_metadata_file(sink)
        stats = footer_stats(sink.getvalue().to_pybytes())
        assert stats == footer_stats(output.read_bytes())
        for rg in stats:
            for name, st in zip(columns, rg):
                expected = (False, False) if name == "cut" else (True, False) if name == "mixed" else (
                    (False, None) if name == "overflow" else
                    (None, None) if name in ["nulls", "nans"] else (True, True))
                for bound, flag, want in zip([6, 5], [8, 7], expected):
                    entry = field(st, flag)
                    got = None if entry is None else entry[2]
                    assert got == want, (name, flag, got, want)
                    assert (field(st, bound) is not None) == (want is not None), (name, bound)
    print("writer defaults: exact/cut/missing bounds survive PyArrow metadata read-back PASS")


def main():
    binary = sys.argv[1]
    with tempfile.TemporaryDirectory(prefix="zpq-writer-defaults-") as temp:
        root = pathlib.Path(temp)
        source, output = root / "source.parquet", root / "output.parquet"
        types = [pa.int32(), pa.int64(), pa.float32(), pa.float64()]
        for nullable in [False, True]:
            n = 1250 if nullable else 1000
            columns = {}
            for ti, dtype in enumerate(types):
                for cardinality in [80, 100, 101]:
                    values = [i % cardinality if i < 1000 else None for i in range(n)]
                    columns[f"v{ti}_{cardinality}"] = pa.array(values, type=dtype)
            table = pa.table(columns)
            # Both the decoded and source-dictionary code paths must apply the same present-row cap.
            for dictionary in [False, True]:
                pq.write_table(table, source, use_dictionary=dictionary)
                write(binary, source, output, table.column_names)
                check_readers(source, output)
                metadata = pq.ParquetFile(output).metadata
                for ci, name in enumerate(table.column_names):
                    column = metadata.row_group(0).column(ci)
                    cardinality = int(name.split("_")[1])
                    assert column.has_dictionary_page == (cardinality <= 100), (name, nullable, dictionary)
                    assert column.compression == "SNAPPY"
                    assert column.has_column_index and column.has_offset_index
        check_exactness(binary, root)
        print("writer defaults: numeric 8%/10%/10.1%, dense/nullable, plain/coded: PyArrow + DuckDB PASS")


if __name__ == "__main__":
    main()
