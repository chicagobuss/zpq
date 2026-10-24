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


def write(binary, source, output, names):
    command = [binary, "query", str(source), "--select", ",".join(names), "-o", str(output)]
    result = subprocess.run(command, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr.decode()


def check_readers(source, output):
    expected = pq.read_table(source)
    assert pq.read_table(output).equals(expected, check_metadata=False)
    con = duckdb.connect()
    for left, right in [(source, output), (output, source)]:
        count = con.execute(f"SELECT count(*) FROM (SELECT * FROM '{left}' EXCEPT ALL SELECT * FROM '{right}')").fetchone()[0]
        assert count == 0, (left, right, count)
    con.close()


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
                for cardinality in [150, 200, 201]:
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
                    assert column.has_dictionary_page == (cardinality <= 200), (name, nullable, dictionary)
                    assert column.compression == "SNAPPY"
                    assert column.has_column_index and column.has_offset_index
        print("writer defaults: numeric 15%/20%/20.1%, dense/nullable, plain/coded: PyArrow + DuckDB PASS")


if __name__ == "__main__":
    main()
