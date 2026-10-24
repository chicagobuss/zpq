#!/usr/bin/env python3
"""Read large string dictionaries with PyArrow and two optional DuckDB installations.

The supplied binary must have the dictionary cap named by --cap-mib. The row-group override is confined to this
interoperability fixture so that a dictionary near that cap is not split by the normal row-group byte target.
"""

import argparse
import hashlib
import itertools
import json
import pathlib
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from gen_stats_order_fixtures import Reader, T_STRUCT, field


def duck_check(connection, source, output):
    # An ordered digest checks every value, its multiplicity, and its position without materializing an EXCEPT table.
    query = ("SELECT count(*), md5(string_agg(md5(s), '' ORDER BY file_row_number)) "
             "FROM read_parquet(?, file_row_number=true)")
    assert connection.execute(query, [str(source)]).fetchone() == connection.execute(query, [str(output)]).fetchone()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--cap-mib", type=int, choices=[1, 4, 16, 64], default=1)
    parser.add_argument("--duckdb-python", help="additional Python interpreter with another DuckDB installation")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="zpq-dictionary-pages-") as temp:
        root = pathlib.Path(temp)
        source, output = root / "source.parquet", root / "output.parquet"
        cardinality, width, rows = args.cap_mib * 256, 3072, 131072
        dictionary = pa.array([hashlib.shake_256(str(i).encode()).hexdigest(width // 2) for i in range(cardinality)])
        indices = pa.array([i % cardinality for i in range(rows)], type=pa.int32())
        table = pa.table({"s": pa.DictionaryArray.from_arrays(indices, dictionary)})
        pq.write_table(table, source, store_schema=False, dictionary_pagesize_limit=args.cap_mib << 20)
        command = [args.binary, "query", str(source), "--select", "s", "--codec", "snappy", "-j", "1",
                   "--row-group-size", str(rows), "--row-group-bytes", "1GB", "-o", str(output)]
        result = subprocess.run(command, capture_output=True, timeout=180)
        assert result.returncode == 0, result.stderr.decode()
        actual = pq.ParquetFile(output)
        assert actual.num_row_groups == 1
        column = actual.metadata.row_group(0).column(0)
        assert column.has_dictionary_page
        with output.open("rb") as stream:
            stream.seek(column.dictionary_page_offset)
            header = Reader(stream.read(1024)).value(T_STRUCT)
        dictionary_bytes = field(header, 2)[2]
        assert dictionary_bytes == cardinality * (width + 4), dictionary_bytes
        expected = pq.ParquetFile(source)
        for left, right in itertools.zip_longest(expected.iter_batches(4096), actual.iter_batches(4096)):
            assert left is not None and right is not None and left.equals(right, check_metadata=False)
        with duckdb.connect() as connection:
            duck_check(connection, source, output)
        versions = [duckdb.__version__]
        if args.duckdb_python:
            code = "import duckdb,sys; " + (
                "q=\"SELECT count(*), md5(string_agg(md5(s), '' ORDER BY file_row_number)) "
                "FROM read_parquet(?, file_row_number=true)\"; "
                "c=duckdb.connect(); "
                "assert c.execute(q,[sys.argv[1]]).fetchone()==c.execute(q,[sys.argv[2]]).fetchone(); "
                "print(duckdb.__version__)"
            )
            checked = subprocess.run([args.duckdb_python, "-c", code, str(source), str(output)],
                                     check=True, capture_output=True, text=True, timeout=180)
            versions.append(checked.stdout.strip())
        print(json.dumps({"dictionary_bytes": dictionary_bytes, "cap_mib": args.cap_mib, "rows": rows,
                          "readers": ["PyArrow " + pa.__version__] + ["DuckDB " + v for v in versions]}))


if __name__ == "__main__":
    main()
