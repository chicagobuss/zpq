#!/usr/bin/env python3
"""Automatic dictionary filters, opt out, explicit precedence, and membership pruning.

Usage: defaults.py ZPQ [LIBZPQ]
"""

import pathlib
import os
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from check import Zpq, physical_values
import sbbf


def verify(source, output, auto, explicit=False):
    expected = pq.read_table(source)
    got = pq.read_table(output)
    assert got.equals(expected, check_metadata=False)
    con = duckdb.connect()
    for left, right in [(source, output), (output, source)]:
        sql = f"SELECT count(*) FROM (SELECT * FROM '{left}' EXCEPT ALL SELECT * FROM '{right}')"
        assert con.execute(sql).fetchone()[0] == 0
    con.close()
    metadata = pq.ParquetFile(output).metadata
    for ri, group in enumerate(sbbf.footer_filters(output)):
        table = pq.ParquetFile(output).read_row_group(ri)
        for ci, (path, physical, offset, length) in enumerate(group):
            column = metadata.row_group(ri).column(ci)
            want = (auto and column.has_dictionary_page) or (explicit and path == ("high",))
            assert (offset is not None) == want, (ri, path, offset, want)
            if offset is None:
                continue
            bits = sbbf.load(output, offset, length)
            for value in set(physical_values(table.column(ci), physical)):
                assert sbbf.check(bits, sbbf.xxh64(sbbf.plain(physical, value))), (path, value)


def main():
    zpq = Zpq(sys.argv[1])
    with tempfile.TemporaryDirectory(prefix="zpq-default-blooms-") as temp:
        root = pathlib.Path(temp)
        n = 4000
        low = [(i % 64) * 2 for i in range(n)]
        table = pa.table({
            "low": pa.array(low, type=pa.int32()),
            "i64": pa.array(low, type=pa.int64()),
            "f32": pa.array(low, type=pa.float32()),
            "f64": pa.array(low, type=pa.float64()),
            "s": pa.array([f"k{v:04d}" for v in low]),
            "fixed": pa.array([f"k{v:04d}".encode() for v in low], type=pa.binary(5)),
            "high": pa.array(range(n), type=pa.int64()),
            "b": pa.array([i % 2 == 0 for i in range(n)]),
        })
        source, output = root / "source.parquet", root / "output.parquet"
        for dictionary in [False, True]:
            pq.write_table(table, source, use_dictionary=dictionary, row_group_size=1000)
            for mode in ["encode", "copy", "recompress", "filtered", "limited"]:
                flags = {"encode": ["--select", ",".join(table.column_names)], "copy": [],
                         "recompress": ["--codec", "zstd"], "filtered": ["--filter", "low >= 0"],
                         "limited": ["--limit", str(n)]}[mode]
                for auto, explicit, options in [
                    (True, False, []), (False, False, ["--no-bloom-filters"]),
                    (False, True, ["--no-bloom-filters", "--bloom-filter", "high:0.001"]),
                ]:
                    if mode == "limited" and explicit:
                        continue  # Existing explicit specifications require unlimited output.
                    zpq.write(str(source), str(output), *flags, *options)
                    verify(source, output, auto, explicit)
                    if auto and (dictionary or mode in ["encode", "limited"]):
                        count, membership = zpq.count(str(output), "low = 1")
                        assert count == 0 and membership["bloom_pruned"] > 0, (mode, membership)
                        assert zpq.count(str(output), "low = 2")[0] == low.count(2)
            # Both serial and parallel aggregate column encoders must honor the opt out.
            for j in [1, 3]:
                for auto, options in [(True, []), (False, ["--no-bloom-filters"])]:
                    sql = f"SELECT high, min(low) AS low FROM '{source}' GROUP BY high"
                    zpq.run("--query", sql, "-j", str(j), "-o", str(output), *options)
                    group = sbbf.footer_filters(output)[0]
                    assert any(c[2] is not None for c in group) == auto, (j, auto, group)
                    assert pq.read_table(output).num_rows == n
                    assert duckdb.sql(f"SELECT sum(low) FROM '{output}'").fetchone()[0] == sum(low)
        carried = root / "carried.parquet"
        zpq.write(str(source), str(carried), "--select", ",".join(table.column_names))
        zpq.write(str(carried), str(output), "--no-bloom-filters")
        verify(carried, output, True)  # Opt out stops creation; byte copies preserve source filters.
        nested = root / "nested.parquet"
        nested_table = pa.table({"xs": pa.array([[i % 7, None, i % 11] for i in range(1000)])})
        pq.write_table(nested_table, nested, use_dictionary=True)
        zpq.write(str(nested), str(output))
        assert pq.read_table(output).equals(nested_table)
        assert duckdb.sql(f"SELECT count(*) FROM '{output}'").fetchone()[0] == 1000
        assert all(c[2] is not None for c in sbbf.footer_filters(output)[0])
        mixed = root / "mixed.parquet"
        mixed_table = pa.table({"s": [f"long value {i:08d}" for i in range(5000)]})
        pq.write_table(mixed_table, mixed, use_dictionary=True, dictionary_pagesize_limit=128, write_batch_size=64)
        assert pq.ParquetFile(mixed).metadata.row_group(0).column(0).has_dictionary_page
        zpq.write(str(mixed), str(output))
        assert sbbf.footer_filters(output)[0][0][2] is None  # PLAIN fallback values are outside the dictionary.
        assert pq.read_table(output).equals(mixed_table)
        assert duckdb.sql(f"SELECT count(*) FROM '{output}' WHERE s = 'long value 00004999'").fetchone()[0] == 1
        if len(sys.argv) > 2:
            os.environ["ZPQ_LIB"] = sys.argv[2]
            sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "python"))
            import zpq as lib
            with lib.Zpq() as context:
                for auto, explicit in [(True, False), (False, False), (False, True)]:
                    request = {"inputs": [str(source)], "select": ",".join(table.column_names),
                               "output_url": str(output), "bloom_filters_enabled": auto}
                    if explicit:
                        request["bloom_filters"] = ["high:0.001"]
                    context.run(request)
                    verify(source, output, auto, explicit)
        print(f"automatic Bloom filters: {zpq.calls} CLI calls, PyArrow + DuckDB + independent SBBF + pruning PASS")


if __name__ == "__main__":
    main()
