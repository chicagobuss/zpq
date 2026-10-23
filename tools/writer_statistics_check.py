#!/usr/bin/env python3
"""Writer-policy regressions: valid modern footers keep base bytes; affected synthetic footers decode the truth.

Usage: writer_statistics_check.py CANDIDATE BASE [POLARS_PYTHON]
Requires pyarrow and duckdb. Temporary footer changes do not alter encoded pages.
"""

import decimal
import json
import pathlib
import os
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from gen_stats_order_fixtures import T_BINARY, field, rewrite_footer


MODERN = [
    None, b"", b"unknown", b"parquet-mr version malformed",
    b"zpq version 0.5.0", b"parquet-cpp-arrow version 24.0.0",
    b"DuckDB version v2.0.0", b"Polars", b"Spark version 4.0.0",
    b"parquet-mr version 1.15.2 (build spark)", b"parquet-cpp version 1.3.0",
]


def change_writer(writer):
    def edit(meta, _body):
        entry = field(meta, 6)
        if writer is None:
            if entry is not None:
                meta.remove(entry)
        elif entry is not None:
            entry[2] = writer
        else:
            meta.append([6, T_BINARY, writer])
    return edit


def run(binary, sql, j, extra=(), env=None):
    result = subprocess.run([binary, "query", "--query", sql, "-j", str(j), *extra],
                            capture_output=True, timeout=60, env=env)
    assert result.returncode == 0, (binary, sql, extra, result.stderr.decode())
    return result.stdout


def main():
    candidate, base = sys.argv[1:3]
    root = pathlib.Path(__file__).resolve().parent.parent
    checks = 0
    with tempfile.TemporaryDirectory(prefix="zpq-writer-statistics-") as temp:
        path = pathlib.Path(temp) / "valid.parquet"
        n = 2048
        table = pa.table({
            "x": pa.array(range(n), type=pa.int32()),
            "s": pa.array([['a', 'é', None, 'z', ''][i % 5] for i in range(n)], type=pa.string()),
            "d": pa.array([None if i % 7 == 0 else decimal.Decimal(i) / 100 for i in range(n)],
                          type=pa.decimal128(9, 2)),
            "u": pa.array([None if i % 7 == 0 else 3_000_000_000 + i for i in range(n)], type=pa.uint32()),
            "f": pa.array([None if i % 7 == 0 else i * .25 for i in range(n)], type=pa.float64()),
        })
        pq.write_table(table, path, row_group_size=256, write_batch_size=64,
                       data_page_size=128, write_page_index=True, use_dictionary=False)
        original = path.read_bytes()
        queries = [
            "SELECT count(*) AS n, count(s) AS ns, count(d) AS nd, count(u) AS nu, min(d) AS m, sum(f) AS f",
            "SELECT count(*) AS n WHERE s = 'é'",
            "SELECT count(*) AS n WHERE s IS NULL",
            "SELECT count(*) AS n WHERE u > 3000001000",
            "SELECT count(*) AS n WHERE d < 7.25",
            "SELECT s, count(*) AS n, sum(x) AS v GROUP BY s ORDER BY n DESC",
        ]
        con = duckdb.connect()
        con.execute("SET disabled_optimizers='filter_pushdown,statistics_propagation'")
        for writer in MODERN:
            path.write_bytes(original)
            rewrite_footer(path, change_writer(writer))
            assert pq.read_table(path).equals(table)
            for query in queries:
                # Insert FROM ahead of a filter/group, or at the end of the aggregate list.
                sql = query
                for keyword in [" WHERE ", " GROUP BY "]:
                    if keyword in sql:
                        sql = sql.replace(keyword, f" FROM '{path}'" + keyword, 1)
                        break
                else:
                    sql += f" FROM '{path}'"
                for j in [1, 3, 16]:
                    for extra in [(), ("--trust-stats",), ("--scan-all",), ("--max-memory", "256K")]:
                        got = run(candidate, sql, j, extra)
                        assert got == run(base, sql, j, extra), (writer, sql, j, extra)
                        if "WHERE" in sql:
                            want = con.execute(sql).fetchone()[0]
                            assert json.loads(got)["n"] == want, (writer, sql, extra, want, got)
                        checks += 1
        print(f"writer statistics: {checks} modern/unknown/malformed outputs byte-identical to base", flush=True)

        # Exercise actual current writer banners and physical encodings as well as the synthetic footer matrix.
        path.write_bytes(original)
        written = []
        duck = pathlib.Path(temp) / "duckdb.parquet"
        con.execute(f"COPY (SELECT * FROM '{path}') TO '{duck}' (FORMAT PARQUET)")
        written.append(("DuckDB", duck))
        zpq = pathlib.Path(temp) / "zpq.parquet"
        command = [candidate, "query", str(path), "--select", "x, s, d, u, f", "-o", str(zpq)]
        result = subprocess.run(command, capture_output=True, timeout=60)
        assert result.returncode == 0, result.stderr.decode()
        written.append(("zpq", zpq))
        if len(sys.argv) > 3:
            polars = pathlib.Path(temp) / "polars.parquet"
            script = "import polars as pl,sys;pl.read_parquet(sys.argv[1]).write_parquet(sys.argv[2])"
            subprocess.run([sys.argv[3], "-c", script, str(path), str(polars)], check=True, timeout=60)
            written.append(("Polars", polars))
        else:
            print("writer statistics: actual Polars generation skipped (pass POLARS_PYTHON)", flush=True)
        written.append(("Arrow", path))
        for label, source in written:
            assert pq.read_table(source).to_pydict() == table.to_pydict(), label
            sql = f"SELECT count(s) AS n, min(d) AS d, max(u) AS u, sum(f) AS f FROM '{source}'"
            for j in [1, 16]:
                for extra in [(), ("--trust-stats",), ("--scan-all",)]:
                    assert run(candidate, sql, j, extra) == run(base, sql, j, extra), (label, j, extra)
                    checks += 1
            print(f"writer statistics: actual {label}: {pq.ParquetFile(source).metadata.created_by}", flush=True)
        spark = root / "ci/fixtures/parquet/int96_from_spark.parquet"
        if spark.is_file():
            pq.read_table(spark)
            sql = f"SELECT count(a) AS n, min(a) AS a FROM '{spark}'"
            for extra in [(), ("--trust-stats",), ("--scan-all",)]:
                assert run(candidate, sql, 3, extra) == run(base, sql, 3, extra), extra
                checks += 1
            print(f"writer statistics: Spark fixture: {pq.ParquetFile(spark).metadata.created_by}", flush=True)

        poisoned = root / "ci/fixtures/parquet/writer_stats.parquet"
        assert pq.read_table(poisoned).num_rows == 3
        for j in [1, 3, 16]:
            for budget in ["64K", "1M"]:
                for rows in ["0", "64", "256"]:
                    env = dict(os.environ, ZPQ_DECODE_BATCH_ROWS=rows)
                    for predicate in ["s = 'a'", "s IS NULL", "d = 1.00", "d IS NULL", "u = 1", "u IS NULL"]:
                        sql = f"SELECT count(*) AS n FROM '{poisoned}' WHERE {predicate}"
                        want = con.execute(sql).fetchone()[0]
                        # DuckDB itself applies writer policy, but scan-all gives an independent zpq decode comparison.
                        got = run(candidate, sql, j, ("--max-memory", budget), env)
                        full = run(candidate, sql, j, ("--scan-all", "--max-memory", budget), env)
                        assert got == full and json.loads(got)["n"] == want == 1, (sql, budget, rows, got, full)
                        checks += 1
        # Copy, recompression and a fully matching filter must not resurrect old statistics when the output
        # advertises the first input's modern writer or zpq. Validate outputs with strict PyArrow and DuckDB.
        clean = pathlib.Path(temp) / "clean.parquet"
        truth = pq.read_table(poisoned)
        pq.write_table(truth, clean, use_dictionary=False, write_page_index=True)
        for label, sources, extra, copies in [
            ("mixed", [clean, poisoned], [], 2),
            ("filtered", [poisoned], ["--filter", "v > 0"], 1),
            ("recompressed", [clean, poisoned], ["--codec", "zstd"], 2),
            ("filtered-copy", [poisoned], ["--filter", "v > 0", "--columns", "s,u,v"], 1),
        ]:
            output = pathlib.Path(temp) / f"{label}.parquet"
            command = [candidate, "query", *map(str, sources), "-o", str(output), *extra]
            result = subprocess.run(command, capture_output=True, timeout=60)
            assert result.returncode == 0, (command, result.stderr.decode())
            want = pa.concat_tables([truth] * copies)
            if label == "filtered-copy":
                want = want.select(["s", "u", "v"])
            assert pq.read_table(output).equals(want, check_metadata=False), label
            assert pq.read_table(output, filters=[("s", "==", "a")]).num_rows == copies, label
            assert pq.read_table(output, filters=[("u", "==", 1)]).num_rows == copies, label
            assert con.execute(f"SELECT count(*) FROM '{output}'").fetchone()[0] == 3 * copies
            aggs = "count(s) AS s, count(u) AS u"
            if label != "filtered-copy":
                aggs += ", count(d) AS d, min(d) AS m"
            query = f"SELECT {aggs} FROM '{output}'"
            got = run(candidate, query, 3, ("--trust-stats",))
            full = run(candidate, query, 3, ("--scan-all",))
            assert got == full, (label, got, full)
            footer = pq.ParquetFile(output).metadata
            for rg in range(footer.num_row_groups - 1, footer.num_row_groups):
                for ci in range(2 if label == "filtered-copy" else 3):
                    stats = footer.row_group(rg).column(ci).statistics
                    if label != "filtered":
                        assert stats is None, (label, ci)
                    elif stats is not None:
                        expected = ["a", decimal.Decimal("1.00"), 1][ci]
                        assert stats.min == expected and stats.null_count == 1, (label, ci, stats)
            checks += 1
        print(f"writer statistics: {checks} total checks including tight-budget old-writer fallback", flush=True)


if __name__ == "__main__":
    main()
