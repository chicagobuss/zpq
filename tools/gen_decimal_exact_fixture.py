#!/usr/bin/env python3
"""Generate the fixture for the exact-DECIMAL tests in src/core/scan.zig.

Three row groups of 100 rows. Decimals are stored at the narrowest physical type (`store_decimal_as_integer`), so
one file covers INT32, INT64 and FIXED_LEN_BYTE_ARRAY backings; dictionary encoding is on, so the coded paths run.

Columns:
  k     utf8, "a"/"b"/"c" by row                      a GROUP BY key
  i     int32, row - 150                              mixed int/decimal operand
  d9    decimal(9,2)  INT32, (row - 150) * 1.01      negative and positive, cents that don't sum exactly in f64
  d18   decimal(18,4) INT64, row * 0.0001 + 0.1      small scale-4 values
  d38   decimal(38,10) FLBA(16), +/-12345678901234567890123456.1234567891 alternating with row: precision 38
  big   decimal(38,0) FLBA(16), 10^38 - 1 in rows 0 and 1 only, else 1: two of them overflow a 38-digit sum
  dn    decimal(15,2) INT64, nullable: row group 1 all null, elsewhere every third row null
  q     decimal(15,2) INT64, row % 4 (a dictionary of four values)

Usage:  .venv/bin/python tools/gen_decimal_exact_fixture.py
"""

import decimal
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

D = decimal.Decimal
ROWS_PER_GROUP = 100
GROUPS = 3
OUT = pathlib.Path(__file__).resolve().parent.parent / "ci" / "fixtures" / "parquet" / "decimal_exact.parquet"


def dn(r: int) -> D | None:
    if r // ROWS_PER_GROUP == 1 or r % 3 == 0:
        return None
    return D(r) * D("2.50") - D("100.00")


def main() -> None:
    decimal.getcontext().prec = 60
    rows = range(ROWS_PER_GROUP * GROUPS)
    big = D(10) ** 38 - 1
    table = pa.table(
        {
            "k": pa.array(["abc"[r % 3] for r in rows], pa.string()),
            "i": pa.array([r - 150 for r in rows], pa.int32()),
            "d9": pa.array([(D(r) - 150) * D("1.01") for r in rows], pa.decimal128(9, 2)),
            "d18": pa.array([D(r) * D("0.0001") + D("0.1") for r in rows], pa.decimal128(18, 4)),
            "d38": pa.array([D("12345678901234567890123456.1234567891") * (1 if r % 2 else -1) + r for r in rows],
                            pa.decimal128(38, 10)),
            "big": pa.array([big if r < 2 else D(1) for r in rows], pa.decimal128(38, 0)),
            "dn": pa.array([dn(r) for r in rows], pa.decimal128(15, 2)),
            "q": pa.array([D(r % 4) for r in rows], pa.decimal128(15, 2)),
        }
    )
    pq.write_table(table, OUT, row_group_size=ROWS_PER_GROUP, compression="snappy", use_dictionary=True,
                   store_decimal_as_integer=True, write_statistics=True)
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
