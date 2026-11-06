"""The joins review's fixtures (zpq-060/REVIEW-joins.md): a fact and dimension that share a column name, key tables
with duplicates and without, a one-row dimension holding a 2 MiB string, and key pairs whose physical integers agree
while their SQL types do not (timestamp units, unsigned against signed, DECIMAL scales). Arguments: one output path
per fixture, in the order `main` writes them.
"""
import decimal
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    out = [pathlib.Path(p) for p in sys.argv[1:]]
    fact, dim, dup, long_string, ts_f, ts_d, u_f, u_d, dec_f, dec_d, uniq = out
    pq.write_table(pa.table({
        "id": range(48),
        "fk": pa.array([None if i % 11 == 0 else i % 6 for i in range(48)], type=pa.int64()),
        "v": range(100, 148),
        "amt": [float(i % 9) / 4 for i in range(48)],
        "q": [i % 3 for i in range(48)],
        "sfk": [i % 6 * 1000000007 for i in range(48)],
    }), fact, row_group_size=8, compression="snappy")
    pq.write_table(pa.table({
        "pk": pa.array([0, 1, 2, 3], type=pa.int64()),
        "v": [1000, 1001, 1002, 1003], "grp": [0, 1, 0, 1], "name": ["a", "b", "c", "d"],
    }), dim, row_group_size=2, compression="snappy")
    pq.write_table(pa.table({"xpk": [0, 0, 1, 2], "xg": [1, 1, 1, 1]}), dup, row_group_size=2, compression="snappy")
    pq.write_table(pa.table({"upk": [0, 1, 2, 4], "ug": [1, 0, 1, 1]}), uniq, row_group_size=2, compression="snappy")
    pq.write_table(pa.table({"pk": pa.array([0], type=pa.int64()), "blob": ["x" * (2 * 1024 * 1024)]}),
                   long_string, compression="snappy")
    # The probe is the larger side of each pair, so it stays the probe.
    pq.write_table(pa.table({"t": pa.array([1000000, 2000000, 1000000, None], type=pa.timestamp("us"))}), ts_f)
    pq.write_table(pa.table({"tk": pa.array([1000, 2000], type=pa.timestamp("ms"))}), ts_d)
    pq.write_table(pa.table({"u": pa.array([2**64 - 1, 2**64 - 1, 0, None], type=pa.uint64())}), u_f)
    pq.write_table(pa.table({"s": pa.array([-1, 0], type=pa.int64())}), u_d)
    pq.write_table(pa.table({"d": pa.array([decimal.Decimal("1.00"), decimal.Decimal("1.50"), decimal.Decimal("0.10"), None],
                                           type=pa.decimal128(9, 2))}), dec_f)
    pq.write_table(pa.table({"dk": pa.array([decimal.Decimal("1.0"), decimal.Decimal("1.5")], type=pa.decimal128(9, 1))}),
                   dec_d)


if __name__ == "__main__":
    main()
