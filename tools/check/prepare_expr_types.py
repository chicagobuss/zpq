"""The expression predicates review's fixture (zpq-060/REVIEW-joins.md, EP-1..EP-5): one row whose column pairs meet in
a common type other than either side's (UINT64 magnitudes, FLOAT against BIGINT, DECIMAL against DOUBLE, DECIMALs
whose common type overflows, INTEGER arithmetic at its edge), plain-encoded as the review wrote it; and 1000 rows of
DECIMALs at scales 2, 9, 18 and 37 beside their correctly rounded DOUBLEs. Arguments: the two output paths.
"""
import decimal
import pathlib
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    out, decs = (pathlib.Path(p) for p in sys.argv[1:3])
    pq.write_table(pa.table({
        "u": pa.array([2**63], type=pa.uint64()),
        "v": pa.array([1], type=pa.uint64()),
        "f": pa.array([16777216.0], type=pa.float32()),
        "i": pa.array([16777217], type=pa.int64()),
        "dz": pa.array([decimal.Decimal("16777217")], type=pa.decimal128(12, 0)),
        "p": pa.array([decimal.Decimal("0.1")], type=pa.decimal128(38, 37)),
        "x": pa.array([0.1], type=pa.float64()),
        "a": pa.array([2147483647], type=pa.int32()),
        "b": pa.array([2], type=pa.int32()),
        "dp": pa.array([decimal.Decimal(0)], type=pa.decimal128(38, 18)),
        "dq": pa.array([decimal.Decimal("100000000000000000000")], type=pa.decimal128(38, 3)),
        "c2": pa.array([decimal.Decimal("1.25")], type=pa.decimal128(12, 2)),
        "c1": pa.array([decimal.Decimal("1.2")], type=pa.decimal128(9, 1)),
    }), out, use_dictionary=False)
    rng = random.Random(20261107)
    cols = {}
    for scale, precision in ((2, 18), (9, 18), (18, 38), (37, 38)):
        values = [decimal.Decimal(rng.randrange(-10**(precision - 1), 10**(precision - 1))).scaleb(-scale) for _ in range(1000)]
        cols[f"p{scale}"] = pa.array(values, type=pa.decimal128(precision, scale))
        cols[f"x{scale}"] = pa.array([float(v) for v in values], type=pa.float64())
    pq.write_table(pa.table(cols), decs, row_group_size=256)


if __name__ == "__main__":
    main()
