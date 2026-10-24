#!/usr/bin/env python3
"""Write the two writer-wire fixtures differential.py builds in its temp dir.

argv: flba.parquet floats.parquet. Same seed, widths, and nulls as that script.
"""

import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    flba, floats = sys.argv[1], sys.argv[2]
    rnd = random.Random(11)
    n = 400
    nan = float("nan")
    pq.write_table(pa.table({
        "id": pa.array(range(n), pa.int32()),
        "lo": pa.array([bytes([i % 3]) * 4 for i in range(n)], pa.binary(4)),
        "lo_null": pa.array([None if i % 7 == 0 else bytes([i % 5]) * 4 for i in range(n)], pa.binary(4)),
        "rand": pa.array([rnd.randbytes(16) for _ in range(n)], pa.binary(16)),
        "rand_null": pa.array([None if i % 3 == 0 else rnd.randbytes(16) for i in range(n)], pa.binary(16)),
        "pre": pa.array([bytes(12) + i.to_bytes(4, "big") for i in range(n)], pa.binary(16)),
        "uuid": pa.array([rnd.randbytes(16) for _ in range(n)], pa.binary(16)).cast(pa.uuid()),
    }), flba, use_dictionary=False)
    pq.write_table(pa.table({
        "id": pa.array(range(n), pa.int32()),
        "nan_first": pa.array([nan if i % 4 == 1 else float(i % 50 - 20) for i in range(n)], pa.float64()),
        "nan_first32": pa.array([nan if i % 4 == 1 else float(i % 50 - 20) for i in range(n)], pa.float32()),
        "all_nan": pa.array([nan] * n, pa.float64()),
        "zero_min": pa.array([0.0 if i % 2 else float(i) for i in range(n)], pa.float64()),
        "zero_max": pa.array([-0.0 if i % 2 else -float(i) for i in range(n)], pa.float64()),
        "dict_nan": pa.array([[nan, 0.0, 2.5][i % 3] for i in range(n)], pa.float64()),
        "nulls_nan": pa.array([None if i % 2 else (nan if i % 3 else 1.5) for i in range(n)], pa.float64()),
    }), floats, use_dictionary=False)


if __name__ == "__main__":
    main()
