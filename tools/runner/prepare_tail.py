#!/usr/bin/env python3
"""Pyarrow fixtures for the tail of tools/differential.py. Paths are positional, in this order:

cut budget plain u64 keys typed levels_dictionary levels_delta levels_delta_length legacy dense_0 dense_1 manycalls
"""

import random
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def main():
    (
        cut,
        budget,
        plain,
        u64f,
        keys,
        typed,
        lev_dict,
        lev_delta,
        lev_dlen,
        legacy,
        dense0,
        dense1,
        many,
    ) = sys.argv[1:]

    rows_err = [(k, q) for k in range(160) for q in range(1, 31)] + [(k, 0) for k in range(160, 2160) for _ in range(23)]
    random.Random(1991).shuffle(rows_err)
    pq.write_table(
        pa.table({"k": [r[0] for r in rows_err], "q": [r[1] for r in rows_err]}),
        cut,
        row_group_size=8192,
        use_dictionary=False,
    )

    keys_b = [k for k in range(160) for _ in range(50)] + [k for k in range(160, 1160) for _ in range(40)]
    vals_b = [f"v-{v:04d}" + "x" * 3994 for v in range(50)] * 160 + ["z"] * 40_000
    order_b = list(range(len(keys_b)))
    random.Random(9113).shuffle(order_b)
    pq.write_table(
        pa.table({"k": [keys_b[i] for i in order_b], "s": [vals_b[i] for i in order_b]}),
        budget,
        row_group_size=1024,
        use_dictionary=False,
    )

    n = 9000
    pq.write_table(
        pa.table(
            {
                "f": pa.array([None if i % 13 == 0 else "ARN"[i % 3] for i in range(n)], pa.string()),
                "s": pa.array(["OF"[(i // 7) % 2] for i in range(n)], pa.string()),
                "u": pa.array([f"user{i % 1500}" for i in range(n)], pa.string()),
                "x": pa.array([float(i % 101) for i in range(n)], pa.float64()),
            }
        ),
        plain,
        use_dictionary=False,
        row_group_size=3000,
    )

    big = [2**64 - 1, 2**63, 2**63 + 5, 7]
    pq.write_table(
        pa.table(
            {
                "k": pa.array([i % 2 for i in range(4000)], pa.int32()),
                "u": pa.array([big[i % 4] for i in range(4000)], pa.uint64()),
            }
        ),
        u64f,
        row_group_size=500,
    )

    pq.write_table(
        pa.table(
            {
                "a": pa.array([None, "r", "xy", "xy", None, "x"] * 50, pa.string()),
                "b": pa.array(["abc", "abc", "abcdef", "abcdefg", "y" * 57, "y" * 57] * 50, pa.string()),
                "h": pa.array([None, 1, 2, None, 3, 4] * 50, pa.int64()),
                "s": pa.array(["abcX\x01\x00\x00\x00Qjkl", "q", "", "zz", "abcX\x01\x00\x00\x00Qjkl", "w"] * 50, pa.string()),
                "g": pa.array([5, 1, 3, 1, 2, 4] * 50, pa.int32()),
            }
        ),
        keys,
    )

    import datetime as dt
    import decimal

    k = range(6)
    pq.write_table(
        pa.table(
            {
                "s": pa.array(["a", "b", "a", "b", "a", None], pa.string()),
                "bin": pa.array([b"x", b"y"] * 3, pa.binary()),
                "d": pa.array([dt.date(2024, 1, 1 + i % 2) for i in k], pa.date32()),
                "ts": pa.array([dt.datetime(2024, 1, 1, 0, 0, i % 2) for i in k], pa.timestamp("ms")),
                "tsz": pa.array([dt.datetime(2024, 1, 1, 0, 0, i % 2) for i in k], pa.timestamp("us", tz="UTC")),
                "b": pa.array([i % 2 == 0 for i in k], pa.bool_()),
                "i8": pa.array([i % 2 - 1 for i in k], pa.int8()),
                "u32": pa.array([4294967295 if i % 2 else 1 for i in k], pa.uint32()),
                "u64": pa.array([2**64 - 1 if i % 2 else 1 for i in k], pa.uint64()),
                "f": pa.array([0.5 if i % 2 else 1.5 for i in k], pa.float32()),
                "tm": pa.array([dt.time(1, i % 2) for i in k], pa.time32("ms")),
                "dec": pa.array([decimal.Decimal("1.25") * (i % 2) for i in k], pa.decimal128(9, 2)),
            }
        ),
        typed,
    )

    n = 3000
    nar = pa.table(
        {
            "h": pa.array(
                np.array([(i % 8) - 4 for i in range(n)], dtype=np.float16) / 2,
                pa.float16(),
                mask=np.array([i % 5 == 0 for i in range(n)]),
            ),
            "s": pa.array([None if i % 7 == 0 else f"v{i % 13}" for i in range(n)]),
            "f": pa.array([None if i % 3 == 0 else bytes([i % 250, 1, 2]) for i in range(n)], pa.binary(3)),
        }
    )
    pq.write_table(nar, lev_dict, use_dictionary=True)
    pq.write_table(nar, lev_delta, use_dictionary=["h", "f"], column_encoding={"s": "DELTA_BYTE_ARRAY"})
    pq.write_table(nar, lev_dlen, use_dictionary=["h", "f"], column_encoding={"s": "DELTA_LENGTH_BYTE_ARRAY"})

    n = 20000
    pq.write_table(
        pa.table(
            {
                "ts": pa.array([i * 1_000_003_000 for i in range(n)], pa.timestamp("ns")),
                "h": pa.array(np.arange(n, dtype=np.float16) / 8),
            }
        ),
        legacy,
        row_group_size=n,
        data_page_size=4096,
        use_deprecated_int96_timestamps=True,
    )

    rng = np.random.default_rng(20261015)
    for path in (dense0, dense1):
        n = 24576
        kk = rng.integers(0, 3000, n)
        kk[8192:10240] = rng.permutation(np.arange(100000, 102048))[:2048]
        kmask = rng.random(n) < 0.03
        v = rng.integers(-1000, 1000, n)
        vmask = rng.random(n) < 0.05
        pq.write_table(
            pa.table(
                {
                    "k": pa.array(kk, pa.int64(), mask=kmask),
                    "k32": pa.array((kk % 2500).astype(np.int32), pa.int32()),
                    "v": pa.array(v, pa.int64(), mask=vmask),
                    "f": pa.array(rng.integers(0, 100, n).astype(np.int32), pa.int32()),
                }
            ),
            path,
            row_group_size=2048,
        )

    rows = np.arange(196608)
    pq.write_table(
        pa.table({"k": pa.array(rows % 1000, pa.int64()), "v": pa.array(rows % 31 - 15, pa.int64())}),
        many,
        row_group_size=8192,
    )


if __name__ == "__main__":
    main()
