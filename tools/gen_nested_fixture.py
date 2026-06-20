#!/usr/bin/env python3
"""Generate a parquet fixture deliberately designed to exercise every
weird edge case in our nested-column encode path:

  - Empty lists ([])
  - Null lists (None)
  - Lists with internal nulls ([1, null, 3])
  - Single-element lists ([42])
  - Long lists (50 elements)
  - Mix of all the above across rows
  - Struct-of-primitive with mixed nullability
  - Struct-of-struct (nesting depth in struct)
  - List-of-struct (multiple leaf columns sharing rep structure)
  - Map<string, int32>
  - Flat columns alongside (id: int64) so we can filter on something
    that's not nested

Schema (with row index where each value lives):

    id           : int64                                     (REQUIRED)
    score        : int32 OPTIONAL
    tags         : LIST<string OPTIONAL>      OPTIONAL
    meta         : STRUCT<k: string, v: int32>OPTIONAL OPTIONAL
    events       : LIST<STRUCT<ts: int64, code: int32>>      OPTIONAL
    counts       : MAP<string, int32>                        OPTIONAL

20 rows, each picked to exercise a specific edge case.
"""
import os
import sys
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

OUT_LOCAL = Path("data/nested_edges.parquet")
OUT_S3_KEY = "zpq_test_data/nested_edges.parquet"

ROWS = [
    # row, score, tags, meta, events, counts
    (0,  10,   ["a", "b"],                {"k": "x", "v": 1},   [{"ts": 100, "code": 1}],                    {"foo": 1}),
    (1,  None, [],                        {"k": "y", "v": 2},   [],                                          {"foo": 2, "bar": 3}),
    (2,  20,   None,                      None,                 None,                                        None),
    (3,  30,   ["a"],                     {"k": "z", "v": None},[{"ts": 200, "code": 2},
                                                                {"ts": 201, "code": 3}],                     {"foo": 4}),
    (4,  None, ["a", None, "b"],          {"k": None, "v": 5},  [{"ts": 300, "code": 4}],                    {}),
    (5,  40,   [None],                    {"k": "single", "v": 6}, None,                                     {"a": 1, "b": 2, "c": 3, "d": 4}),
    (6,  50,   ["x"] * 50,                {"k": "long", "v": 7},[{"ts": i, "code": i} for i in range(50)],   None),
    (7,  None, None,                      {"k": "n", "v": None},[],                                          {"k1": None}),
    (8,  60,   [],                        None,                 [{"ts": 0, "code": 0}],                      {"empty_string_key": 0}),
    (9,  70,   ["only"],                  {"k": "y", "v": 8},   None,                                        None),
    (10, None, [None, None, None],        None,                 [{"ts": 1, "code": 1},
                                                                 {"ts": 2, "code": 2}],                      {"a": 0}),
    (11, 80,   ["mixed", None, "values"], {"k": "m", "v": 9},   [],                                          {"x": 1, "y": 2}),
    (12, 90,   None,                      None,                 None,                                        None),
    (13, 100,  ["ε", "λ", "π"],           {"k": "unicode", "v": 10}, [{"ts": 10, "code": -1}],              {"y": -1}),
    (14, None, [],                        {"k": "empty_tags", "v": 11},
                                                                [],                                          {"z": -2}),
    (15, 110,  ["a"] * 1,                 None,                 [{"ts": 999, "code": 7}],                    None),
    (16, 120,  ["b"],                     {"k": "z", "v": None},None,                                        {"deep": 1}),
    (17, None, None,                      None,                 None,                                        None),
    (18, 130,  ["x", "y"],                {"k": "z", "v": 13},  [{"ts": 50, "code": 8},
                                                                 {"ts": 51, "code": 9}],                     {"k": 5}),
    (19, 140,  ["last"],                  {"k": "last", "v": 14}, [{"ts": 60, "code": 10}],                  {"final": 1}),
]


def build_table() -> pa.Table:
    ids = pa.array([r[0] for r in ROWS], type=pa.int64())
    scores = pa.array([r[1] for r in ROWS], type=pa.int32())
    tags = pa.array([r[2] for r in ROWS], type=pa.list_(pa.string()))
    meta = pa.array(
        [r[3] for r in ROWS],
        type=pa.struct([("k", pa.string()), ("v", pa.int32())]),
    )
    events = pa.array(
        [r[4] for r in ROWS],
        type=pa.list_(pa.struct([("ts", pa.int64()), ("code", pa.int32())])),
    )
    counts = pa.array(
        [r[5] for r in ROWS],
        type=pa.map_(pa.string(), pa.int32()),
    )
    return pa.table({
        "id": ids,
        "score": scores,
        "tags": tags,
        "meta": meta,
        "events": events,
        "counts": counts,
    })


def main() -> int:
    OUT_LOCAL.parent.mkdir(parents=True, exist_ok=True)
    table = build_table()
    pq.write_table(table, OUT_LOCAL, compression="snappy")
    print(f"wrote {OUT_LOCAL}  rows={table.num_rows}  columns={table.num_columns}")
    print("schema:")
    print(table.schema)

    bucket = os.environ.get("AWS_S3_BUCKET")
    if not bucket:
        print("AWS_S3_BUCKET unset; skipping upload", file=sys.stderr)
        return 0
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=OUT_S3_KEY,
        Body=OUT_LOCAL.read_bytes(),
    )
    print(f"uploaded s3://{bucket}/{OUT_S3_KEY}  ({OUT_LOCAL.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
