#!/usr/bin/env python3
"""Split benchmark_100mb.parquet into 10 Hive-partitioned slices and upload.

Layout:
  s3://${AWS_S3_BUCKET}/zpq_test_data/partitioned/year=2026/month={01..10}/data.parquet

Each slice is ~52K rows (~10 MB compressed). Skips files that already exist
in S3 so re-runs are cheap.
"""
import os
import sys
import io
import boto3
import polars as pl

SRC = "data/benchmark_100mb.parquet"
BUCKET = os.environ["AWS_S3_BUCKET"]
PREFIX = "zpq_test_data/partitioned/year=2026"
MONTHS = [f"{m:02d}" for m in range(1, 11)]


def main() -> int:
    s3 = boto3.client("s3")

    df = pl.read_parquet(SRC)
    n = df.height
    chunk = n // len(MONTHS)
    print(f"source rows={n}, chunk={chunk}")

    for i, mm in enumerate(MONTHS):
        key = f"{PREFIX}/month={mm}/data.parquet"
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            print(f"  [skip] s3://{BUCKET}/{key}  ({head['ContentLength']} bytes)")
            continue
        except s3.exceptions.ClientError:
            pass

        start = i * chunk
        end = n if i == len(MONTHS) - 1 else (i + 1) * chunk
        slice_df = df.slice(start, end - start)

        buf = io.BytesIO()
        slice_df.write_parquet(buf, compression="snappy")
        body = buf.getvalue()

        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        print(f"  [put]  s3://{BUCKET}/{key}  ({len(body)} bytes, rows={slice_df.height})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
