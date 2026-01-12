import polars as pl
import os
import time
import sys

def main():
    bucket = os.environ.get("AWS_S3_BUCKET", "skyway-staging-perf-test")
    input_path = f"s3://{bucket}/zpq_test_data/benchmark/benchmark_100mb.parquet"
    output_path = "/tmp/polars_bench_output.parquet"
    
    print(f"Reading from {input_path}")
    start_time = time.time()
    
    # Read from S3 and write to local parquet
    df = pl.read_parquet(input_path)
    df.write_parquet(output_path)
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f"Polars processed {len(df)} rows in {elapsed:.2f}s")

if __name__ == "__main__":
    main()
