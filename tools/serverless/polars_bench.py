import os
import time
import json
import polars as pl
import boto3

def get_s3_storage_options():
    return {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "aws_session_token": os.environ.get("AWS_SESSION_TOKEN"),
        "aws_region": os.environ.get("AWS_REGION", "us-west-2"),
    }

def lambda_handler(event, context):
    input_path = event.get("file")
    output_path = event.get("output")
    scenario = event.get("scenario", "pass-through")
    
    if not input_path or not output_path:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing input or output path"})
        }

    storage_opts = get_s3_storage_options()
    
    print(f"Polars | {input_path} -> {output_path} | Scenario: {scenario}")
    
    start = time.time()
    
    # Lazy scan setup
    lf = pl.scan_parquet(input_path, storage_options=storage_opts)
    
    # Apply Scenario (Matching tools/bench_engines.py)
    if scenario == "pass-through":
        pass
    elif scenario == "filter":
        lf = lf.filter(pl.col("int32_random") > 0)
    elif scenario == "select-1":
        lf = lf.select(["int64_random"])
    elif scenario == "select-3":
        lf = lf.select(["int32_sorted", "string_dict_low", "float64"])
    
    # Execute and write
    lf.sink_parquet(output_path, storage_options=storage_opts)
    
    end = time.time()
    duration_ms = (end - start) * 1000
    
    print(f"Completed in {duration_ms:.2f}ms")
    
    return {
        "statusCode": 200,
        "rows": 0,  # sink_parquet doesn't return row count easily without collect()
        "matched": 0,
        "ms": duration_ms
    }
