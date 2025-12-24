import sys
import time
import pyarrow.parquet as pq
import boto3
import os
import io

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} s3://bucket/key [iterations]")
        sys.exit(1)

    s3_path = sys.argv[1]
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    
    # Parse bucket/key
    if not s3_path.startswith("s3://"):
        print("Error: Path must start with s3://")
        sys.exit(1)
    
    parts = s3_path[5:].split("/", 1)
    bucket = parts[0]
    key = parts[1]

    print("Benchmarking PyArrow E2E")
    print(f"Target: {s3_path}")
    print(f"Iterations: {iterations}")

    s3 = boto3.client('s3')

    durations = []

    for i in range(iterations):
        print(f"\nRun {i+1}/{iterations}...")
        start_time = time.time()
        
        # 1. Open File (We use boto3 to get the object as a file-like object 
        # because PyArrow's S3FileSystem is complex to configure identically 
        # to the environment-based boto3).
        # HOWEVER: Downloading the whole file first is "cheating" if we want to test streaming.
        # But for "cold start" + "scan" comparison, let's use the standard PyArrow S3 fs 
        # if possible, or just read the body to memory to test pure throughput + network.
        
        # Let's try the most standard way: S3FileSystem
        # Requires `s3fs` or `pyarrow` built with S3.
        # Fallback: Boto3 get_object + PyArrow.read_table
        
        # Method A: Boto3 -> BytesIO -> PyArrow (Memory Heavy, but simple)
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        f = io.BytesIO(body)
        
        # Only read the first column to match ZPQ's behavior
        parquet_file = pq.ParquetFile(f)
        column_name = parquet_file.schema.names[0]
        table = parquet_file.read(columns=[column_name])
        
        # Count values to ensure work is done
        count = table.num_rows
        
        end_time = time.time()
        duration = (end_time - start_time) * 1000 # ms
        durations.append(duration)
        
        print(f"  Scanned {count} rows in {duration:.2f}ms")
        
    min_dur = min(durations)
    max_dur = max(durations)
    avg_dur = sum(durations) / len(durations)

    print(f"\n--- Results ({iterations} runs) ---")
    print(f"Min: {min_dur:.2f}ms")
    print(f"Max: {max_dur:.2f}ms")
    print(f"Avg: {avg_dur:.2f}ms")

if __name__ == "__main__":
    main()

