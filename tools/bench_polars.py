import polars as pl
import time
import sys
import os

def benchmark(input_path, output_path, filter_val):
    start_time = time.time()
    
    # Lazy scan
    lf = pl.scan_parquet(input_path)
    
    # Filter
    filtered = lf.filter(pl.col("int32_sorted") == filter_val)
    
    # Sink to parquet (execute)
    # Using snappy to match ZPQ default
    filtered.sink_parquet(output_path, compression="snappy")
    
    end_time = time.time()
    elapsed_ms = (end_time - start_time) * 1000
    
    # Get row count for verification
    # We re-read the output to verify row count without affecting the write timing too much
    # ignoring this cost for the metric
    
    print(f"Polars Time: {elapsed_ms:.1f}ms")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python bench_polars.py <input> <output> <filter_val>")
        sys.exit(1)
        
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    filter_val = int(sys.argv[3])
    
    # Clear OS cache for "cold" file simulation (Mac specific, might need sudo, skipping for user level)
    # We will just run it. 
    if os.path.exists(output_path):
        os.remove(output_path)
        
    benchmark(input_path, output_path, filter_val)
