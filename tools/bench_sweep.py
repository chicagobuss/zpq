#!/usr/bin/env python3
import subprocess
import json
import re
import sys
import os
import argparse

COLUMNS = [
    "int8", "int16", "int32_sorted", "int32_random", "int64_sorted", "int64_random",
    "uint8", "uint16", "uint32", "uint64", "float32", "float64", "float64_sorted",
    "bool", "bool_sparse", "string_random", "string_dict_low", "string_dict_high",
    "string_sorted", "binary", "timestamp", "timestamp_sorted", "date"
]

def run_bench(parquet_path, output_path, filter_str=None, select_str=None):
    env = os.environ.copy()
    cmd = ["./zig-out/bin/zpq", parquet_path, output_path, "--benchmark"]
    if filter_str:
        cmd += ["--filter", filter_str]
    if select_str:
        cmd += ["--select", select_str]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60, env=env)
        output = result.stderr if "Mrows/sec" in result.stderr else result.stdout
        
        match = re.search(r"\(([\d\.]+) Mrows/sec\)", output)
        if match:
            return float(match.group(1))
        return 0.0
    except subprocess.CalledProcessError as e:
        print(f"Error running bench for filter={filter_str}, select={select_str}: {e.stderr.strip()}", file=sys.stderr)
        return -1.0
    except subprocess.TimeoutExpired:
        print(f"Timeout running bench for filter={filter_str}, select={select_str}", file=sys.stderr)
        return -2.0

def main():
    parser = argparse.ArgumentParser(description="ZPQ Benchmark Sweep")
    parser.add_argument("what", nargs="?", default="native", help="Benchmarking backend (native)")
    parser.add_argument("input", nargs="?", default="local", help="Input source (local or s3)")
    parser.add_argument("output", nargs="?", default="null", help="Output destination (local or null)")
    args = parser.parse_args()

    if not os.environ.get("AWS_REGION"):
        os.environ["AWS_REGION"] = "us-west-2"

    if not os.path.exists("./zig-out/bin/zpq"):
        print("Error: zpq binary not found at ./zig-out/bin/zpq. Run 'just build' first.", file=sys.stderr)
        sys.exit(1)
        
    if args.input == "s3":
        bucket = os.environ.get("AWS_S3_BUCKET", "skyway-staging-perf-test")
        parquet_path = f"s3://{bucket}/zpq_test_data/benchmark/benchmark_100mb.parquet"
    else:
        parquet_path = "/tmp/zpq_r2_bucket/benchmark/benchmark_100mb.parquet"
        if not os.path.exists(parquet_path):
             parquet_path = "data/benchmark/benchmark_100mb.parquet"

    output_path = "/dev/null"
    if args.output == "local":
        output_path = "/tmp/bench_zpq_sweep.parquet"
    elif args.output != "null":
        output_path = args.output

    if args.input != "s3" and not os.path.exists(parquet_path):
        print(f"Error: Benchmark file not found at {parquet_path}.", file=sys.stderr)
        sys.exit(1)

    print(f"Starting Benchmark Sweep: {args.input} ({parquet_path}) -> {args.output} ({output_path})...")
    print(f"{'Column':<20} | {'Filtered':<12} | {'Selected':<12}")
    print(f"{' ':<20} | {'(Mrows/s)':<12} | {'(Mrows/s)':<12}")
    print("-" * 55)
    
    for col in COLUMNS:
        # 1. Selective read (just this col)
        scan_rate = run_bench(parquet_path, args.output, select_str=col)
        
        # 2. Filter rate (specific to col type)
        filter_val = "100" 
        if "bool" in col:
            filter_val = "true"
        elif "string" in col or "binary" in col:
            if col == "string_dict_low": filter_val = "category_0001"
            elif col == "string_dict_high": filter_val = "unique_00001"
            elif col == "string_sorted": filter_val = "sort_00000001"
            else: filter_val = "val_0"
        
        filter_rate = run_bench(parquet_path, args.output, filter_str=f"{col}={filter_val}", select_str=col)
        
        # Format the numbers
        f_filter = f"{filter_rate:.2f}" if filter_rate >= 0 else ("ERR" if filter_rate == -1 else "TO")
        f_select = f"{scan_rate:.2f}" if scan_rate >= 0 else ("ERR" if scan_rate == -1 else "TO")
        
        print(f"{col:<20} | {f_filter:>12} | {f_select:>12}")

if __name__ == "__main__":
    main()
