#!/usr/bin/env python3
import subprocess
import json
import re
import sys
import os

COLUMNS = [
    "int8", "int16", "int32_sorted", "int32_random", "int64_sorted", "int64_random",
    "uint8", "uint16", "uint32", "uint64", "float32", "float64", "float64_sorted",
    "bool", "bool_sparse", "string_random", "string_dict_low", "string_dict_high",
    "string_sorted", "binary", "timestamp", "timestamp_sorted", "date"
]

PARQUET_PATH = "/tmp/zpq_r2_bucket/benchmark/benchmark_100mb.parquet"

def run_bench(filter_str=None, select_str=None):
    cmd = ["./zig-out/bin/zpq", PARQUET_PATH, "/dev/null", "--benchmark"]
    if filter_str:
        cmd += ["--filter", filter_str]
    if select_str:
        cmd += ["--select", select_str]
    
    try:
        # We use a timeout to prevent hanging on bad filters
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
        output = result.stderr if "Mrows/sec" in result.stderr else result.stdout
        
        # Parse: Scanned 2500000 rows (23503 matched) in 31.99ms (78.15 Mrows/sec)
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
    if not os.path.exists("./zig-out/bin/zpq"):
        print("Error: zpq binary not found at ./zig-out/bin/zpq. Run 'just build' first.", file=sys.stderr)
        sys.exit(1)
        
    if not os.path.exists(PARQUET_PATH):
        print(f"Error: Benchmark file not found at {PARQUET_PATH}.", file=sys.stderr)
        print("Run 'just build gen-benchmark' (if it exists) or 'python3 tools/fixtures/gen_benchmark.py /tmp/zpq_r2_bucket/benchmark/benchmark_100mb.parquet 2500000'", file=sys.stderr)
        sys.exit(1)

    print(f"Starting Benchmark Sweep against {PARQUET_PATH}...")
    print(f"{'Column':<20} | {'Full Scan':<12} | {'Filtered':<12} | {'Selected':<12}")
    print(f"{' ':<20} | {'(Mrows/s)':<12} | {'(Mrows/s)':<12} | {'(Mrows/s)':<12}")
    print("-" * 65)
    
    # establish baseline full scan
    baseline = run_bench()
    print(f"{'OVERALL (Baseline)':<20} | {baseline:>12.2f} | {'-':>12} | {'-':>12}")

    for col in COLUMNS:
        # 1. Selective read (just this col)
        scan_rate = run_bench(select_str=col)
        
        # 2. Filter rate (specific to col type)
        filter_val = "100" # Arbitrary numeric default
        if "bool" in col:
            filter_val = "true"
        elif "string" in col or "binary" in col:
            if col == "string_dict_low": filter_val = "category_0001"
            elif col == "string_dict_high": filter_val = "unique_00001"
            elif col == "string_sorted": filter_val = "sort_00000001"
            else: filter_val = "val_0"
        
        filter_rate = run_bench(filter_str=f"{col}={filter_val}", select_str=col)
        
        # Format the numbers nicely
        f_scan = f"{baseline:.2f}"
        f_filter = f"{filter_rate:.2f}" if filter_rate >= 0 else ("ERR" if filter_rate == -1 else "TO")
        f_select = f"{scan_rate:.2f}" if scan_rate >= 0 else ("ERR" if scan_rate == -1 else "TO")
        
        print(f"{col:<20} | {f_scan:>12} | {f_filter:>12} | {f_select:>12}")

if __name__ == "__main__":
    main()
