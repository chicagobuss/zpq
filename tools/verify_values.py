import duckdb
import os
import sys

SOURCE = "/tmp/zpq_r2_bucket/benchmark/benchmark_100mb.parquet"
BENCH_DIR = "/tmp/zpq_bench_columns"

def verify_column(col_name, filter_expr):
    print(f"Verifying {col_name} with filter: {filter_expr}")
    zpq_output = os.path.join(BENCH_DIR, f"{col_name}.parquet")
    
    if not os.path.exists(zpq_output):
        print(f"  FAILED: Output file {zpq_output} missing")
        return False
    
    conn = duckdb.connect()
    
    # Get ZPQ count
    zpq_count = conn.execute(f"SELECT COUNT(*) FROM '{zpq_output}'").fetchone()[0]
    
    # Get Reference count
    ref_count = conn.execute(f"SELECT COUNT(*) FROM '{SOURCE}' WHERE {filter_expr}").fetchone()[0]
    
    if zpq_count != ref_count:
        print(f"  FAILED: Row count mismatch. ZPQ: {zpq_count}, Reference: {ref_count}")
        return False
    
    if zpq_count > 0:
        # Check first 10 values
        zpq_vals_raw = conn.execute(f"SELECT {col_name} FROM '{zpq_output}' ORDER BY {col_name} LIMIT 10").fetchall()
        ref_vals_raw = conn.execute(f"SELECT {col_name} FROM '{SOURCE}' WHERE {filter_expr} ORDER BY {col_name} LIMIT 10").fetchall()
        
        # Normalize to handle bytes vs strings
        def normalize(vals):
            return [tuple(v.decode('utf-8') if isinstance(v, bytes) else v for v in row) for row in vals]
        
        zpq_vals = normalize(zpq_vals_raw)
        ref_vals = normalize(ref_vals_raw)
        
        if zpq_vals != ref_vals:
            print(f"  FAILED: Value mismatch in first 10 rows")
            print(f"    ZPQ: {zpq_vals}")
            print(f"    Ref: {ref_vals}")
            return False
            
    print(f"  SUCCESS: {col_name} matches reference.")
    return True

# Columns and filters from bench_columns.sh
VERIFY_LIST = [
    ("int8", "int8 = -1"),
    ("bool", "bool = true"),
    ("string_dict_low", "string_dict_low = 'category_0001'"),
    ("int32_sorted", "int32_sorted = 0"),
]

all_ok = True
for col, filt in VERIFY_LIST:
    if not verify_column(col, filt):
        all_ok = False

if not all_ok:
    sys.exit(1)
