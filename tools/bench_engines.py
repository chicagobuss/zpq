import argparse
import time
import os
import sys

# Optional imports handled inside functions to avoid strict dependencies unless used
def get_s3_storage_options():
    return {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "aws_region": os.environ.get("AWS_REGION", "us-east-1"),
    }

def run_polars(args):
    import polars as pl
    import s3fs
    
    storage_opts = get_s3_storage_options()
    
    print(f"Polars | {args.input} -> {args.output} | Scenario: {args.scenario}")
    
    durations = []
    for i in range(args.runs):
        start = time.time()
        
        # Lazy scan setup
        lf = pl.scan_parquet(args.input, storage_options=storage_opts)
        
        # Apply Scenario
        if args.scenario == "pass-through":
            pass # Select *
        elif args.scenario == "filter":
            # string_dict_low=category_0001 (Standard) or int32_random > 0
            if "string_dict_low" in lf.collect_schema().names():
                lf = lf.filter(pl.col("string_dict_low") == "category_0001")
            else:
                 # Fallback for benchmark files
                lf = lf.filter(pl.col("int32_random") > 0)
        elif args.scenario == "select-1":
            col = "int64_random" if "int64_random" in lf.collect_schema().names() else lf.collect_schema().names()[0]
            lf = lf.select([col])
        elif args.scenario == "select-3":
            # Try specific benchmark columns, else take first 3
            schema_cols = lf.collect_schema().names()
            target_cols = ["int32_sorted", "string_dict_low", "float64"]
            cols = [c for c in target_cols if c in schema_cols]
            if not cols: 
                cols = schema_cols[:3]
            lf = lf.select(cols)
        elif args.scenario == "default":
             # Match old bench.sh default: Filter string_dict_low + Select 3 cols
            lf = lf.filter(pl.col('string_dict_low') == 'category_0001')
            lf = lf.select(['int32_sorted', 'string_dict_low', 'float64'])
        else:
            print(f"Unknown scenario: {args.scenario}")
            return

        # Sink
        lf.sink_parquet(args.output, storage_options=storage_opts)
        
        dur = (time.time() - start) * 1000
        durations.append(dur)
        print(f"  Run {i+1}: {dur:.2f}ms")

    return durations

def run_duckdb(args):
    import duckdb
    
    print(f"DuckDB | {args.input} -> {args.output} | Scenario: {args.scenario}")
    
    durations = []
    for i in range(args.runs):
        start = time.time()
        con = duckdb.connect()
        
        # S3 Setup
        region = os.environ.get("AWS_REGION", "us-east-1")
        con.execute(f"SET s3_region='{region}';")
        con.execute(f"SET s3_access_key_id='{os.environ.get('AWS_ACCESS_KEY_ID')}';")
        con.execute(f"SET s3_secret_access_key='{os.environ.get('AWS_SECRET_ACCESS_KEY')}';")
        con.execute("SET enable_http_metadata_cache=true;")
        con.execute("SET enable_object_cache=true;")
        
        # Build Query
        select_clause = "*"
        where_clause = ""
        
        if args.scenario == "pass-through":
            pass
        elif args.scenario == "filter":
            # Check schema? Harder in DuckDB before query. Assuming benchmark file.
            where_clause = "WHERE int32_random > 0"
            # Fallback for old default compatibility if needed, but 'filter' usually implies benchmark
        elif args.scenario == "select-1":
            select_clause = "int64_random"
        elif args.scenario == "select-3":
            select_clause = "int64_random, int32_sorted, float64"
        elif args.scenario == "default":
             select_clause = "int32_sorted, string_dict_low, float64"
             where_clause = "WHERE string_dict_low = 'category_0001'"

        query = f"COPY (SELECT {select_clause} FROM '{args.input}' {where_clause}) TO '{args.output}' (FORMAT PARQUET)"
        
        try:
            con.execute(query)
        except Exception as e:
            # Fallback for schema mismatch in 'default' scenario vs benchmark file
             print(f"Error executing query: {e}")
             if args.scenario == "filter":
                 print("Retrying with string_dict_low filter...")
                 query = f"COPY (SELECT * FROM '{args.input}' WHERE string_dict_low='category_0001') TO '{args.output}' (FORMAT PARQUET)"
                 con.execute(query)

        dur = (time.time() - start) * 1000
        durations.append(dur)
        print(f"  Run {i+1}: {dur:.2f}ms")
        con.close()
        
    return durations

def run_pyarrow(args):
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    import boto3
    import io

    print(f"PyArrow | {args.input} -> {args.output} | Scenario: {args.scenario}")
    
    s3 = boto3.client('s3')
    
    def get_file_obj(path, mode='rb'):
        if path.startswith("s3://"):
             parts = path[5:].split("/", 1)
             if mode == 'rb':
                 obj = s3.get_object(Bucket=parts[0], Key=parts[1])
                 return io.BytesIO(obj['Body'].read())
             else:
                 return io.BytesIO() # Write buffer
        return path

    def write_file_obj(path, buf):
        if path.startswith("s3://"):
             parts = path[5:].split("/", 1)
             buf.seek(0)
             s3.put_object(Bucket=parts[0], Key=parts[1], Body=buf.getvalue())
    
    durations = []
    for i in range(args.runs):
        start = time.time()
        
        # Logic is tricky for PyArrow streaming. We'll do naive read-table for now as in old bench.
        # This biases against PyArrow for large files (RAM usage), but acceptable for baseline.
        
        # 1. Read
        cols = None
        if args.scenario == "select-1": cols = ["int64_random"]
        if args.scenario == "select-3": cols = ["int64_random", "int32_sorted", "float64"]
        if args.scenario == "default": cols = ["int32_sorted", "string_dict_low", "float64"]
        
        inp = get_file_obj(args.input)
        table = pq.read_table(inp, columns=cols)
        
        # 2. Filter
        if args.scenario == "filter":
             if "int32_random" in table.column_names:
                 table = table.filter(pc.field("int32_random") > 0)
        if args.scenario == "default":
             if "string_dict_low" in table.column_names:
                 table = table.filter(pc.field("string_dict_low") == "category_0001")

        # 3. Write
        out_buf = io.BytesIO() # Always buffer write for S3 comparison parity w/ old script
        pq.write_table(table, out_buf)
        
        if args.output.startswith("s3://"):
             write_file_obj(args.output, out_buf)
        else:
             with open(args.output, 'wb') as f:
                 f.write(out_buf.getvalue())

        dur = (time.time() - start) * 1000
        durations.append(dur)
        print(f"  Run {i+1}: {dur:.2f}ms")

    return durations

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True, choices=["polars", "duckdb", "pyarrow"])
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--scenario", default="default", choices=["default", "pass-through", "filter", "select-1", "select-3"])
    args = parser.parse_args()

    durations = []
    if args.engine == "polars":
        durations = run_polars(args)
    elif args.engine == "duckdb":
        durations = run_duckdb(args)
    elif args.engine == "pyarrow":
        durations = run_pyarrow(args)
        
    if durations:
        avg = sum(durations) / len(durations)
        print(f"Stats: Min={min(durations):.2f}ms Max={max(durations):.2f}ms Avg={avg:.2f}ms")

if __name__ == "__main__":
    main()
