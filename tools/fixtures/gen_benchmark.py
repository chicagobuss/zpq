import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os
import sys

def gen_benchmark(output_path, num_rows=250000): # ~10MB approx
    print(f"Generating {output_path} with {num_rows} rows...")
    
    # Ensure dir exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # int32 variants
    int32_sorted = np.arange(num_rows, dtype=np.int32)
    int32_random = np.random.randint(0, 100000, size=num_rows, dtype=np.int32)
    int8 = np.random.randint(0, 127, size=num_rows, dtype=np.int32)
    int16 = np.random.randint(0, 32000, size=num_rows, dtype=np.int32)
    uint8 = np.random.randint(0, 255, size=num_rows, dtype=np.int32)
    uint16 = np.random.randint(0, 65000, size=num_rows, dtype=np.int32)
    uint32 = np.random.randint(0, 100000, size=num_rows, dtype=np.int32)
    
    # int64 variants
    int64_sorted = np.arange(num_rows, dtype=np.int64)
    int64_random = np.random.randint(0, 1000000, size=num_rows, dtype=np.int64)
    uint64 = np.random.randint(0, 1000000, size=num_rows, dtype=np.int64)
    timestamp = np.arange(num_rows, dtype=np.int64)
    timestamp_sorted = np.arange(num_rows, dtype=np.int64)
    
    # float variants
    float32 = np.random.rand(num_rows).astype(np.float32)
    float64 = np.random.rand(num_rows).astype(np.float64)
    float64_sorted = np.arange(num_rows, dtype=np.float64)
    
    # bool
    bool_col = np.random.choice([True, False], size=num_rows)
    bool_sparse = np.random.choice([True, False], size=num_rows, p=[0.01, 0.99])
    
    # string/binary
    # string_dict_low: 100 unique values
    cardinality = 100
    indices_low = np.random.randint(0, cardinality, size=num_rows)
    dict_low = [f"category_{i:04d}" for i in range(cardinality)]
    string_dict_low = pa.DictionaryArray.from_arrays(indices_low, dict_low)
    
    # string_dict_high: 10000 unique values
    cardinality_high = 10000
    indices_high = np.random.randint(0, cardinality_high, size=num_rows)
    dict_high = [f"unique_{i:05d}" for i in range(cardinality_high)]
    string_dict_high = pa.DictionaryArray.from_arrays(indices_high, dict_high)
    
    string_random = np.array([f"val_{i}" for i in np.random.randint(0, 1000, size=num_rows)])
    string_sorted = np.array([f"sort_{i:08d}" for i in range(num_rows)])
    binary = np.array([b"bin" for _ in range(num_rows)])
    
    date_col = np.random.randint(0, 10000, size=num_rows, dtype=np.int32)
    
    table = pa.Table.from_pydict({
        "int8": int8,
        "int16": int16,
        "int32_sorted": int32_sorted,
        "int32_random": int32_random,
        "int64_sorted": int64_sorted,
        "int64_random": int64_random,
        "uint8": uint8,
        "uint16": uint16,
        "uint32": uint32,
        "uint64": uint64,
        "float32": float32,
        "float64": float64,
        "float64_sorted": float64_sorted,
        "bool": bool_col,
        "bool_sparse": bool_sparse,
        "string_random": string_random,
        "string_dict_low": string_dict_low,
        "string_dict_high": string_dict_high,
        "string_sorted": string_sorted,
        "binary": binary,
        "timestamp": timestamp,
        "timestamp_sorted": timestamp_sorted,
        "date": date_col
    })
    
    pq.write_table(table, output_path, version='2.6', compression='SNAPPY')
    print(f"Done. {num_rows} rows written.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: gen_benchmark.py <output_path> [rows]")
        sys.exit(1)
    
    out = sys.argv[1]
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 250000
    gen_benchmark(out, rows)
