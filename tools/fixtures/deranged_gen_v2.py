import pyarrow as pa
import pyarrow.parquet as pq
import random
import string
import datetime
import os

def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_deranged_parquet(filename, num_rows=10000):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    data = {
        "sparse_int32": [],
        "bloat_string": [],
        "alignment_fixed": [],
        "extreme_timestamp": [],
        "mixed_ints": [],
        "repetitive_str": []
    }
    
    for i in range(num_rows):
        # 1. Sparse INT32
        data["sparse_int32"].append(i if random.random() < 0.01 else None)
        
        # 2. Bloat String
        if random.random() < 0.01:
            data["bloat_string"].append(random_string(70000))
        else:
            data["bloat_string"].append(random_string(10))
            
        # 3. Alignment Fixed (7 bytes)
        data["alignment_fixed"].append(random.choice([b"1234567", b"ABCDEFG", b"!@#$%^&"]))
        
        # 4. Extreme Timestamp (INT96 candidate)
        dt = datetime.datetime(1900, 1, 1) + datetime.timedelta(days=random.randint(0, 50000))
        data["extreme_timestamp"].append(dt)
        
        # 5. Mixed Ints
        data["mixed_ints"].append(random.randint(-2**31, 2**31 - 1))
        
        # 6. Repetitive String
        data["repetitive_str"].append(f"category_{i % 10}")

    # Define schema to force specific types
    schema = pa.schema([
        ("sparse_int32", pa.int32()),
        ("bloat_string", pa.string()),
        ("alignment_fixed", pa.binary(7)), # FIXED_LEN_BYTE_ARRAY
        ("extreme_timestamp", pa.timestamp('ns')), # Will be INT96 if use_deprecated_int96_timestamps=True
        ("mixed_ints", pa.int32()),
        ("repetitive_str", pa.string())
    ])
    
    table = pa.Table.from_pydict(data, schema=schema)
    
    pq.write_table(
        table, 
        filename, 
        use_deprecated_int96_timestamps=True,
        row_group_size=1000,
        compression='snappy'
    )
    print(f"Generated {filename}")

if __name__ == "__main__":
    generate_deranged_parquet("data/deranged.parquet")

