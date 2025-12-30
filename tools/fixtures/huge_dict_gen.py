import pyarrow as pa
import pyarrow.parquet as pq
import random
import string
import os

def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_huge_dict_parquet(filename, num_rows=100000):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Column with HUGE dictionary (every row unique)
    huge_dict = [f"unique_{i}_{random_string(10)}" for i in range(num_rows)]
    
    data = {
        "huge_dict": huge_dict
    }
    
    schema = pa.schema([
        ("huge_dict", pa.string())
    ])
    
    table = pa.Table.from_pydict(data, schema=schema)
    
    pq.write_table(
        table, 
        filename, 
        row_group_size=num_rows, # Force it all into one row group to maximize dict size
        compression='snappy'
    )
    print(f"Generated {filename}")

if __name__ == "__main__":
    generate_huge_dict_parquet("data/huge_dict.parquet")

