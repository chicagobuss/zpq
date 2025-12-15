import pyarrow as pa
import pyarrow.parquet as pq
import json
import os
import shutil
import random

OUTPUT_DIR = "data"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def save_fixture(name, table, metadata=None):
    base_path = os.path.join(OUTPUT_DIR, name)
    parquet_path = f"{base_path}.parquet"
    json_path = f"{base_path}.json"
    
    # We use version='2.6' to encourage RLE_DICTIONARY
    pq.write_table(table, parquet_path, version='2.6', compression='NONE', write_statistics=True, data_page_size=1024*1024)
    
    # JSON for large files might be too big, so we limit preview or skip data if huge
    data_list = table.to_pylist()
    meta_info = {
        "num_rows": table.num_rows,
        "schema": [f.name for f in table.schema],
        "data_preview": data_list[:100], # Preview only
        "full_data_check": len(data_list) < 1000, # Only dump full data for small files
        "data": data_list if len(data_list) < 1000 else [],
        "custom_metadata": metadata
    }
    
    with open(json_path, 'w') as f:
        json.dump(meta_info, f, indent=2)
    
    print(f"Generated {name}.parquet ({table.num_rows} rows)")

def gen_simple():
    data = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}, {'id': 3, 'name': 'Charlie'}]
    schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
    table = pa.Table.from_pylist(data, schema=schema)
    save_fixture("simple", table)

def gen_required():
    data = [{'id': 1, 'val': 10}, {'id': 2, 'val': 20}, {'id': 3, 'val': 30}]
    schema = pa.schema([pa.field('id', pa.int32(), nullable=False), pa.field('val', pa.int32(), nullable=False)])
    table = pa.Table.from_pylist(data, schema=schema)
    save_fixture("required", table)

def gen_types():
    data = [{
        'bool_col': True, 'int32_col': 42, 'int64_col': 9999999999,
        'float_col': 3.14, 'double_col': 2.71828, 'string_col': "Hello"
    }]
    schema = pa.schema([
        ('bool_col', pa.bool_()), ('int32_col', pa.int32()), ('int64_col', pa.int64()),
        ('float_col', pa.float32()), ('double_col', pa.float64()), ('string_col', pa.string())
    ])
    table = pa.Table.from_pylist(data, schema=schema)
    save_fixture("types", table)

def gen_large_rle():
    # 10,000 rows of value 1. Guaranteed RLE.
    # Required to avoid Def Levels.
    data = [{'val': 1} for _ in range(10000)]
    schema = pa.schema([pa.field('val', pa.int32(), nullable=False)])
    table = pa.Table.from_pylist(data, schema=schema)
    save_fixture("large_rle", table)

def gen_large_bitpacked():
    # 10,000 rows of random small ints. Should trigger BitPacked runs.
    # Using 0..7 (3 bits).
    random.seed(42)
    data = [{'val': random.randint(0, 7)} for _ in range(10000)]
    schema = pa.schema([pa.field('val', pa.int32(), nullable=False)])
    table = pa.Table.from_pylist(data, schema=schema)
    save_fixture("large_bitpacked", table)

def gen_high_width():
    # Values > 255. 16-bit width.
    data = [{'val': i} for i in range(1000)]
    # 0..999. Max 999 needs 10 bits.
    schema = pa.schema([pa.field('val', pa.int32(), nullable=False)])
    table = pa.Table.from_pylist(data, schema=schema)
    save_fixture("high_width", table)

def gen_many_pages():
    # Force multiple pages by having many rows and small page size.
    # But PyArrow control of page size is tricky in python API directly.
    # We generate 100,000 rows.
    data = [{'val': i % 100} for i in range(100000)]
    schema = pa.schema([pa.field('val', pa.int32(), nullable=False)])
    table = pa.Table.from_pylist(data, schema=schema)
    
    # Use write_table with row_group_size? Page size is hard to force small.
    # But 100k ints is 400KB. Default page is 1MB.
    # We might get 1 page.
    # Let's try 1M rows. 4MB. Should be multiple pages.
    save_fixture("many_rows", table)

if __name__ == "__main__":
    gen_simple()
    gen_required()
    gen_types()
    gen_large_rle()
    gen_large_bitpacked()
    gen_high_width()
    gen_many_pages()
