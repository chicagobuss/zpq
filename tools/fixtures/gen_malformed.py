import pyarrow as pa
import pyarrow.parquet as pq
import os
import struct

OUTPUT_DIR = "data/malformed"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def create_base_file():
    """Creates a valid base parquet file to corrupt."""
    data = [{'id': i, 'val': str(i)} for i in range(100)]
    table = pa.Table.from_pylist(data)
    path = os.path.join(OUTPUT_DIR, "base.parquet")
    pq.write_table(table, path, compression='SNAPPY')
    return path

def create_bad_magic(base_path):
    """Corrupts the magic bytes at the end of the file."""
    with open(base_path, 'rb') as f:
        content = bytearray(f.read())
    
    # Parquet ends with 4 bytes file metadata length + "PAR1"
    # Corrupt "PAR1" to "PAR2"
    content[-1] = 0x32 # '2'
    
    with open(os.path.join(OUTPUT_DIR, "bad_magic.parquet"), 'wb') as f:
        f.write(content)
    print("Generated bad_magic.parquet")

def create_truncated(base_path):
    """Truncates the file in the middle of the footer."""
    with open(base_path, 'rb') as f:
        content = f.read()
    
    # Cut off the last 10 bytes (magic + length + some thrift)
    truncated = content[:-10]
    
    with open(os.path.join(OUTPUT_DIR, "truncated.parquet"), 'wb') as f:
        f.write(truncated)
    print("Generated truncated.parquet")

def create_garbage_footer_len(base_path):
    """Sets the footer length to be larger than the file."""
    with open(base_path, 'rb') as f:
        content = bytearray(f.read())
    
    # Footer length is 4 bytes little endian before the last 4 magic bytes
    # Index: -8 to -4
    # Set to 1GB
    struct.pack_into('<I', content, len(content) - 8, 1024 * 1024 * 1024)
    
    with open(os.path.join(OUTPUT_DIR, "garbage_footer_len.parquet"), 'wb') as f:
        f.write(content)
    print("Generated garbage_footer_len.parquet")

def create_random_garbage():
    """Creates a file with valid magic bytes but garbage content."""
    # Start with PAR1
    content = bytearray(b"PAR1")
    # Add random garbage
    content.extend(os.urandom(1024))
    # End with random garbage + PAR1
    content.extend(b"PAR1")
    
    with open(os.path.join(OUTPUT_DIR, "random_garbage.parquet"), 'wb') as f:
        f.write(content)
    print("Generated random_garbage.parquet")

if __name__ == "__main__":
    base = create_base_file()
    create_bad_magic(base)
    create_truncated(base)
    create_garbage_footer_len(base)
    create_random_garbage()

