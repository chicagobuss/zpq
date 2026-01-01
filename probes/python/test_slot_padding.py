#!/usr/bin/env python3
"""
Probe: Can Parquet readers handle files with padding/gaps between row groups?

Theory: If we write row groups to fixed-size "slots" with zero-padding,
readers should still work because page headers are self-describing.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import struct
import os
import tempfile

def create_normal_parquet(path: str, num_row_groups: int = 3, rows_per_rg: int = 1000):
    """Create a normal parquet file for comparison."""
    tables = []
    for i in range(num_row_groups):
        data = {
            'id': list(range(i * rows_per_rg, (i + 1) * rows_per_rg)),
            'value': [f'row_{j}' for j in range(i * rows_per_rg, (i + 1) * rows_per_rg)]
        }
        tables.append(pa.table(data))

    # Write with multiple row groups
    writer = pq.ParquetWriter(path, tables[0].schema)
    for t in tables:
        writer.write_table(t)
    writer.close()

    return sum(len(t) for t in tables)

def analyze_parquet_structure(path: str):
    """Analyze the byte structure of a parquet file."""
    with open(path, 'rb') as f:
        content = f.read()

    # Check magic bytes
    assert content[:4] == b'PAR1', "Missing header magic"
    assert content[-4:] == b'PAR1', "Missing footer magic"

    # Get footer length
    footer_len = struct.unpack('<I', content[-8:-4])[0]
    print(f"File size: {len(content)} bytes")
    print(f"Footer length: {footer_len} bytes")
    print(f"Footer starts at: {len(content) - 8 - footer_len}")

    # Read metadata
    pf = pq.ParquetFile(path)
    meta = pf.metadata
    print(f"\nRow groups: {meta.num_row_groups}")

    for i in range(meta.num_row_groups):
        rg = meta.row_group(i)
        print(f"\n  Row group {i}:")
        print(f"    Rows: {rg.num_rows}")
        print(f"    Total byte size: {rg.total_byte_size}")
        for j in range(rg.num_columns):
            col = rg.column(j)
            print(f"    Column {j} ({col.path_in_schema}):")
            print(f"      File offset: {col.file_offset}")
            print(f"      Compressed size: {col.total_compressed_size}")
            print(f"      Dictionary page offset: {col.dictionary_page_offset}")

    return meta

def create_padded_parquet(path: str, slot_size: int = 100_000):
    """
    Create a parquet file with fixed-size slots and padding.

    This simulates what parallel writers would produce.
    """
    # First, create individual row group data
    rg_data = []
    for i in range(3):
        # Create a small table for each row group
        data = {
            'id': list(range(i * 100, (i + 1) * 100)),
            'value': [x * 1.5 for x in range(i * 100, (i + 1) * 100)]
        }
        table = pa.table(data)

        # Write to a temp file to get the raw bytes
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            pq.write_table(table, tmp.name)
            tmp_path = tmp.name

        # Read back just the row group data (skip header, footer)
        with open(tmp_path, 'rb') as f:
            content = f.read()

        # Get footer info to find row group boundaries
        footer_len = struct.unpack('<I', content[-8:-4])[0]
        pf = pq.ParquetFile(tmp_path)
        rg_meta = pf.metadata.row_group(0)

        # Extract just the row group bytes (after PAR1, before footer)
        # This is approximate - we're getting the column data
        first_col = rg_meta.column(0)
        rg_start = first_col.file_offset
        rg_end = len(content) - footer_len - 8
        rg_bytes = content[4:rg_end]  # Skip PAR1, take up to footer

        rg_data.append({
            'bytes': rg_bytes,
            'num_rows': rg_meta.num_rows,
            'original_meta': rg_meta,
            'schema': pf.schema_arrow
        })

        os.unlink(tmp_path)

    print(f"\nRow group sizes: {[len(rg['bytes']) for rg in rg_data]}")
    print(f"Slot size: {slot_size}")

    # Now write a file with padded slots
    # This is a simplified version - real implementation would rebuild footer

    # For now, let's just test if pyarrow can read a file with extra zeros
    # appended to each row group

    return rg_data

def test_padding_tolerance():
    """
    Test: Can we append zeros to a parquet file and still read it?
    """
    print("\n" + "="*60)
    print("TEST: Padding tolerance")
    print("="*60)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        path = tmp.name

    # Create normal file
    create_normal_parquet(path, num_row_groups=2, rows_per_rg=100)

    # Read original
    original = pq.read_table(path)
    print(f"\nOriginal file rows: {len(original)}")

    # Now append zeros before the footer
    with open(path, 'rb') as f:
        content = f.read()

    footer_len = struct.unpack('<I', content[-8:-4])[0]
    data_part = content[:-footer_len-8]  # Everything before footer
    footer_part = content[-footer_len-8:]  # Footer + length + magic

    # Insert padding
    padding = b'\x00' * 10000  # 10KB of zeros

    padded_path = path + '.padded'
    with open(padded_path, 'wb') as f:
        f.write(data_part)
        f.write(padding)
        f.write(footer_part)

    print(f"Original size: {len(content)}")
    print(f"Padded size: {os.path.getsize(padded_path)}")

    # Try to read padded file
    try:
        padded = pq.read_table(padded_path)
        print(f"Padded file rows: {len(padded)}")
        print("SUCCESS: Padding before footer is tolerated!")

        # Verify data integrity
        assert original.equals(padded), "Data mismatch!"
        print("Data integrity verified!")
    except Exception as e:
        print(f"FAILED: {e}")

    os.unlink(path)
    os.unlink(padded_path)

def test_sparse_file():
    """
    Test: Can we use sparse files with holes for parallel writes?
    """
    print("\n" + "="*60)
    print("TEST: Sparse file with pwrite simulation")
    print("="*60)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        path = tmp.name

    # Create a small parquet file to get valid data
    data = {'id': list(range(100)), 'value': [x * 2.0 for x in range(100)]}
    table = pa.table(data)
    pq.write_table(table, path)

    # Read the structure
    with open(path, 'rb') as f:
        content = f.read()

    original_size = len(content)

    # Now create a new file with a "hole" in the middle
    # This simulates slot-based allocation where slot 1 might be written before slot 0

    sparse_path = path + '.sparse'

    # Pre-allocate large file (simulating slots)
    total_size = original_size * 3  # 3 slots worth

    with open(sparse_path, 'wb') as f:
        # Write PAR1 header
        f.write(content[:4])

        # Seek to slot 1 position and write data (simulating out-of-order write)
        f.seek(original_size)
        f.write(b'\x00' * 100)  # Some placeholder

        # Seek back to slot 0 and write actual data
        f.seek(4)
        f.write(content[4:-footer_len-8] if 'footer_len' in dir() else content[4:])

        # Go to end and ensure file is right size
        f.seek(total_size - 1)
        f.write(b'\x00')

    print(f"Original size: {original_size}")
    print(f"Sparse file size: {os.path.getsize(sparse_path)}")

    # Check if original still readable
    try:
        result = pq.read_table(path)
        print(f"Original still readable: {len(result)} rows")
    except Exception as e:
        print(f"Original read failed: {e}")

    os.unlink(path)
    os.unlink(sparse_path)

def test_slot_based_write():
    """
    Test: Full slot-based parallel write simulation.

    This is the key test - can we:
    1. Pre-compute offsets for N row groups
    2. Write row groups to those offsets (with padding)
    3. Write a footer that points to those offsets
    4. Have pyarrow successfully read the result
    """
    print("\n" + "="*60)
    print("TEST: Full slot-based write simulation")
    print("="*60)

    SLOT_SIZE = 50_000  # 50KB slots
    NUM_SLOTS = 3
    ROWS_PER_RG = 500

    # Step 1: Create individual row group parquet files
    rg_files = []
    for i in range(NUM_SLOTS):
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            data = {
                'id': pa.array(list(range(i * ROWS_PER_RG, (i + 1) * ROWS_PER_RG)), type=pa.int32()),
                'value': pa.array([x * 1.5 for x in range(ROWS_PER_RG)], type=pa.float64()),
            }
            table = pa.table(data)
            pq.write_table(table, tmp.name, row_group_size=ROWS_PER_RG)
            rg_files.append(tmp.name)

    # Step 2: Analyze each file's structure
    rg_info = []
    for i, path in enumerate(rg_files):
        with open(path, 'rb') as f:
            content = f.read()

        footer_len = struct.unpack('<I', content[-8:-4])[0]
        pf = pq.ParquetFile(path)
        rg_meta = pf.metadata.row_group(0)

        # Get the raw column data (between PAR1 and footer)
        data_bytes = content[4:-footer_len-8]

        rg_info.append({
            'data': data_bytes,
            'data_len': len(data_bytes),
            'num_rows': rg_meta.num_rows,
            'columns': [
                {
                    'file_offset': rg_meta.column(j).file_offset,
                    'compressed_size': rg_meta.column(j).total_compressed_size,
                    'uncompressed_size': rg_meta.column(j).total_uncompressed_size,
                }
                for j in range(rg_meta.num_columns)
            ]
        })

        print(f"RG {i}: {len(data_bytes)} bytes, {rg_meta.num_rows} rows")

    # Step 3: Pre-compute slot offsets
    slot_offsets = [4 + i * SLOT_SIZE for i in range(NUM_SLOTS)]
    footer_offset = 4 + NUM_SLOTS * SLOT_SIZE

    print(f"\nSlot offsets: {slot_offsets}")
    print(f"Footer offset: {footer_offset}")

    # Step 4: Write to slots (simulating parallel writes)
    output_path = tempfile.mktemp(suffix='.parquet')

    # Pre-allocate file
    with open(output_path, 'wb') as f:
        f.write(b'PAR1')  # Header magic
        f.seek(footer_offset + 10000)  # Generous footer space
        f.write(b'\x00')  # Extend file

    # Write row groups to their slots (could be parallel!)
    import os
    fd = os.open(output_path, os.O_WRONLY)
    for i, info in enumerate(rg_info):
        offset = slot_offsets[i]
        os.pwrite(fd, info['data'], offset)
        # Pad with zeros to fill slot
        padding_needed = SLOT_SIZE - info['data_len']
        if padding_needed > 0:
            os.pwrite(fd, b'\x00' * padding_needed, offset + info['data_len'])
        print(f"Wrote RG {i} to offset {offset} ({info['data_len']} bytes + {padding_needed} padding)")
    os.close(fd)

    # Step 5: Unfortunately, we can't easily rebuild the footer in Python
    # without reimplementing Thrift serialization.
    # But we CAN test if pyarrow can read a file with gaps!

    print("\nNote: Full footer reconstruction requires Thrift serialization.")
    print("Testing padding tolerance with a simpler approach...")

    # Cleanup
    for path in rg_files:
        os.unlink(path)
    if os.path.exists(output_path):
        os.unlink(output_path)

def test_etag_computation():
    """
    Test: Can we predict S3 multipart ETags?
    """
    print("\n" + "="*60)
    print("TEST: S3 ETag computation")
    print("="*60)

    import hashlib

    def compute_part_md5(data: bytes) -> bytes:
        """Compute MD5 of a single part."""
        return hashlib.md5(data).digest()

    def compute_multipart_etag(parts: list[bytes]) -> str:
        """Compute the final multipart ETag."""
        # Concatenate all part MD5s
        combined = b''.join(compute_part_md5(p) for p in parts)
        # MD5 of the combined hashes
        final_hash = hashlib.md5(combined).hexdigest()
        # Append part count
        return f'"{final_hash}-{len(parts)}"'

    # Test with known data
    part1 = b'Hello, this is part 1' * 1000
    part2 = b'And this is part 2!' * 1000
    part3 = b'Finally, part 3 here' * 1000

    parts = [part1, part2, part3]

    predicted_etag = compute_multipart_etag(parts)
    print(f"Predicted ETag: {predicted_etag}")

    # Verify individual MD5s
    for i, part in enumerate(parts):
        md5 = hashlib.md5(part).hexdigest()
        print(f"  Part {i+1} MD5: {md5}")

    print("\nThis ETag can be computed BEFORE uploading to S3!")
    print("This means we can build CompleteMultipartUpload request in advance.")

def test_pwrite_parallel():
    """
    Test: Does pwrite actually allow parallel writes to different offsets?
    """
    print("\n" + "="*60)
    print("TEST: Parallel pwrite")
    print("="*60)

    import concurrent.futures
    import time

    output_path = tempfile.mktemp(suffix='.bin')

    NUM_SLOTS = 10
    SLOT_SIZE = 1_000_000  # 1MB per slot

    # Pre-allocate file
    with open(output_path, 'wb') as f:
        f.seek(NUM_SLOTS * SLOT_SIZE - 1)
        f.write(b'\x00')

    def write_slot(slot_num: int) -> tuple[int, float]:
        """Write random data to a slot."""
        fd = os.open(output_path, os.O_WRONLY)
        try:
            offset = slot_num * SLOT_SIZE
            data = bytes([slot_num] * SLOT_SIZE)  # Fill with slot number
            start = time.perf_counter()
            os.pwrite(fd, data, offset)
            elapsed = time.perf_counter() - start
            return slot_num, elapsed
        finally:
            os.close(fd)

    # Sequential writes
    start = time.perf_counter()
    for i in range(NUM_SLOTS):
        write_slot(i)
    sequential_time = time.perf_counter() - start

    # Parallel writes
    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_SLOTS) as executor:
        futures = [executor.submit(write_slot, i) for i in range(NUM_SLOTS)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    parallel_time = time.perf_counter() - start

    print(f"Sequential: {sequential_time*1000:.2f}ms")
    print(f"Parallel:   {parallel_time*1000:.2f}ms")
    print(f"Speedup:    {sequential_time/parallel_time:.2f}x")

    # Verify file contents
    with open(output_path, 'rb') as f:
        for i in range(NUM_SLOTS):
            f.seek(i * SLOT_SIZE)
            first_byte = f.read(1)[0]
            assert first_byte == i, f"Slot {i} has wrong content!"
    print("File integrity verified!")

    os.unlink(output_path)

if __name__ == '__main__':
    print("Parquet Parallel Write Probes")
    print("="*60)

    test_padding_tolerance()
    test_pwrite_parallel()
    test_etag_computation()
    test_slot_based_write()
