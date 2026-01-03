"""
Parquet Footer Building Utilities

This module handles building valid Parquet footers for the morsel architecture.
The footer contains FileMetaData with:
- Schema
- Row group metadata with byte offsets
- Column chunk metadata
- Key-value metadata

Parquet file format:
┌─────────────────────────────────────┐
│ Row Group 1 Data                    │
├─────────────────────────────────────┤
│ Row Group 2 Data                    │
├─────────────────────────────────────┤
│ ...                                 │
├─────────────────────────────────────┤
│ Row Group N Data                    │
├─────────────────────────────────────┤
│ FileMetaData (Thrift)               │
├─────────────────────────────────────┤
│ FileMetaData Length (4 bytes, LE)   │
├─────────────────────────────────────┤
│ Magic "PAR1" (4 bytes)              │
└─────────────────────────────────────┘

The key challenge for morsel architecture:
- Row group byte offsets aren't known until all preceding parts are uploaded
- We accumulate metadata during upload, then calculate offsets at the end
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from enum import IntEnum
import struct


class ParquetType(IntEnum):
    """Parquet physical types."""
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    INT96 = 3
    FLOAT = 4
    DOUBLE = 5
    BYTE_ARRAY = 6
    FIXED_LEN_BYTE_ARRAY = 7


class ConvertedType(IntEnum):
    """Parquet converted (logical) types."""
    NONE = -1
    UTF8 = 0
    MAP = 1
    MAP_KEY_VALUE = 2
    LIST = 3
    ENUM = 4
    DECIMAL = 5
    DATE = 6
    TIME_MILLIS = 7
    TIME_MICROS = 8
    TIMESTAMP_MILLIS = 9
    TIMESTAMP_MICROS = 10
    UINT_8 = 11
    UINT_16 = 12
    UINT_32 = 13
    UINT_64 = 14
    INT_8 = 15
    INT_16 = 16
    INT_32 = 17
    INT_64 = 18
    JSON = 19
    BSON = 20
    INTERVAL = 21


class Encoding(IntEnum):
    """Parquet encodings."""
    PLAIN = 0
    PLAIN_DICTIONARY = 2
    RLE = 3
    BIT_PACKED = 4
    DELTA_BINARY_PACKED = 5
    DELTA_LENGTH_BYTE_ARRAY = 6
    DELTA_BYTE_ARRAY = 7
    RLE_DICTIONARY = 8
    BYTE_STREAM_SPLIT = 9


class CompressionCodec(IntEnum):
    """Parquet compression codecs."""
    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZO = 3
    BROTLI = 4
    LZ4 = 5
    ZSTD = 6
    LZ4_RAW = 7


@dataclass
class SchemaElement:
    """Parquet schema element."""
    name: str
    type: Optional[ParquetType] = None
    converted_type: Optional[ConvertedType] = None
    type_length: Optional[int] = None  # For FIXED_LEN_BYTE_ARRAY
    repetition_type: int = 1  # 0=REQUIRED, 1=OPTIONAL, 2=REPEATED
    num_children: int = 0

    # For nested types
    field_id: Optional[int] = None


@dataclass
class Statistics:
    """Column statistics."""
    min_value: Optional[bytes] = None
    max_value: Optional[bytes] = None
    null_count: Optional[int] = None
    distinct_count: Optional[int] = None


@dataclass
class ColumnChunkMetaData:
    """Metadata for a column chunk within a row group."""
    # Required fields
    type: ParquetType
    encodings: List[Encoding]
    path_in_schema: List[str]  # e.g., ["root", "field_name"]
    codec: CompressionCodec
    num_values: int
    total_uncompressed_size: int
    total_compressed_size: int

    # Offsets (calculated after all parts uploaded)
    data_page_offset: int = 0
    dictionary_page_offset: Optional[int] = None
    index_page_offset: Optional[int] = None

    # Statistics
    statistics: Optional[Statistics] = None

    # For offset calculation
    relative_offset: int = 0  # Offset within the row group


@dataclass
class RowGroupMetaData:
    """Metadata for a row group."""
    columns: List[ColumnChunkMetaData]
    total_byte_size: int
    num_rows: int

    # Calculated after all parts uploaded
    file_offset: int = 0

    # For sorting index (optional)
    sorting_columns: Optional[List[Dict]] = None


@dataclass
class FileMetaData:
    """Parquet file metadata (footer)."""
    version: int = 2
    schema: List[SchemaElement] = field(default_factory=list)
    num_rows: int = 0
    row_groups: List[RowGroupMetaData] = field(default_factory=list)
    key_value_metadata: Optional[List[Dict[str, str]]] = None
    created_by: str = "zpq morsel-poc"


class SimplifiedThriftWriter:
    """
    Simplified Thrift compact protocol writer.

    This is a minimal implementation sufficient for Parquet footers.
    Real implementation would use the full Thrift library.

    Compact protocol field header:
    - If delta from previous field ID <= 15: 1 byte (delta << 4 | type)
    - Else: 1 byte (0 | type) + varint field ID
    """

    def __init__(self):
        self.buffer = bytearray()
        self._last_field_id = 0

    def write_struct_begin(self):
        """Begin a struct (no bytes written)."""
        self._last_field_id = 0

    def write_struct_end(self):
        """End a struct."""
        self.buffer.append(0)  # STOP field

    def write_field_begin(self, field_id: int, type_id: int):
        """Write field header."""
        delta = field_id - self._last_field_id
        if 0 < delta <= 15:
            self.buffer.append((delta << 4) | type_id)
        else:
            self.buffer.append(type_id)
            self._write_varint(field_id)
        self._last_field_id = field_id

    def write_i32(self, value: int):
        """Write 32-bit integer (zigzag + varint)."""
        zigzag = (value << 1) ^ (value >> 31)
        self._write_varint(zigzag)

    def write_i64(self, value: int):
        """Write 64-bit integer (zigzag + varint)."""
        zigzag = (value << 1) ^ (value >> 63)
        self._write_varint(zigzag)

    def write_string(self, value: str):
        """Write string (length + bytes)."""
        encoded = value.encode('utf-8')
        self._write_varint(len(encoded))
        self.buffer.extend(encoded)

    def write_binary(self, value: bytes):
        """Write binary (length + bytes)."""
        self._write_varint(len(value))
        self.buffer.extend(value)

    def write_bool(self, value: bool):
        """Write boolean."""
        self.buffer.append(1 if value else 0)

    def write_list_begin(self, elem_type: int, size: int):
        """Write list header."""
        if size <= 14:
            self.buffer.append((size << 4) | elem_type)
        else:
            self.buffer.append(0xF0 | elem_type)
            self._write_varint(size)

    def _write_varint(self, value: int):
        """Write unsigned varint."""
        while value > 0x7F:
            self.buffer.append((value & 0x7F) | 0x80)
            value >>= 7
        self.buffer.append(value & 0x7F)

    def get_bytes(self) -> bytes:
        return bytes(self.buffer)


# Thrift compact protocol type IDs
THRIFT_STOP = 0
THRIFT_BOOL_TRUE = 1
THRIFT_BOOL_FALSE = 2
THRIFT_I8 = 3
THRIFT_I16 = 4
THRIFT_I32 = 5
THRIFT_I64 = 6
THRIFT_DOUBLE = 7
THRIFT_BINARY = 8
THRIFT_LIST = 9
THRIFT_SET = 10
THRIFT_MAP = 11
THRIFT_STRUCT = 12


def build_parquet_footer(metadata: FileMetaData) -> bytes:
    """
    Build a complete Parquet footer.

    Returns bytes containing:
    - Thrift-encoded FileMetaData
    - 4-byte metadata length (little-endian)
    - "PAR1" magic
    """
    writer = SimplifiedThriftWriter()
    _write_file_metadata(writer, metadata)

    thrift_bytes = writer.get_bytes()

    # Footer format: [metadata][4-byte length][PAR1]
    length_bytes = struct.pack('<I', len(thrift_bytes))
    magic = b'PAR1'

    return thrift_bytes + length_bytes + magic


def _write_file_metadata(writer: SimplifiedThriftWriter, meta: FileMetaData):
    """Write FileMetaData struct."""
    writer.write_struct_begin()

    # Field 1: version (i32)
    writer.write_field_begin(1, THRIFT_I32)
    writer.write_i32(meta.version)

    # Field 2: schema (list<SchemaElement>)
    writer.write_field_begin(2, THRIFT_LIST)
    writer.write_list_begin(THRIFT_STRUCT, len(meta.schema))
    for elem in meta.schema:
        _write_schema_element(writer, elem)

    # Field 3: num_rows (i64)
    writer.write_field_begin(3, THRIFT_I64)
    writer.write_i64(meta.num_rows)

    # Field 4: row_groups (list<RowGroup>)
    writer.write_field_begin(4, THRIFT_LIST)
    writer.write_list_begin(THRIFT_STRUCT, len(meta.row_groups))
    for rg in meta.row_groups:
        _write_row_group(writer, rg)

    # Field 5: key_value_metadata (optional)
    if meta.key_value_metadata:
        writer.write_field_begin(5, THRIFT_LIST)
        writer.write_list_begin(THRIFT_STRUCT, len(meta.key_value_metadata))
        for kv in meta.key_value_metadata:
            _write_key_value(writer, kv)

    # Field 6: created_by (optional)
    if meta.created_by:
        writer.write_field_begin(6, THRIFT_BINARY)
        writer.write_string(meta.created_by)

    writer.write_struct_end()


def _write_schema_element(writer: SimplifiedThriftWriter, elem: SchemaElement):
    """Write SchemaElement struct."""
    writer.write_struct_begin()

    # Field 1: type (optional)
    if elem.type is not None:
        writer.write_field_begin(1, THRIFT_I32)
        writer.write_i32(elem.type.value)

    # Field 2: type_length (optional)
    if elem.type_length is not None:
        writer.write_field_begin(2, THRIFT_I32)
        writer.write_i32(elem.type_length)

    # Field 3: repetition_type (optional)
    writer.write_field_begin(3, THRIFT_I32)
    writer.write_i32(elem.repetition_type)

    # Field 4: name (required)
    writer.write_field_begin(4, THRIFT_BINARY)
    writer.write_string(elem.name)

    # Field 5: num_children (optional)
    if elem.num_children > 0:
        writer.write_field_begin(5, THRIFT_I32)
        writer.write_i32(elem.num_children)

    # Field 6: converted_type (optional)
    if elem.converted_type is not None and elem.converted_type != ConvertedType.NONE:
        writer.write_field_begin(6, THRIFT_I32)
        writer.write_i32(elem.converted_type.value)

    writer.write_struct_end()


def _write_row_group(writer: SimplifiedThriftWriter, rg: RowGroupMetaData):
    """Write RowGroup struct."""
    writer.write_struct_begin()

    # Field 1: columns (list<ColumnChunk>)
    writer.write_field_begin(1, THRIFT_LIST)
    writer.write_list_begin(THRIFT_STRUCT, len(rg.columns))
    for col in rg.columns:
        _write_column_chunk(writer, col)

    # Field 2: total_byte_size (i64)
    writer.write_field_begin(2, THRIFT_I64)
    writer.write_i64(rg.total_byte_size)

    # Field 3: num_rows (i64)
    writer.write_field_begin(3, THRIFT_I64)
    writer.write_i64(rg.num_rows)

    # Field 4: sorting_columns (optional)
    # Skipped for simplicity

    # Field 5: file_offset (optional)
    if rg.file_offset > 0:
        writer.write_field_begin(5, THRIFT_I64)
        writer.write_i64(rg.file_offset)

    writer.write_struct_end()


def _write_column_chunk(writer: SimplifiedThriftWriter, col: ColumnChunkMetaData):
    """Write ColumnChunk struct."""
    writer.write_struct_begin()

    # Field 1: file_path (optional) - not used for single file

    # Field 2: file_offset (i64)
    writer.write_field_begin(2, THRIFT_I64)
    writer.write_i64(col.data_page_offset)

    # Field 3: meta_data (ColumnMetaData struct)
    writer.write_field_begin(3, THRIFT_STRUCT)
    _write_column_metadata(writer, col)

    writer.write_struct_end()


def _write_column_metadata(writer: SimplifiedThriftWriter, col: ColumnChunkMetaData):
    """Write ColumnMetaData struct."""
    writer.write_struct_begin()

    # Field 1: type
    writer.write_field_begin(1, THRIFT_I32)
    writer.write_i32(col.type.value)

    # Field 2: encodings
    writer.write_field_begin(2, THRIFT_LIST)
    writer.write_list_begin(THRIFT_I32, len(col.encodings))
    for enc in col.encodings:
        writer.write_i32(enc.value)

    # Field 3: path_in_schema
    writer.write_field_begin(3, THRIFT_LIST)
    writer.write_list_begin(THRIFT_BINARY, len(col.path_in_schema))
    for path in col.path_in_schema:
        writer.write_string(path)

    # Field 4: codec
    writer.write_field_begin(4, THRIFT_I32)
    writer.write_i32(col.codec.value)

    # Field 5: num_values
    writer.write_field_begin(5, THRIFT_I64)
    writer.write_i64(col.num_values)

    # Field 6: total_uncompressed_size
    writer.write_field_begin(6, THRIFT_I64)
    writer.write_i64(col.total_uncompressed_size)

    # Field 7: total_compressed_size
    writer.write_field_begin(7, THRIFT_I64)
    writer.write_i64(col.total_compressed_size)

    # Field 8: key_value_metadata (optional) - skipped

    # Field 9: data_page_offset
    writer.write_field_begin(9, THRIFT_I64)
    writer.write_i64(col.data_page_offset)

    # Field 10: index_page_offset (optional)
    if col.index_page_offset is not None:
        writer.write_field_begin(10, THRIFT_I64)
        writer.write_i64(col.index_page_offset)

    # Field 11: dictionary_page_offset (optional)
    if col.dictionary_page_offset is not None:
        writer.write_field_begin(11, THRIFT_I64)
        writer.write_i64(col.dictionary_page_offset)

    # Field 12: statistics (optional)
    if col.statistics:
        writer.write_field_begin(12, THRIFT_STRUCT)
        _write_statistics(writer, col.statistics)

    writer.write_struct_end()


def _write_statistics(writer: SimplifiedThriftWriter, stats: Statistics):
    """Write Statistics struct."""
    writer.write_struct_begin()

    if stats.max_value is not None:
        writer.write_field_begin(1, THRIFT_BINARY)
        writer.write_binary(stats.max_value)

    if stats.min_value is not None:
        writer.write_field_begin(2, THRIFT_BINARY)
        writer.write_binary(stats.min_value)

    if stats.null_count is not None:
        writer.write_field_begin(3, THRIFT_I64)
        writer.write_i64(stats.null_count)

    if stats.distinct_count is not None:
        writer.write_field_begin(4, THRIFT_I64)
        writer.write_i64(stats.distinct_count)

    writer.write_struct_end()


def _write_key_value(writer: SimplifiedThriftWriter, kv: Dict[str, str]):
    """Write KeyValue struct."""
    writer.write_struct_begin()

    writer.write_field_begin(1, THRIFT_BINARY)
    writer.write_string(kv.get("key", ""))

    if "value" in kv:
        writer.write_field_begin(2, THRIFT_BINARY)
        writer.write_string(kv["value"])

    writer.write_struct_end()


def calculate_offsets(
    row_groups: List[RowGroupMetaData],
    header_size: int = 4,  # "PAR1" magic at start
) -> int:
    """
    Calculate file offsets for all row groups and column chunks.

    Args:
        row_groups: List of row group metadata (will be modified in place)
        header_size: Size of file header (4 bytes for PAR1 magic)

    Returns:
        Total data size (before footer)
    """
    offset = header_size

    for rg in row_groups:
        rg.file_offset = offset

        col_offset = 0
        for col in rg.columns:
            col.data_page_offset = offset + col.relative_offset
            col_offset = col.relative_offset + col.total_compressed_size

        offset += rg.total_byte_size

    return offset


if __name__ == "__main__":
    # Test footer building
    print("Testing Parquet footer building...")

    # Create a simple schema
    schema = [
        SchemaElement(name="root", num_children=3),
        SchemaElement(name="id", type=ParquetType.INT64),
        SchemaElement(name="name", type=ParquetType.BYTE_ARRAY, converted_type=ConvertedType.UTF8),
        SchemaElement(name="value", type=ParquetType.DOUBLE),
    ]

    # Create row groups
    row_groups = [
        RowGroupMetaData(
            columns=[
                ColumnChunkMetaData(
                    type=ParquetType.INT64,
                    encodings=[Encoding.PLAIN],
                    path_in_schema=["id"],
                    codec=CompressionCodec.UNCOMPRESSED,
                    num_values=1000,
                    total_uncompressed_size=8000,
                    total_compressed_size=8000,
                    relative_offset=0,
                ),
                ColumnChunkMetaData(
                    type=ParquetType.BYTE_ARRAY,
                    encodings=[Encoding.PLAIN],
                    path_in_schema=["name"],
                    codec=CompressionCodec.UNCOMPRESSED,
                    num_values=1000,
                    total_uncompressed_size=10000,
                    total_compressed_size=10000,
                    relative_offset=8000,
                ),
                ColumnChunkMetaData(
                    type=ParquetType.DOUBLE,
                    encodings=[Encoding.PLAIN],
                    path_in_schema=["value"],
                    codec=CompressionCodec.UNCOMPRESSED,
                    num_values=1000,
                    total_uncompressed_size=8000,
                    total_compressed_size=8000,
                    relative_offset=18000,
                ),
            ],
            total_byte_size=26000,
            num_rows=1000,
        ),
    ]

    # Calculate offsets
    total_data_size = calculate_offsets(row_groups)
    print(f"Total data size: {total_data_size}")
    print(f"Row group offset: {row_groups[0].file_offset}")
    for col in row_groups[0].columns:
        print(f"  Column {col.path_in_schema[0]} offset: {col.data_page_offset}")

    # Build footer
    metadata = FileMetaData(
        schema=schema,
        num_rows=1000,
        row_groups=row_groups,
        key_value_metadata=[{"key": "zpq.version", "value": "0.1.0"}],
    )

    footer = build_parquet_footer(metadata)
    print(f"\nFooter size: {len(footer)} bytes")
    print(f"Footer magic: {footer[-4:]}")  # Should be b'PAR1'

    # Verify length encoding
    length = struct.unpack('<I', footer[-8:-4])[0]
    print(f"Metadata length: {length}")
    print(f"Thrift bytes: {len(footer) - 8}")
    assert length == len(footer) - 8, "Length mismatch!"

    print("\nFooter building test passed!")
