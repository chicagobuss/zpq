"""
MorselCoordinator - State machine for parallel S3 multipart uploads.

This coordinator manages the lifecycle of a parallel Parquet write operation
where each row group is encoded independently and uploaded as a separate
S3 multipart part.

State Machine:
    INIT → UPLOADING → DRAINING → FINALIZING → COMPLETING → DONE
                ↓           ↓           ↓            ↓
              ERROR       ERROR       ERROR        ERROR

Key responsibilities:
    1. Create/abort multipart upload
    2. Assign part numbers to morsels
    3. Track in-flight and completed parts
    4. Accumulate metadata for footer
    5. Calculate byte offsets when all parts complete
    6. Build and upload footer
    7. Complete multipart upload
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Callable, Any
from threading import Lock, Condition
import time


class CoordinatorState(Enum):
    INIT = auto()        # Waiting to start multipart upload
    UPLOADING = auto()   # Parts in flight, accepting new morsels
    DRAINING = auto()    # No more morsels, waiting for in-flight parts
    FINALIZING = auto()  # All parts done, building and uploading footer
    COMPLETING = auto()  # Footer uploaded, calling CompleteMultipartUpload
    DONE = auto()        # Success
    ERROR = auto()       # Failed


@dataclass
class ColumnChunkMetadata:
    """Metadata for a single column chunk within a row group."""
    column_index: int
    relative_offset: int  # Offset within the row group bytes
    compressed_size: int
    uncompressed_size: int
    num_values: int
    # For footer assembly
    file_offset: int = 0  # Will be calculated after all parts complete

    def __repr__(self):
        return f"ColumnChunk(col={self.column_index}, size={self.compressed_size})"


@dataclass
class RowGroupMetadata:
    """Metadata for a single row group (morsel)."""
    row_group_index: int
    num_rows: int
    total_byte_size: int
    columns: List[ColumnChunkMetadata] = field(default_factory=list)
    # For footer assembly
    file_offset: int = 0  # Will be calculated after all parts complete

    def __repr__(self):
        return f"RowGroup(idx={self.row_group_index}, rows={self.num_rows}, size={self.total_byte_size})"


@dataclass
class CompletedPart:
    """Record of a successfully uploaded part."""
    part_number: int
    etag: str
    size: int
    metadata: RowGroupMetadata


@dataclass
class InFlightPart:
    """Record of a part currently being uploaded."""
    part_number: int
    encoded_bytes: bytes
    metadata: RowGroupMetadata
    start_time: float = field(default_factory=time.time)


class MorselCoordinator:
    """
    Coordinates parallel S3 multipart uploads for Parquet row groups.

    Thread-safe: Multiple workers can call submit_morsel() concurrently.
    """

    def __init__(
        self,
        s3_client: Any,
        bucket: str,
        key: str,
        max_in_flight: int = 8,
        on_part_complete: Optional[Callable[[int, str], None]] = None,
    ):
        """
        Initialize coordinator.

        Args:
            s3_client: S3 client (boto3 or mock)
            bucket: Target S3 bucket
            key: Target S3 key (object path)
            max_in_flight: Maximum concurrent uploads (backpressure threshold)
            on_part_complete: Optional callback when part completes
        """
        self.s3 = s3_client
        self.bucket = bucket
        self.key = key
        self.max_in_flight = max_in_flight
        self.on_part_complete_callback = on_part_complete

        # State
        self._state = CoordinatorState.INIT
        self._lock = Lock()
        self._backpressure_cv = Condition(self._lock)

        # Multipart upload state
        self.upload_id: Optional[str] = None
        self._next_part_number = 1

        # Part tracking
        self._in_flight: Dict[int, InFlightPart] = {}
        self._completed: List[CompletedPart] = []

        # Schema and row group tracking
        self.schema: Any = None
        self.total_row_groups: int = 0
        self._submitted_count: int = 0

        # Error tracking
        self.error: Optional[Exception] = None

        # Stats
        self.stats = {
            "parts_uploaded": 0,
            "bytes_uploaded": 0,
            "upload_time_ms": 0,
        }

    @property
    def state(self) -> CoordinatorState:
        with self._lock:
            return self._state

    @property
    def in_flight_count(self) -> int:
        with self._lock:
            return len(self._in_flight)

    def start(self, schema: Any, num_row_groups: int) -> None:
        """
        Initialize multipart upload.

        Args:
            schema: Parquet schema for the output file
            num_row_groups: Total number of row groups to expect

        Raises:
            RuntimeError: If not in INIT state
            Exception: If CreateMultipartUpload fails
        """
        with self._lock:
            if self._state != CoordinatorState.INIT:
                raise RuntimeError(f"Cannot start: state is {self._state}")

            self.schema = schema
            self.total_row_groups = num_row_groups

            try:
                response = self.s3.create_multipart_upload(
                    Bucket=self.bucket,
                    Key=self.key
                )
                self.upload_id = response["UploadId"]
                self._state = CoordinatorState.UPLOADING
                print(f"[Coordinator] Started multipart upload: {self.upload_id}")
            except Exception as e:
                self._state = CoordinatorState.ERROR
                self.error = e
                raise

    def submit_morsel(
        self,
        encoded_bytes: bytes,
        metadata: RowGroupMetadata,
        upload_fn: Optional[Callable[[int, bytes], str]] = None,
    ) -> int:
        """
        Submit an encoded row group for upload.

        This method may block if max_in_flight parts are already uploading
        (backpressure).

        Args:
            encoded_bytes: Encoded Parquet row group bytes
            metadata: Row group metadata
            upload_fn: Optional function to perform upload (for async simulation)
                       Signature: (part_number, bytes) -> etag

        Returns:
            Assigned part number

        Raises:
            RuntimeError: If not in UPLOADING state
        """
        with self._lock:
            # Wait for backpressure to clear
            while (
                self._state == CoordinatorState.UPLOADING
                and len(self._in_flight) >= self.max_in_flight
            ):
                print(f"[Coordinator] Backpressure: {len(self._in_flight)} in flight, waiting...")
                self._backpressure_cv.wait()

            if self._state != CoordinatorState.UPLOADING:
                raise RuntimeError(f"Cannot submit: state is {self._state}")

            # Assign part number
            part_number = self._next_part_number
            self._next_part_number += 1
            self._submitted_count += 1

            # Track in-flight
            self._in_flight[part_number] = InFlightPart(
                part_number=part_number,
                encoded_bytes=encoded_bytes,
                metadata=metadata,
            )

            print(f"[Coordinator] Submitted part {part_number} ({len(encoded_bytes)} bytes)")

            # Check if this is the last morsel
            if self._submitted_count >= self.total_row_groups:
                self._state = CoordinatorState.DRAINING
                print(f"[Coordinator] Last morsel submitted, draining...")

        # Perform upload outside lock
        if upload_fn:
            try:
                etag = upload_fn(part_number, encoded_bytes)
                self.on_part_complete(part_number, etag)
            except Exception as e:
                self._handle_error(e)

        return part_number

    def on_part_complete(self, part_number: int, etag: str) -> None:
        """
        Called when an UploadPart succeeds.

        Args:
            part_number: Completed part number
            etag: S3 ETag for the part
        """
        with self._lock:
            if part_number not in self._in_flight:
                print(f"[Coordinator] Warning: part {part_number} not in flight")
                return

            in_flight = self._in_flight.pop(part_number)

            completed = CompletedPart(
                part_number=part_number,
                etag=etag,
                size=len(in_flight.encoded_bytes),
                metadata=in_flight.metadata,
            )
            self._completed.append(completed)

            # Update stats
            self.stats["parts_uploaded"] += 1
            self.stats["bytes_uploaded"] += completed.size
            self.stats["upload_time_ms"] += (time.time() - in_flight.start_time) * 1000

            print(f"[Coordinator] Part {part_number} complete, ETag={etag}")

            # Signal backpressure waiters
            self._backpressure_cv.notify_all()

            # Callback
            if self.on_part_complete_callback:
                self.on_part_complete_callback(part_number, etag)

            # Check if we should finalize
            if self._state == CoordinatorState.DRAINING and len(self._in_flight) == 0:
                self._finalize_locked()

    def _finalize_locked(self) -> None:
        """Build footer and complete upload. Must hold lock."""
        self._state = CoordinatorState.FINALIZING
        print(f"[Coordinator] All parts complete, finalizing...")

        # Sort completed parts by part number to get correct byte order
        self._completed.sort(key=lambda p: p.part_number)

        # Calculate cumulative byte offsets
        offset = 0
        for completed in self._completed:
            completed.metadata.file_offset = offset
            col_offset = 0
            for col in completed.metadata.columns:
                col.file_offset = offset + col.relative_offset
                col_offset += col.compressed_size
            offset += completed.size

        print(f"[Coordinator] Calculated offsets, total size: {offset}")

        # Build footer
        footer_bytes = self._build_footer()

        # Upload footer as final part
        footer_part_number = self._next_part_number
        try:
            response = self.s3.upload_part(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.upload_id,
                PartNumber=footer_part_number,
                Body=footer_bytes,
            )
            footer_etag = response["ETag"]
            print(f"[Coordinator] Footer uploaded as part {footer_part_number}")
        except Exception as e:
            self._state = CoordinatorState.ERROR
            self.error = e
            raise

        self._state = CoordinatorState.COMPLETING

        # Build parts list for completion
        parts = [
            {"PartNumber": p.part_number, "ETag": p.etag}
            for p in self._completed
        ]
        parts.append({"PartNumber": footer_part_number, "ETag": footer_etag})

        # Complete multipart upload
        try:
            self.s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.upload_id,
                MultipartUpload={"Parts": parts},
            )
            self._state = CoordinatorState.DONE
            print(f"[Coordinator] Multipart upload complete!")
        except Exception as e:
            self._state = CoordinatorState.ERROR
            self.error = e
            raise

    def _build_footer(self) -> bytes:
        """
        Build Parquet footer bytes.

        This is a simplified placeholder - the real implementation will use
        parquet_footer.py to build proper Thrift-encoded metadata.
        """
        # Placeholder: return mock footer
        # Real implementation will serialize FileMetaData with:
        # - Schema
        # - Row group metadata with file offsets
        # - Key-value metadata
        row_groups_meta = []
        for completed in self._completed:
            rg = completed.metadata
            row_groups_meta.append({
                "file_offset": rg.file_offset,
                "num_rows": rg.num_rows,
                "total_byte_size": rg.total_byte_size,
                "columns": [
                    {
                        "file_offset": col.file_offset,
                        "compressed_size": col.compressed_size,
                    }
                    for col in rg.columns
                ],
            })

        # For now, just return a marker
        import json
        footer_json = json.dumps({
            "schema": str(self.schema),
            "row_groups": row_groups_meta,
            "num_rows": sum(p.metadata.num_rows for p in self._completed),
        })

        # Real Parquet footer format:
        # [Thrift-encoded FileMetaData][4-byte metadata length][PAR1 magic]
        footer_data = footer_json.encode("utf-8")
        footer_len = len(footer_data).to_bytes(4, "little")
        magic = b"PAR1"

        return footer_data + footer_len + magic

    def _handle_error(self, error: Exception) -> None:
        """Handle upload error."""
        with self._lock:
            if self._state == CoordinatorState.ERROR:
                return  # Already in error state

            self._state = CoordinatorState.ERROR
            self.error = error
            print(f"[Coordinator] Error: {error}")

            # Signal waiters so they don't block forever
            self._backpressure_cv.notify_all()

    def abort(self) -> None:
        """Abort the multipart upload."""
        with self._lock:
            if self.upload_id:
                try:
                    self.s3.abort_multipart_upload(
                        Bucket=self.bucket,
                        Key=self.key,
                        UploadId=self.upload_id,
                    )
                    print(f"[Coordinator] Aborted multipart upload")
                except Exception as e:
                    print(f"[Coordinator] Error aborting: {e}")

            self._state = CoordinatorState.ERROR
            self._backpressure_cv.notify_all()

    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for the coordinator to reach a terminal state.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if completed successfully, False if error or timeout
        """
        import time
        start = time.time()

        while True:
            state = self.state
            if state in (CoordinatorState.DONE, CoordinatorState.ERROR):
                return state == CoordinatorState.DONE

            if timeout and (time.time() - start) > timeout:
                return False

            time.sleep(0.01)  # Small sleep to avoid busy loop

    def get_stats(self) -> dict:
        """Get upload statistics."""
        with self._lock:
            return {
                **self.stats,
                "state": self._state.name,
                "in_flight": len(self._in_flight),
                "completed": len(self._completed),
                "total_row_groups": self.total_row_groups,
            }


# Simple mock S3 client for testing
class MockS3Client:
    """Mock S3 client that simulates multipart upload behavior."""

    def __init__(self, upload_delay_ms: float = 10):
        self.upload_delay_ms = upload_delay_ms
        self._uploads: Dict[str, Dict] = {}
        self._next_upload_id = 1
        self._objects: Dict[str, bytes] = {}

    def create_multipart_upload(self, Bucket: str, Key: str) -> dict:
        upload_id = f"upload-{self._next_upload_id}"
        self._next_upload_id += 1
        self._uploads[upload_id] = {
            "bucket": Bucket,
            "key": Key,
            "parts": {},
        }
        return {"UploadId": upload_id}

    def upload_part(
        self,
        Bucket: str,
        Key: str,
        UploadId: str,
        PartNumber: int,
        Body: bytes,
    ) -> dict:
        if UploadId not in self._uploads:
            raise Exception(f"Unknown upload ID: {UploadId}")

        # Simulate upload delay
        time.sleep(self.upload_delay_ms / 1000)

        etag = f'"{hash(Body) & 0xFFFFFFFF:08x}"'
        self._uploads[UploadId]["parts"][PartNumber] = {
            "body": Body,
            "etag": etag,
        }
        return {"ETag": etag}

    def complete_multipart_upload(
        self,
        Bucket: str,
        Key: str,
        UploadId: str,
        MultipartUpload: dict,
    ) -> dict:
        if UploadId not in self._uploads:
            raise Exception(f"Unknown upload ID: {UploadId}")

        upload = self._uploads[UploadId]
        parts = MultipartUpload["Parts"]

        # Concatenate parts in order
        data = b""
        for part in sorted(parts, key=lambda p: p["PartNumber"]):
            pn = part["PartNumber"]
            if pn not in upload["parts"]:
                raise Exception(f"Part {pn} not found")
            data += upload["parts"][pn]["body"]

        # Store final object
        self._objects[f"{Bucket}/{Key}"] = data

        del self._uploads[UploadId]
        return {"Location": f"s3://{Bucket}/{Key}"}

    def abort_multipart_upload(
        self,
        Bucket: str,
        Key: str,
        UploadId: str,
    ) -> dict:
        if UploadId in self._uploads:
            del self._uploads[UploadId]
        return {}

    def get_object(self, Bucket: str, Key: str) -> bytes:
        """Helper to retrieve uploaded object."""
        return self._objects.get(f"{Bucket}/{Key}")


if __name__ == "__main__":
    # Simple test
    print("Testing MorselCoordinator with MockS3Client...")

    s3 = MockS3Client(upload_delay_ms=5)
    coordinator = MorselCoordinator(
        s3_client=s3,
        bucket="test-bucket",
        key="output/test.parquet",
        max_in_flight=4,
    )

    # Simulate schema and row groups
    schema = {"columns": ["id", "name", "value"]}
    num_row_groups = 5

    coordinator.start(schema, num_row_groups)

    # Submit morsels with synchronous upload
    def sync_upload(part_number: int, data: bytes) -> str:
        response = s3.upload_part(
            Bucket="test-bucket",
            Key="output/test.parquet",
            UploadId=coordinator.upload_id,
            PartNumber=part_number,
            Body=data,
        )
        return response["ETag"]

    for i in range(num_row_groups):
        encoded = f"row_group_{i}_data".encode() * 1000
        metadata = RowGroupMetadata(
            row_group_index=i,
            num_rows=1000,
            total_byte_size=len(encoded),
            columns=[
                ColumnChunkMetadata(
                    column_index=0,
                    relative_offset=0,
                    compressed_size=len(encoded) // 3,
                    uncompressed_size=len(encoded) // 3,
                    num_values=1000,
                ),
            ],
        )
        coordinator.submit_morsel(encoded, metadata, upload_fn=sync_upload)

    # Wait for completion
    success = coordinator.wait_for_completion(timeout=10)

    print(f"\nResult: {'SUCCESS' if success else 'FAILED'}")
    print(f"Stats: {coordinator.get_stats()}")

    # Check the uploaded object
    obj = s3.get_object("test-bucket", "output/test.parquet")
    print(f"Uploaded object size: {len(obj)} bytes")
    print(f"Footer magic: {obj[-4:]}")  # Should be b'PAR1'
