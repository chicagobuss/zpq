#!/usr/bin/env python3
"""
Unit tests for MorselCoordinator state machine.

Tests cover:
- State transitions (happy path)
- Backpressure behavior
- Error handling
- Concurrent submissions
- Footer assembly with correct offsets
"""

import unittest
from unittest.mock import Mock, MagicMock
import threading
import time

from .coordinator import (
    MorselCoordinator,
    CoordinatorState,
    MockS3Client,
    RowGroupMetadata,
    ColumnChunkMetadata,
)


class TestCoordinatorStateTransitions(unittest.TestCase):
    """Test basic state transitions."""

    def test_initial_state(self):
        """Coordinator starts in INIT state."""
        s3 = MockS3Client()
        coord = MorselCoordinator(s3, "bucket", "key")
        self.assertEqual(coord.state, CoordinatorState.INIT)

    def test_start_transitions_to_uploading(self):
        """start() transitions from INIT to UPLOADING."""
        s3 = MockS3Client()
        coord = MorselCoordinator(s3, "bucket", "key")

        coord.start(schema={}, num_row_groups=5)

        self.assertEqual(coord.state, CoordinatorState.UPLOADING)
        self.assertIsNotNone(coord.upload_id)

    def test_cannot_start_twice(self):
        """start() fails if not in INIT state."""
        s3 = MockS3Client()
        coord = MorselCoordinator(s3, "bucket", "key")
        coord.start(schema={}, num_row_groups=5)

        with self.assertRaises(RuntimeError):
            coord.start(schema={}, num_row_groups=5)

    def test_submit_transitions_to_draining_on_last(self):
        """submit_morsel transitions to DRAINING on last morsel."""
        s3 = MockS3Client(upload_delay_ms=0)
        coord = MorselCoordinator(s3, "bucket", "key")
        coord.start(schema={}, num_row_groups=2)

        def sync_upload(pn, data):
            return s3.upload_part(Bucket="bucket", Key="key",
                                  UploadId=coord.upload_id,
                                  PartNumber=pn, Body=data)["ETag"]

        # First morsel
        coord.submit_morsel(b"data1", _make_rg_meta(0), upload_fn=sync_upload)
        self.assertEqual(coord.state, CoordinatorState.UPLOADING)

        # Second (last) morsel - triggers transition to DRAINING
        coord.submit_morsel(b"data2", _make_rg_meta(1), upload_fn=sync_upload)

        # After sync upload completes, should finalize
        self.assertEqual(coord.state, CoordinatorState.DONE)

    def test_full_lifecycle(self):
        """Test complete lifecycle: INIT → UPLOADING → DRAINING → DONE."""
        s3 = MockS3Client(upload_delay_ms=0)
        coord = MorselCoordinator(s3, "bucket", "key")

        coord.start(schema={}, num_row_groups=3)
        self.assertEqual(coord.state, CoordinatorState.UPLOADING)

        def sync_upload(pn, data):
            return s3.upload_part(Bucket="bucket", Key="key",
                                  UploadId=coord.upload_id,
                                  PartNumber=pn, Body=data)["ETag"]

        for i in range(3):
            coord.submit_morsel(f"data{i}".encode() * 100, _make_rg_meta(i), upload_fn=sync_upload)

        self.assertEqual(coord.state, CoordinatorState.DONE)
        self.assertEqual(coord.stats["parts_uploaded"], 3)


class TestBackpressure(unittest.TestCase):
    """Test backpressure behavior."""

    def test_blocks_when_max_in_flight_reached(self):
        """submit_morsel blocks when max_in_flight parts are uploading."""
        s3 = MockS3Client(upload_delay_ms=100)  # Slow uploads
        coord = MorselCoordinator(s3, "bucket", "key", max_in_flight=2)
        coord.start(schema={}, num_row_groups=5)

        submitted = []
        blocked_event = threading.Event()

        def slow_upload(pn, data):
            time.sleep(0.05)  # 50ms upload
            return f'"etag{pn}"'

        def submit_worker(idx):
            if idx >= 2:  # After first 2, should block
                blocked_event.set()
            coord.submit_morsel(f"data{idx}".encode(), _make_rg_meta(idx), upload_fn=slow_upload)
            submitted.append(idx)

        # Start 3 submissions in threads
        threads = [
            threading.Thread(target=submit_worker, args=(i,))
            for i in range(3)
        ]
        for t in threads:
            t.start()

        # Wait a bit - first 2 should submit, third should block
        time.sleep(0.02)

        # Should have 2 in flight
        self.assertLessEqual(coord.in_flight_count, 2)

        # Wait for all to complete
        for t in threads:
            t.join(timeout=1)

        self.assertEqual(len(submitted), 3)


class TestErrorHandling(unittest.TestCase):
    """Test error handling."""

    def test_abort_sets_error_state(self):
        """abort() transitions to ERROR state."""
        s3 = MockS3Client()
        coord = MorselCoordinator(s3, "bucket", "key")
        coord.start(schema={}, num_row_groups=5)

        coord.abort()

        self.assertEqual(coord.state, CoordinatorState.ERROR)

    def test_cannot_submit_after_abort(self):
        """submit_morsel fails after abort."""
        s3 = MockS3Client()
        coord = MorselCoordinator(s3, "bucket", "key")
        coord.start(schema={}, num_row_groups=5)
        coord.abort()

        with self.assertRaises(RuntimeError):
            coord.submit_morsel(b"data", _make_rg_meta(0))

    def test_upload_error_transitions_to_error(self):
        """Upload failure transitions to ERROR state."""
        s3 = MockS3Client()
        coord = MorselCoordinator(s3, "bucket", "key")
        coord.start(schema={}, num_row_groups=2)

        def failing_upload(pn, data):
            raise Exception("Upload failed!")

        # Should not raise, but coordinator should be in error state
        try:
            coord.submit_morsel(b"data", _make_rg_meta(0), upload_fn=failing_upload)
        except:
            pass

        self.assertEqual(coord.state, CoordinatorState.ERROR)


class TestOffsetCalculation(unittest.TestCase):
    """Test byte offset calculation for footer."""

    def test_offsets_calculated_correctly(self):
        """Verify byte offsets are calculated correctly after all parts complete."""
        s3 = MockS3Client(upload_delay_ms=0)
        coord = MorselCoordinator(s3, "bucket", "key")
        coord.start(schema={}, num_row_groups=3)

        def sync_upload(pn, data):
            return s3.upload_part(Bucket="bucket", Key="key",
                                  UploadId=coord.upload_id,
                                  PartNumber=pn, Body=data)["ETag"]

        # Submit with different sizes
        sizes = [1000, 2000, 1500]
        for i, size in enumerate(sizes):
            data = b"x" * size
            meta = _make_rg_meta(i, total_size=size, col_sizes=[size // 2, size // 2])
            coord.submit_morsel(data, meta, upload_fn=sync_upload)

        # Wait for completion
        coord.wait_for_completion(timeout=5)
        self.assertEqual(coord.state, CoordinatorState.DONE)

        # Check offsets in completed parts
        coord._completed.sort(key=lambda p: p.part_number)

        expected_offset = 0
        for completed in coord._completed:
            self.assertEqual(completed.metadata.file_offset, expected_offset)
            expected_offset += completed.size

    def test_column_offsets_within_row_group(self):
        """Verify column chunk offsets are calculated within row group."""
        s3 = MockS3Client(upload_delay_ms=0)
        coord = MorselCoordinator(s3, "bucket", "key")
        coord.start(schema={}, num_row_groups=1)

        def sync_upload(pn, data):
            return s3.upload_part(Bucket="bucket", Key="key",
                                  UploadId=coord.upload_id,
                                  PartNumber=pn, Body=data)["ETag"]

        # Create metadata with 3 columns at different relative offsets
        meta = RowGroupMetadata(
            row_group_index=0,
            num_rows=1000,
            total_byte_size=3000,
            columns=[
                ColumnChunkMetadata(
                    column_index=0,
                    relative_offset=0,
                    compressed_size=1000,
                    uncompressed_size=1000,
                    num_values=1000,
                ),
                ColumnChunkMetadata(
                    column_index=1,
                    relative_offset=1000,
                    compressed_size=1200,
                    uncompressed_size=1200,
                    num_values=1000,
                ),
                ColumnChunkMetadata(
                    column_index=2,
                    relative_offset=2200,
                    compressed_size=800,
                    uncompressed_size=800,
                    num_values=1000,
                ),
            ],
        )

        coord.submit_morsel(b"x" * 3000, meta, upload_fn=sync_upload)
        coord.wait_for_completion(timeout=5)

        # Check column offsets
        completed = coord._completed[0]
        rg_offset = completed.metadata.file_offset

        self.assertEqual(completed.metadata.columns[0].file_offset, rg_offset + 0)
        self.assertEqual(completed.metadata.columns[1].file_offset, rg_offset + 1000)
        self.assertEqual(completed.metadata.columns[2].file_offset, rg_offset + 2200)


class TestConcurrency(unittest.TestCase):
    """Test concurrent operations."""

    def test_concurrent_submissions(self):
        """Multiple threads can submit morsels concurrently."""
        s3 = MockS3Client(upload_delay_ms=5)
        coord = MorselCoordinator(s3, "bucket", "key", max_in_flight=8)
        coord.start(schema={}, num_row_groups=10)

        def sync_upload(pn, data):
            return s3.upload_part(Bucket="bucket", Key="key",
                                  UploadId=coord.upload_id,
                                  PartNumber=pn, Body=data)["ETag"]

        results = []
        lock = threading.Lock()

        def worker(idx):
            pn = coord.submit_morsel(f"data{idx}".encode() * 100, _make_rg_meta(idx), upload_fn=sync_upload)
            with lock:
                results.append((idx, pn))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        coord.wait_for_completion(timeout=10)

        self.assertEqual(len(results), 10)
        self.assertEqual(coord.state, CoordinatorState.DONE)
        self.assertEqual(coord.stats["parts_uploaded"], 10)

    def test_part_numbers_are_sequential(self):
        """Part numbers are assigned sequentially."""
        s3 = MockS3Client(upload_delay_ms=0)
        coord = MorselCoordinator(s3, "bucket", "key")
        coord.start(schema={}, num_row_groups=5)

        def sync_upload(pn, data):
            return s3.upload_part(Bucket="bucket", Key="key",
                                  UploadId=coord.upload_id,
                                  PartNumber=pn, Body=data)["ETag"]

        part_numbers = []
        for i in range(5):
            pn = coord.submit_morsel(f"data{i}".encode(), _make_rg_meta(i), upload_fn=sync_upload)
            part_numbers.append(pn)

        self.assertEqual(part_numbers, [1, 2, 3, 4, 5])


class TestStats(unittest.TestCase):
    """Test statistics tracking."""

    def test_stats_accumulated(self):
        """Stats are accumulated correctly."""
        s3 = MockS3Client(upload_delay_ms=0)
        coord = MorselCoordinator(s3, "bucket", "key")
        coord.start(schema={}, num_row_groups=3)

        def sync_upload(pn, data):
            return s3.upload_part(Bucket="bucket", Key="key",
                                  UploadId=coord.upload_id,
                                  PartNumber=pn, Body=data)["ETag"]

        sizes = [100, 200, 150]
        for i, size in enumerate(sizes):
            coord.submit_morsel(b"x" * size, _make_rg_meta(i), upload_fn=sync_upload)

        coord.wait_for_completion(timeout=5)

        stats = coord.get_stats()
        self.assertEqual(stats["parts_uploaded"], 3)
        self.assertEqual(stats["bytes_uploaded"], sum(sizes))
        self.assertEqual(stats["completed"], 3)


def _make_rg_meta(
    idx: int,
    num_rows: int = 1000,
    total_size: int = 1000,
    col_sizes: list = None,
) -> RowGroupMetadata:
    """Helper to create RowGroupMetadata for tests."""
    if col_sizes is None:
        col_sizes = [total_size]

    columns = []
    offset = 0
    for i, size in enumerate(col_sizes):
        columns.append(ColumnChunkMetadata(
            column_index=i,
            relative_offset=offset,
            compressed_size=size,
            uncompressed_size=size,
            num_values=num_rows,
        ))
        offset += size

    return RowGroupMetadata(
        row_group_index=idx,
        num_rows=num_rows,
        total_byte_size=total_size,
        columns=columns,
    )


if __name__ == "__main__":
    unittest.main(verbosity=2)
