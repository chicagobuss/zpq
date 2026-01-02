#!/usr/bin/env python3
"""
State machine model for chunked S3 multipart uploads.

This models the flow we need to implement in Zig:
1. Connect to S3 endpoint (TLS handshake)
2. Send HTTP request in chunks (to avoid TLS buffer issues)
3. Wait for response
4. Extract ETag from response

The key insight: uploads >130KB fail with TLS errors because we don't
interleave reads during large writes. Solution: chunk writes to ~64KB.
"""

import hashlib
import requests
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional
import time


class UploadState(Enum):
    """States for a single part upload."""
    IDLE = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    WRITING_CHUNK = auto()
    WAITING_WRITE_COMPLETE = auto()
    READING_RESPONSE = auto()
    COMPLETE = auto()
    ERROR = auto()


@dataclass
class ChunkedUploadContext:
    """Context for a single chunked upload."""
    part_number: int
    upload_id: str
    data: bytes

    # Chunking state
    chunk_size: int = 64 * 1024  # 64KB chunks
    write_offset: int = 0

    # Result
    state: UploadState = UploadState.IDLE
    etag: Optional[str] = None
    error: Optional[str] = None

    # Timing
    start_time: float = 0
    end_time: float = 0

    @property
    def remaining_bytes(self) -> int:
        return len(self.data) - self.write_offset

    @property
    def is_write_complete(self) -> bool:
        return self.write_offset >= len(self.data)

    def next_chunk(self) -> bytes:
        """Get the next chunk to write."""
        end = min(self.write_offset + self.chunk_size, len(self.data))
        chunk = self.data[self.write_offset:end]
        return chunk

    def advance_offset(self, bytes_written: int):
        """Called after a chunk is written."""
        self.write_offset += bytes_written


def simulate_chunked_upload(ctx: ChunkedUploadContext, verbose: bool = True) -> bool:
    """
    Simulate the chunked upload state machine.

    This is what the Zig code needs to implement with callbacks:
    - on_connect: transition to CONNECTED, start first chunk
    - on_write_complete: advance offset, write next chunk or wait for response
    - on_data: accumulate response, check for completion
    - on_error: set ERROR state
    """
    ctx.start_time = time.time()
    ctx.state = UploadState.CONNECTING

    if verbose:
        print(f"[{ctx.part_number}] CONNECTING")

    # Simulate connection (in real code this is async)
    ctx.state = UploadState.CONNECTED
    if verbose:
        print(f"[{ctx.part_number}] CONNECTED - {len(ctx.data)} bytes to send in {ctx.chunk_size//1024}KB chunks")

    # Write chunks
    chunk_num = 0
    while not ctx.is_write_complete:
        ctx.state = UploadState.WRITING_CHUNK
        chunk = ctx.next_chunk()
        chunk_num += 1

        if verbose:
            print(f"[{ctx.part_number}] WRITING_CHUNK {chunk_num}: offset={ctx.write_offset}, size={len(chunk)}, remaining={ctx.remaining_bytes - len(chunk)}")

        # Simulate write (in real code: conn.write(chunk))
        ctx.state = UploadState.WAITING_WRITE_COMPLETE

        # Simulate write complete callback
        ctx.advance_offset(len(chunk))

        if verbose:
            print(f"[{ctx.part_number}] WRITE_COMPLETE: new_offset={ctx.write_offset}")

    # All data written, wait for response
    ctx.state = UploadState.READING_RESPONSE
    if verbose:
        print(f"[{ctx.part_number}] READING_RESPONSE")

    # Simulate response (in real code: on_data callback)
    # For testing, compute expected MD5 as ETag
    ctx.etag = hashlib.md5(ctx.data).hexdigest()
    ctx.state = UploadState.COMPLETE
    ctx.end_time = time.time()

    if verbose:
        elapsed = (ctx.end_time - ctx.start_time) * 1000
        throughput = len(ctx.data) / (1024 * 1024) / (elapsed / 1000) if elapsed > 0 else 0
        print(f"[{ctx.part_number}] COMPLETE: etag={ctx.etag}, {elapsed:.1f}ms, {throughput:.1f} MB/s")

    return True


@dataclass
class ParallelUploadOrchestrator:
    """
    Orchestrates multiple parallel chunked uploads.

    Key pattern (maps to Zig xev loop):
    1. Create all upload contexts
    2. Start all connections (non-blocking)
    3. Run event loop until all complete
    4. Collect results (ETags for CompleteMultipartUpload)
    """
    contexts: list[ChunkedUploadContext] = field(default_factory=list)

    def add_part(self, part_number: int, upload_id: str, data: bytes):
        ctx = ChunkedUploadContext(
            part_number=part_number,
            upload_id=upload_id,
            data=data,
        )
        self.contexts.append(ctx)

    def run_all(self, verbose: bool = True) -> list[tuple[int, str]]:
        """
        Run all uploads and return list of (part_number, etag) tuples.

        In Zig, this is:
        1. Set up callbacks for each context
        2. Call conn.connect() for each (non-blocking)
        3. loop.run(.until_done)
        4. Iterate contexts and collect results
        """
        results = []

        # In real async code, these would run in parallel
        for ctx in self.contexts:
            success = simulate_chunked_upload(ctx, verbose)
            if success and ctx.etag:
                results.append((ctx.part_number, ctx.etag))

        return results


def build_complete_multipart_request(bucket: str, key: str, upload_id: str,
                                      parts: list[tuple[int, str]]) -> str:
    """
    Build the CompleteMultipartUpload XML request body.

    Key optimization: We can pre-compute this BEFORE uploads complete
    if we pre-compute MD5 hashes locally!
    """
    xml_parts = []
    for part_num, etag in sorted(parts):
        xml_parts.append(f"""    <Part>
      <PartNumber>{part_num}</PartNumber>
      <ETag>"{etag}"</ETag>
    </Part>""")

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
{chr(10).join(xml_parts)}
</CompleteMultipartUpload>"""


def demo():
    """Demo the chunked upload model."""
    print("=" * 60)
    print("Chunked Upload State Machine Demo")
    print("=" * 60)
    print()

    # Simulate 3 parallel 1MB uploads
    orchestrator = ParallelUploadOrchestrator()

    for i in range(3):
        # Create 1MB of test data
        data = bytes([i % 256] * (1024 * 1024))
        orchestrator.add_part(
            part_number=i + 1,
            upload_id="test-upload-id",
            data=data,
        )

    print("Starting parallel uploads...")
    print()

    results = orchestrator.run_all()

    print()
    print("=" * 60)
    print("Results")
    print("=" * 60)
    for part_num, etag in results:
        print(f"Part {part_num}: {etag}")

    print()
    print("CompleteMultipartUpload request body:")
    print("-" * 40)
    xml = build_complete_multipart_request("test-bucket", "test-key", "test-upload-id", results)
    print(xml)


if __name__ == "__main__":
    demo()
