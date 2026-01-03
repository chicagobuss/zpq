# ZPQ Morsel Architecture Design

**Last Updated**: Jan 2, 2025
**Status**: Design Phase - Pseudocode and Python proof-of-concept

---

## Vision: Fully Pipelined Parallel S3 I/O

The goal is to overlap all pipeline stages so that row groups flow through independently:

```
Time →
────────────────────────────────────────────────────────────────────────────
S3 Read:    [RG1 bytes][RG2 bytes][RG3 bytes][RG4 bytes]
Decode:          [RG1]     [RG2]     [RG3]     [RG4]
Filter:            [RG1]     [RG2]     [RG3]     [RG4]
Encode:              [RG1]     [RG2]     [RG3]     [RG4]
S3 Write:              [part1]   [part2]   [part3]  [part4][footer]
```

Each row group is a **morsel** - an independent unit that flows through the pipeline without blocking other morsels.

---

## DuckDB vs zpq Parallelism

DuckDB is constrained by sequential file writes:

```
DuckDB:         [prepare RG1] [prepare RG2] [prepare RG3]
                     │             │             │
                     ▼             ▼             ▼
                [write RG1] → [write RG2] → [write RG3] → [footer]
                     └──────── sequential ────────┘
```

zpq can leverage S3 multipart upload for parallel writes:

```
zpq:            [prepare RG1] [prepare RG2] [prepare RG3]
                     │             │             │
                     ▼             ▼             ▼
                [upload part1] [upload part2] [upload part3]  ← parallel!
                     │             │             │
                     └─────────────┼─────────────┘
                                   ▼
                            [footer part] → CompleteMultipartUpload
```

**Key insight**: S3 multipart parts can upload in any order. We just need to track:
1. Part number → row group mapping
2. Part sizes (for footer byte offsets)
3. ETags (for CompleteMultipartUpload)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Pipeline Coordinator                             │
│  - Initiates multipart upload                                           │
│  - Assigns part numbers to morsels                                      │
│  - Tracks in-flight parts and completed ETags                           │
│  - Accumulates metadata (byte offsets, row counts, stats)               │
│  - Builds footer when all parts complete                                │
│  - Calls CompleteMultipartUpload                                        │
└─────────────────────────────────────────────────────────────────────────┘
         ▲                    ▲                    ▲
         │                    │                    │
    ┌────┴────┐          ┌────┴────┐          ┌────┴────┐
    │ Morsel  │          │ Morsel  │          │ Morsel  │
    │ Worker  │          │ Worker  │          │ Worker  │
    │   #1    │          │   #2    │          │   #3    │
    └────┬────┘          └────┬────┘          └────┬────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         S3 Upload Pool                                   │
│  - Connection pooling (reuse HTTP connections)                          │
│  - Parallel UploadPart requests                                         │
│  - Returns ETag on completion                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Morsel Lifecycle

```
1. S3 Read          → Row group bytes downloaded
2. Decode           → Columns decoded to in-memory batches  
3. Filter           → Selection vector applied
4. Encode           → Re-encoded to Parquet row group bytes
5. Coordinator      → Assigned part number, metadata recorded
6. S3 Upload        → UploadPart with part number
7. Complete         → ETag returned, morsel done
```

### Coordinator State Machine

```
States:
  INIT              → Waiting to start multipart upload
  UPLOADING         → Parts in flight, accepting new morsels
  DRAINING          → No more morsels, waiting for in-flight parts
  FINALIZING        → All parts done, building and uploading footer
  COMPLETING        → Footer uploaded, calling CompleteMultipartUpload
  DONE              → Success
  ERROR             → Failed (abort multipart upload)

Transitions:
  INIT → UPLOADING          : CreateMultipartUpload succeeds
  UPLOADING → UPLOADING     : Morsel ready, part started
  UPLOADING → DRAINING      : Last morsel submitted
  DRAINING → DRAINING       : Part completes, more in flight
  DRAINING → FINALIZING     : Last part completes
  FINALIZING → COMPLETING   : Footer part uploaded
  COMPLETING → DONE         : CompleteMultipartUpload succeeds
  * → ERROR                 : Any failure
```

---

## The Footer Problem

Parquet footers contain byte offsets for each row group and column chunk. These offsets aren't known until all preceding data is written.

### Solution: Two-Phase Metadata

**Phase 1 - During Upload:**
- Each morsel reports its encoded size after compression
- Coordinator tracks: `{part_number, row_group_index, compressed_size, row_count, column_stats}`
- Parts upload in parallel (order doesn't matter for upload)

**Phase 2 - Footer Assembly:**
- After all parts complete, coordinator knows all sizes
- Calculate cumulative byte offsets: `offset[i] = sum(sizes[0..i])`
- Build FileMetaData with correct offsets
- Upload footer as final part
- CompleteMultipartUpload with parts in order

```python
# Pseudocode for offset calculation
parts = sorted(completed_parts, key=lambda p: p.part_number)
offset = 0
for part in parts:
    part.row_group_metadata.file_offset = offset
    for col in part.row_group_metadata.columns:
        col.file_offset = offset + col.relative_offset
    offset += part.compressed_size

footer = build_footer(parts)
upload_footer_part(footer)
complete_multipart_upload(upload_id, all_parts_with_etags)
```

---

## Backpressure

To prevent unbounded memory growth when encoding is faster than uploading:

```
MAX_IN_FLIGHT_PARTS = 8  # Tunable based on memory budget

Morsel Worker:
    encoded_data = encode(row_group)
    
    # Block if too many parts in flight
    while coordinator.in_flight_count >= MAX_IN_FLIGHT_PARTS:
        wait_for_part_completion()
    
    coordinator.submit_part(encoded_data, metadata)
```

---

## Mapping to Existing Zig Modules

| Component | Existing Code | Changes Needed |
|-----------|---------------|----------------|
| Coordinator | `pipeline.zig` | Add multipart state machine, metadata accumulation |
| Morsel Worker | `row_group_worker.zig` | Add encoding output, submit to coordinator |
| S3 Upload Pool | `orchestrator.zig` (reads) | Adapt for writes with UploadPart |
| Part Encoding | `s3_writer.zig` | Extract row group encoding from full-file writing |

---

## Pseudocode: Coordinator

```python
class MorselCoordinator:
    def __init__(self, s3_client, bucket, key, max_in_flight=8):
        self.s3 = s3_client
        self.bucket = bucket
        self.key = key
        self.max_in_flight = max_in_flight
        
        self.state = "INIT"
        self.upload_id = None
        self.next_part_number = 1
        
        self.in_flight = {}      # part_number -> MorselData
        self.completed = []      # [(part_number, etag, size, metadata)]
        
        self.total_row_groups = 0
        self.schema = None
    
    def start(self, schema, num_row_groups):
        """Initialize multipart upload."""
        self.schema = schema
        self.total_row_groups = num_row_groups
        
        response = self.s3.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.key
        )
        self.upload_id = response["UploadId"]
        self.state = "UPLOADING"
    
    def submit_morsel(self, encoded_bytes, row_group_metadata):
        """Submit an encoded row group for upload."""
        assert self.state == "UPLOADING"
        
        # Backpressure: wait if too many in flight
        while len(self.in_flight) >= self.max_in_flight:
            self._wait_for_completion()
        
        part_number = self.next_part_number
        self.next_part_number += 1
        
        self.in_flight[part_number] = {
            "bytes": encoded_bytes,
            "metadata": row_group_metadata,
            "size": len(encoded_bytes)
        }
        
        # Start async upload
        self._start_upload(part_number, encoded_bytes)
        
        # Check if this was the last morsel
        if self.next_part_number > self.total_row_groups:
            self.state = "DRAINING"
    
    def _on_part_complete(self, part_number, etag):
        """Called when UploadPart succeeds."""
        morsel = self.in_flight.pop(part_number)
        self.completed.append((
            part_number,
            etag,
            morsel["size"],
            morsel["metadata"]
        ))
        
        if self.state == "DRAINING" and len(self.in_flight) == 0:
            self._finalize()
    
    def _finalize(self):
        """Build footer and complete upload."""
        self.state = "FINALIZING"
        
        # Sort by part number to get correct byte order
        self.completed.sort(key=lambda x: x[0])
        
        # Calculate cumulative offsets
        offset = 0
        row_groups = []
        for part_number, etag, size, metadata in self.completed:
            metadata.file_offset = offset
            for col in metadata.columns:
                col.file_offset = offset + col.relative_offset
            row_groups.append(metadata)
            offset += size
        
        # Build footer
        footer = build_parquet_footer(self.schema, row_groups)
        footer_bytes = serialize_footer(footer)
        
        # Upload footer as final part
        footer_part = self.next_part_number
        response = self.s3.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            PartNumber=footer_part,
            Body=footer_bytes
        )
        footer_etag = response["ETag"]
        
        self.state = "COMPLETING"
        
        # Complete multipart upload
        parts = [
            {"PartNumber": pn, "ETag": et}
            for pn, et, _, _ in self.completed
        ]
        parts.append({"PartNumber": footer_part, "ETag": footer_etag})
        
        self.s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={"Parts": parts}
        )
        
        self.state = "DONE"
    
    def abort(self):
        """Abort on error."""
        if self.upload_id:
            self.s3.abort_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.upload_id
            )
        self.state = "ERROR"
```

---

## Pseudocode: Morsel Worker

```python
class MorselWorker:
    def __init__(self, coordinator, encoder):
        self.coordinator = coordinator
        self.encoder = encoder
    
    def process(self, row_group_reader, selection_vector=None):
        """Process a single row group and submit to coordinator."""
        
        # Decode columns
        columns = {}
        for col_idx in row_group_reader.selected_columns:
            batch_reader = row_group_reader.get_column(col_idx)
            values = []
            while batch_reader.has_next():
                batch = batch_reader.next_batch(1024)
                if selection_vector:
                    batch = apply_selection(batch, selection_vector)
                values.extend(batch)
            columns[col_idx] = values
        
        # Encode to Parquet row group bytes
        encoded_bytes, metadata = self.encoder.encode_row_group(columns)
        
        # Submit to coordinator (may block on backpressure)
        self.coordinator.submit_morsel(encoded_bytes, metadata)
```

---

## Pseudocode: Pipeline Integration

```python
class Pipeline:
    def __init__(self, s3_source, s3_writer_config):
        self.source = s3_source
        self.writer_config = s3_writer_config
    
    def run(self, filter_expr, selected_columns, output_path):
        """Execute the full pipeline."""
        
        # Parse input file
        file_reader = ParquetFileReader(self.source)
        schema = file_reader.schema
        
        # Count row groups (may skip some based on stats)
        row_groups = [
            rg for rg in file_reader.row_groups
            if not should_skip_row_group(rg, filter_expr)
        ]
        
        # Initialize coordinator
        coordinator = MorselCoordinator(
            s3_client=self.writer_config.s3_client,
            bucket=parse_bucket(output_path),
            key=parse_key(output_path)
        )
        coordinator.start(schema, len(row_groups))
        
        # Process row groups in parallel
        workers = [MorselWorker(coordinator, Encoder(schema)) for _ in range(4)]
        
        # Fan out row groups to workers
        for i, rg in enumerate(row_groups):
            worker = workers[i % len(workers)]
            
            # Phase 1: Filter column only
            rg.prefetch_columns([filter_expr.column_idx])
            selection = evaluate_filter(rg, filter_expr)
            
            if selection.count > 0:
                # Phase 2: Remaining columns + encode + upload
                rg.prefetch_remaining_columns()
                worker.process(rg, selection)
        
        # Wait for completion
        coordinator.wait_for_done()
        
        return coordinator.final_stats()
```

---

## Next Steps

1. **Python Proof-of-Concept**: Implement the coordinator state machine in Python
   - Simulate S3 multipart upload with mock delays
   - Verify state transitions and error handling
   - Test backpressure behavior
   - Validate footer byte offset calculation

2. **Verify with Real S3**: Run Python PoC against real S3
   - Use boto3 for actual multipart upload
   - Write minimal Parquet files (header + empty row groups + footer)
   - Verify DuckDB can read the output

3. **Zig Implementation**: Port coordinator to Zig
   - Add to `pipeline.zig`
   - Integrate with existing `row_group_worker.zig`
   - Adapt `orchestrator.zig` pattern for writes

---

## File Structure for Python PoC

```
probes/
├── morsel_poc/
│   ├── __init__.py
│   ├── coordinator.py      # MorselCoordinator state machine
│   ├── worker.py           # MorselWorker (mock encode)
│   ├── pipeline.py         # Pipeline integration
│   ├── parquet_footer.py   # Footer building utilities
│   ├── test_state_machine.py   # Unit tests for state transitions
│   └── test_s3_integration.py  # Real S3 integration test
```

---

## Open Questions

1. **Part Size Minimum**: S3 requires parts to be at least 5MB (except last part). How do we handle small row groups?
   - Option A: Buffer multiple small row groups into one part
   - Option B: Pad to 5MB (wasteful)
   - Option C: Use single PutObject for small files

2. **Error Recovery**: If a part upload fails, can we retry just that part?
   - S3 allows retry with same part number
   - Need to track which parts are "in progress" vs "failed"

3. **Memory Budget**: How much encoded data can we hold in flight?
   - 8 parts × 10MB row groups = 80MB typical
   - May need to tune based on Lambda memory settings

4. **Schema Evolution**: Should output schema match input exactly, or allow column reordering?
   - Current plan: Match input schema for selected columns only
