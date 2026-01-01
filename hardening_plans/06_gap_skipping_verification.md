# Plan 06: Gap Skipping Verification

## Criticism
**"Zero-Allocation Gap Skipping is Not Demonstrated."**
We claim it, but where is the proof?

## Response
**Valid.** The feature exists in the `scheduler.zig` (generating `null` segments) and `request.zig` (handling `null` buffers), but we lack an explicit test proving no memory was touched for the gap.

## Action Plan

### 1. Create `test_gap_skipping.zig`
A new integration test.
- **Setup**: Mock S3 server returns 1MB of data "A...A[GAP]B...B".
- **Action**: `AsyncS3Source.readRanges` for byte `0..10` and `1000..1010`.
- **Assertion**:
    1.  Data is correct.
    2.  **Memory Assertion**: Use a custom `TestAllocator` that tracks total allocated bytes.
    3.  Assert `total_allocated < 1MB`. It should be roughly `size(ResponseHeaders) + size(Range1) + size(Range2) + socket_buffers`.
    4.  If we allocated 1MB (to buffer the gap), the test fails.

### 2. Verify `AsyncRequest` Logic
Review `AsyncRequest.onData`:
- Ensure that when `current_segment.buffer` is `null`, we strictly advance `received_len` without copying/allocating.
- Ensure `xev` read buffers are reused, not accumulated.

### 3. Documentation
Add a link to this test in `README.md` under "Performance Features" to prove the claim.

