# Plan 07: Fuzzing Strategy

## Criticism
**"No Fuzz Testing."**
Parsing untrusted network data (Thrift, HTTP, S3 XML) without fuzzing is negligent.

## Response
**Valid.** We parse Thrift compact protocol and HTTP headers manually. These are prime targets for buffer overreads.

## Action Plan

### 1. Harness Targets
Create `src/fuzz/`:
- `fuzz_thrift.zig`: Fuzz `zpq.encodings.thrift.deserialize`. Input: Random bytes.
- `fuzz_http.zig`: Fuzz `AsyncRequest.parseHeaders` (or equivalent response parser). Input: Random HTTP response strings.
- `fuzz_rle.zig`: Fuzz `zpq.encodings.rle.Decoder`. Input: Random bytes.

### 2. Tooling
Use `kccul/zig-afl-kit` or native `std.testing.fuzz` (if available/stable in 0.16).
- If native fuzzing is immature, write a simple C wrapper to use vanilla AFL++ against the Zig static lib.

### 3. CI Integration
Add a `fuzz` job to CI.
- Run for a fixed duration (e.g., 10 mins) per commit? (Usually too slow).
- Better: Run nightly for 6 hours.

### 4. Immediate Goal
Run 24 hours of local fuzzing on the **Thrift parser**. This is the most complex binary parser we have. Fix any crashes found.

