# Plan 09: Lambda End-to-End Verification

## Criticism
**"Lambda-First Goal is Untested End-to-End."**
We claim Lambda optimization but verify with a micro-benchmark loop, not a real S3 scan in Lambda environment.

## Response
**Valid.** We assume our binary size and memory footprint translates to better Lambda performance, but we haven't proven it for the full Parquet scan.

## Action Plan

### 1. `tools/lambda_bench/`
Create a deployable project.
- `bootstrap`: The ZPQ binary (built for `aarch64-linux-musl`).
- `function.zip`: Packaging.

### 2. Comparator Functions
- **Python**: A Lambda with the standard AWS SDK + PyArrow layer.
- **Rust**: A Lambda with the Rust runtime + `parquet` crate.

### 3. Workload
- Trigger: Event with S3 Bucket/Key.
- Action: Download last 1GB file, scan Column "A", sum values.
- Metric: Report `Duration` and `Max Memory Used` from CloudWatch Logs.

### 4. Cold Start vs. Warm
Measure:
- **Cold Start**: First invocation.
- **Warm**: Subsequent invocations.

### 5. Validation
If ZPQ is not significantly faster/cheaper, we revisit our architecture. (Spoiler: Zig startup is <1ms vs Python ~200ms, so we should win Cold Start easily. Throughput is the real test).

