# ZPQ Benchmarking & Observability

Lightweight tracing and benchmarking infrastructure for ZPQ performance analysis.

## Quick Start

```bash
# Run a benchmark with tracing
zig build && ./zig-out/bin/zpq scan data/large.parquet --filter status=active --trace bench/traces/run1.json

# Compare runs
python bench/compare_runs.py bench/traces/*.json

# View in Jaeger (optional)
docker-compose -f bench/docker-compose.yml up -d
python bench/load_to_jaeger.py bench/traces/run1.json
# Open http://localhost:16686
```

## Design Principles

1. **Zero overhead when disabled**: Tracing compiles to no-ops via `trace.enabled = false`
2. **No allocations in hot path**: Just increment counters
3. **JSON-first**: Portable, git-trackable, tool-agnostic
4. **Jaeger optional**: Nice to have, not required

## Files

```
bench/
├── docker-compose.yml    # Jaeger all-in-one
├── compare_runs.py       # Generate markdown comparison tables  
├── load_to_jaeger.py     # Push JSON traces to Jaeger (requires opentelemetry)
├── traces/               # JSON trace output directory
└── README.md
```

## Trace JSON Format

```json
{
  "run": {
    "timestamp_ms": 1735470000000,
    "git_commit": "58735b1",
    "file_path": "data/large.parquet",
    "file_size_bytes": 157286400,
    "total_rows": 1000000,
    "filter_column": "status",
    "filter_value": "active"
  },
  "metrics": {
    "total_ms": 45.0,
    "filter_decode_ms": 12.3,
    "skip_ms": 8.7,
    "materialize_ms": 18.2,
    "loop_overhead_ms": 5.8,
    "rows_scanned": 1000000,
    "rows_selected": 10000,
    "rows_skipped": 990000,
    "selectivity": 0.01,
    "throughput_mval_s": 22.2
  }
}
```

## Comparing Runs

```bash
# Basic comparison
python bench/compare_runs.py trace1.json trace2.json

# With baseline for delta %
python bench/compare_runs.py traces/*.json --baseline traces/baseline.json

# Sort by throughput
python bench/compare_runs.py traces/*.json --sort throughput
```

Example output:

```markdown
## ZPQ Benchmark Comparison

### Summary

| Run | Commit | Selectivity | Total (ms) | Throughput (MVal/s) |
|-----|--------|-------------|------------|---------------------|
| baseline | 58735b1 | 1.00% | 45.00 | 22.22 |
| optimized | abc1234 | 1.00% | 32.00 (-28.9%) | 31.25 (+40.6%) |
```

## Jaeger Setup (Optional)

For interactive trace visualization:

```bash
# Start Jaeger
docker-compose -f bench/docker-compose.yml up -d

# Install Python dependencies  
pip install opentelemetry-exporter-otlp-proto-http

# Load traces
python bench/load_to_jaeger.py bench/traces/*.json

# Open UI
open http://localhost:16686
```

## Adding Instrumentation

In Zig code:

```zig
const trace = @import("trace.zig");

// In benchmark harness
var tracer = trace.Tracer.init(.{
    .timestamp_ms = trace.nowMs(),
    .file_path = path,
    .total_rows = meta.num_rows,
});

// In hot path - just increment counters
tracer.recordFilterDecode(elapsed_ns, row_count);
tracer.recordSkip(elapsed_ns, rows_skipped);
tracer.recordMaterialize(elapsed_ns, rows_selected);

// After benchmark
try tracer.writeToFile("bench/traces/run.json");
```
