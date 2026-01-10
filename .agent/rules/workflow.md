---
trigger: always_on
---

# Workflow & Process

## Environment
- **Sandbox**: ALWAYS request `required_permissions: ['all']` for `run_terminal_cmd`.
- **Python**: Use `.venv` and manage deps with `uv`. Scripts in `tools/`.

## Testing and Benchmarking
- **Standard Benchmark File**: [data/benchmark/benchmark_100mb.parquet]
- **S3 Benchmark Path**: `s3://$AWS_S3_BUCKET/zpq_test_data/benchmark/benchmark_100mb.parquet`.
- **Environment**: Always source `.env` for cloud benchmarks (contains `AWS_S3_BUCKET` and credentials).
- **Core Benchmarks**:
  - `just bench native local local local`: Local-to-local scan.
  - `just bench native s3 local s3 100mb 3`: S3-to-S3 scan (3 runs).
  - `just bench-sweep` for all types
  - `just bench serverless s3 lambda s3`: Real AWS Lambda test.
  - `just engine duckdb s3 100mb`: Compare against DuckDB on S3 data.
  - `just zig-bench e2e <path>`: Direct native scan without `bench.sh` wrapper.

## Task Management
- **Just**: Use `just` as the primary runner (e.g., `just lint`, `just test`).
- **Linting**: Run `just lint` frequently to verify compilation cheaply.

## Problem Solving
- **Isolation**: Stop hacking `src/` when stuck. Create isolated `probes/probe_X.zig` immediately.
- **Probing**: Don't guess Zig master APIs. Probe them. Use `std.c.printf` or `std.debug.print` in probes if needed.
- **Session Start Protocol**: At the start of every session, lookup/probe the `std` lib reference to detect breaking changes (e.g., namespace renames like `std.io` -> `std.Io`).
- **Persistence**: Keep probe files; do not delete them. They are regression tests.

## CI/CD
- **Local**: Use `act` to run GitHub Actions locally.

## Running Builds
- Always use `just lint` before attempting to build
- `just build-debug` builds in debug mode
- `just build` builds in release fast mode
- **Always use `--summary all`**: When running `zig build`, append `--summary all` to verify success or see full error details.
- **Do not source `.env` for builds**: Separate build verification from execution. Only source `.env` when running the artifact requires it.
- **Probe Construction**: Use `build_probe.zig` for probes to avoid cluttering `build.zig`.