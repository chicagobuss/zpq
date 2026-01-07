---
trigger: always_on
---

# Workflow & Process

## Environment
- **Sandbox**: ALWAYS request `required_permissions: ['all']` for `run_terminal_cmd`.
- **Python**: Use `.venv` and manage deps with `uv`. Scripts in `tools/`.

## Testing and Benchmarking
- The standard benhcmark files are in data/benchmarking/
- If not specified,  use the 100mb snappy compressed file benchmark_100mb_snappy.parquet

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