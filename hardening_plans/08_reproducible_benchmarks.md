# Plan 08: Reproducible Benchmarks

## Criticism
**"Benchmark Comparison is Disingenuous."**
Comparing a specialized reader to a generic library with default settings is unfair.

## Response
**Valid.** We want to win on merit, not configuration mismatch.

## Action Plan

### 1. `benches/` Directory
Create a dedicated folder for reproducible performance tests.

### 2. Dockerized Environment
Create a `Dockerfile` that installs:
- Specific version of Python + PyArrow.
- Specific version of Rust + `parquet` crate example.
- Zig (pinned version).

### 3. Fair Scripts
- **Python**: `bench_pyarrow.py` using `use_threads=False` (since ZPQ is currently single-threaded in user-space execution, though async I/O) OR `use_threads=True` (to be fair to the hardware). *Decision: Compare against "Best Practice" PyArrow.*
- **Rust**: Build the `parquet-read` example from the arrow-rs repo with `--release`.

### 4. Dataset
Define a standard test file generation script (using PyArrow) to ensure all runners operate on the exact same data structure (e.g., 1GB file, 10 RowGroups, Snappy compression).

### 5. Publish
Update `README.md` with a link to `benches/README.md` and the full results table including version numbers.

