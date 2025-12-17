# List available recipes
default:
    @just --list

# Run all checks (tests + build + verify)
all: test build verify

# Build the project
build:
    zig build

# Check compilation (Lint)
lint:
    zig build check

# Run unit tests (Fast)
test:
    zig build test --summary all

# Verify compilation for all major targets (Critical for Lambda)
cross-check:
    @echo "Building for x86_64-linux (AWS Lambda)..."
    zig build -Dtarget=x86_64-linux
    @echo "Building for aarch64-linux (AWS Graviton)..."
    zig build -Dtarget=aarch64-linux
    @echo "Building for x86_64-macos..."
    zig build -Dtarget=x86_64-macos
    @echo "All targets compile successfully!"

# Generate all test fixtures (Fast to Moderate)
gen-fixtures:
    python3 tools/fixtures/gen.py

# Run the CLI tools against verified fixtures (Moderate)
verify: build
    @echo "Verifying CLI commands..."
    # Schema check
    zig build run -- schema data/simple.parquet
    # Metadata check
    zig build run -- meta data/required.parquet
    # Pages check (deep dive)
    zig build run -- pages data/simple.parquet > /dev/null
    # Cat check (data dump)
    zig build run -- cat data/simple.parquet 5
    @echo "Verification passed."

# Run comprehensive tests including heavy data and edge cases (Slow)
comprehensive: build gen-fixtures
    @echo "Running comprehensive checks..."
    
    # 1. Correctness on Edge Cases
    @echo "[Check] Large RLE Decoding..."
    zig build run -- cat data/large_rle.parquet 10 > /dev/null
    
    @echo "[Check] Bit-Packed Handling..."
    zig build run -- cat data/large_bitpacked.parquet 10 > /dev/null
    
    @echo "[Check] High Bit Widths..."
    zig build run -- cat data/high_width.parquet 10 > /dev/null
    
    # 2. Performance / Throughput
    @echo "[Bench] Scanning Many Rows (Throughput)..."
    zig build run -- scan data/many_rows.parquet
    
    # 3. Complex/Real-world Data (if available)
    # If skyway-subset exists, test it
    @if [ -f data/skyway-subset.parquet ]; then \
        echo "[Check] Real-world Subset (Snappy + Dict + Nulls)..."; \
        zig build run -- cat data/skyway-subset.parquet 5 > /dev/null; \
        zig build run -- meta data/skyway-subset.parquet; \
    fi

    @echo "Comprehensive suite finished."

# Run a simple benchmark
bench: build
    @echo "Benchmarking scan on large RLE..."
    zig build run -- scan data/large_rle.parquet
    @echo "Benchmarking scan on many rows..."
    zig build run -- scan data/many_rows.parquet

# Run comparative benchmark (ZPQ vs Rust vs Python)
bench-compare: build
    @echo "=== ZPQ ==="
    zig build run -Doptimize=ReleaseFast -- scan data/skyway-export-00002.snappy.parquet

    @echo "\n=== Python (PyArrow) ==="
    uv run python tools/bench/pyarrow_bench.py data/skyway-export-00002.snappy.parquet

    @echo "\n=== Rust (Arrow RecordBatchReader) ==="
    @cd tools/bench/rust_bench && cargo run --release --quiet -- ../../../data/skyway-export-00002.snappy.parquet

    @echo "\n=== Rust (Official CLI: parquet-read) ==="
    @echo "Note: Includes formatting overhead (piped to /dev/null)"
    @time parquet-read data/skyway-export-00002.snappy.parquet > /dev/null

# Generate malformed fixtures
gen-malformed:
    uv run python tools/fixtures/gen_malformed.py

# Verify that the reader correctly handles malformed files (Should fail gracefully)
verify-malformed: build gen-malformed
    @echo "Testing corrupt file handling..."
    
    @echo "[Check] Bad Magic Bytes (Should fail)..."
    @! zig build run -- inspect data/malformed/bad_magic.parquet > /dev/null 2>&1 && echo "  -> Failed as expected" || (echo "  -> UNEXPECTED SUCCESS" && exit 1)

    @echo "[Check] Truncated Footer (Should fail)..."
    @! zig build run -- inspect data/malformed/truncated.parquet > /dev/null 2>&1 && echo "  -> Failed as expected" || (echo "  -> UNEXPECTED SUCCESS" && exit 1)

    @echo "[Check] Garbage Footer Length (Should fail)..."
    @! zig build run -- inspect data/malformed/garbage_footer_len.parquet > /dev/null 2>&1 && echo "  -> Failed as expected" || (echo "  -> UNEXPECTED SUCCESS" && exit 1)
    
    @echo "[Check] Random Garbage with Valid Magic (Should fail)..."
    @! zig build run -- inspect data/malformed/random_garbage.parquet > /dev/null 2>&1 && echo "  -> Failed as expected" || (echo "  -> UNEXPECTED SUCCESS" && exit 1)

    @echo "Malformed tests passed (all files rejected)."

# Clean build artifacts
clean:
    rm -rf zig-cache zig-out
    rm -rf data/*.parquet data/*.json

# Run local CI via act (requires act installed)
ci:
    act

# Run local CI in watch mode (requires act)
watch-ci:
    act --watch
