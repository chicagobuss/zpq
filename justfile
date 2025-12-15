# List available recipes
default:
    @just --list

# Run all checks (tests + build + verify)
all: test build verify

# Build the project
build:
    zig build

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

# Run the inspector against basic verified fixtures (Moderate)
verify: build
    zig build run -- inspect data/simple.parquet
    zig build run -- inspect data/required.parquet

# Run comprehensive tests including heavy data and edge cases (Slow)
comprehensive: build gen-fixtures
    @echo "Running comprehensive checks..."
    zig build run -- inspect data/large_rle.parquet
    zig build run -- inspect data/large_bitpacked.parquet
    zig build run -- inspect data/high_width.parquet
    zig build run -- inspect data/many_rows.parquet
    @echo "Done."

# Run a simple benchmark (requires 'time' command)
bench: build
    @echo "Benchmarking large RLE read..."
    @/usr/bin/time -p zig build run -- inspect data/large_rle.parquet > /dev/null

# Clean build artifacts
clean:
    rm -rf zig-cache zig-out
    rm -rf data/*.parquet data/*.json

# Run local CI via act (requires act installed)
ci:
    act
