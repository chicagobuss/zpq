# Configuration
GH_OWNER := "chicagobuss"
GH_REPO := "zpq"
DEPS_TAG := "deps-v0.2"
ARM_BENCH_HOST := "oci-josh-arm-vm"

# List available recipes
default:
    @just --list

# === Development Setup ===

# === Testing ===
# Run all checks (tests + build + verify)
all: test build verify

# Build the project (Core only)
build:
    @./tools/just_helpers.sh fetch_deps
    zig build

# Build everything including tests and probes
build-and-test:
    @./tools/just_helpers.sh fetch_deps
    zig build -Dall

# Check compilation (Lint)
lint:
    @./tools/just_helpers.sh fetch_deps
    zig build check

# Run unit tests (Fast)
test *args="":
    @./tools/just_helpers.sh fetch_deps
    zig build test --summary all -- {{args}}

# List all tests defined in the codebase
list-tests:
    @grep -rhE 'test ".+"' src tests | sed 's/test "//;s/" {//' | sort

# Run tests with verbose output (shows every compiler command and test names)
test-verbose *args="":
    zig build test -Dverbose-tests --summary all --verbose -- {{args}}

# Run specific tests by name filter (useful for debugging)
test-filter *filter:
    zig build test --summary all -- --test-filter "{{filter}}"

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

# Generate deranged test fixtures
gen-deranged:
    python3 tools/fixtures/deranged_gen.py
    duckdb -c "COPY (SELECT * FROM read_csv('data/deranged.csv')) TO 'data/deranged.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 1000);"
    @echo "Generated data/deranged.parquet using DuckDB"

# Run the CLI tools against verified fixtures (Moderate)
verify: build check-tls
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

# Verify TLS Transport and S3 Factory (Requires Network)
check-tls: build
    @echo "Verifying TLS Transport (Google HEAD)..."
    python3 tools/no_output_timeout.py --idle-seconds 10 -- zig build probe-tls-echo
    @echo "Verifying S3 Integration (MinIO Parquet)..."
    @ZPQ_TEST_MINIO=1 python3 tools/no_output_timeout.py --idle-seconds 10 -- zig build -Dexperimental test-parquet-s3

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
    # If zpq-subset exists, test it
    @if [ -f data/zpq-subset.parquet ]; then \
        echo "[Check] Real-world Subset (Snappy + Dict + Nulls)..."; \
        zig build run -- cat data/zpq-subset.parquet 5 > /dev/null; \
        zig build run -- meta data/zpq-subset.parquet; \
    fi

    @echo "Comprehensive suite finished."

# === Lambda ===

# Build and run the minimal Hello World Lambda locally
lambda-01:
    @echo "Building Lambda 01-minimal..."
    zig build example-lambda-01-minimal -Dexamples -Dtarget=native
    @echo "Starting Lambda RIE..."
    @echo "To test: curl -XPOST \"http://localhost:8080/2015-03-31/functions/function/invocations\" -d '{}'"
    # Mocking AWS_LAMBDA_RUNTIME_API for local RIE (using 8080)
    AWS_LAMBDA_RUNTIME_API=localhost:8080 ./zig-out/lambda/lambda-01-minimal/bootstrap

# Build and run the DNS warming Lambda locally
lambda-02:
    @echo "Building Lambda 02-dns-warming..."
    zig build example-lambda-02-dns-warming -Dexamples -Dtarget=native
    @echo "Starting Lambda RIE..."
    AWS_LAMBDA_RUNTIME_API=localhost:8080 ./zig-out/lambda/lambda-02-dns-warming/bootstrap

# Build and run the S3 warming Lambda locally
lambda-03:
    @echo "Building Lambda 03-warm-s3..."
    zig build example-lambda-03-warm-s3 -Dexamples -Dtarget=native
    @echo "Starting Lambda RIE..."
    AWS_LAMBDA_RUNTIME_API=localhost:8080 ./zig-out/lambda/lambda-03-warm-s3/bootstrap

# Build and run the Parquet scan benchmark Lambda locally
lambda-04:
    @echo "Building Lambda 04-scan-benchmark..."
    zig build example-lambda-04-scan-benchmark -Dexamples -Dtarget=native
    @echo "Starting Lambda RIE..."
    @echo "To test: curl -XPOST \"http://localhost:8080/2015-03-31/functions/function/invocations\" -d '{\"file\": \"s3://bucket/path.parquet\"}'"
    AWS_LAMBDA_RUNTIME_API=localhost:8080 ./zig-out/lambda/lambda-04-scan-benchmark/bootstrap

# Run a specific benchmark (dns, ping, e2e, scan, pyarrow)
bench +args:
    ./benchmarks/bench.sh {{args}}

# Run DNS resolver benchmark
bench-dns:
    ./benchmarks/bench.sh dns

# Run TCP ping-pong benchmark
bench-ping:
    ./benchmarks/bench.sh ping

# Run E2E parquet scan benchmark
bench-e2e path *args:
    ./benchmarks/bench.sh e2e {{path}} {{args}}

# Compare ZPQ vs PyArrow on a file
bench-compare path *args:
    ./benchmarks/bench.sh compare e2e {{path}} {{args}}

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

# Fetch pre-built dependencies to speed up build
fetch-deps:
    @echo "Fetching pre-built BoringSSL static libraries ({{DEPS_TAG}})..."
    @TRIPLE=$(uname -m)-$(uname -s | tr '[:upper:]' '[:lower:]') && \
        mkdir -p vendor/boring_tls/prebuilt/$$TRIPLE && \
        echo "Detected triple: $$TRIPLE" && \
        curl -fL https://github.com/{{GH_OWNER}}/{{GH_REPO}}/releases/download/{{DEPS_TAG}}/libcrypto-$$TRIPLE.a -o vendor/boring_tls/prebuilt/$$TRIPLE/libcrypto.a || echo "Warning: Could not fetch libcrypto.a" && \
        curl -fL https://github.com/{{GH_OWNER}}/{{GH_REPO}}/releases/download/{{DEPS_TAG}}/libssl-$$TRIPLE.a -o vendor/boring_tls/prebuilt/$$TRIPLE/libssl.a || echo "Warning: Could not fetch libssl.a"

# Clean build artifacts
clean:
    rm -rf zig-cache zig-out
    rm -rf data/*.parquet data/*.json

# Run local CI via act (requires act installed)
# Note: First run is slow (~5-10min) due to BoringSSL compilation if pre-built deps unavailable
ci:
    act push -W .github/workflows/ci.yml --container-architecture $([ $(uname -m) == "arm64" ] && echo "linux/arm64" || echo "linux/amd64") -P ubuntu-latest=catthehacker/ubuntu:act-latest

# Run quick local CI (native, no Docker) - much faster for iteration
ci-quick: lint test
    @echo "Quick CI passed (lint + tests)"

# Run full local CI without verbose debug output
ci-quiet:
    act push -W .github/workflows/ci.yml --container-architecture $([ $(uname -m) == "arm64" ] && echo "linux/arm64" || echo "linux/amd64") -P ubuntu-latest=catthehacker/ubuntu:act-latest 2>&1 | grep -E "^\\[|Success|Failed|Error"

# Build Linux BoringSSL deps locally (cross-compile, caches in Docker volume)
# Run this once to speed up future `just ci` runs
ci-warm-cache:
    @echo "Building BoringSSL for Linux (this takes ~10 min first time)..."
    docker run --rm -v zpq-zig-cache:/root/.cache/zig -v $(pwd):/work -w /work \
        catthehacker/ubuntu:act-latest \
        bash -c 'curl -L https://raw.githubusercontent.com/tristanisham/zvm/master/install.sh | bash && \
                 export PATH=$$HOME/.zvm/bin:$$HOME/.zvm/self:$$PATH && \
                 zvm install master && zvm use master && \
                 cd vendor/boring_tls && zig build -Duse-prebuilt=false --summary all'
    @echo "Cache warmed! Future 'just ci' runs will be faster."

# Run local CI (alias)
test-ci: ci

# Run local CI for ARM64 (Native on M1/M2 Mac)
ci-arm:
    act -j test --container-architecture linux/arm64 -P ubuntu-latest=catthehacker/ubuntu:act-latest

# Remote CI (real hardware)
remote-ci-arm:
    ./tools/remote_ci.sh {{ARM_BENCH_HOST}}

# Run remote CI against an amd64 host you can SSH to:
#   just remote-ci-amd64 jrmediapyro-zt
remote-ci-amd64 HOST:
    ./tools/remote_ci.sh {{HOST}}

# Heavy CI: local fast checks + real-hardware remote CI (arm + amd64)
#   just heavyci jrmediapyro-zt
heavyci AMD64_HOST:
    just lint
    just cross-check-experimental
    just remote-ci-arm
    just remote-ci-amd64 {{AMD64_HOST}}

# Cross-check experimental stack
cross-check-experimental:
    @echo "Building experimental stack for x86_64-linux..."
    zig build -Dexperimental -Dtarget=x86_64-linux
    @echo "Building experimental stack for aarch64-linux..."
    zig build -Dexperimental -Dtarget=aarch64-linux
    @echo "Experimental targets compile successfully!"

# Run local CI in watch mode (requires act)
watch-ci:
    act --watch

# === Apache Parquet Testing Suite ===

# Test against apache/parquet-testing files (schema parsing)
test-parquet-testing:
    @./tools/just_helpers.sh test_parquet_testing schema

# Test parquet-testing with full scan (slower, more thorough)
test-parquet-testing-scan:
    @./tools/just_helpers.sh test_parquet_testing scan

# Validate zpq output against reference impl (duckdb or pyarrow)
validate *args:
    @./tools/just_helpers.sh validate_parquet {{args}}

# Validate all parquet-testing files (default: duckdb)
validate-all:
    @./tools/just_helpers.sh validate_parquet --all

# Validate with specific backend
validate-duckdb *args:
    @./tools/just_helpers.sh validate_parquet --backend duckdb {{args}}

validate-pyarrow *args:
    @./tools/just_helpers.sh validate_parquet --backend pyarrow {{args}}

# 3-way comparison: zpq vs duckdb vs pyarrow
validate-compare *args:
    @./tools/just_helpers.sh validate_parquet --compare {{args}}

# Run benchmarks on remote ARM64 host
remote-bench:
    ./tools/remote_bench.sh

# Build AWS Lambda Zip
build-lambda:
    zig build build-lambda
    cd zig-out/lambda && zip lambda_function.zip bootstrap

build-lambda-bench:
    zig build build-lambda-bench

bench-sweep:
    ./tools/bench_memory_sweep.sh
    @echo "Created zig-out/lambda/lambda_function.zip"
