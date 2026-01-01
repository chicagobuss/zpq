#!/bin/bash
# Build Rust/Arrow Lambda package using Docker for cross-compilation
set -e

cd "$(dirname "$0")"

# Build using Docker for reliable cross-compilation
echo "Building Rust/Arrow Lambda for ARM64 using Docker..."

docker run --rm \
  -v "$(pwd)":/code \
  -v cargo-cache-nightly:/root/.cargo/registry \
  -w /code \
  --platform linux/arm64 \
  rust:1.75-slim-bookworm \
  bash -c "
    apt-get update && apt-get install -y pkg-config libssl-dev zip
    cargo build --release
    cp target/release/rust-arrow-filter bootstrap
    zip lambda-rust-arm64.zip bootstrap
    rm bootstrap
  "

echo "Created lambda-rust-arm64.zip ($(du -h lambda-rust-arm64.zip | cut -f1))"

echo "Building Rust/Arrow Lambda for x86_64 using Docker..."

docker run --rm \
  -v "$(pwd)":/code \
  -v cargo-cache-nightly:/root/.cargo/registry \
  -w /code \
  --platform linux/amd64 \
  rust:1.75-slim-bookworm \
  bash -c "
    apt-get update && apt-get install -y pkg-config libssl-dev zip
    cargo build --release
    cp target/release/rust-arrow-filter bootstrap
    zip lambda-rust-x86.zip bootstrap
    rm bootstrap
  "

echo "Created lambda-rust-x86.zip ($(du -h lambda-rust-x86.zip | cut -f1))"
