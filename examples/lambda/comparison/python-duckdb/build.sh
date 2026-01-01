#!/bin/bash
# Build Python/DuckDB Lambda package
set -e

cd "$(dirname "$0")"

echo "Building Python/DuckDB Lambda package..."

# Create a clean build directory
rm -rf build dist
mkdir -p build

# Install dependencies into build dir
pip install -r requirements.txt -t build/ --platform manylinux2014_aarch64 --only-binary=:all: --python-version 3.12

# Copy handler
cp handler.py build/

# Create zip
cd build
zip -r ../lambda-duckdb-arm64.zip .
cd ..

echo "Created lambda-duckdb-arm64.zip ($(du -h lambda-duckdb-arm64.zip | cut -f1))"

# Also build x86_64 version
rm -rf build
mkdir -p build
pip install -r requirements.txt -t build/ --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.12
cp handler.py build/
cd build
zip -r ../lambda-duckdb-x86.zip .
cd ..

echo "Created lambda-duckdb-x86.zip ($(du -h lambda-duckdb-x86.zip | cut -f1))"

rm -rf build
