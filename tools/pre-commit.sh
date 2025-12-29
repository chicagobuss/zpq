#!/bin/bash
set -e

echo "--- [Pre-Commit] Running Lint Check ---"
just lint

echo "--- [Pre-Commit] Running Unit Tests ---"
just test

echo "--- [Pre-Commit] All checks passed! ---"
