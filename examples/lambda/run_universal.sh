#!/usr/bin/env bash
set -euo pipefail

# examples/lambda/run_universal.sh
# Runs the Lambda example locally, using Docker on Mac/Windows and native execution on Linux.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BOOTSTRAP_PATH="$PROJECT_ROOT/zig-out/lambda/bootstrap"

# Detect platform
OS=$(uname -s)
ARCH=$(uname -m)

if [[ ! -f "$BOOTSTRAP_PATH" ]]; then
    echo "Bootstrap binary not found at $BOOTSTRAP_PATH"
    echo "Running build..."
    cd "$PROJECT_ROOT"
    zig build lambda
fi

if [[ "$OS" == "Linux" ]]; then
    echo "Running natively on Linux..."
    
    # We need to emulate the Lambda Runtime API env vars
    export AWS_LAMBDA_RUNTIME_API="127.0.0.1:8080"
    
    # Start a mock RIE in the background if not running
    # (Implementation detail: for now we just warn that RIE is needed)
    echo "NOTE: Native Linux execution requires a local Runtime API emulator listening on port 8080."
    echo "This script assumes you have one running or will adapt the bootstrap to run standalone."
    
    "$BOOTSTRAP_PATH"
else
    echo "Running via Docker (cross-platform)..."
    
    # Use the official AWS Lambda adapter for "provided.al2"
    # We mount the bootstrap into /var/runtime
    
    docker run --rm -v "$BOOTSTRAP_PATH":/var/runtime/bootstrap \
        --entrypoint /var/runtime/bootstrap \
        -p 9000:8080 \
        -e AWS_LAMBDA_FUNCTION_MEMORY_SIZE=128 \
        public.ecr.aws/lambda/provided:al2023 hello
        
    # Note: 'hello' is the handler name (ignored by custom bootstrap usually)
    # The container will start and listen on port 9000.
    # You can then curl it: curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
fi

