#!/bin/bash
set -euo pipefail

# Kill any existing mock server processes
pkill -f mock_s3_server || true

# Build the server
echo "Building mock server..."
zig build-exe tools/mock_s3_server.zig

# Run the server in the background, redirecting output to a log file
echo "Starting mock server..."
nohup ./mock_s3_server > mock_server.log 2>&1 &
echo "Mock server started with PID $! Logs in mock_server.log"

# Give it a moment to start up
sleep 1
