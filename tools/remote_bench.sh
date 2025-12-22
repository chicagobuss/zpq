#!/bin/bash
set -e

# Configuration
HOST="josh-oci-work-box-0"
REMOTE_DIR="~/code/zpq-work"
S3_PATH="s3://diat-bench-output-076397969038/manual_check/python/stream_disk_upload.parquet"
ITERATIONS=${2:-3}

# 1. Sync code
echo "Syncing code to remote..."
git push backup main

# 2. Sync credentials and tools
echo "Syncing credentials and tools..."
scp .env.staging.bot $HOST:$REMOTE_DIR/.env.staging.bot
scp tools/no_output_timeout.py $HOST:$REMOTE_DIR/tools/no_output_timeout.py

# 3. Fetch latest Zig URL locally
echo "Fetching latest Zig version info..."
ZIG_URL=$(curl -s https://ziglang.org/download/index.json | jq -r '.master."aarch64-linux".tarball')

if [ -z "$ZIG_URL" ] || [ "$ZIG_URL" == "null" ]; then
    echo "Error: Failed to fetch Zig URL"
    exit 1
fi

STEP=${1:-matrix}

# 4. Remote execution
ssh -t $HOST "
    export ZIG_URL='$ZIG_URL'
    export S3_PATH='$S3_PATH'
    export ITERATIONS='$ITERATIONS'
    export STEP='$STEP'
    set -e
    
    cd $REMOTE_DIR
    echo 'Updating repository...'
    git fetch origin && git reset --hard origin/main
    
    # Setup PATH
    export PATH=\"\$HOME/zig:\$HOME/.local/bin:\$PATH\"

    # Ensure Zig
    if ! command -v zig &> /dev/null; then
        echo 'Zig not found. Installing Zig...'
        mkdir -p ~/zig
        cd ~/zig
        curl -L \"\$ZIG_URL\" -o zig.tar.xz
        tar -xf zig.tar.xz --strip-components=1
        rm zig.tar.xz
        cd $REMOTE_DIR
    fi
    
    echo \"Zig version: \$(zig version)\"

    # Build artifacts
    echo 'Building all artifacts...'
    zig build

    run_bench() {
        local mode=\$1
        local extra_args=\$2
        echo \"--- Running ZPQ (\$mode) ---\"
        export \$(grep -v '^#' .env.staging.bot | xargs)
        python3 tools/no_output_timeout.py --idle-seconds 10 -- ./zig-out/bin/bench-e2e \"\$S3_PATH\" \"\$ITERATIONS\" \$extra_args
    }

    run_python() {
        echo \"--- Running Python Competitor ---\"
        export \$(grep -v '^#' .env.staging.bot | xargs)
        # Use uv run directly - it will handle deps and env
        uv run --with pyarrow --with boto3 python3 tools/bench_e2e/competitor.py \"\$S3_PATH\" \"\$ITERATIONS\"
    }

    case \"\$STEP\" in
        \"matrix\")
            run_bench \"Sync\" \"--sync\"
            run_bench \"Async Basic\" \"--dns=basic\"
            run_bench \"Async Fancy\" \"--async\"
            run_python
            ;;
        \"sync\")
            run_bench \"Sync\" \"--sync\"
            ;;
        \"async-basic\")
            run_bench \"Async Basic\" \"--dns=basic\"
            ;;
        \"async-fancy\")
            run_bench \"Async Fancy\" \"--async\"
            ;;
        \"python\")
            run_python
            ;;
        *)
            echo \"Unknown step: \$STEP\"
            echo \"Valid steps: matrix, sync, async-basic, async-fancy, python\"
            exit 1
            ;;
    esac
"
