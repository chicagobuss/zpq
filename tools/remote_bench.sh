#!/bin/bash
set -e

# Sync changes to the remote backup repo
echo "Syncing to remote..."
git push backup main

# Fetch latest Zig URL locally
echo "Fetching latest Zig version info..."
ZIG_URL=$(curl -s https://ziglang.org/download/index.json | jq -r '.master."aarch64-linux".tarball')

if [ -z "$ZIG_URL" ] || [ "$ZIG_URL" == "null" ]; then
    echo "Error: Failed to fetch Zig URL"
    exit 1
fi

echo "Latest Zig URL: $ZIG_URL"

STEP=${1:-test-s3-head}

# Remote execution
ssh oci-josh-arm-vm "
    export STEP='$STEP'
    export ZIG_URL='$ZIG_URL'
    set -e
    mkdir -p ~/code/zpq-work
    
    # Ensure we have a clean checkout
    if [ ! -d ~/code/zpq-work/.git ]; then
        echo 'Cloning repository...'
        git clone ~/code/zpq.git ~/code/zpq-work
    else
        echo 'Updating repository...'
        cd ~/code/zpq-work && git fetch origin && git reset --hard origin/main
    fi
    cd ~/code/zpq-work
    
    # Setup PATH to include ~/zig if it exists
    export PATH=\"\$HOME/zig:\$PATH\"

    # Check Zig
    if ! command -v zig &> /dev/null; then
        echo 'Zig not found. Installing Zig...'
        mkdir -p ~/zig
        cd ~/zig
        # Use the URL passed from local
        curl -L \"\$ZIG_URL\" -o zig.tar.xz
        tar -xf zig.tar.xz --strip-components=1
        rm zig.tar.xz
        cd ~/code/zpq-work
    fi
    
    echo 'Zig version:'
    zig version
    
    echo \"Cleaning zig-cache to ensure fresh build...\"
    rm -rf .zig-cache zig-out

    echo \"Running \$STEP...\"
    zig build -Dexperimental \"\$STEP\"
"
