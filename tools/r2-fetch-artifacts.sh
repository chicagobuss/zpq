#!/usr/bin/env bash
# Fetch pre-built BoringSSL artifacts from Cloudflare R2
#
# Usage: ./tools/r2-fetch-artifacts.sh [target]
#   target: aarch64-linux, x86_64-linux, aarch64-macos, x86_64-macos
#           If not specified, fetches for current platform
#
# No credentials required - uses public R2 URL

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Public R2 URL (no auth needed)
R2_PUBLIC_URL="https://pub-4d2e7e2925bb43dc9d3c0323d6d61a84.r2.dev"
PREBUILT_DIR="$PROJECT_ROOT/vendor/boring_tls/prebuilt"

detect_target() {
    local arch=$(uname -m)
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')

    case "$arch" in
        x86_64|amd64) arch="x86_64" ;;
        arm64|aarch64) arch="aarch64" ;;
        *) echo "Unknown architecture: $arch" >&2; exit 1 ;;
    esac

    case "$os" in
        linux) os="linux" ;;
        darwin) os="macos" ;;
        *) echo "Unknown OS: $os" >&2; exit 1 ;;
    esac

    echo "${arch}-${os}"
}

fetch_target() {
    local target="$1"
    local target_dir="$PREBUILT_DIR/$target"

    echo "Fetching artifacts for $target..."
    mkdir -p "$target_dir"

    local files=("libcrypto.a" "libssl.a")

    for filename in "${files[@]}"; do
        local url="$R2_PUBLIC_URL/boring_tls/$target/$filename"
        local dest="$target_dir/$filename"

        echo "  Downloading $filename..."
        if curl -fSL "$url" -o "$dest" 2>/dev/null; then
            local size=$(ls -lh "$dest" | awk '{print $5}')
            echo "    OK ($size)"
        else
            echo "    FAILED (artifact may not exist for this target)"
            rm -f "$dest"
        fi
    done

    # Verify we got both files
    if [[ -f "$target_dir/libcrypto.a" && -f "$target_dir/libssl.a" ]]; then
        echo "Done: $target"
        return 0
    else
        echo "Warning: Incomplete artifacts for $target"
        return 1
    fi
}

# Main
if [[ $# -gt 0 ]]; then
    target="$1"
else
    target=$(detect_target)
    echo "Detected target: $target"
fi

fetch_target "$target"
