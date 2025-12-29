#!/usr/bin/env bash
# Upload pre-built BoringSSL artifacts to Cloudflare R2
#
# Usage: ./tools/r2-upload-artifacts.sh [target]
#   target: aarch64-linux, x86_64-linux, aarch64-macos, x86_64-macos
#           If not specified, uploads all available targets
#
# Requires: AWS CLI, .env with R2 credentials

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load environment
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo "Error: .env file not found. Copy .env.example to .env and configure R2 credentials."
    exit 1
fi

# Validate required vars
: "${R2_ACCOUNT_ID:?R2_ACCOUNT_ID not set}"
: "${R2_ACCESS_KEY_ID:?R2_ACCESS_KEY_ID not set}"
: "${R2_SECRET_ACCESS_KEY:?R2_SECRET_ACCESS_KEY not set}"
: "${R2_BUCKET:?R2_BUCKET not set}"

R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
PREBUILT_DIR="$PROJECT_ROOT/vendor/boring_tls/prebuilt"

upload_target() {
    local target="$1"
    local target_dir="$PREBUILT_DIR/$target"

    if [[ ! -d "$target_dir" ]]; then
        echo "Warning: No prebuilt artifacts found for $target"
        return 1
    fi

    echo "Uploading artifacts for $target..."

    for file in "$target_dir"/*.a; do
        if [[ -f "$file" ]]; then
            local filename=$(basename "$file")
            echo "  Uploading $filename..."
            AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" \
            AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" \
            aws s3 cp "$file" "s3://$R2_BUCKET/boring_tls/$target/$filename" \
                --endpoint-url "$R2_ENDPOINT"
        fi
    done

    echo "Done: $target"
}

list_remote() {
    echo "Current R2 contents:"
    AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" \
    aws s3 ls "s3://$R2_BUCKET/boring_tls/" --recursive \
        --endpoint-url "$R2_ENDPOINT"
}

# Main
if [[ $# -gt 0 ]]; then
    case "$1" in
        --list)
            list_remote
            ;;
        *)
            upload_target "$1"
            ;;
    esac
else
    # Upload all available targets
    echo "Uploading all available targets..."
    for target_dir in "$PREBUILT_DIR"/*/; do
        if [[ -d "$target_dir" ]]; then
            target=$(basename "$target_dir")
            upload_target "$target" || true
        fi
    done
    echo ""
    list_remote
fi
