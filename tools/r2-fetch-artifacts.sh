#!/usr/bin/env bash
# Fill vendor/boring_tls/prebuilt/<target>/ with the pinned BoringSSL
# libraries, for builds that must not touch the network. A normal build
# doesn't need this: build.zig fetches and verifies missing libraries itself.
#
# Usage: ./tools/r2-fetch-artifacts.sh [target]
#   target: aarch64-linux, x86_64-linux, aarch64-macos, x86_64-macos
#           If not specified, fetches for current platform
#
# URLs and sha256 digests come from vendor/boring_tls/prebuilt.sha256. Each
# file is downloaded beside its destination, verified, then renamed into
# place, so an interrupted or mismatched download never leaves a library the
# build would take for a good one.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$PROJECT_ROOT/vendor/boring_tls/prebuilt.sha256"
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

sha256_of() {
    if command -v sha256sum > /dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1
}

fetch_target() {
    local target="$1"
    local target_dir="$PREBUILT_DIR/$target"

    echo "Fetching artifacts for $target..."
    mkdir -p "$target_dir"

    for filename in libcrypto.a libssl.a; do
        local want url
        read -r want url < <(awk -v k="$target/$filename" '$1 == k { print $2, $3 }' "$MANIFEST") || true
        if [[ -z "${url:-}" ]]; then
            echo "  no pinned $filename for $target in $MANIFEST" >&2
            return 1
        fi
        local dest="$target_dir/$filename"
        local tmp="$dest.partial.$$"

        echo "  Downloading $filename..."
        if ! curl -fsSL "$url" -o "$tmp"; then
            rm -f "$tmp"
            echo "    FAILED: $url" >&2
            return 1
        fi
        local got
        got=$(sha256_of "$tmp")
        if [[ "$got" != "$want" ]]; then
            rm -f "$tmp"
            echo "    sha256 mismatch: pinned $want, served $got" >&2
            return 1
        fi
        mv -f "$tmp" "$dest"
        echo "    OK ($(ls -lh "$dest" | awk '{print $5}'), sha256 verified)"
    done
    echo "Done: $target"
}

# Main
if [[ $# -gt 0 ]]; then
    target="$1"
else
    target=$(detect_target)
    echo "Detected target: $target"
fi

fetch_target "$target"
