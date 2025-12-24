#!/usr/bin/env bash
# tools/just_helpers.sh
# Universal helper library for justfile recipes.
# Designed to be portable (Bash 3.x+), fail-open on errors, and robust.

set -uo pipefail

# Configuration
DEPS_TAG="deps-v0.2"
GH_OWNER="chicagobuss"
GH_REPO="zpq"

# --- Logging ---
info() { echo -e "\033[0;34m[info]\033[0m $*"; }
warn() { echo -e "\033[0;33m[warn]\033[0m $*"; }
error() { echo -e "\033[0;31m[error]\033[0m $*" >&2; }
success() { echo -e "\033[0;32m[ok]\033[0m $*"; }

# --- Platform Detection ---
# Returns a triple like "aarch64-linux" or "x86_64-macos"
detect_triple() {
    local arch=$(uname -m)
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')

    # Normalize arch
    case "$arch" in
        arm64|aarch64) arch="aarch64" ;;
        x86_64|amd64) arch="x86_64" ;;
        *)
            warn "Unknown architecture: $arch. Assuming native build."
            echo "unknown-unknown"
            return
            ;;
    esac

    # Normalize OS
    case "$os" in
        darwin) os="macos" ;;
        linux) os="linux" ;;
        *)
            warn "Unknown OS: $os. Assuming native build."
            echo "unknown-unknown"
            return
            ;;
    esac

    echo "${arch}-${os}"
}

# --- Fetch Pre-built Dependencies ---
# Optimistically attempts to download static libraries.
# If fails, it warns but exits 0 so the build can proceed from source.
fetch_deps() {
    local triple=$(detect_triple)
    if [[ "$triple" == "unknown-unknown" ]]; then
        exit 0
    fi

    local prebuilt_dir="vendor/boring_tls/prebuilt/$triple"
    local libcrypto="$prebuilt_dir/libcrypto.a"
    local libssl="$prebuilt_dir/libssl.a"

    # 1. Check if files already exist
    if [[ -f "$libcrypto" && -f "$libssl" ]]; then
        # Check if they are valid archives (basic sanity check)
        if grep -q "!<arch>" "$libcrypto" 2>/dev/null; then
            # success "Pre-built dependencies found for $triple"
            exit 0
        else
            warn "Found corrupt pre-built libs. Re-fetching..."
            rm -f "$libcrypto" "$libssl"
        fi
    fi

    # 2. Prepare directory
    mkdir -p "$prebuilt_dir"

    info "Fetching pre-built BoringSSL for $triple..."

    # 3. Attempt Download (Fail-Open)
    # We use curl with --fail to detect 404s/auth errors
    local base_url="https://github.com/${GH_OWNER}/${GH_REPO}/releases/download/${DEPS_TAG}"
    
    if curl -fL --connect-timeout 5 --max-time 60 "$base_url/libcrypto-$triple.a" -o "$libcrypto" 2>/dev/null; then
        success "Downloaded libcrypto.a"
    else
        warn "Failed to download libcrypto.a (Network/Auth/Missing). Build will be slower."
        rm -f "$libcrypto" # Clean up partial/error file
        exit 0
    fi

    if curl -fL --connect-timeout 5 --max-time 60 "$base_url/libssl-$triple.a" -o "$libssl" 2>/dev/null; then
        success "Downloaded libssl.a"
    else
        warn "Failed to download libssl.a. Build will be slower."
        rm -f "$libssl"
        exit 0
    fi

    success "Deps ready."
}

# --- Dispatcher ---
# Allows calling functions by name: ./tools/just_helpers.sh fetch_deps
cmd="${1:-}"
shift || true

case "$cmd" in
    fetch_deps) fetch_deps "$@" ;;
    detect_triple) detect_triple "$@" ;;
    *)
        error "Unknown command: $cmd"
        exit 1
        ;;
esac

