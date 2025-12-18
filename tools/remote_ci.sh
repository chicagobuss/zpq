#!/bin/bash
set -euo pipefail

# Remote CI runner: run zpq "real CI" on real Linux hosts over SSH.
#
# Usage:
#   tools/remote_ci.sh <ssh-host> [suite]
#
# suite:
#   - "fast" (default): check + unit tests + experimental http smoke
#   - "full": fast + test-io
#
# Notes:
# - This avoids Docker/emulation flakiness on macOS/ARM by running on real hardware.
# - It uploads the current git HEAD via `git archive` (tracked files only), so the remote
#   run is always clean and deterministic.

HOST="${1:-}"
SUITE="${2:-fast}"

if [[ -z "${HOST}" ]]; then
  echo "Usage: tools/remote_ci.sh <ssh-host> [fast|full]" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT_DIR}"

GIT_SHA="$(git rev-parse --short=12 HEAD)"

REMOTE_BASE="~/code/zpq-ci"
REMOTE_DIR="${REMOTE_BASE}/${GIT_SHA}"

echo "Remote CI:"
echo "  host:   ${HOST}"
echo "  sha:    ${GIT_SHA}"
echo "  suite:  ${SUITE}"

REMOTE_UNAME="$(ssh "${HOST}" "uname -m")"

case "${REMOTE_UNAME}" in
  x86_64|amd64)
    ZIG_ARCH="x86_64-linux"
    ;;
  aarch64|arm64)
    ZIG_ARCH="aarch64-linux"
    ;;
  *)
    echo "Unsupported remote arch from uname -m: ${REMOTE_UNAME}" >&2
    exit 2
    ;;
esac

echo "  remote arch: ${REMOTE_UNAME} -> ${ZIG_ARCH}"

echo "Fetching Zig URL..."
ZIG_URL="$(curl -s https://ziglang.org/download/index.json | jq -r ".master.\"${ZIG_ARCH}\".tarball")"
if [[ -z "${ZIG_URL}" || "${ZIG_URL}" == "null" ]]; then
  echo "Error: Failed to fetch Zig URL for ${ZIG_ARCH}" >&2
  exit 1
fi
echo "  Zig URL: ${ZIG_URL}"

echo "Uploading source (git archive)..."
ssh "${HOST}" "mkdir -p ${REMOTE_DIR}"
git archive --format=tar HEAD | ssh "${HOST}" "tar -xf - -C ${REMOTE_DIR}"

echo "Running remote suite..."
ssh "${HOST}" "bash -lc '
  set -euo pipefail

  cd ${REMOTE_DIR}

  # Install Zig if needed (pinned to master build for this session)
  export PATH=\"\$HOME/zig:\$PATH\"
  if ! command -v zig >/dev/null 2>&1; then
    echo \"Installing Zig...\"
    rm -rf \"\$HOME/zig\"
    mkdir -p \"\$HOME/zig\"
    cd \"\$HOME/zig\"
    curl -L \"${ZIG_URL}\" -o zig.tar.xz
    tar -xf zig.tar.xz --strip-components=1
    rm -f zig.tar.xz
    cd ${REMOTE_DIR}
  fi

  echo \"Zig version: \$(zig version)\"

  # Prefer the no-output timeout wrapper if python3 exists
  run() {
    if command -v python3 >/dev/null 2>&1; then
      python3 tools/no_output_timeout.py --idle-seconds 20 \"\$@\"
    else
      \"\$@\"
    fi
  }

  echo \"== zig build check ==\"
  run zig build check

  echo \"== zig build test ==\"
  run zig build test --summary all

  echo \"== zig build -Dexperimental test-http-client ==\"
  run zig build -Dexperimental test-http-client

  if [[ \"${SUITE}\" == \"full\" ]]; then
    echo \"== zig build test-io ==\"
    run zig build test-io
  fi

  echo \"Remote CI OK\"
'"

echo "Remote CI finished: ${HOST} (${ZIG_ARCH}) OK"


