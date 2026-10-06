#!/usr/bin/env bash
# Fetch Hardwood (hardwood-hq/hardwood), the strict-reader oracle for tools/triangulate.py, into the gitignored
# tools/hardwood/:
#   tools/hardwood/bin/hardwood   the CLI the harness picks up automatically
#   tools/hardwood/fixtures       Hardwood's own test fixtures (core/src/test/resources) at the same commit
#
# Usage:
#   tools/fetch_hardwood.sh                    pinned prebuilt native CLI + sparse fixture checkout (no JDK needed)
#   tools/fetch_hardwood.sh --from-source DIR  JVM launcher built from a Hardwood checkout (needs JDK 25+); fixtures
#                                              link to DIR's own, so this tracks whatever DIR has checked out
set -euo pipefail

repo=https://github.com/hardwood-hq/hardwood
# The pin. The `1.0-early-access` release asset is rebuilt on every push to Hardwood's main, so the digest is what
# pins it: when upstream refreshes the asset this script fails rather than silently swapping the oracle. To bump,
# take the new digests from the release page, and the commit `hardwood --version` reports. Pinned to the
# early-access build, which this harness is tested against.
tag=1.0-early-access
commit=5a60a1497b95f36cc30d6e74e25884d03c988904
pinned_sha256() {
  case "$1" in
    linux-x86_64) echo c9052895cdd3d2202d9e5adcc83e0a49048779449157228940e4435835c9a849 ;;
    linux-aarch64) echo 9484331a382c4a688f9382fe06dd071df19a3fc77926a63bb33153b3ad095021 ;;
    macos-x86_64) echo f5766b7619b9af2909e056682b1ea4cb395f57bb88e270f7b75de89502be212c ;;
    macos-aarch64) echo fbd85bc28c57070dc23c43a0ea6f8e3ec8fc4ee19ccc6fec23d750eae6083cad ;;
  esac
}

root="$(cd "$(dirname "$0")/.." && pwd)"
dest="$root/tools/hardwood"
mkdir -p "$dest/bin"

from_source() {
  local src
  src="$(cd "$1" && pwd)"
  [[ -x "$src/mvnw" && -d "$src/cli" ]] || { echo "not a Hardwood checkout: $src" >&2; exit 1; }
  # One reactor run so the sibling modules resolve to their freshly built jars without touching ~/.m2 installs.
  (cd "$src" && ./mvnw -q -B -DskipTests package dependency:build-classpath \
    -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=runtime -pl cli -am)
  local jar
  jar="$(ls "$src"/cli/target/hardwood-cli-*.jar | grep -v -e '-sources' -e '-javadoc' | head -1)"
  rm -f "$dest/bin/hardwood"  # may be a symlink into a prebuilt dist; never write through it
  cat > "$dest/bin/hardwood" <<EOF
#!/bin/sh
exec java --enable-native-access=ALL-UNNAMED \\
  -cp "$jar:\$(cat "$src/cli/target/classpath.txt")" dev.hardwood.cli.Main "\$@"
EOF
  chmod +x "$dest/bin/hardwood"
  ln -sfn "$src/core/src/test/resources" "$dest/fixtures"
}

prebuilt() {
  local os arch plat asset dir
  case "$(uname -s)" in Linux) os=linux ;; Darwin) os=macos ;; *) echo "unsupported OS" >&2; exit 1 ;; esac
  case "$(uname -m)" in
    x86_64|amd64) arch=x86_64 ;;
    aarch64|arm64) arch=aarch64 ;;
    *) echo "unsupported arch" >&2; exit 1 ;;
  esac
  plat="$os-$arch"
  asset="hardwood-cli-early-access-$plat.tar.gz"
  dir="$dest/dist/${commit:0:7}"
  if [[ ! -x "$dir/bin/hardwood" ]]; then
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    curl -fsSL -o "$tmp/$asset" "$repo/releases/download/$tag/$asset"
    local got
    if command -v sha256sum >/dev/null; then
      got="$(sha256sum "$tmp/$asset")"
    else
      got="$(shasum -a 256 "$tmp/$asset")"
    fi
    got="${got%% *}"
    if [[ "$got" != "$(pinned_sha256 "$plat")" ]]; then
      echo "Hardwood $asset digest is $got, pinned $(pinned_sha256 "$plat")." >&2
      echo "Upstream refreshed the early-access build; review it and bump the pin in $0." >&2
      exit 1
    fi
    mkdir -p "$dir"
    tar -xzf "$tmp/$asset" -C "$dir" --strip-components=1
  fi
  ln -sfn "$dir/bin/hardwood" "$dest/bin/hardwood"

  # Fixtures at the CLI's commit: a blobless, sparse, depth-1 checkout of core/src/test/resources only.
  local src="$dest/src"
  if [[ ! -d "$src/.git" ]]; then
    git init -q "$src"
    git -C "$src" remote add origin "$repo"
    git -C "$src" sparse-checkout set core/src/test/resources
  fi
  git -C "$src" fetch --quiet --depth 1 --filter=blob:none origin "$commit"
  git -C "$src" checkout --quiet --detach "$commit"
  ln -sfn "$src/core/src/test/resources" "$dest/fixtures"
}

case "${1:-}" in
  --from-source) [[ -n "${2:-}" ]] || { echo "usage: $0 --from-source DIR" >&2; exit 2; }; from_source "$2" ;;
  "") prebuilt ;;
  *) echo "usage: $0 [--from-source DIR]" >&2; exit 2 ;;
esac

"$dest/bin/hardwood" --version 2>/dev/null | tail -1
echo "fixtures: $(find -L "$dest/fixtures" -name '*.parquet' | wc -l) parquet files in $dest/fixtures"
