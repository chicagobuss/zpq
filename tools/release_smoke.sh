#!/usr/bin/env bash
# Cross-build release artifacts; execute only targets native to this host.
# Missing native checks block unless same-revision verify-arch evidence proves them.
# Usage: tools/release_smoke.sh [--run-only] [--arch-summary PATH] [--accept-deferred NAME] [OUT_DIR [TARGET...]]
set -euo pipefail
cd "$(dirname "$0")/.."
ZIG="${ZIG:-zig}"
build=true
summaries=()
accept_deferred=false
revision="${RELEASE_SMOKE_REVISION:-$(git rev-parse HEAD)}"
[[ "$revision" =~ ^[0-9a-f]{40}$ ]] || { echo 'full revision SHA required' >&2; exit 2; }
while [ $# -gt 0 ]; do
    case "$1" in
        --run-only) build=false; shift ;;
        --arch-summary) summaries+=(--summary "${2:?summary required}"); shift 2 ;;
        --accept-deferred)
            [ "${2:-}" = x86_64-macos ] || { echo 'unknown deferred target' >&2; exit 2; }
            accept_deferred=true; shift 2 ;;
        *) break ;;
    esac
done
OUT="${1:-zig-out/release}"
if [ $# -gt 0 ]; then shift; fi
FIX="ci/fixtures/parquet/full_match.parquet"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
ran=()
not_run=()
verified=()
blocked=0
case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) native=x86_64-linux-musl ;;
    Linux/aarch64) native=aarch64-linux-musl ;;
    Darwin/arm64) native=aarch64-macos ;;
    Darwin/x86_64) native=x86_64-macos ;;
    *) native= ;;
esac

expect() {
    local what="$1" want="$2" got="$3"
    if [[ "$got" != "$want" ]]; then
        echo "FAIL $what: want '$want', got '$got'" >&2
        exit 1
    fi
    echo "  ok $what"
}

smoke_cli() {
    local t="$1" bin="$OUT/$1/bin/zpq"
    echo "== run $t CLI"
    local v
    v="$("$bin" --version)"
    [[ "$v" == "zpq "* && "$v" == *"$revision"* ]] || { echo "FAIL --version: '$v'" >&2; exit 1; }
    echo "  ok --version: $v"
    v="$("$bin" query "$FIX" --aggregate 'count(*) AS n, sum(b) AS s, max(s) AS m')"
    expect "aggregate" '{"n":1600,"s":12000,"m":"k001599-pppppppppppppppppppppppp"}' \
        "$v"
    v="$("$bin" query --query "SELECT b * 2 AS x FROM '$FIX' WHERE b >= 14 ORDER BY x LIMIT 1")"
    expect "sql" '{"x":28}' \
        "$v"
    "$bin" query "$FIX" --filter 'b >= 14' --columns b,s -o "$TMP/$t.parquet"
    v="$("$bin" query "$TMP/$t.parquet" --aggregate 'count(*) AS n, sum(b) AS s')"
    expect "write, read back" '{"n":200,"s":2900}' \
        "$v"
    ran+=("$t")
}

if [ $# -eq 0 ]; then set -- x86_64-linux-musl aarch64-linux-musl x86_64-macos aarch64-macos; fi
for t in "$@"; do
    case "$t" in
        x86_64-linux-musl|aarch64-linux-musl|aarch64-macos) ;;
        x86_64-macos)
            if "$accept_deferred"; then
                not_run+=("x86_64-macos (deferred; accepted by name)")
            else
                not_run+=("x86_64-macos (deferred; acceptance by name required)")
                blocked=2
            fi
            continue ;;
        *) echo "unknown release target: $t" >&2; exit 2 ;;
    esac
    if "$build"; then
        echo "== build $t"
        "$ZIG" build -j8 -Dtarget="$t" -Doptimize=ReleaseFast -Dstrip=true \
            -Drevision="$revision" --prefix "$OUT/$t" cli lambda
    fi
    if [ "$t" != "$native" ]; then
        if python3 tools/verify/release_coverage.py --sha "$revision" --target "$t" \
            --bin-dir "$OUT/$t/bin" \
            "${summaries[@]+"${summaries[@]}"}"; then
            verified+=("$t (verify arch; same SHA)")
        else
            not_run+=("$t (native runtime evidence required)")
            blocked=2
        fi
        continue
    fi
    smoke_cli "$t"
    if [[ "$t" == *-linux-musl ]]; then
        mkdir -p zig-out
        if "$build"; then
            ZPQ_ZIG="$ZIG" python3 - "$OUT/$t/bin" "$t" <<'PY_INTEGRATION'
import sys
from pathlib import Path
sys.path.insert(0, "tools/verify")
import revision
dest = Path(sys.argv[1]).resolve()
revision.integration(Path.cwd(), dest, sys.argv[2], False, runtime=str(dest))
PY_INTEGRATION
        fi
        "$OUT/$t/bin/zpq-integration"
        ran+=("$t:lambda")
    fi
done

echo "release smoke summary"
for r in "${ran[@]+"${ran[@]}"}"; do echo "  RAN $r"; done
for r in "${verified[@]+"${verified[@]}"}"; do echo "  VERIFIED $r"; done
for r in "${not_run[@]+"${not_run[@]}"}"; do echo "  NOT RUN $r"; done
if [ "$blocked" -ne 0 ]; then echo "release smoke: BLOCKED"; exit 2; fi
echo "release smoke: PASS (named deferrals remain NOT RUN)"
