#!/usr/bin/env bash
# Cross-build release artifacts; execute only targets native to this host.
# Foreign runtime checks belong to verify arch, with no host-wide emulation setup.
# Usage: tools/release_smoke.sh [--run-only] [OUT_DIR [TARGET...]]
set -euo pipefail
cd "$(dirname "$0")/.."
ZIG="${ZIG:-zig}"
build=true
if [ "${1:-}" = --run-only ]; then build=false; shift; fi
OUT="${1:-zig-out/release}"
if [ $# -gt 0 ]; then shift; fi
FIX="ci/fixtures/parquet/full_match.parquet"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
ran=()
elsewhere=()
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
    [[ "$v" == "zpq "* ]] || { echo "FAIL --version: '$v'" >&2; exit 1; }
    echo "  ok --version: $v"
    expect "aggregate" '{"n":1600,"s":12000,"m":"k001599-pppppppppppppppppppppppp"}' \
        "$("$bin" query "$FIX" --aggregate 'count(*) AS n, sum(b) AS s, max(s) AS m')"
    expect "sql" '{"x":28}' \
        "$("$bin" query --query "SELECT b * 2 AS x FROM '$FIX' WHERE b >= 14 ORDER BY x LIMIT 1")"
    "$bin" query "$FIX" --filter 'b >= 14' --columns b,s -o "$TMP/$t.parquet"
    expect "write, read back" '{"n":200,"s":2900}' \
        "$("$bin" query "$TMP/$t.parquet" --aggregate 'count(*) AS n, sum(b) AS s')"
    ran+=("$t")
}

if [ $# -eq 0 ]; then set -- x86_64-linux-musl aarch64-linux-musl x86_64-macos aarch64-macos; fi
for t in "$@"; do
    if "$build"; then
        echo "== build $t"
        "$ZIG" build -j8 -Dtarget="$t" -Doptimize=ReleaseFast -Dstrip=true --prefix "$OUT/$t" cli lambda
    fi
    if [ "$t" != "$native" ]; then
        elsewhere+=("$t: elsewhere: verify arch")
        continue
    fi
    smoke_cli "$t"
    if [[ "$t" == *-linux-musl ]]; then
        "$ZIG" build -j8 -Dtarget="$t" -Doptimize=ReleaseFast -Dstrip=true test-integration --summary failures
        ran+=("$t:lambda")
    fi
done

echo "release smoke summary"
for r in "${ran[@]+"${ran[@]}"}"; do echo "  RAN $r"; done
for r in "${elsewhere[@]+"${elsewhere[@]}"}"; do echo "  $r"; done
echo "release smoke: PASS (native checks; foreign checks: verify arch)"
