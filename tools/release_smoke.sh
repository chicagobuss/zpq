#!/usr/bin/env bash
# Build every release artifact and RUN it, where this host can.
#
# A cross-build gate that only compiled let through a static-musl CLI and Lambda that crashed on every query (a weak
# extern resolved to address 0). So each target here is built exactly as it ships (ReleaseFast) and then executed:
#   - x86_64-linux-musl: the CLI natively (--version, an aggregate, SQL, a filtered -o write read back), then the
#     Lambda bootstrap against the fake runtime API of tests/lambda_integration.zig (`zig build test-integration`
#     built for the target: the same `zpq-lambda` artifact tools/serverless/aws.sh zips as `bootstrap`).
#   - aarch64-linux-musl: the same under qemu-aarch64 when installed (Ubuntu: qemu-user, plus qemu-user-binfmt so the
#     Lambda bootstrap the integration test spawns runs too); otherwise reported as NOT RUN, with what to install.
#   - aarch64-macos: built only. It runs on a Mac (the grantedmbp run), never here.
# Exit 1 on any failed build or run; a target this host cannot run is listed at the end, not a failure.
#
# Usage: tools/release_smoke.sh [OUT_DIR]   (default zig-out/release; `zig build release-smoke` passes ZIG)
set -euo pipefail
cd "$(dirname "$0")/.."
ZIG="${ZIG:-zig}"
OUT="${1:-zig-out/release}"
FIX="ci/fixtures/parquet/full_match.parquet"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
not_run=()

build() {
    local t="$1"
    echo "== build $t"
    "$ZIG" build -Dtarget="$t" -Doptimize=ReleaseFast --prefix "$OUT/$t" cli lambda
}

expect() {
    local what="$1" want="$2" got="$3"
    if [[ "$got" != "$want" ]]; then
        echo "FAIL $what: want '$want', got '$got'" >&2
        exit 1
    fi
    echo "  ok $what"
}

# The CLI of target `t`, run through `runner` (empty for native).
smoke_cli() {
    local t="$1" runner="$2" bin="$OUT/$1/bin/zpq"
    echo "== run $t CLI${runner:+ under $runner}"
    local v
    v="$($runner "$bin" --version)"
    [[ "$v" == "zpq "* ]] || { echo "FAIL --version: '$v'" >&2; exit 1; }
    echo "  ok --version: $v"
    expect "aggregate" '{"n":1600,"s":12000,"m":"k001599-pppppppppppppppppppppppp"}' \
        "$($runner "$bin" query "$FIX" --aggregate 'count(*) AS n, sum(b) AS s, max(s) AS m')"
    expect "sql" '{"x":28}' \
        "$($runner "$bin" query --query "SELECT b * 2 AS x FROM '$FIX' WHERE b >= 14 ORDER BY x LIMIT 1")"
    $runner "$bin" query "$FIX" --filter 'b >= 14' --columns b,s -o "$TMP/$t.parquet"
    expect "write, read back" '{"n":200,"s":2900}' \
        "$($runner "$bin" query "$TMP/$t.parquet" --aggregate 'count(*) AS n, sum(b) AS s')"
}

# The Lambda bootstrap of target `t` against the fake runtime API; extra arguments go to `zig build` (`-fqemu` runs a
# foreign test binary under qemu, whose bootstrap child binfmt hands to qemu in turn).
smoke_lambda() {
    local t="$1"
    shift
    echo "== run $t Lambda bootstrap (fake runtime API)${1:+ $*}"
    "$ZIG" build -Dtarget="$t" -Doptimize=ReleaseFast "$@" test-integration --summary failures
    echo "  ok test-integration"
}

host="$(uname -m)"
[[ "$(uname -s)" == Darwin ]] && not_run+=("linux-musl targets: built only on a Mac")

t=x86_64-linux-musl
build "$t"
if [[ "$(uname -s)" == Linux && "$host" == x86_64 ]]; then
    smoke_cli "$t" ""
    smoke_lambda "$t"
else
    not_run+=("$t: not an x86_64 host")
fi

t=aarch64-linux-musl
build "$t"
if [[ "$(uname -s)" == Linux && "$host" == aarch64 ]]; then
    smoke_cli "$t" ""
    smoke_lambda "$t"
elif [[ "$(uname -s)" == Linux ]] && qemu="$(command -v qemu-aarch64 || command -v qemu-aarch64-static)"; then
    smoke_cli "$t" "$qemu"
    if [[ -r /proc/sys/fs/binfmt_misc/qemu-aarch64 ]] && grep -q '^enabled' /proc/sys/fs/binfmt_misc/qemu-aarch64; then
        smoke_lambda "$t" -fqemu
    else
        not_run+=("$t Lambda bootstrap: no binfmt for aarch64 to run the child the harness spawns (install qemu-user-binfmt)")
    fi
else
    not_run+=("$t CLI and Lambda: no qemu-aarch64 (install qemu-user and qemu-user-binfmt; qemu-user-static elsewhere)")
fi

t=aarch64-macos
build "$t"
if [[ "$(uname -s)" == Darwin && "$host" == arm64 ]]; then
    smoke_cli "$t" ""
else
    not_run+=("$t: built only; run tools/release_smoke.sh on a Mac (the grantedmbp run)")
fi

echo "release smoke: PASS"
for n in "${not_run[@]}"; do echo "  NOT RUN $n"; done
