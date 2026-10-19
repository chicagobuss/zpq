#!/usr/bin/env bash
# Build every release artifact and RUN it.
#
# A cross-build gate that only compiled let through a static-musl CLI and Lambda that crashed on every query (a weak
# extern resolved to address 0). So each release target (tools/package_release.py's four) is built exactly as it ships
# (ReleaseFast) and then executed:
#   - CLI: --version, an aggregate, SQL, and a filtered -o write read back.
#   - Lambda (Linux targets): the bootstrap against the fake runtime API of tests/lambda_integration.zig
#     (`zig build test-integration` built for the target: the same `zpq-lambda` artifact tools/serverless/aws.sh zips).
# Where the runs happen:
#   - x86_64-linux-musl: natively on an x86_64 Linux host.
#   - aarch64-linux-musl: natively on aarch64 Linux, else under qemu-aarch64 (Ubuntu: qemu-user; the Lambda run also
#     needs qemu-user-binfmt, as the harness spawns the bootstrap as a child; qemu-user-static elsewhere).
#   - aarch64-macos, x86_64-macos: on a Mac (x86_64 under Rosetta on Apple silicon). Never on Linux.
# A run this host cannot do FAILS the gate unless it is listed to skip (comma-separated targets, or `<target>:lambda`
# for the Lambda run alone) in --skip or RELEASE_SMOKE_SKIP, e.g. on Linux:
#   zig build release-smoke -Drelease-smoke-skip=aarch64-macos,x86_64-macos
#   RELEASE_SMOKE_SKIP=aarch64-macos,x86_64-macos tools/release_smoke.sh
# Every target is built whatever is skipped. Exit 1 on a failed build, a failed run, or an unlisted run not done.
#
# Usage: tools/release_smoke.sh [--skip LIST] [OUT_DIR]   (default zig-out/release; `zig build release-smoke` passes
# ZIG and, from -Drelease-smoke-skip, --skip)
set -euo pipefail
cd "$(dirname "$0")/.."
ZIG="${ZIG:-zig}"
skip_list="${RELEASE_SMOKE_SKIP:-}"
if [[ "${1:-}" == --skip ]]; then
    skip_list="${skip_list:+$skip_list,}${2:?--skip needs a list}"
    shift 2
fi
OUT="${1:-zig-out/release}"
FIX="ci/fixtures/parquet/full_match.parquet"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
SKIP=()
IFS=',' read -r -a SKIP <<< "$skip_list"
ran=()
skipped=()
missing=()
os="$(uname -s)"
host="$(uname -m)"

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

listed() {
    local name="$1" s
    for s in "${SKIP[@]+"${SKIP[@]}"}"; do
        [[ "$s" == "$name" ]] && return 0
    done
    return 1
}

# A run this host cannot do: skipped if listed (as the run or as its whole target), else a failure, with why.
cannot() {
    local run="$1" why="$2"
    if listed "$run" || listed "${run%%:*}"; then
        skipped+=("$run ($why)")
    else
        missing+=("$run ($why; list it in -Drelease-smoke-skip / RELEASE_SMOKE_SKIP to skip)")
    fi
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
    ran+=("$t${runner:+ (under $runner)}")
}

# The Lambda bootstrap of target `t` against the fake runtime API; extra arguments go to `zig build` (`-fqemu` runs a
# foreign test binary under qemu, whose bootstrap child binfmt hands to qemu in turn).
smoke_lambda() {
    local t="$1"
    shift
    echo "== run $t Lambda bootstrap (fake runtime API)${1:+ $*}"
    "$ZIG" build -Dtarget="$t" -Doptimize=ReleaseFast "$@" test-integration --summary failures
    echo "  ok test-integration"
    ran+=("$t:lambda${1:+ ($*)}")
}

for t in x86_64-linux-musl aarch64-linux-musl x86_64-macos aarch64-macos; do build "$t"; done

t=x86_64-linux-musl
if [[ "$os" == Linux && "$host" == x86_64 ]]; then
    smoke_cli "$t" ""
    smoke_lambda "$t"
else
    cannot "$t" "not an x86_64 Linux host"
fi

t=aarch64-linux-musl
qemu="$(command -v qemu-aarch64 || command -v qemu-aarch64-static || true)"
if [[ "$os" == Linux && "$host" == aarch64 ]]; then
    smoke_cli "$t" ""
    smoke_lambda "$t"
elif [[ "$os" == Linux && -n "$qemu" ]]; then
    smoke_cli "$t" "$qemu"
    if [[ -r /proc/sys/fs/binfmt_misc/qemu-aarch64 ]] && grep -q '^enabled' /proc/sys/fs/binfmt_misc/qemu-aarch64; then
        smoke_lambda "$t" -fqemu
    else
        cannot "$t:lambda" "no binfmt for aarch64 (install qemu-user-binfmt)"
    fi
else
    cannot "$t" "no qemu-aarch64 (install qemu-user and qemu-user-binfmt, or qemu-user-static)"
fi

t=aarch64-macos
if [[ "$os" == Darwin && "$host" == arm64 ]]; then
    smoke_cli "$t" ""
else
    cannot "$t" "runs on an Apple silicon Mac (the grantedmbp run)"
fi

t=x86_64-macos
if [[ "$os" == Darwin && "$host" == x86_64 ]]; then
    smoke_cli "$t" ""
elif [[ "$os" == Darwin ]] && arch -x86_64 /usr/bin/true 2>/dev/null; then
    smoke_cli "$t" "arch -x86_64"
else
    cannot "$t" "runs on a Mac (Intel, or Apple silicon with Rosetta)"
fi

echo
echo "release smoke summary (built: x86_64-linux-musl aarch64-linux-musl x86_64-macos aarch64-macos)"
for r in "${ran[@]+"${ran[@]}"}"; do echo "  RAN     $r"; done
for r in "${skipped[@]+"${skipped[@]}"}"; do echo "  SKIPPED $r"; done
for r in "${missing[@]+"${missing[@]}"}"; do echo "  NOT RUN $r"; done
if (( ${#missing[@]} > 0 )); then
    echo "release smoke: FAIL (runs not done and not listed to skip)"
    exit 1
fi
echo "release smoke: PASS"
