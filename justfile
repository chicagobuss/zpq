# ZPQ task runner. Stay slim: only recipes that work against the current
# source tree. Don't ship recipes that fail when invoked; they're noise in
# `just --list`.

# Ensure we use Zig 0.17.0 release. If it's not on the PATH but is installed
# under ~/.zvm/0.17.0, add that directory to PATH.
export PATH := `[ -d ~/.zvm/0.17.0 ] && echo "$HOME/.zvm/0.17.0:$PATH" || echo "$PATH"`

# Python for tooling (conformance / regression / smoke): zpq's in-project uv
# .venv if present (it has pyarrow — system python3 is 3.14, no wheel), else
# system python3. Set up with: uv venv --python 3.12 && uv pip install pyarrow
python := `[ -x .venv/bin/python ] && echo .venv/bin/python || echo python3`

# List available recipes
default:
    @just --list

# === Build ===

# Build (ReleaseFast) — produces zpq + zpq-lambda.
build:
    zig build -Doptimize=ReleaseFast

# Build with debug symbols.
build-debug:
    zig build

# Build the lean-core CLI — no SQL frontend (`-Dsql=false`), so liteparser
# and its C dep are excluded. "Build the binary you want." The Lambda binary
# never includes SQL regardless; this is the minimal *CLI*. CI builds this
# variant too, so the lean path can't bit-rot.
build-minimal:
    zig build -Dsql=false -Doptimize=ReleaseFast cli

# The shipped CLI as it ships (ReleaseFast, stripped) through the release cases; CI runs this on each native runner.
release-smoke:
    zig build -Doptimize=ReleaseFast -Dstrip=true --prefix zig-out/release cli
    zig build check-runner -Doptimize=ReleaseSafe
    zig-out/bin/zpq-check --cases tools/check/cases/release.json --zpq zig-out/release/bin/zpq --no-cache

# The Lambda deploy zips (musl static, ReleaseFast) for both archs, built by
# tools/serverless/aws.sh. ReleaseFast, not ReleaseSmall: on Lambda the small
# build started no faster and ran up to 2x slower cold, and the ~10 MB zip is
# far inside the 50 MB direct-upload limit.
lambda-build:
    ./tools/serverless/aws.sh build arm64
    ./tools/serverless/aws.sh build x86_64
    @echo "zig-out/lambda/zpq-lambda-{arm64,x86_64}.zip ready."

# === Setup ===

# One-shot dev-env bootstrap on a fresh machine: install the pinned Zig
# under ~/.zvm if it's missing, then warm the build.
bootstrap:
    #!/usr/bin/env bash
    set -euo pipefail
    ZV=0.17.0
    if ! ~/.zvm/$ZV/zig version >/dev/null 2>&1; then
      echo "installing zig $ZV -> ~/.zvm/$ZV"
      mkdir -p ~/.zvm
      curl -fsSL "https://ziglang.org/download/$ZV/zig-x86_64-linux-$ZV.tar.xz" | tar -xJ -C ~/.zvm
      mv ~/.zvm/zig-x86_64-linux-$ZV ~/.zvm/$ZV
    fi
    ~/.zvm/$ZV/zig version
    PATH="$HOME/.zvm/$ZV:$PATH" zig build --summary none
    echo "bootstrap done — add ~/.zvm/$ZV to PATH (or alias zig)"

# === Tests: the gates are build.zig steps (tools/check/README.md) ===
#
#   edit    `just test-core [filter]` (`zig build check -Dtier=edit`)  core unit tests, ReleaseSafe
#   commit  `just check`     (`zig build check -Dtier=commit`)  fixtures, core units, the runner's self-test and
#                            every commit-tier case against the cached DuckDB oracle
#   full    `just full`      (`zig build check -Dtier=full`)  Debug units, integration, every case uncached, the
#                            parquet-testing corpus, ClickBench, path parity, OS contracts, libzpq
#   fleet   `just fleet`     the full tier's shards over the fleet (tools/check/fleet.sh), one verdict
# Each stage leaves a log and a receipt in zig-out/receipts; a tier passes only on clean receipts for HEAD.

# The unit tests in Debug. Pass a filter substring to narrow, e.g. `just test decimal`.
test *args="":
    zig build test --summary all -- {{args}}

test-core filter="":
    zig build check -Dtier=edit {{ if filter != "" { "-Dtest-filter=" + filter } else { "" } }}

check:
    zig build check -Dtier=commit

full:
    zig build check -Dtier=full

fleet *args:
    tools/check/fleet.sh {{args}}

# apache/parquet-testing at tools/check/corpus.pin, and the generated fixtures (every tier runs this first).
fixtures:
    tools/check/fixtures.sh

# Lambda integration tests — spawns the binary against an in-process fake.
test-integration:
    zig build test-integration --summary all

# libzpq, the C ABI: its C test plain and under AddressSanitizer/LeakSanitizer (1,000 open/run/close cycles), the
# Python binding and service, and the differential corpus through libzpq, the CLI and the Lambda, byte for byte.
# Needs gcc (the sanitizer runtime) and duckdb+pyarrow in the .venv. Part of the full tier.
libzpq:
    PYTHON={{python}} OPTIMIZE=ReleaseFast tests/libzpq/run.sh

# Every apache/parquet-testing file against its declared outcome (tools/check/cases/corpus.json).
corpus:
    zig build check -Dtier=full -Dshard=corpus

# Idle-gap work (no gate waits on it): the encoded/materialized A/B aggregate fuzz against the same binary, then
# coverage-guided fuzzing. Commit any crasher as a fixture with a case.
idle seed="1234" queries="600":
    zig build -Doptimize=ReleaseSafe cli
    {{python}} tools/agg_ab_fuzz.py --zpq zig-out/bin/zpq --seed {{seed}} --queries {{queries}}

# Coverage-guided fuzzing on the pinned Zig toolchain; the fuzz-lite seeds also run in `test`.
fuzz filter="fuzz:":
    zig build fuzz -Dtest-filter={{filter}} --fuzz

# === Lambda lifecycle ===

# Deploy a Lambda function (function-name + arch). Loads .env for AWS creds.
lambda-deploy fn arch="arm64":
    @./tools/serverless/aws.sh deploy {{fn}} zig-out/lambda/zpq-lambda-{{arch}}.zip {{arch}}

# Invoke a Lambda function with a JSON payload.
lambda-invoke fn payload="{}":
    @./tools/serverless/aws.sh invoke {{fn}} '{{payload}}'

# Tail the most recent CloudWatch log stream for a function.
lambda-logs fn:
    @./tools/serverless/aws.sh logs {{fn}}

# === Profiling ===

# Record a perf trace of `zpq <args>` and render a flamegraph SVG.
# Open prof/flame-<ts>.svg in a browser to drill into hotspots.
#
# Args are forwarded to zpq verbatim with quoting preserved. Examples:
#   just flamegraph query data/benchmark_100mb.parquet --aggregate 'sum(int64_sorted) AS s'
#   just flamegraph query 's3://my-bucket/path/*.parquet' --filter 'cost > 100'
#
# Stack quality: build.zig sets `omit_frame_pointer = false` for both
# zpq modules (regression suite shows zero measurable cost), so
# `--call-graph=fp` walks frame pointers in O(1) instead of DWARF
# unwinding. Cleaner stacks, lower record overhead. Sub-100ms runs
# are still too short to sample usefully; aim for queries that take
# >500ms (multi-RG aggregates, multi-file globs).
#
# Requires: perf + references/FlameGraph (cloned).
[positional-arguments]
flamegraph *args:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p prof
    ts=$(date +%Y%m%d-%H%M%S)
    echo "[perf] recording prof/perf-$ts.data ..."
    perf record -F 4999 -g --call-graph=fp -e cycles:u -q -o "prof/perf-$ts.data" -- \
        ./zig-out/bin/zpq "$@" > /dev/null
    echo "[perf] rendering prof/flame-$ts.svg ..."
    perf script -i "prof/perf-$ts.data" 2>/dev/null \
        | references/FlameGraph/stackcollapse-perf.pl \
        | references/FlameGraph/flamegraph.pl --title "zpq $*" \
        > "prof/flame-$ts.svg"
    echo "wrote prof/flame-$ts.svg ($(wc -c < prof/flame-$ts.svg) bytes)"

# Stack-sample a long-running zpq via eBPF (no perf overhead, lower
# kernel cost). Use this for queries that take >1s — for quick runs
# `just flamegraph` is simpler. PID is auto-discovered from the
# zpq process; pass --duration to control sample window.
#
# Requires: bpfcc-tools (profile-bpfcc) + sudo.
flamegraph-bpf duration="5":
    @mkdir -p prof
    @ts=$(date +%Y%m%d-%H%M%S); \
        echo "start zpq in another shell, then this records {{duration}}s of stacks ..."; \
        sudo profile-bpfcc -F 99 -f -d {{duration}} \
            $(pgrep -n zpq && echo "-p $(pgrep -n zpq)" || echo "") \
            > prof/folded-$ts.txt; \
        references/FlameGraph/flamegraph.pl prof/folded-$ts.txt > prof/flame-bpf-$ts.svg; \
        echo "wrote prof/flame-bpf-$ts.svg"

# === Maintenance ===

# Wipe build outputs.
clean:
    rm -rf zig-out .zig-cache

# Fill vendor/boring_tls/prebuilt/ with verified BoringSSL prebuilts so later
# builds need no network (a normal build fetches them into .zig-cache itself).
fetch-deps:
    @./tools/r2-fetch-artifacts.sh

# OS-level contracts (publication, permissions, pipes, S3 uploads, sync and write faults); full tier's `os` shard. The
# loop-device and device-mapper contracts skip by name: on a dedicated host with passwordless sudo, add --privileged.
contracts:
    zig build -Doptimize=ReleaseFast
    zig build libzpq -Doptimize=ReleaseFast
    {{python}} tools/check/contracts.py zig-out/bin/zpq --lib zig-out/lib/libzpq.so
