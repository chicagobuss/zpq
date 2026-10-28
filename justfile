# zpq task runner. Every recipe here must work on the current tree.

# Zig 0.17.0: prefer ~/.zvm/0.17.0 (where `just bootstrap` installs it) when present.
export PATH := `[ -d ~/.zvm/0.17.0 ] && echo "$HOME/.zvm/0.17.0:$PATH" || echo "$PATH"`

# Python for the tooling: the project .venv if present, else python3. The fixtures stage creates .venv from
# tools/check/requirements.txt on first use.
python := `[ -x .venv/bin/python ] && echo .venv/bin/python || echo python3`

# List available recipes
default:
    @just --list

# === Build ===

# ReleaseFast zpq and zpq-lambda.
build:
    zig build -Doptimize=ReleaseFast

# Debug build.
build-debug:
    zig build

# The CLI without the SQL frontend (`-Dsql=false`, no liteparser). CI builds it so the lean path keeps working.
build-minimal:
    zig build -Dsql=false -Doptimize=ReleaseFast cli

# The CLI as it ships (ReleaseFast, stripped) through the release cases; CI runs it on each native runner.
release-smoke:
    zig build -Doptimize=ReleaseFast -Dstrip=true --prefix zig-out/release cli
    zig build check-runner -Doptimize=ReleaseSafe
    zig-out/bin/zpq-check --cases tools/check/cases/release.json --zpq zig-out/release/bin/zpq --no-cache

# Lambda deploy zips (static musl, ReleaseFast) for arm64 and x86_64. ReleaseSmall started no faster on Lambda
# and ran up to 2x slower cold; the ~10 MB zip is well under the 50 MB upload limit.
[doc("Lambda deploy zips (static musl, ReleaseFast) for arm64 and x86_64.")]
lambda-build:
    ./tools/serverless/aws.sh build arm64
    ./tools/serverless/aws.sh build x86_64
    @echo "zig-out/lambda/zpq-lambda-{arm64,x86_64}.zip ready."

# === Setup ===

# Fresh Linux x86_64 machine: install Zig 0.17.0 under ~/.zvm if missing, then build once.
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

# Unit tests in Debug; `just test decimal` narrows by substring.
test *args="":
    zig build test --summary all -- {{args}}

# Edit tier: core unit tests in ReleaseSafe; `just test-core decimal` narrows.
test-core filter="":
    zig build check -Dtier=edit {{ if filter != "" { "-Dtest-filter=" + filter } else { "" } }}

# Commit tier: run before every commit.
check:
    zig build check -Dtier=commit

# Full tier: run before a release.
full:
    zig build check -Dtier=full

# The architecture check (and with --full, the full tier) across the hosts in your fleet inventory.
fleet *args:
    tools/check/fleet.sh {{args}}

# apache/parquet-testing at tools/check/corpus.pin, and the generated fixtures (every tier runs this first).
fixtures:
    tools/check/fixtures.sh

# Lambda integration tests: the binary against an in-process fake runtime.
test-integration:
    zig build test-integration --summary all

# libzpq, the C ABI: its C test plain and under AddressSanitizer/LeakSanitizer (1,000 open/run/close cycles), the
# Python binding and service, and the differential corpus through libzpq, the CLI and the Lambda, byte for byte.
# Needs gcc (the sanitizer runtime) and duckdb+pyarrow in the .venv. Part of the full tier.
[doc("libzpq, the C ABI: C test plain and under ASan/LSan, the Python binding, and the differential corpus.")]
libzpq:
    PYTHON={{python}} OPTIMIZE=ReleaseFast tests/libzpq/run.sh

# Every apache/parquet-testing file against its declared outcome (tools/check/cases/corpus.json).
corpus:
    zig build check -Dtier=full -Dshard=corpus

# Encoded-vs-materialized aggregate A/B fuzz (no gate runs it). Commit any crasher as a fixture with a case.
idle seed="1234" queries="600":
    zig build -Doptimize=ReleaseSafe cli
    {{python}} tools/agg_ab_fuzz.py --zpq zig-out/bin/zpq --seed {{seed}} --queries {{queries}}

# Coverage-guided fuzzing; the seed corpus also runs in `test`.
fuzz filter="fuzz:":
    zig build fuzz -Dtest-filter={{filter}} --fuzz

# === Lambda lifecycle ===

# Deploy `zpq-lambda` as function FN (arch arm64 or x86_64). AWS credentials come from .env.
lambda-deploy fn arch="arm64":
    @./tools/serverless/aws.sh deploy {{fn}} zig-out/lambda/zpq-lambda-{{arch}}.zip {{arch}}

# Invoke function FN with a JSON payload.
lambda-invoke fn payload="{}":
    @./tools/serverless/aws.sh invoke {{fn}} '{{payload}}'

# Show the latest CloudWatch log stream for function FN.
lambda-logs fn:
    @./tools/serverless/aws.sh logs {{fn}}

# === Profiling ===

# perf-record `zpq <args>` (frame-pointer stacks) and write prof/flame-<ts>.svg. Needs perf and
# references/FlameGraph. Aim for runs over 500 ms; loop or glob shorter ones. Example:
#   just flamegraph query data/benchmark_100mb.parquet --aggregate 'sum(int64_sorted) AS s'
[doc("perf-record `zpq <args>` and write prof/flame-<ts>.svg.")]
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

# Sample a running zpq with eBPF for DURATION seconds (default 5) and write prof/flame-bpf-<ts>.svg. For runs over
# a second; start zpq first. Needs bpfcc-tools (profile-bpfcc) and sudo.
[doc("Sample a running zpq with eBPF for DURATION seconds and write prof/flame-bpf-<ts>.svg.")]
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

# Remove build outputs.
clean:
    rm -rf zig-out .zig-cache

# Fetch the verified BoringSSL prebuilts into vendor/boring_tls/prebuilt/ for offline builds.
fetch-deps:
    @./tools/r2-fetch-artifacts.sh

# OS contracts (publication, permissions, pipes, S3 uploads, sync and write faults): the full tier's `os` shard.
# Loop-device and device-mapper contracts skip unless run with --privileged on a host with passwordless sudo.
[doc("OS contracts: the full tier's `os` shard (--privileged for loop-device and device-mapper ones).")]
contracts:
    zig build -Doptimize=ReleaseFast
    zig build libzpq -Doptimize=ReleaseFast
    {{python}} tools/check/contracts.py zig-out/bin/zpq --lib zig-out/lib/libzpq.so
