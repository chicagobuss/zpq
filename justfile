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

# Build releases and run native CLI/Lambda; foreign checks: verify arch.
release-smoke:
    zig build release-smoke

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

# === Tests (three tiers, fastest first) ===
#
#   Tier 1  `just test`      inner loop — pure-Zig, no external deps,
#                            safety-checked (Debug). Known-output unit tests
#                            + the fuzz-lite PRNG loop. Seconds. Run as you go.
#   Tier 2  `just check`     pre-commit / per-PR — Tier 1 + lambda integration
#                            + cross-impl smoke (ZPQ writes → pyarrow reads).
#                            Needs pyarrow. ~1 min. Mirrors CI.
#   Tier 3  `just gauntlet`  occasional / pre-release — fetches the
#                            apache/parquet-testing corpus into
#                            data/parquet-testing so the fixture-gated decode
#                            tests run against real Spark/foreign-writer files
#                            (cross-impl decode validation), then the full
#                            suite + conformance + smoke.
#
# Cross-impl validation lives in Tiers 2-3 by design (Tier-2 doctrine:
# "synthetic fixtures lie"). Tier 1 stays dependency-free for speed; it
# checks our work against hand-known outputs, the gauntlet checks it against
# the rest of the ecosystem.

# Tier 1 — inner loop. Pass a filter substring to narrow, e.g. `just test decimal`.
test *args="":
    zig build test --summary all -- {{args}}

# Tier 2 — run before every commit.
check: test-modules test test-integration smoke duckdb-smoke differential write-layout-check write-failure-check private-decode-check libzpq

# Static guard for Zig's lazy test discovery. Every src file containing a
# top-level test must be directly rooted by one of the two test artifacts.
test-modules:
    python3 tools/check_test_modules.py

# Tier 3 — the corpus gauntlet. Run on a cadence and before releases.
# fetch-corpus first so `test` picks up the real decimal/etc. fixtures.
gauntlet: fetch-corpus test test-integration smoke duckdb-smoke differential (conform "data/parquet-testing")
    @echo "gauntlet: all tiers green (corpus-backed cross-impl + differential + conformance)."

# Fetch the same apache/parquet-testing revision used by CI into the gitignored
# data directory. Pinning keeps the local release gate from changing whenever
# upstream adds a new format fixture; bump this SHA and CI's together.
parquet_testing_sha := "1a2a75127be06fc0123f03ebd36c966f7beda27d"
fetch-corpus:
    #!/usr/bin/env bash
    set -euo pipefail
    corpus=data/parquet-testing
    if [[ ! -d "$corpus/.git" ]]; then
      git clone --filter=blob:none --no-checkout https://github.com/apache/parquet-testing "$corpus"
    fi
    git -C "$corpus" fetch --quiet --depth 1 origin {{parquet_testing_sha}}
    git -C "$corpus" checkout --quiet --detach {{parquet_testing_sha}}

# Lambda integration tests — spawns the binary against an in-process fake.
test-integration:
    zig build test-integration --summary all

# CLI smoke + cross-impl validation (ZPQ writes → pyarrow reads). Needs
# pyarrow on python3's path. Part of Tier 2 (`just check`).
smoke:
    zig build -Doptimize=ReleaseFast
    tools/cli_smoke.sh

# Cross-impl decode validation, the other direction: DuckDB writes parquet →
# ZPQ reads/filters/aggregates, compared against DuckDB's own answer. Catches
# decode bugs a ZPQ-vs-ZPQ round-trip can't. Needs the duckdb CLI. Tier 2.
duckdb-smoke:
    zig build -Doptimize=ReleaseFast
    tools/duckdb_smoke.sh

# Differential test (adopted from Hardwood): ZPQ vs DuckDB ROW-BY-ROW across a
# matrix of filter × projection × select × aggregate shapes, on an adversarial
# multi-row-group fixture. Catches composition bugs aggregate-scalar checks miss
# (it found the dict-index bit-width-0 write corruption). Needs duckdb+pyarrow in
# the .venv. Tier 2.
differential:
    {{python}} tools/verify/verify.py stage differential

# Reducing samples must return their private funding without adding a scan for a large dictionary tail.
private-decode-check:
    zig build -Doptimize=ReleaseFast
    {{python}} tools/private_decode_check.py zig-out/bin/zpq --out-dir zig-out/private-decode-check

# libzpq, the C ABI (Tier 2): its C test plain and under AddressSanitizer/LeakSanitizer (1,000 open/run/close
# cycles), the Python binding and service, and the differential corpus through libzpq, the CLI and the Lambda, byte
# for byte. Needs gcc (the sanitizer runtime) and duckdb+pyarrow in the .venv.
libzpq:
    PYTHON={{python}} OPTIMIZE=ReleaseFast tests/libzpq/run.sh

# Python hand-off gate (not yet in `check`): zpq from Python (python/zpq.py over libzpq) and the CLI answer as DuckDB
# does on the source; the Parquet they write reads back whole in pyarrow and DuckDB with the same values and logical
# types; and files DuckDB and pyarrow write read back in zpq alike. Cells with a filed finding are listed in
# tests/handoff/known.json. Large study tables come from $ZPQ_HANDOFF_STUDY when present. Needs duckdb+pyarrow.
handoff *args:
    zig build -Doptimize=ReleaseFast
    zig build libzpq -Doptimize=ReleaseFast
    {{python}} -I tests/handoff/run.py {{args}}

# Conformance against apache/parquet-testing (Tier 3). Clones the corpus on
# first run. Floor of 72 full-passes (was 64; +5: RLE-BOOLEAN, multi-member GZIP, empty datapage,
# dict-page-offset-zero; +3: the legacy LZ4 files); hard failures always fail.
conform corpus="/tmp/parquet-testing":
    @test -d {{corpus}} || git clone --depth 1 https://github.com/apache/parquet-testing {{corpus}}
    zig build -Doptimize=ReleaseFast
    {{python}} tools/conformance.py --corpus {{corpus}}/data --min-pass 72

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

# Re-encoding writes: bytes the same at every -j, explicit budgets at every codec (T9/T10), row-group and page
# targets, and every page index checked against its values.
write-layout-check:
    zig build -Doptimize=ReleaseFast
    {{python}} tools/write_layout_check.py

# Every failure probe has a timeout, including sink errors with encodes pending.
write-failure-check:
    zig build
    {{python}} tools/write_failure_check.py zig-out/bin/zpq
