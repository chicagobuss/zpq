# ZPQ task runner. Stay slim: only recipes that work against the current
# source tree. Don't ship recipes that fail when invoked; they're noise in
# `just --list`.

# Ensure we use Zig 0.16.0 release. If it's not on the PATH but is installed
# under ~/.zvm/0.16.0, add that directory to PATH.
export PATH := `[ -d ~/.zvm/0.16.0 ] && echo "$HOME/.zvm/0.16.0:$PATH" || echo "$PATH"`

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
    @./tools/r2-fetch-artifacts.sh
    zig build -Doptimize=ReleaseFast

# Build with debug symbols.
build-debug:
    @./tools/r2-fetch-artifacts.sh
    zig build

# Build the lean-core CLI — no SQL frontend (`-Dsql=false`), so liteparser
# and its C dep are excluded. "Build the binary you want." The Lambda binary
# never includes SQL regardless; this is the minimal *CLI*. CI builds this
# variant too, so the lean path can't bit-rot.
build-minimal:
    @./tools/r2-fetch-artifacts.sh
    zig build -Dsql=false -Doptimize=ReleaseFast cli

# Verify all release targets compile (CLI + Lambda, x86_64 + arm64).
cross-check:
    @echo "[x86_64-linux]"  ; zig build -Dtarget=x86_64-linux  -Doptimize=ReleaseFast
    @echo "[aarch64-linux]" ; zig build -Dtarget=aarch64-linux -Doptimize=ReleaseFast
    @echo "[x86_64-macos]"  ; zig build -Dtarget=x86_64-macos  -Doptimize=ReleaseFast
    @echo "[aarch64-macos]" ; zig build -Dtarget=aarch64-macos -Doptimize=ReleaseFast
    @echo "All targets OK."

# Just the Lambda binary (musl static, ReleaseSmall) for both archs.
lambda-build:
    zig build -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSmall lambda
    @cp zig-out/bin/zpq-lambda zig-out/zpq-lambda-arm64
    zig build -Dtarget=x86_64-linux-musl  -Doptimize=ReleaseSmall lambda
    @cp zig-out/bin/zpq-lambda zig-out/zpq-lambda-x86_64
    @echo "zig-out/zpq-lambda-{arm64,x86_64} ready."

# === Setup ===

# One-shot dev-env bootstrap on a fresh machine: install the pinned Zig
# under ~/.zvm if it's missing, then warm the build.
bootstrap:
    #!/usr/bin/env bash
    set -euo pipefail
    ZV=0.16.0
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
check: test test-integration smoke duckdb-smoke differential

# Tier 3 — the corpus gauntlet. Run on a cadence and before releases.
# fetch-corpus first so `test` picks up the real decimal/etc. fixtures.
gauntlet: fetch-corpus test test-integration smoke duckdb-smoke (conform "data/parquet-testing") triangulate
    @echo "gauntlet: all tiers green (corpus-backed cross-impl + conformance + triangulation)."

# Fetch apache/parquet-testing into data/parquet-testing (gitignored under
# data/) so the in-tree fixture decode tests run against real foreign-writer
# files instead of skipping. Refresh: git -C data/parquet-testing pull.
fetch-corpus:
    @test -d data/parquet-testing || git clone --depth 1 https://github.com/apache/parquet-testing data/parquet-testing

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
    zig build -Doptimize=ReleaseFast
    {{python}} tools/differential.py

# Conformance against apache/parquet-testing (Tier 3). Clones the corpus on
# first run. Floor of 69 full-passes = the 2026-06-14 baseline (was 64;
# +5: RLE-BOOLEAN, multi-member GZIP, empty datapage, dict-page-offset-zero); hard failures always fail.
conform corpus="/tmp/parquet-testing":
    @test -d {{corpus}} || git clone --depth 1 https://github.com/apache/parquet-testing {{corpus}}
    zig build -Doptimize=ReleaseFast
    {{python}} tools/conformance.py --corpus {{corpus}}/data --min-pass 69

# Heavy correctness triangulation (ZPQ vs Hardwood vs DuckDB) (Tier 3).
triangulate: fetch-corpus
    zig build -Doptimize=ReleaseFast
    {{python}} tools/triangulate.py


# Coverage-guided fuzzing of the parquet decode path (Q0a/Q1). NOTE: blocked
# on a Zig 0.16.0 bug — its bundled test_runner.zig won't compile in `-ffuzz`
# mode (passes @errorReturnTrace() to writeStackTrace, wrong StackTrace type).
# Until a toolchain fix, the fuzz-lite PRNG loop in fuzz_decode.zig runs under
# `just test` (Tier 1). This is the real-fuzzer entry for when --fuzz works.
fuzz filter="fuzz:":
    zig build test --fuzz -- {{filter}}

# === Perf regression ===

# Lock-in regression suite. Runs canonical CLI query shapes, asserts
# golden outputs + wall/RSS bounds. Pass --lambda to also exercise the
# deployed function. See benchmarks/regression/README.md.
bench-regression *args:
    zig build -Doptimize=ReleaseFast
    {{python}} benchmarks/regression/runner.py {{args}}

# Re-capture goldens + baseline.json from the current build. Run this
# after intentional behavior changes; commit the result alongside the
# code.
bench-regression-update *args:
    zig build -Doptimize=ReleaseFast
    {{python}} benchmarks/regression/runner.py --update-golden --update-baseline {{args}}

# Cross-engine perf comparison over ZPQ's type/operator surface (zpq vs
# duckdb-cli vs polars-py), best-of-N cold wall-clock. Generates the 5M-row
# typed fixture if absent. Needs duckdb + the .venv (polars). Results log:
# docs/measurements/type_perf.md — append a dated block on meaningful runs.
type-bench runs="5":
    @test -f data/bench_types.parquet || duckdb -c "COPY (SELECT i::INTEGER AS id, (i*1.5)::DOUBLE AS amt, CAST(i % 100000 AS DECIMAL(18,2)) AS price, ('row'||(i%1000)) AS name, (DATE '2015-01-01' + (i%4000)::INTEGER) AS d, (TIMESTAMP '2015-01-01 00:00:00' + ((i%4000)::INTEGER * INTERVAL 1 HOUR)) AS ts, (i%3=0) AS flag FROM range(5000000) t(i)) TO 'data/bench_types.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)"
    zig build -Doptimize=ReleaseFast
    {{python}} benchmarks/type_bench.py -n {{runs}}

# Decode-path microbench. Times consumer.decodeColumnT against every
# column of data/benchmark_100mb.parquet — strips glob/mmap/aggregate
# noise so per-(encoding × type × bit_width) decode cost is visible.
microbench fixture="data/benchmark_100mb.parquet" runs="7" warmup="2":
    zig build microbench -Doptimize=ReleaseFast
    @benchmarks/microbench/run.sh {{fixture}} {{runs}} {{warmup}}

# === Lambda lifecycle ===

# Deploy a Lambda function (function-name + arch). Loads .env for AWS creds.
lambda-deploy fn arch="arm64":
    @./tools/serverless/aws.sh deploy {{fn}} zig-out/zpq-lambda-{{arch}}.zip {{arch}}

# Invoke a Lambda function with a JSON payload.
lambda-invoke fn payload="{}":
    @./tools/serverless/aws.sh invoke {{fn}} '{{payload}}'

# Tail the most recent CloudWatch log stream for a function.
lambda-logs fn:
    @./tools/serverless/aws.sh logs {{fn}}

# === Lambda capability probe ===

# Run the Lambda capability probe locally (CLI mode prints JSON to stdout).
probe-local:
    zig build-exe probes/probe_lambda_caps/main.zig -O ReleaseSmall \
        -femit-bin=zig-out/probe_lambda_caps_native
    @./zig-out/probe_lambda_caps_native | jq

# Build + invoke the probe in Lambda. Re-run to recheck AWS's seccomp policy.
probe-lambda arch="arm64":
    zig build-exe probes/probe_lambda_caps/main.zig -O ReleaseSmall \
        -target {{arch}}-linux-musl \
        -femit-bin=zig-out/probe_lambda_caps_{{arch}}
    @mkdir -p zig-out/lambda_probe
    @cp zig-out/probe_lambda_caps_{{arch}} zig-out/lambda_probe/bootstrap
    @cd zig-out/lambda_probe && zip -j -q probe_lambda_caps_{{arch}}.zip bootstrap && rm bootstrap
    @./tools/serverless/aws.sh deploy zpq-probe-caps-{{arch}} zig-out/lambda_probe/probe_lambda_caps_{{arch}}.zip {{arch}}
    @./tools/serverless/aws.sh invoke zpq-probe-caps-{{arch}} '{}'

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

# Pre-fetch BoringSSL prebuilt artifacts.
fetch-deps:
    @./tools/r2-fetch-artifacts.sh
