# ZPQ task runner. Stay slim: only recipes that work *today* against the
# current source tree. As the v2 rewrite ports code into core/io, add new
# recipes (or revive old ones from git history). Don't ship recipes that
# fail when invoked — they're noise in `just --list`.

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

# === Tests ===

test *args="":
    zig build test --summary all -- {{args}}

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

# === Maintenance ===

# Wipe build outputs.
clean:
    rm -rf zig-out .zig-cache

# Pre-fetch BoringSSL prebuilt artifacts.
fetch-deps:
    @./tools/r2-fetch-artifacts.sh
