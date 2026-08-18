# Local Lambda Testing

Three layers, fastest first. None require Docker.

## 1. In-process fake runtime API (default; `zig build test-integration`)

The Lambda integration test harness in `tests/lambda_integration.zig`:

1. Binds a localhost TCP socket on a kernel-assigned port.
2. Spawns `./zig-out/bin/zpq-lambda` as a subprocess with `AWS_LAMBDA_RUNTIME_API=127.0.0.1:<port>` (plus the standard
   `AWS_LAMBDA_FUNCTION_NAME` / `AWS_REGION` / etc. env vars).
3. Serves AWS Lambda's runtime API HTTP/1.1 contract: synthetic `/runtime/invocation/next` responses, captures
   `/runtime/invocation/{id}/response` posts.
4. Asserts on response shape, count, headers.

```bash
just test-integration
# 2/2 tests pass in ~21 ms total.
```

**This is the default for TDD.** Iteration latency is single-digit milliseconds — comparable to unit tests. Use it for:

- Adding a new field to the handler response → write the assertion, run, watch it fail, implement, watch it pass.
- Reproducing a production bug → replay the exact event payload as a regression test.
- Property-testing — drive 1000 randomly-shaped events through the fake and assert no leaks / consistent shape.

**Adding a new test scenario**: copy one of the existing tests and modify. Each test scenario:

```zig
test "scenario name" {
    var server = try FakeServer.start();
    defer server.deinit();
    const endpoint = try std.fmt.allocPrint(...);
    defer ...;
    var child = try spawnLambda(std.testing.allocator, endpoint);
    defer killChild(&child);

    // serve N invocations, capture N responses, assert on bodies
}
```

The fake does **not** simulate Lambda's CPU metering, memory limits, seccomp filter, or kernel-version constraints. For
that you need layer 3 below, or a real AWS deploy.

## 2. AWS Lambda RIE — for interactive `curl` testing

`aws-lambda-rie` is a 5 MB Go binary that emulates the Lambda runtime locally. The Docker base images for
`provided.al2023` ship it; you don't need Docker to use it.

```bash
# One-time install:
curl -L https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie \
  -o ~/.local/bin/aws-lambda-rie
chmod +x ~/.local/bin/aws-lambda-rie

# Per-invocation:
aws-lambda-rie ./zig-out/bin/zpq-lambda &
RIE_PID=$!

curl -XPOST http://localhost:8080/2015-03-31/functions/function/invocations \
  -d '{"hello":"world"}'
# {"ok":true,"loop":"epoll","request_id":"...","echo_bytes":17}

kill $RIE_PID
```

Use when:
- You want to attach a debugger to the bootstrap binary and step through interactively (gdb, lldb, perf record).
- You're sanity-checking before a real AWS deploy.
- You want to see the binary's startup behavior under the *real* Lambda runtime API, byte-for-byte.

Don't use for TDD — fake (#1) is much faster and integrates with `zig build`.

## 3. Cross-arch testing — qemu user + binfmt_misc

For testing the ARM64 Lambda binary on an x86_64 dev machine (or x86_64 binary on an ARM64 dev machine). **Process-level
emulation, not a container** — gdb, ptrace, perf all work.

### Linux

```bash
sudo apt install qemu-user-static binfmt-support
# binfmt_misc auto-registers; foreign-arch ELF binaries run transparently
zig build -Dtarget=aarch64-linux-musl lambda
./zig-out/bin/zpq-lambda  # ARM64 binary, runs via qemu transparently

# Same for the test runner:
zig build -Dtarget=aarch64-linux-musl test-integration
```

Caveats:
- Slower than native (~2–3× for compute, less for syscall-heavy code).
- For correctness testing, perfect. For benchmarks, deploy to a real ARM host or use `bench/` infra.
- The kernel still reports x86_64 in `uname -m` to the emulated binary's perspective is mostly fine, but anything that
  introspects via `/proc/cpuinfo` will see the host arch.

### macOS

`brew install qemu`. No binfmt auto-trigger — invoke explicitly:

```bash
qemu-aarch64 ./zig-out/bin/zpq-lambda
```

Slower than Linux's binfmt path. macOS users primarily run native ARM64 builds; this is for testing the x86_64 Lambda
binary if you ship both.

## What's intentionally *not* here

- **Docker / Lambda Docker images.** Containerized tests add cold-start latency to test runs and reproducibility issues
  across host platforms. The fake (#1) and RIE without Docker (#2) cover the testing surface without the container
  layer.
- **AWS SAM CLI.** Wraps RIE in Docker. Use raw RIE if you want RIE behavior; use the fake if you want speed.
- **LocalStack.** Aimed at AWS-API mocking generally, not Lambda specifically; massive surface area for what we need
  (one HTTP contract).

## Adding new env vars to the fake

If the binary starts depending on a new Lambda env var, add it to `spawnLambda` in `tests/lambda_integration.zig`.
Default to a plausible canned value:

```zig
try env_map.put("AWS_LAMBDA_LOG_GROUP_NAME", "/aws/lambda/test-fn");
```

Otherwise the binary may panic on a missing env var that production always provides.
