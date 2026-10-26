# Local Lambda Testing

Three layers, fastest first. None require Docker.

Test the cold path first. zpq-lambda is meant to run as many short, cold invocations (see
[`lambda_capabilities.md`](lambda_capabilities.md#cold-starts-are-the-design-point)), so the case that matters most is a
freshly started bootstrap answering its first event: every scenario below starts a new `zpq-lambda` process, which is
exactly that. Tests that send several events to one process (back-to-back invocations, connections the server dropped
while the process sat idle) cover what happens when Lambda reuses a sandbox; zpq has to handle reuse correctly, but
does not rely on it.

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
# 14 tests; two skip unless data/benchmark_100mb.parquet is present.
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
# {"error":"bad_json","reason":"MissingField"}   (no inputs; see lambda_requests.md for real requests)

kill $RIE_PID
```

Each `aws-lambda-rie` start is a cold start of the bootstrap; the first `curl` after it is the invocation that
matters most.

Use when:
- You want to attach a debugger to the bootstrap binary and step through interactively (gdb, lldb, perf record).
- You're sanity-checking before a real AWS deploy.
- You want to see the binary's startup behavior under the *real* Lambda runtime API, byte-for-byte.

Don't use for TDD — fake (#1) is much faster and integrates with `zig build`.

## 3. Native architecture checks

`tools/check/fleet.sh --host oci-zt` cross-builds the ARM64 bundle (unit tests, runner, CLI), runs it natively on
oci-zt with no remote compiler, and requires its answers to every case to equal this host's
([tools/check/README.md](../tools/check/README.md)). CI's ARM runner runs the shipped ARM64 CLI through the release
cases and the shipped Lambda through the fake runtime.

## What's intentionally *not* here

- **Docker / Lambda Docker images.** Containerized tests add cold-start latency to test runs and reproducibility issues
  across host platforms. The fake (#1) and RIE without Docker (#2) cover the testing surface without the container
  layer.
- **AWS SAM CLI.** Wraps RIE in Docker. Use raw RIE if you want RIE behavior; use the fake if you want speed.
- **LocalStack.** Aimed at AWS-API mocking generally, not Lambda specifically; massive surface area for what we need
  (one HTTP contract).

## Adding new env vars to the fake

If the binary starts depending on a new Lambda env var, add it to `spawnLambdaWithEnv` in
`tests/lambda_integration.zig`.
Default to a plausible canned value:

```zig
try env_map.put("AWS_LAMBDA_LOG_GROUP_NAME", "/aws/lambda/test-fn");
```

Otherwise the binary may panic on a missing env var that production always provides.
