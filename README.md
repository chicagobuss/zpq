# ZPQ

A high-performance Parquet engine for cloud and serverless workloads, written in Zig.

> **This branch (`v2-sans-io`) is mid-rewrite.** The skeleton compiles
> and the Lambda capability probe deploys, but the Parquet decoder and
> S3 pipeline have not been ported into the new tree yet. For the
> previous shipping version see `main`.

## What's here

```
src/
  zpq.zig            Pure module surface — exports core.* + io.*
  core/              Sans-IO logic (schema, thrift)
  io/strategy.zig    IOStrategy duck-typing trait + MemoryReader
  cli/main.zig       zpq binary (workstation / native)
  lambda/main.zig    zpq-lambda binary (AWS Lambda bootstrap)

probes/
  probe_lambda_caps/ Capability probe — what Lambda actually allows.

docs/
  lambda_capabilities.md         Empirical seccomp + kernel findings
  tier_3_s3_pipeline_strategy.md S3-to-S3 orchestration plan
  COMPARISON_TO_HARDWOOD.md      Feature-completeness map

.agent/rules/        Tier 1–3 operating manuals (philosophy / knowledge / strategy)
vendor/boring_tls/   Vendored prebuilt-only BoringSSL bindings
```

## Build

Requires **Zig 0.16.0** (release, not master) — pinned in `.zig-version`.

```bash
just build           # ReleaseFast for both binaries
just test            # unit tests
just cross-check     # confirm linux+macos targets compile
just lambda-build    # static musl Lambda binary, both archs
```

The first build runs `tools/r2-fetch-artifacts.sh` to fetch prebuilt
BoringSSL artifacts. No source-builds. No AWS SDK. No system OpenSSL.

## Lambda capability probe

The `probe_lambda_caps` binary enumerates what AWS Lambda actually allows
(seccomp filter, kernel version, allowed setsockopt options, CPU
affinity at each memory tier, `/tmp` throughput). Re-run any time AWS
announces a runtime change — seccomp policy is not API contract.

```bash
just probe-local              # JSON to stdout from your workstation
just probe-lambda             # deploy + invoke in AWS, prints JSON
just probe-lambda x86_64      # same for x86_64
```

Findings driving the v2 architecture are written up in
[`docs/lambda_capabilities.md`](docs/lambda_capabilities.md).

## Philosophy

See [`.agent/rules/tier1_soul.md`](.agent/rules/tier1_soul.md). The short
version: laziness as performance, sans-IO core, two binaries (CLI with
io_uring, Lambda with epoll-only) sharing one core, vendored prebuilt
crypto, native S3/SigV4 for a 4 MB static binary.

## License

MIT
