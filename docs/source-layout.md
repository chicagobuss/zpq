# Source layout

Moved from the README for 0.5.0: where each part of the engine lives.

```
src/
  zpq.zig                Public surface — exports core.* + io.*
  cli/main.zig           zpq binary entry (workstation)
  lambda/main.zig        zpq-lambda binary entry (AWS Lambda bootstrap)
  engine.zig             runQuery: input dispatch, parallel fetch, scan, output
  profile.zig            Query profiling: per-thread spans, counters, trace export
  profile_summary.zig    Profile summary: phases, wall shares, top chunks
  core/
    schema.zig           Parquet thrift types
    thrift.zig           In-tree thrift reader/writer
    consumer.zig         Per-RG scan→consumer protocol (decode+filter+encode)
    expr/                Typed expression AST + parser + evaluator
    filter/              Filter AST, parser, prune, partition, eval
    parquet/             Page/column readers, codecs, encodings
    writer/              Encoder, fastpath, streaming sink writer
  io/
    s3.zig               SigV4 + HTTP/1.1, transport-agnostic
    multipart_sink.zig   Streaming S3 multipart upload coordinator
    epoll.zig            In-tree event loop (Lambda-mandatory backend)
    tls.zig              BoringSSL via memory BIOs
    http.zig             HTTP/1.1
    pool.zig             Connection pool
    coalescer.zig        Range coalescer for byte-range fetches
    local_reader.zig     Gated local reads: pread / io_uring into a windowed buffer
    sigv4.zig            SigV4 signer

tools/serverless/aws.sh     Lambda lifecycle (build, deploy, invoke)
docs/                       Demos, capability findings, measurements, validation notes
vendor/boring_tls/          Vendored prebuilt-only BoringSSL bindings
```
