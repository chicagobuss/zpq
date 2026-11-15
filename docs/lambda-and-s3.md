# zpq on AWS Lambda, and reading S3

Moved from the README for 0.5.0. How zpq runs on AWS Lambda, the fan-out coordinator, and how it reaches S3.

`zpq-lambda` is a static binary for the `provided.al2023` runtime, built for fast cold starts. On 2026-10-05 in
us-west-2, with every function at 3008 MB, zpq 0.4.0's median Lambda `Init Duration` was 18–22 ms on arm64 and
26–35 ms on x86_64 across eleven scenarios (three forced cold starts each). DuckDB 1.5.6 in a Python 3.12 container
image, same region and memory, measured 448–832 ms, and its cold Init + Duration was 2.4× to 5.6× zpq's (x86_64 against
x86_64) in every scenario. Polars 1.44.2, packaged the same way, measured 478–806 ms. The full tables, inputs and
method are in [`docs/measurements/lambda_cold_start_2026-10-05.md`](measurements/lambda_cold_start_2026-10-05.md).

So the intended pattern is cold, bursty and wide: one invocation per file, partition or compaction unit, fanned out
in parallel when the work arrives, with each sandbox free to be thrown away afterwards. Nothing from an earlier
invocation is needed for a correct answer, and concurrency, not sandbox reuse, is how it scales.

Keeping sandboxes warm is not the intended use: keep-warm pings, provisioned concurrency bought to hide
initialisation, or designs that count on a sandbox's connections and caches surviving between calls pay for idle
capacity to save tens of milliseconds. When Lambda reuses a sandbox on its own, zpq handles it: connections idle for
more than 4 s are closed rather than reused (they do not survive a frozen sandbox), and a reused connection found dead
is retried at once on a new one. See [`docs/lambda_capabilities.md`](lambda_capabilities.md) and the request and
response format in [`docs/lambda_requests.md`](lambda_requests.md).

## Fanning one query out: `zpq fanout`

The native Zig coordinator splits explicit inputs into contiguous fractional row-group runs, invokes workers in
parallel and merges their partial states. It makes no footer or capability call before scattering explicit inputs.
Optional `--counts` hints affect balancing, never coverage. Glob inputs first negotiate structured-plan v1 and
prepare the inputs through a worker.

```sh
zpq fanout --function zpq-worker:7 --workers 32 \
  --input s3://bucket/lineitem.parquet \
  --aggregate "sum(l_quantity) AS qty, avg(l_discount) AS disc, count(*) AS n" \
  --group-by "l_returnflag, l_linestatus" --order-by "l_returnflag, l_linestatus"
```

Repeat `--endpoint http://host:port` to distribute invocations among local Lambda endpoints. A base URL gets the
Lambda Invoke path; a URL with a path uses that path verbatim. Local endpoint requests are unsigned unless
`--sign-endpoints` is given (with dummy environment credentials, to exercise SigV4 against an emulator). AWS Invoke
uses the same HTTP/TLS pool and SigV4 signer as S3; function ARNs and qualifiers are preserved. AWS mode requires
`--region` or `AWS_REGION` / `AWS_DEFAULT_REGION` and execution-role credentials in the environment.

COUNT, SUM, MIN, MAX and AVG merge with or without GROUP BY. AVG stays sum/count until finalization. Numeric sums
use checked decimal arithmetic and refuse overflow. Worker responses must agree on schema, plan and merge metadata.
ORDER BY and LIMIT apply after merging, with NULLs last. Holistic aggregates and expressions over aggregates are
refused by workers before input I/O. A failed worker never yields a partial answer.

The same `zpq-lambda` binary coordinates events containing `"operation":"fanout"`:

```json
{"operation":"fanout","function":"zpq-worker:7","workers":32,"request":{"inputs":["s3://bucket/data.parquet"],"aggregate":"count(*) AS n"}}
```

Events also accept `endpoints`, `sign_endpoints`, `region`, `counts` (or `row_group_counts`), `concurrency`, `order_by` and `limit`. The CLI accepts a complete
worker request as `--request JSON`. `zig build test-fanout -Doptimize=ReleaseSafe` exercises real worker processes
behind localhost Runtime and Invoke APIs and compares answers with whole-query execution and DuckDB 2.0.0.

Each response reports planning, scatter, decoding and merge time, transport connection counts and wait time, and
per-worker issue offsets, Invoke time and request/response bytes. Worker statistics contain execution metadata;
partial rows are consumed by the coordinator and omitted from its response.

### Aligned and replicated joins

Pass a join description in `--request` (or the coordinator event's `request`). By default the coordinator tries
aligned splits: a worker prepares disjoint key ranges from the fact and build footers, then each worker reads and
joins the row groups overlapping its range. Both sides filter to that range, so keys shared at row-group boundaries
belong to one worker and no rows move between workers. This permits ordered fact/fact joins such as PDS-H
lineitem/orders on orderkey, with replicated small dimensions beside the aligned build.

```json
{"inputs":["s3://bucket/facts.parquet"],"aggregate":"count(*) AS n, sum(amount) AS total","group_by":"region","join":{"kind":"inner","fk":"customer_id","build":{"path":"s3://bucket/customers.parquet","key":"id","columns":["region"]}}}
```

Alignment currently takes signed integer keys with complete, non-null footer bounds. Row-group ranges must be
ordered without strict overlaps; an equal key at adjacent boundaries is supported. The planner derives physical
selections from real footers rather than count hints. Degenerate repeated keys can yield fewer workers than requested.
If alignment is unprovable, auto mode falls back to a replicated build. Set `"join_strategy":"aligned"` to require
alignment and receive `AlignedJoinUnordered`, `AlignedJoinMissingStats`, `AlignedJoinKeyType` or `AlignedJoinNoKeys`.
Set `"join_strategy":"replicated"` for a known small side, skipping alignment preparation. Queries that prepare
alignment report `footer_ms:null`; direct replicated and non-join splits retain `footer_ms:0`.

The fact inputs must be explicit S3 paths; joins with glob inputs are refused. Every build input must also be S3.
Inner and left joins require unique build keys; semi joins test membership, and anti joins use `NOT IN` NULL
semantics. Keys must use the engine's integer-compatible key types; string and composite keys are not supported.
Stars and nested dimensions are accepted, and each auxiliary replicated build must fit its cap even beside an aligned
main edge.

The worker fetches only the build columns it needs. Replicated source buffers and lookup storage together share the
smaller of 64 MiB and one eighth of its query memory budget, counted in retained projected bytes rather than the
object's size. The aligned main slice uses the query budget. A build that does not fit fails with
`JoinBuildExceedsBudget`.

Each source needs a strong ETag. Planned generations are checked before column fetching and pinned with `If-Match`
on ranged reads; a mismatch fails with `AlignedJoinSnapshotChanged`. Workers also agree on build snapshots, fact
schema and the common query before physical key restrictions. The versioned `partial_join_aggregate` and
`partial_aligned_join_aggregate` operations make older workers refuse before input I/O. Immutable qualified workers
cache positive capability acknowledgements. There is no shuffle or exchange pass.

S3 access: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` / `AWS_REGION` (on Lambda, the execution
role's). `S3_`-prefixed variables of the same names, plus `S3_ENDPOINT_URL`, take precedence and target S3-compatible
stores such as R2 or RustFS. `S3_NO_SIGN_REQUEST=1` sends requests unsigned, for buckets that allow public reads; like
signed requests, these need `AWS_REGION` or `S3_REGION`, and zpq reports a missing region as such.

Every S3 connection has deadlines, in milliseconds, each overridable from the environment:

| wait | default | variable |
|---|---:|---|
| TCP connect (per address) | 5 000 | `ZPQ_S3_CONNECT_TIMEOUT_MS` |
| TLS handshake | 5 000 | `ZPQ_S3_HANDSHAKE_TIMEOUT_MS` |
| first byte of a response, after the request is sent | 10 000 | `ZPQ_S3_FIRST_BYTE_TIMEOUT_MS` |
| no byte received or sent mid-transfer | 10 000 | `ZPQ_S3_IDLE_TIMEOUT_MS` |
| a request and its response in all, at least | 30 000 | `ZPQ_S3_REQUEST_TIMEOUT_MS` |
| the slowest transfer let run, in KiB/s (0: none is bounded by size) | 64 | `ZPQ_S3_MIN_RATE_KIBS` |
| CompleteMultipartUpload in all | 900 000 | `ZPQ_S3_COMPLETE_TIMEOUT_MS` |

The idle timeout restarts at every byte, so a transfer that keeps moving is cut off only by the whole-request
bound: the longer of 30 s and its bytes at the minimum rate, which a server trickling a byte at a time cannot outlast.
On a link slower than 64 KiB/s, lower the rate. CompleteMultipartUpload has its own bound instead: S3 can take
minutes over a large object, sending whitespace meanwhile, which keeps the idle timeout from firing. A request that
times out is sent again on a new connection when that is safe (GETs, HEADs, object and part PUTs). The retries are
charged to its usual retry budget, so a server that keeps stalling fails the query with the timeout instead of
hanging it. Creating and completing a multipart upload are never sent twice; one that could not connect (DNS, TCP
or the TLS handshake failed, so nothing was sent) is tried again on a new connection within the same budget. When
one of them fails, the error names the request, whether it failed while connecting or after it was sent, and the HTTP
status and S3 error code it was answered with: on stderr from the CLI, and in the Lambda's error JSON as `"s3"`.

An upload that fails part-way is aborted, but an abort can fail too, and a Create whose answer was lost may have
started an upload zpq never hears of. Give the output bucket a lifecycle rule with `AbortIncompleteMultipartUpload`
(a day or so after initiation) so such uploads, and the storage their parts hold, are cleaned up whatever happened.

# Lambda capabilities

Findings driving the architecture are in [`docs/lambda_capabilities.md`](lambda_capabilities.md).
