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

## Fanning one query out: `tools/serverless/fanout.py`

One large file, or many, can be split across workers by row group. `tools/serverless/fanout.py` is a small
coordinator (stdlib plus boto3) that reads each input's row-group count from its footer, cuts the row groups into N
contiguous runs, invokes N `zpq-lambda` workers in parallel with the request's `row_groups` field, and merges their
answers:

```sh
python3 tools/serverless/fanout.py --function zpq-worker --workers 32 \
  --input s3://bucket/lineitem.parquet --filter "l_shipdate <= '1998-09-02'" \
  --aggregate "sum(l_quantity) AS qty, avg(l_discount) AS disc, count(*) AS n" \
  --group-by "l_returnflag, l_linestatus" --order-by "l_returnflag, l_linestatus"
```

COUNT, SUM, MIN, MAX and AVG merge, with or without GROUP BY (AVG is asked of each worker as `avg_state` and divided
once at the end, and decimal sums stay exact); ORDER BY and LIMIT apply after the merge. COUNT(DISTINCT), percentiles
and other holistic aggregates, window functions and expressions over aggregates are refused before any worker runs.
The same file is a coordinator Lambda: `fanout.handler` takes `{"function", "workers", "request", "order_by",
"limit"}` and, optionally, `row_group_counts` to skip reading the footers. `tools/serverless/test_fanout.py` runs it
against real `zpq-lambda` processes behind a fake Lambda runtime and checks the merged answers against one invocation
and against DuckDB.

S3 access: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` / `AWS_REGION` (on Lambda, the execution
role's). `S3_`-prefixed variables of the same names, plus `S3_ENDPOINT_URL`, take precedence and target S3-compatible
stores such as R2 or MinIO. `S3_NO_SIGN_REQUEST=1` sends requests unsigned, for buckets that allow public reads; like
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
