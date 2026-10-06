# Demo: 80 M rows, 6 milliseconds

A reproducible demo of ZPQ on the
[NYC Taxi yellow trip-data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
public dataset (24 monthly Parquet files, 1.3 GB, 79.5 M rows). Two
shapes: local CLI against on-disk files, and AWS Lambda against
Cloudflare R2.

## The headlines

**Local CLI** on one 24-thread workstation, one in-process
invocation (measured in June 2026 with zpq 0.1.x):

| Query | Wall | Notes |
|-------|------|-------|
| `count(*)` across 24 files / 79.5 M rows | **6 ms** | Zero decode; answer comes from row-group footers. Less than the cost of starting `python`. |
| `count(*), max(tip), sum(fare)` — full f64 decode | **254 ms** | $1.54 B in fares totalled across 2023-2024. Row-group-parallel decode through libzstd. |
| Six aggregates (sum × 4 + count + avg) | **404 ms** | Heavier decode pass, same files. |
| With `--filter "tip_amount > 50"` | **215 ms** | 16 611 rows kept; filter eval is folded into the same decode pass. |

**AWS Lambda** (5 GB memory, x86_64, `provided.al2023`, data in R2; same June 2026 measurements):

| Query | Wall | Cost |
|-------|------|------|
| Per-file fan-out, 24 lambdas in parallel | 6.15 s | ~$0.01 |
| 4-file compaction: filter + project + write one Parquet | 31 s | ~$0.003 |

Every one of those invocations can be a cold start, and that is the
intended way to run it: the fan-out above runs 24 sandboxes at once
rather than leaning on a warm one. The runtime is one static binary —
no AWS SDK, no system OpenSSL, no event-loop dependency — built by
`just lambda-build` as a ReleaseFast zip of about 10 MB. On 2026-10-05
(zpq 0.4.0, us-west-2, 3008 MB) its median Lambda `Init Duration` was
18–22 ms on arm64 and 26–35 ms on x86_64; see
[Comparison](#comparison-cold-lambda-invocations) below.

## Why these numbers exist

ZPQ does the absolute minimum work required to satisfy a query:

* `count(*)` answers from row-group `num_rows` in the footer — never
  decode a value page. **6 ms across 80 M rows** is the floor for any
  serverless engine; everything else is overhead.
* `min`/`max`/`sum` decode the data by default. With `--trust-stats`,
  `min`/`max` answer from `Statistics.min_value`/`max_value` when the
  writer populated them (NYC TLC's writer does not, so the demo's
  `max(tip)` decodes either way).
* Filters prune row groups by stats before any column bytes are
  fetched. Survivors decode only the projected columns.
* Per-row-group decode → encode pipelines stream straight into a
  multipart S3 upload. Memory ceiling is one row group, not one file.
* Talks to any S3-compatible store. Custom endpoints (Cloudflare R2,
  MinIO) use path-style URLs; AWS S3 uses virtual-hosted URLs, or
  path-style for bucket names containing dots. Same code path, no SDK
  switch.

## Reproduce: local CLI

You'll need a release build of `zpq` and the dataset locally:

```bash
just build
mkdir -p /tmp/nyc-taxi
for year in 2023 2024; do
  for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
    f=yellow_tripdata_${year}-${month}.parquet
    curl -sf "https://d37ci6vzurychx.cloudfront.net/trip-data/$f" \
      > "/tmp/nyc-taxi/$f"
  done
done
```

### `count(*)` — answers from metadata

```bash
zpq query '/tmp/nyc-taxi/*.parquet' --aggregate "count(*)"
```

```json
{
  "ok": true,
  "files_in": 24,
  "rows_in": 79479946,
  "agg": {"count": 79479946},
  "total_ms": 2,
  "phase": {"decode_ms": 0}
}
```

`decode_ms: 0`. The footer says how many rows are in each row group;
ZPQ adds them up. Real wall time including process spawn is ~6 ms.

### Full decode aggregate

```bash
zpq query '/tmp/nyc-taxi/*.parquet' \
  --aggregate "count(*), max(tip_amount) AS max_tip, sum(fare_amount) AS total_fare"
```

```json
{
  "ok": true,
  "files_in": 24,
  "rows_in": 79479946,
  "bytes_in": 1328745331,
  "agg": {
    "count": 79479946,
    "max_tip": 4174,
    "total_fare": 1541180984.31
  },
  "total_ms": 254
}
```

The 69 row groups across the 24 files are shared out over one worker
per CPU (`-j` overrides). Each worker decodes value pages into its own
accumulators; the main thread merges them at the end.

### Filter + aggregate

```bash
zpq query '/tmp/nyc-taxi/*.parquet' \
  --filter "tip_amount > 50" \
  --aggregate "count(*) AS n, sum(fare_amount) AS fare"
```

Returns 16 611 trips with > $50 tips, $2.1 M in matching fares — in
~215 ms. The filter is evaluated in the same decode pass; tip + fare
columns are fetched once and walked together.

### Glob patterns

`zpq query` accepts a single path, multiple paths, or a `prefix*suffix`
glob — basename-only, like `data/*.parquet`. Same UX as
`duckdb -c "SELECT * FROM 'data/*.parquet'"`. Schemas are validated
across all files (case-insensitively, so the real-world
`Airport_fee` / `airport_fee` drift in the NYC dataset doesn't trip).

## Reproduce: AWS Lambda + R2

Stage the dataset in your S3-compatible bucket:

```bash
export AWS_ACCESS_KEY_ID=...        # your R2 / S3 access key
export AWS_SECRET_ACCESS_KEY=...
export ENDPOINT=https://<accountid>.r2.cloudflarestorage.com
export BUCKET=your-bucket

for year in 2023 2024; do
  for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
    f=yellow_tripdata_${year}-${month}.parquet
    curl -sf "https://d37ci6vzurychx.cloudfront.net/trip-data/$f" \
      | aws s3 cp - "s3://$BUCKET/demo/nyc-taxi/yellow/$f" \
        --endpoint-url "$ENDPOINT" --region auto
  done
done
```

Build and deploy, under a function name of your choosing:

```bash
FN=zpq-filter-r2   # example name; use your own
just lambda-build
just lambda-deploy "$FN" x86_64
```

For R2 (or any non-AWS S3-compatible endpoint), use `S3_*`-prefixed
env vars on the Lambda — the `AWS_*` ones are reserved by the
runtime and pair with the execution role:

```bash
aws lambda update-function-configuration \
  --function-name "$FN" \
  --environment "Variables={
    S3_ACCESS_KEY_ID=<r2-key>,
    S3_SECRET_ACCESS_KEY=<r2-secret>,
    S3_REGION=auto,
    S3_ENDPOINT_URL=https://<accountid>.r2.cloudflarestorage.com
  }"
```

The endpoint host may be a DNS name or an IPv4 dotted quad. IPv6 endpoints (`https://[::1]:9000`) are not
supported, and legacy IPv4 spellings such as `127.1` or `010.0.0.1` are refused rather than reinterpreted.

### Single-file aggregate

```bash
URL="s3://$BUCKET/demo/nyc-taxi/yellow/yellow_tripdata_2024-01.parquet"
just lambda-invoke "$FN" \
  "$(jq -nc --arg u "$URL" '{s3_url:$u, aggregate:"count(*)"}')"
```

Returns `count: 2964624` in ~620 ms — most of which is TLS
handshake + tail GET to R2; ZPQ's internal time is ~1 ms.

### 24-file fan-out (per-file)

Fan out one Lambda per file. This is the shape zpq-lambda is built
for: a burst of short invocations, most of them landing in new
sandboxes, with nothing to keep warm in between.

```bash
mkdir -p /tmp/results
for f in yellow_tripdata_{2023,2024}-{01..12}.parquet; do
  payload=$(jq -nc --arg u "s3://$BUCKET/demo/nyc-taxi/yellow/$f" \
    '{s3_url:$u, aggregate:"count(*),max(tip_amount),sum(fare_amount)"}')
  aws lambda invoke --function-name "$FN" \
    --payload "$(echo -n "$payload" | base64 -w 0)" \
    --cli-binary-format base64 "/tmp/results/$f.json" \
    --region us-west-2 >/dev/null &
done
wait
jq -s 'map(.agg.sum) | add' /tmp/results/*.json
```

Wall time ~6 s; cost ~$0.01.

### Iceberg-style compaction

```bash
INPUTS=$(jq -nc --arg b "$BUCKET" '[
  "s3://"+$b+"/demo/nyc-taxi/yellow/yellow_tripdata_2024-01.parquet",
  "s3://"+$b+"/demo/nyc-taxi/yellow/yellow_tripdata_2024-02.parquet",
  "s3://"+$b+"/demo/nyc-taxi/yellow/yellow_tripdata_2024-03.parquet",
  "s3://"+$b+"/demo/nyc-taxi/yellow/yellow_tripdata_2024-04.parquet"
]')
OUT="s3://$BUCKET/demo/output/big-tippers-2024-01-to-04.parquet"

just lambda-invoke "$FN" "$(jq -nc \
  --argjson inputs "$INPUTS" --arg out "$OUT" \
  '{inputs: $inputs,
    filter: "tip_amount > 50",
    output_url: $out,
    columns: ["tpep_pickup_datetime","trip_distance","fare_amount","tip_amount","total_amount"],
    output_codec: "zstd"}')"
```

13 M rows → 2 387 surviving, 63 KB written. ~31 s, ~$0.003.

## Comparison: cold Lambda invocations

zpq on Lambda is meant to be invoked cold, so that is the comparison
that matters. Measured 2026-10-05 in us-west-2, every function at
3008 MB and x86_64: zpq 0.4.0 as a `provided.al2023` zip, DuckDB 1.5.6
as a Python 3.12 container image with its S3 extensions bundled. Median
of three forced cold starts per scenario, Lambda-reported
`Init Duration` + `Duration` in ms. Inputs: a 174.8 MB synthetic file
(B), TPC-H `lineitem` SF1 in 207.1 MB of Snappy Parquet (LS), and an
868 MB Overture Places file filtered and written back to S3 (OV).

| Scenario | zpq 0.4.0 | DuckDB 1.5.6 |
|----------|----------:|-------------:|
| B1 full scan | 27 + 667 | 536 + 1,954 |
| B2 3-column projection | 27 + 365 | 668 + 1,431 |
| B3 broad filter | 27 + 379 | 448 + 1,512 |
| B4 selective filter | 27 + 373 | 532 + 1,550 |
| B5 simple aggregates | 26 + 346 | 462 + 1,371 |
| B6 string equality | 27 + 271 | 518 + 1,153 |
| LS1 full scan | 27 + 1,356 | 505 + 2,811 |
| LS2 TPC-H Q6 | 26 + 838 | 832 + 3,831 |
| LS3 Q1-like GROUP BY | 26 + 819 | 499 + 3,475 |
| LS4 key range | 26 + 204 | 535 + 762 |
| OV filter + write | 35 + 1,726 | 738 + 4,063 |

In these runs DuckDB's cold total was 2.4× to 5.6× zpq's. DuckDB's
`Init Duration` includes starting Python and importing the engine,
which is part of what a cold DuckDB Lambda costs. The full tables,
arm64 figures, memory use and warm figures (where Polars 1.44.2 was
faster than zpq x86_64 in 9 of 11 scenarios) are in
[`measurements/lambda_cold_start_2026-10-05.md`](measurements/lambda_cold_start_2026-10-05.md).

What zpq is for:

* **Cold, bursty, fan-out Lambda work.** Initialisation in tens of
  milliseconds means each file, partition or compaction unit can get
  its own invocation, fired when the work arrives; there is nothing to
  keep warm.
* **No catalog required.** Drop a Parquet file anywhere — local
  disk, R2, MinIO, S3 — and query it. No Glue, no DDL, no partition
  registration. Same UX as `duckdb -c "SELECT * FROM 'data/*.parquet'"`.
* **The data doesn't have to be in S3.** Cloudflare R2, MinIO,
  GCS-compat — same code, same speed envelope, no migration to AWS.
* **Writes stream S3-to-S3.** ZPQ Lambda filters, projects and
  re-encodes straight into a multipart upload; multi-file compaction
  is the iceberg-maintenance shape it was designed for.

## Caveats

* **`count(*)` is metadata-only**, but `min` / `max` / `sum` decode
  unless you pass `--trust-stats` and the writer populated
  `Statistics`. NYC TLC's writer doesn't, so the demo's full-aggregate
  decodes everything either way.
* **Single-thread decode rate is now ~870 MB/s** of decoded f64
  through libzstd → RLE_DICTIONARY → f64 sum. (Pre-2026-05-06 it was
  ~89 MB/s; Zig stdlib's pure-Zig zstd decoder was the bottleneck —
  92% of CLI aggregate time per `perf record`. Swap to libzstd's
  `ZSTD_decompress` C API gave a 10× speedup.) Further headroom is in
  SIMD on the value-decode loop.
* **Writer schema drift is handled case-insensitively only** — if a
  workload reorders columns or adds optional fields, the CLI errors
  with `SchemaMismatch`. Schema evolution is a real follow-up.
