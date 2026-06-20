# Demo: 80 M rows, 6 milliseconds

A reproducible demo of ZPQ on the
[NYC Taxi yellow trip-data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
public dataset (24 monthly Parquet files, 1.3 GB, 79.5 M rows). Two
shapes: local CLI against on-disk files, and AWS Lambda against
Cloudflare R2.

## The headlines

**Local CLI** (workstation, 13th-gen i7-13700HX, 24 threads), one
in-process invocation:

| Query | Wall | Notes |
|-------|------|-------|
| `count(*)` across 24 files / 79.5 M rows | **6 ms** | Zero decode; answer comes from row-group footers. Less than the cost of starting `python`. |
| `count(*), max(tip), sum(fare)` — full f64 decode | **254 ms** | $1.54 B in fares totalled across 2023-2024. 24-way per-file decode through libzstd. |
| Six aggregates (sum × 4 + count + avg) | **404 ms** | Heavier decode pass, same files. |
| With `--filter "tip_amount > 50"` | **215 ms** | 16 611 rows kept; filter eval is folded into the same decode pass. |

**AWS Lambda** (5 GB memory, x86_64, `provided.al2023`, data in R2):

| Query | Wall | Cost |
|-------|------|------|
| Per-file fan-out, 24 lambdas in parallel | 6.15 s | ~$0.01 |
| 4-file compaction: filter + project + write one Parquet | 31 s | ~$0.003 |

Cold-start init: **12 ms**. Whole runtime is a 3.3 MB static musl
binary (measured 2026-06-12, ReleaseSmall x86_64 — grew from 1.5 MB
with the vendored snappy/zstd encoders) — no AWS SDK, no system
OpenSSL, no event-loop dependency.

## Why these numbers exist

ZPQ does the absolute minimum work required to satisfy a query:

* `count(*)` answers from row-group `num_rows` in the footer — never
  decode a value page. **6 ms across 80 M rows** is the floor for any
  serverless engine; everything else is overhead.
* `min`/`max` answer from `Statistics.min_value`/`max_value` when the
  writer populated them (DuckDB output, Spark output — always; NYC
  TLC's writer — never, hence the demo's `max(tip)` decodes).
* Filters prune row groups by stats before any column bytes are
  fetched. Survivors decode only the projected columns.
* Per-row-group decode → encode pipelines stream straight into a
  multipart S3 upload. Memory ceiling is one row group, not one file.
* Talks to any S3-compatible store via path-style URLs. Cloudflare R2,
  AWS S3, MinIO — same code path, no SDK switch.

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

24 worker threads, one per file. Each thread decodes its file's value
pages into a per-thread accumulator slice; the main thread merges
them at the end.

### Filter + aggregate

```bash
zpq query '/tmp/nyc-taxi/*.parquet' \
  --filter "tip_amount > 50" \
  --aggregate "count(*) AS n, sum(fare_amount) AS fare"
```

Returns 16 611 trips with > $50 tips, $2.1 M in matching fares — in
~1.5 s. The filter is evaluated in the same decode pass; tip + fare
columns are fetched once and walked together.

### Glob patterns

`zpq query` accepts a single path, multiple paths, or a `prefix*suffix`
glob — basename-only, like `data/*.parquet`. Same UX as
`duckdb 'SELECT * FROM "data/*.parquet"'`. Schemas are validated
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

Build and deploy:

```bash
just lambda-build
just lambda-deploy zpq-filter-r2 x86_64
```

For R2 (or any non-AWS S3-compatible endpoint), use `S3_*`-prefixed
env vars on the Lambda — the `AWS_*` ones are reserved by the
runtime and pair with the execution role:

```bash
aws lambda update-function-configuration \
  --function-name zpq-filter-r2 \
  --environment "Variables={
    S3_ACCESS_KEY_ID=<r2-key>,
    S3_SECRET_ACCESS_KEY=<r2-secret>,
    S3_REGION=auto,
    S3_ENDPOINT_URL=https://<accountid>.r2.cloudflarestorage.com
  }"
```

### Single-file aggregate

```bash
URL="s3://$BUCKET/demo/nyc-taxi/yellow/yellow_tripdata_2024-01.parquet"
just lambda-invoke zpq-filter-r2 \
  "$(jq -nc --arg u "$URL" '{s3_url:$u, aggregate:"count(*)"}')"
```

Returns `count: 2964624` in ~620 ms — most of which is TLS
handshake + tail GET to R2; ZPQ's internal time is ~1 ms.

### 24-file fan-out (per-file)

Fan out one Lambda per file:

```bash
mkdir -p /tmp/results
for f in yellow_tripdata_{2023,2024}-{01..12}.parquet; do
  payload=$(jq -nc --arg u "s3://$BUCKET/demo/nyc-taxi/yellow/$f" \
    '{s3_url:$u, aggregate:"count(*),max(tip_amount),sum(fare_amount)"}')
  aws lambda invoke --function-name zpq-filter-r2 \
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
OUT="s3://$BUCKET/demo/output/big-tippers-2024-q1.parquet"

just lambda-invoke zpq-filter-r2 "$(jq -nc \
  --argjson inputs "$INPUTS" --arg out "$OUT" \
  '{inputs: $inputs,
    filter: "tip_amount > 50",
    output_url: $out,
    columns: ["tpep_pickup_datetime","trip_distance","fare_amount","tip_amount","total_amount"],
    output_codec: "zstd"}')"
```

13 M rows → 2 387 surviving, 63 KB written. ~31 s, ~$0.003.

## Comparison

Local CLI head-to-head, same 24 files / 79.5 M rows / 1.3 GB
warm-cache, same workstation (i7-13700HX, 24 threads):

| Query | ZPQ | DuckDB v1.2.2 |
|-------|-----|---------------|
| `count(*)` | **6 ms** | 29 ms |
| `count(*), max(tip), sum(fare)` | 254 ms | **148 ms** |
| With `WHERE tip > 50` | 215 ms | **128 ms** |
| 6 aggregates (4 sums + count + avg) | 404 ms | **206 ms** |

ZPQ wins on metadata-only queries (`count(*)` is 5× faster — it
answers from row-group footers; DuckDB pays at minimum to open files
and build a plan). DuckDB wins on full-decode aggregates by ~2× —
years of vectorized-column SIMD optimization on the value-decode
inner loop that ZPQ doesn't have yet. Future decode work should be
driven by flamegraphs, not by the demo headline.

vs. AWS Athena on the same `count + max + sum` workload (warm,
estimated):

| Engine | Wall | Cost | Where data has to live |
|--------|------|------|------------------------|
| ZPQ CLI (workstation) | **254 ms** | $0 | local disk or any S3-compat |
| DuckDB CLI (workstation) | 148 ms | $0 | local disk |
| ZPQ Lambda (24-way fan-out) | 6.15 s | ~$0.01 | any S3-compatible |
| Athena (warm-path) | 2–6 s | ~$0.001 | AWS S3 + Glue table |

The pitch isn't "ZPQ beats DuckDB on a workstation." It's:

* **ZPQ runs where DuckDB doesn't.** A 3.3 MB static binary that
  boots in 12 ms on AWS Lambda. DuckDB is a 50+ MB shared library
  that pays seconds of cold-start. Different category.
* **No catalog required.** Drop a Parquet file anywhere — local
  disk, R2, MinIO, S3 — and query it. No Glue, no DDL, no partition
  registration. Same UX as `duckdb 'data/*.parquet'`.
* **The data doesn't have to be in S3.** Cloudflare R2, MinIO,
  GCS-compat — same code, same speed envelope, no migration to AWS.
* **Write workloads invert the cost gap.** Athena's CTAS / INSERT
  charges for scan AND for what it writes back through the engine.
  ZPQ Lambda streams S3-to-S3 directly; multi-file compaction is the
  iceberg-maintenance shape it was designed for.

## Caveats

* **`count(*)` is metadata-only**, but `min` / `max` / `sum` decode
  unless the writer populated `Statistics`. NYC TLC's writer doesn't,
  so the demo's full-aggregate decodes everything. DuckDB and Spark
  output do populate stats and would short-circuit.
* **Single-thread decode rate is now ~870 MB/s** of decoded f64
  through libzstd → RLE_DICTIONARY → f64 sum. (Pre-2026-05-06 it was
  ~89 MB/s; Zig stdlib's pure-Zig zstd decoder was the bottleneck —
  92% of CLI aggregate time per `perf record`. Swap to libzstd's
  `ZSTD_decompress` C API gave a 10× speedup.) Further headroom is in
  SIMD on the value-decode loop.
* **Writer schema drift is handled case-insensitively only** — if a
  workload reorders columns or adds optional fields, the CLI errors
  with `SchemaMismatch`. Schema evolution is a real follow-up.
