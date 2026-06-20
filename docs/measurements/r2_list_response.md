# ListObjectsV2 against R2 — probe results

`probes/probe_r2_list` issues a SigV4-signed `GET ?list-type=2&prefix=...&max-keys=N`
against the R2 endpoint and follows `NextContinuationToken` for up to 5 pages.

Confirmed: R2's response shape matches AWS S3 v2 listing spec — `<Contents>`,
`<Key>`, `<Size>`, `<NextContinuationToken>`, `<IsTruncated>` all parse
cleanly with the same code path that would work for AWS S3 directly.

## Single-page (max_keys=100)

```json
{
  "schema_version": 1,
  "bucket": "zpq",
  "prefix": "demo/nyc-taxi/yellow/",
  "max_keys": 100,
  "pages": [
    {"page": 0, "status": 200, "ms": 118.05, "body_bytes": 6350, "keys_in_page": 24, "is_truncated": false, "sample_key": "demo/nyc-taxi/yellow/yellow_tripdata_2023-01.parquet", "sample_size": 47673370}
  ],
  "total_keys_seen": 24,
  "pages_fetched": 1
}
```

## Paginated (max_keys=10, 24 objects in prefix → 3 pages)

```json
{
  "schema_version": 1,
  "bucket": "zpq",
  "prefix": "demo/nyc-taxi/yellow/",
  "max_keys": 10,
  "pages": [
    {"page": 0, "status": 200, "ms": 112.61, "body_bytes": 3053, "keys_in_page": 10, "is_truncated": true, "sample_key": "demo/nyc-taxi/yellow/yellow_tripdata_2023-01.parquet", "sample_size": 47673370},
    {"page": 1, "status": 200, "ms": 58.29, "body_bytes": 3306, "keys_in_page": 10, "is_truncated": true, "sample_key": "demo/nyc-taxi/yellow/yellow_tripdata_2023-11.parquet", "sample_size": 56094653},
    {"page": 2, "status": 200, "ms": 53.55, "body_bytes": 1521, "keys_in_page": 4, "is_truncated": false, "sample_key": "demo/nyc-taxi/yellow/yellow_tripdata_2024-09.parquet", "sample_size": 61170186}
  ],
  "total_keys_seen": 24,
  "pages_fetched": 3
}
```

## Findings

- ListObjectsV2 latency from a workstation is ~110 ms cold (first page
  on a fresh TLS connection), ~60 ms warm.
- One page of 24 keys is a single round-trip; no glob filtering needed
  on the server — we'd post-filter basenames against `*.parquet` etc.
- Pagination is straightforward: `<NextContinuationToken>` round-trips
  back as `continuation-token=...` query param. SigV4 canonical query
  must include it sorted lex with the other params.

## Implications for ZPQ design

Glob expansion against R2/S3 = one HTTP round-trip for ≤1 000 keys
(default `MaxKeys`); add ~60 ms per additional page. For the demo
dataset (24 files) it's a single 60–115 ms call, **comparable to a
single Range GET**. That makes glob support effectively free latency-
wise — no reason to defer it from the demo path.
