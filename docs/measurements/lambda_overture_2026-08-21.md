# Lambda Overture comparison — 2026-08-21

This is a warm Lambda figure for the 0.3.2 candidate, collected by
`benchmarks/run_lambda_overture_compare.sh` with run id
`20260821Tlambda032clean`.

## Workload and controls

- Input: `test/overture_places.snappy.parquet`, 868,247,374 bytes, 4,717,270
  rows, five row groups, and nested LIST/MAP fields.
- Operation: filter `confidence > 0.9`, project `id, confidence`, and write
  Snappy Parquet to the same us-west-2 S3 bucket.
- Functions: dedicated x86_64 Lambda functions, 3008 MB and 120 seconds each.
  ZPQ was the 0.3.2 candidate; Polars was 1.43.2; DuckDB was 1.5.5 with its
  `httpfs` and `aws` extensions bundled in the image.
- Measurement: one warm-up per function, then five sequential samples. The
  table uses Lambda-reported `total_ms`, not workstation-to-Lambda wall time.

## Figure

| engine | samples | median Lambda ms | highest observed ms | median output bytes |
| --- | ---: | ---: | ---: | ---: |
| ZPQ | 5 | 2083 | 5288 | 41,145,517 |
| Polars | 5 | 1804 | 2518 | 41,511,852 |
| DuckDB | 5 | 2619 | 2749 | 41,520,451 |

Raw Lambda-reported milliseconds:

| engine | samples |
| --- | --- |
| ZPQ | 5288, 1779, 2277, 1918, 2083 |
| Polars | 1989, 2518, 1648, 1514, 1804 |
| DuckDB | 2504, 2749, 2526, 2681, 2619 |

## Validity checks

Every emitted warm-up and measured object (18 total) opened successfully in
both PyArrow and DuckDB. All contain 1,042,690 rows, two nullable columns, and
valid Parquet metadata. For the first measured sample, an ordered SHA-256 over
the `id, confidence` values was identical for all engines:

`242901f7c009120684591c28b431b37887976268ece59fe554754c76068082ab`

The full raw samples and reader-validation transcript are retained in
`benchmarks/overture_lambda_results.tsv` and
`benchmarks/overture_lambda_validation.tsv`.

## Interpretation

This is a real nested-data filter/projection/compaction comparison, not a
cold-start figure or a read-only scan benchmark. On this workload Polars has
the best observed median; ZPQ is faster than DuckDB but has a high first-sample
outlier. Do not generalize this single five-sample warm run to cold starts or
to unfiltered projection workloads.
