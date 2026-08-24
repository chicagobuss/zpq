# Lambda comparison protocol

ZPQ's claim is a narrow one: S3-to-S3 Parquet filtering, projection, and
compaction in a Lambda. A chart is useful only when every engine runs that
same shape under the same Lambda configuration.

## What to compare

- Match architecture, memory, timeout, region, input objects, and output
  codec. Record the function code hash and configuration with every run.
- Run the same ten-file partitioned fixture through ZPQ, Polars, and DuckDB.
  `benchmarks/run_selectivity.sh` covers four non-partition filter
  selectivities, so every engine must decode and re-encode rather than winning
  only through partition pruning.
- Validate every output with `benchmarks/validate_outputs.py`; it opens each
  output with PyArrow and DuckDB. Do not publish a timing row whose output was
  not validated.
- Report Lambda-reported `total_ms` for warm execution, separately from caller
  wall-clock time and cold-start `Init Duration`. They measure different things.
  `benchmarks/summarize_lambda_results.py` refuses to produce a table without
  the validation sidecar from the same run.

The scripts take `ENV_FILE`, `ZPQ_BENCH_FUNCTION`, and
`PYTHON_BENCH_FUNCTION`, so a release candidate can use dedicated benchmark
functions without changing a developer's saved `.env`. The cold-start runner
preserves the existing function environment when it changes `BENCH_NONCE` to
force a cold container.

For the 0.3.2 candidate, run `benchmarks/deploy_lambda_comparators.sh` first.
It creates only `zpq-032-*` dedicated x86_64 functions and makes the ZPQ,
Polars, and DuckDB configurations identical (3008 MB, 120 seconds, us-west-2).
It also builds one image per Python engine, pins DuckDB 1.5.5 and Polars 1.43.2,
and bundles DuckDB's S3 extensions in the image so an invocation never measures
an extension download.

`benchmarks/run_lambda_overture_compare.sh` is the complementary real-world
fixture: 868 MB / 4.7 M-row Overture Places, a nested five-row-group Parquet
file. It makes every engine filter `confidence > 0.9`, project `id, confidence`,
and write Snappy Parquet. `summarize_lambda_overture.py` refuses to emit a
figure unless every warm-up and sample output was validated by PyArrow and
DuckDB.

## 2026-08-20 — 0.3.2 candidate release probes

These are workstation-to-R2 correctness/performance probes, not Lambda or
cross-engine figures. They establish the remote GROUP BY cases that v0.3.0
could not execute because its remote fetch plan omitted key columns.

| probe | samples | min ms | median ms | highest observed ms |
| --- | ---: | ---: | ---: | ---: |
| one NYC taxi file, dictionary-string GROUP BY | 5 | 1138 | 1289 | 1419 |
| five explicit taxi files, same GROUP BY | 5 | 1423 | 1542 | 1608 |

The single-file workload groups `store_and_fwd_flag`, a low-cardinality
dictionary-encoded string over roughly three million rows. The five-file case
also covers multi-file remote scheduling and GROUP BY finalization. Both ran
after one warm-up on the same R2 object set.

No current Lambda-versus-alternative figure is claimed here: the available
alternative Lambda deployments predate the current candidate and are not
configuration-matched. Re-deploy matching benchmark functions before using
the comparison scripts for a release chart.
