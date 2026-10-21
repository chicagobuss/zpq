# Lambda request format

`zpq-lambda` takes one JSON object per invocation (`src/lambda/request.zig`). It maps onto the same query options as
the CLI flags of the same name.

| Field | Type | Meaning |
|-------|------|---------|
| `inputs` | array of strings | Input URLs, read as one table. |
| `s3_url` | string | One input; used only when `inputs` is absent. |
| `filter` | string | As `--filter`. |
| `columns` | array of strings | As `--columns`, one name per entry; see below. |
| `select` | string | As `--select`. |
| `aggregate` | string | As `--aggregate`. |
| `group_by` | string | As `--group-by`. |
| `column_order` | string | As `--column-order`. |
| `output_url` | string | Write the result here, as `-o`. |
| `output_codec` | string | `snappy`, `zstd`, `gzip`, `lz4`/`lz4_raw` or `uncompressed`; anything else is snappy. |
| `compress_priority` | string | As `--compress-priority`: `size` or `speed`; anything else is `bad_json`/`BadCompressPriority`. Absent: `size`. |
| `scan_all`, `trust_stats` | boolean | As `--scan-all`, `--trust-stats` (which also reads a float chunk's missing NaN count as zero when pruning, and trusts plausible statistics, never impossible ones). |
| `ordered` | boolean | `false` is `--unordered`. Absent or `null`: ordered. |
| `max_memory` | positive number, or size string (`"512MB"`) | As `--max-memory`, which bounds the S3 fetch window (a quarter of it, 64 MiB to 1 GiB) and decoded row groups (half of it; see the README). Defaults to three quarters of the function's memory less the warm Bloom filter cache's share (a sixteenth, at most 256 MiB). |
| `profile` | boolean | Add a profile summary to the response, as `--profile-summary=json`. |
| `profile_url` | string | Also upload the full profile trace to this S3 URL, as `--profile`. Implies `profile`. |
| `bloom_filters` | array of strings | As `--bloom-filter`, one `"col"` or `"col:fpp"` per entry; only with `output_url`. |
| `row_group_size` | positive integer | As `--row-group-size`: rows per re-encoded output row group (default 131072). |
| `row_group_bytes` | positive number, or size string | As `--row-group-bytes`: uncompressed bytes per re-encoded output row group (default 64MB). A `max_memory` the request names caps it at a sixteenth of that. |

A `columns` entry is one name, commas included; entries are trimmed, and empty entries or an empty array mean no
projection. Every field may be `null`, which means absent. Unknown fields are ignored; the parser skips them
without building them, so they cost no memory. With `aggregate` or `group_by` the response is the aggregate JSON;
otherwise with `output_url` it is the write summary. A request with neither gets the older single-file diagnostic
answer for its first input.

## Responses

Every response is one JSON object. Durations are whole milliseconds; `pool` and `phase` describe this invocation only.

**Aggregate** (`aggregate` or `group_by` set):

| Field | Meaning |
|-------|---------|
| `ok` | `true`. |
| `files_in`, `rows_in`, `bytes_in` | Inputs read, their rows, and the bytes fetched or mapped. |
| `rows_scanned`, `rows_matched` | Rows in the row groups read (not pruned), and of those the rows the filter kept, which the aggregates fold. |
| `row_groups_in`, `row_groups_pruned`, `row_groups_full_match`, `cols_stat_pruned` | Row groups seen, skipped by statistics, known to match the filter whole; columns answered from statistics. |
| `agg` | The answer: an object of alias → value, or with `group_by` an array of such objects, one per group. |
| `total_ms` | Time inside the handler. |
| `wall_ms` | Wall-clock phases in ms: `open`, `parse`, `scan`, `footer`. |
| `worker_ms` | Each scan step summed over the workers, in ms: `decode`, `filter`, `fold`, `encode` (0), `sink`. With N workers a step can reach N times `wall_ms.scan`. |
| `membership` | Bloom filter and dictionary pruning, below. |
| `pool` | Connection pool counters, below. |

**Write** (`output_url` set, no aggregate): `ok`, `output` (the URL, escaped), `files_in`, `rows_in`, `rows_scanned`,
`rows_matched` (the rows written), `bytes_in`, `bytes_out`, `row_groups_in`, `row_groups_kept`, `total_ms`; `wall_ms`
with `open`, `parse`, `scan`, `footer`, `mp_await`, `mp_complete`; `worker_ms` with `decode`, `filter`, `fold` (0),
`encode`, `sink`, summed over the workers; `meta_cache` with the
container's cumulative footer-cache counters `hits`, `misses`, `revalidations`, `invalidations`, `inserts`,
`evictions`; `membership`; and `pool`.

**`membership`** (both responses), for row groups an `=` or `IN` filter ruled out beyond their statistics (they are
also counted in `row_groups_pruned`):

| Field | Meaning |
|-------|---------|
| `bloom_pruned`, `dict_pruned` | Row groups a Bloom filter, or failing that a fully dictionary-encoded chunk's dictionary, proved free of every literal. |
| `filters`, `dictionaries` | Filters and dictionaries probed. |
| `requests`, `bytes` | S3 requests made, and bytes fetched over HTTP, to read them (0 for local files, for filters the warm cache held, and for bytes copied from the retained suffix). |
| `suffix_bytes` | Bytes of those filters and dictionaries copied from the retained suffix rather than fetched. |
| `bloom_stopped`, `dict_stopped` | The first eight row groups probed pruned too few to pay for reading the rest of that kind. |
| `request_kb` | A request's fixed cost, as the KiB a connection moves meanwhile, that the last read plan assumed (S3 only). |
| `us` | Time spent, in microseconds. |

**`pool`** (both responses), for the sandbox's S3 connection pool during this invocation. On a cold invocation, the
intended pattern, the pool starts empty: `reuses` counts only connections reused within this invocation,
`idle_evictions` is 0, and the `meta_cache` counters cover only this invocation. Idle evictions, stale retries and
cache hits from earlier invocations appear only when Lambda reuses a sandbox, which zpq handles but does not rely on.

| Field | Meaning |
|-------|---------|
| `acquires`, `opens`, `reuses`, `discards` | Connections taken from the pool; of those, newly opened or reused idle ones; connections closed after a failure. |
| `idle_evictions` | Idle connections closed unused because they had been idle more than 4 s (counted across a frozen sandbox). |
| `stale_retries` | Requests retried at once on a new connection after a reused one turned out dead. |
| `backoff_retries` | Requests retried after a backoff: throttling, server errors, connect failures. |
| `max_idle_ms` | The longest any idle connection had waited when a request looked at it. |
| `acquire_wait_ms`, `acquire_lock_ms` | Time spent waiting for a connection permit, and holding the pool lock. |

Response changes: the aggregate and write responses gained `profile` and `profile_trace` when asked for; `phase` is
replaced by `wall_ms` and `worker_ms` (the CLI summary's, defined in [output-format.md](output-format.md)), whose worker
steps sum every worker's time; the write response's `rows_kept` is now `rows_matched`, beside the new `rows_scanned`,
which the aggregate response also gained with `rows_matched`. The aggregate response gained `row_groups_full_match` and
`pool`; in the write response `output` is now JSON-escaped, and `pool` moved after `meta_cache` and gained
`idle_evictions`, `stale_retries`, `backoff_retries` and `max_idle_ms`.

**`profile`** (aggregate and write responses, when the request set `profile` or `profile_url`): the profile summary
described in [profiling.md](profiling.md), as one object: `total_ns`, `idle_ns`, `threads`, `events`, `dropped`,
`event_cap`; `cpu` (this run's `user_ns`, `sys_ns`, `minor_faults`, `major_faults`; `peak_rss`, this invocation's peak
resident memory, `start_rss`, the resident memory when it began, and `peak_rss_scope`, how it was measured: `run`,
`sampled` or `process`, see [profiling.md](profiling.md); `ledger_peak` and `ledger_limit`, the most the query's memory
ledger held at once and its limit, `--max-memory`, both 0 when no ledger ran); `meta` (`zpq_version`, `build_mode`,
`target`, `cpu_count`, `parallelism`, `memory_bytes`, `on_lambda`, `lambda_memory_mb`, `start_unix_s`, `query`, the
request cut to 512 bytes);
`phases` (per phase: `wall_ns`, `busy_ns`, `share_ns`, `alone_ns`, `spans`); `chunks`, `chunk_ns` and `top_chunks` (the
ten costliest column chunks: `file`, `rg`, `column`, `codec`, `encoding`, `bytes_in`, `bytes_out`, `ns`,
`decompress_ns`, `pages`); `io` (`gets`, `puts`, `bytes`, `ttfb_ns`, `body_ns`, `acquire_ns`, `reused`, `retries`,
`errors`, `ranges`, `merged`, `fetch_jobs`, `mapped_files`, `mapped_bytes`, `peak_in_flight`, `peak_bytes_in_flight`);
`prune` (`row_groups`, `skipped`, `full_match`, `filtered`, `pages`, `pages_skipped`, `stats_answered`, `skipped_by`:
filter column → row groups its statistics skipped); `workers` (`count`, `user_ns`, `sys_ns`, `minor_faults`); `engine`
(`read_ns`, `parse_ns`, `decode_ns`, `decode_wall_ns`, `eval_ns`, `agg_ns`, `encode_ns`: the sums behind `phase`, in
nanoseconds); and `peak_buffered_bytes`. Times are nanoseconds. The member's size does not grow with the query (names
are cut to 128 bytes, and only ten chunks and five skip reasons are listed), so it stays a few KB, far under Lambda's 6
MB response limit; the trace itself never rides in the response.

**`profile_trace`** (when the request set `profile_url`): `{"url": U, "bytes": N, "omitted_events": K}` after uploading
the Chrome trace to `U` with one PUT over the invocation's connection pool, or `{"url": U, "error": E}` when the upload
failed (`E` is `BadProfileUrl` for a URL that is not `s3://bucket/key`). The trace is cut at 1/16 of the memory budget
(1 to 256 MiB); `K` counts the events left out. A failed upload does not fail the query, and neither does a summary that
cannot be made: `profile` is then `{"error": E}`. The trace holds the full `s3://` path of every input, the column names
and the request body; see [What a profile contains](profiling.md#what-a-profile-contains).

Profiling a request costs its own time (a few hundred microseconds to start the RSS sampler and summarize, and the
spans themselves; see [profiling.md](profiling.md)). A request without `profile` pays nothing for it.

### Multipart writes whose answer is lost

A write of more than one part goes through S3 multipart upload. If the CompleteMultipartUpload answer is lost on the
network, zpq HEADs the output key and reports success only if the object's ETag is the one the uploaded parts complete
to; otherwise the write fails as `{"error":"engine","reason":"ClosedBeforeResponse"}` (previously `BadStatusLine`, even
when the object had been written) and the upload is aborted. If the CreateMultipartUpload answer is lost, the write
fails with `CreateMultipartFailed` and zpq aborts the in-progress uploads of exactly that key that started after it sent
the request. That cleanup is best effort, so give the output bucket a lifecycle rule that aborts incomplete multipart
uploads (for example `AbortIncompleteMultipartUpload` with `DaysAfterInitiation: 1`).

## Errors

A request that cannot be read answers `{"error":"bad_json","reason":R}`, plus `"field":F` when R is `BadFieldType`.
A query the engine rejects answers `{"error":"engine","reason":R}`, plus `"column":C` when the error is about one
column (for example `UnknownColumn`, `AmbiguousColumn`, `DuplicateOutputColumn`, `AmbiguousOutputColumn`), and
`"input":I`, with `"cause":E` when known, when it is about one input file. An empty event answers
`{"error":"empty_body"}`.

## Lambda request changes

The event used to be read by searching for each field's key anywhere in the body and taking the text up to the next
`"`. Requests of these shapes used to be accepted and are now rejected:

- Malformed JSON: syntax errors, truncation, text after the object, unknown escapes (`\q`), unpaired or reversed
  surrogate escapes, invalid UTF-8, raw control characters in strings. `bad_json` with the scanner's reason.
- A known field given twice. `bad_json`/`DuplicateField`; the first one used to win.
- A known field of the wrong type. `bad_json`/`BadFieldType`, naming the field:
  - a string field (`s3_url`, `filter`, `select`, `aggregate`, `group_by`, `column_order`, `output_url`,
    `output_codec`) holding anything but a string;
  - `columns` as a comma-separated string, which used to be ignored (every column was written), or an array holding
    a non-string;
  - `inputs` that is not an array of strings, which used to fall back to `s3_url`;
  - `scan_all` or `trust_stats` holding anything but `true`/`false`, which used to read as false;
  - `max_memory` that is negative, not a number or size string, or a size string that does not parse, which used to
    be ignored in favour of the detected memory; and a `max_memory` of 0, which used to be accepted.
- A `columns` entry naming no column. `engine`/`UnknownColumn`, naming it; it used to be dropped from the output.

Accepted requests whose meaning changed: string escapes (`\"`, `\\`, `\n`, `\uXXXX`) are decoded rather than passed
through, so quoted column names and string literals holding a quote work; a `columns` entry is no longer split at
commas; and a field's key appearing inside another field's string value is no longer mistaken for the field.
