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
| `scan_all`, `trust_stats` | boolean | As `--scan-all`, `--trust-stats`. |
| `max_memory` | non-negative number, or size string (`"512MB"`) | As `--max-memory`. |

A `columns` entry is one name, commas included; entries are trimmed, and empty entries or an empty array mean no
projection. Every field may be `null`, which means absent. Unknown fields are ignored; the parser skips them
without building them, so they cost no memory. With `aggregate` or `group_by` the response is the aggregate JSON;
otherwise with `output_url` it is the write summary.

## Responses

Every response is one JSON object. Durations are whole milliseconds; `pool` and `phase` describe this invocation only.

**Aggregate** (`aggregate` or `group_by` set):

| Field | Meaning |
|-------|---------|
| `ok` | `true`. |
| `files_in`, `rows_in`, `bytes_in` | Inputs read, their rows, and the bytes fetched or mapped. |
| `row_groups_in`, `row_groups_pruned`, `row_groups_full_match`, `cols_stat_pruned` | Row groups seen, skipped by statistics, known to match the filter whole; columns answered from statistics. |
| `agg` | The answer: an object of alias → value, or with `group_by` an array of such objects, one per group. |
| `total_ms` | Time inside the handler. |
| `phase` | `read_ms`, `decode_ms`, `eval_ms`, `encode_ms`. |
| `pool` | Connection pool counters, below. |

**Write** (`output_url` set, no aggregate): `ok`, `output` (the URL, escaped), `files_in`, `rows_in`, `rows_kept`,
`bytes_in`, `bytes_out`, `row_groups_in`, `row_groups_kept`, `total_ms`; `phase` with `read_ms`, `parse_ms`,
`decode_ms`, `eval_ms`, `encode_ms`, `sink_ms`, `footer_ms`, `mp_await_ms`, `mp_complete_ms`; `meta_cache` with the
container's cumulative footer-cache counters `hits`, `misses`, `revalidations`, `invalidations`, `inserts`,
`evictions`; and `pool`.

**`pool`** (both responses), for the container's S3 connection pool during this invocation:

| Field | Meaning |
|-------|---------|
| `acquires`, `opens`, `reuses`, `discards` | Connections taken from the pool; of those, newly opened or reused idle ones; connections closed after a failure. |
| `idle_evictions` | Idle connections closed unused because they had been idle more than 4 s (counted across a frozen sandbox). |
| `stale_retries` | Requests retried at once on a new connection after a reused one turned out dead. |
| `backoff_retries` | Requests retried after a backoff: throttling, server errors, connect failures. |
| `max_idle_ms` | The longest any idle connection had waited when a request looked at it. |
| `acquire_wait_ms`, `acquire_lock_ms` | Time spent waiting for a connection permit, and holding the pool lock. |

Response changes: the aggregate response gained `pool`; in the write response `pool` moved
after `meta_cache` and gained `idle_evictions`, `stale_retries`, `backoff_retries` and `max_idle_ms`.

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
column (`UnknownColumn`, `AmbiguousColumn`, `DuplicateOutputColumn`, `AmbiguousOutputColumn`).

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
    be ignored in favour of the detected memory.
- A `columns` entry naming no column. `engine`/`UnknownColumn`, naming it; it used to be dropped from the output.

Accepted requests whose meaning changed: string escapes (`\"`, `\\`, `\n`, `\uXXXX`) are decoded rather than passed
through, so quoted column names and string literals holding a quote work; a `columns` entry is no longer split at
commas; and a field's key appearing inside another field's string value is no longer mistaken for the field.
