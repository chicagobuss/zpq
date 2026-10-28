# CLI output contract

Moved from the README for 0.5.0. The full output contract is [output-format.md](output-format.md).

Every query emits answer rows as NDJSON by default, including scans, projections, scalar aggregates and grouped
aggregates. Each line is a row object; scalar aggregates produce one row. `jsonl` remains an alias for `ndjson`.
`--format json` emits an array of row objects, `--format csv` RFC 4180 CSV, and `--format parquet` Parquet. These
flags apply to SQL and flag queries alike.

CSV has a header row, then one row per answer row, for scans and aggregates alike. A NULL is an empty, unquoted field
and an empty string `""`; a field is quoted when it holds a comma, a quote or a line break. BOOLEAN prints `true` /
`false`, DECIMAL every digit of its scale, DATE `YYYY-MM-DD`, TIME `HH:MM:SS.fff` and TIMESTAMP (INT96 included)
`YYYY-MM-DD HH:MM:SS.fff`, with the column's own number of fractional digits and a trailing `Z` when it is adjusted to
UTC. Bytes follow the JSON rules ([docs/binary-output.md](binary-output.md)): text as text, a UUID as
8-4-4-4-12, and what JSON prints as an object (bytes that are not UTF-8, BSON and GEOMETRY, INTERVAL) as that object's
JSON text in a quoted field. An average is one column; an `avg_state` is two, `<name>__sum` and `<name>__count`, as a
Parquet write holds it. `--limit` and SQL `LIMIT` / `OFFSET` count rows, never the header.

```sh
zpq query input.parquet --limit 10                          # NDJSON rows
zpq query input.parquet --aggregate 'count(*) AS n'          # {"n":...}
zpq query --query "SELECT k, count(*) AS n FROM 'input.parquet' GROUP BY k" --format json
zpq query input.parquet -o answer.parquet                    # file only; stdout empty
zpq query input.parquet -o answer.json --format json         # JSON file
zpq query input.parquet --format csv > rows.csv                # CSV, header row first
zpq query input.parquet -o - --format parquet > answer.parquet
zpq query input.parquet --summary=json > rows.ndjson 2> diagnostics.ndjson
```

A named `-o FILE` defaults to Parquet regardless of its extension and leaves stdout empty. Explicit `--format`
overrides that default. `-o -` selects stdout and defaults to NDJSON; explicit Parquet also streams to stdout without
`-o`. Binary stdout refuses a terminal. Limited Parquet row output rejects nested leaves and INT96; unlimited
passthrough preserves both. A limited Parquet write of FLOAT16 widens to DOUBLE, as filtered writes do.
Parquet aggregate output rejects integer sums outside int64; text output retains exact i128 sums.
An S3 `-o` takes Parquet copies and filtered writes only: text formats, aggregate writes and limited Parquet writes
to S3 are unsupported. `--explain` still prints one aggregate plan JSON object without executing or writing an answer,
even when `-o` is supplied.

`--limit N` caps final result rows for every format and destination, after grouping and ordering. SQL LIMIT has the
same semantics; combining it with `--limit` is an error. LIMIT 0 validates the query and schema without decoding data
pages. Empty NDJSON is zero bytes, empty JSON is `[]` followed by a newline, and empty Parquet retains its schema.
A scalar aggregate over empty input still emits one row unless the final limit or offset excludes it. Column order,
input/group order, exact numeric digits and nulls are the same in every format: avg is one value, avg_state its
`{sum,count}` object in JSON and its split columns in CSV and Parquet. Grouped results still materialize.

Successful queries are silent on stderr unless diagnostics were requested. `--summary=json` emits one typed
`{"type":"summary","ok":true,"rows":N,"output":...}` line after the answer is flushed and a named file is published.
It counts rows four ways: `rows` in the answer, `rows_in` in the inputs, `rows_scanned` in the row groups read, and
`rows_matched` kept by the filter. Its times are wall clock (`total_ms`, `output_us`, `wall_ms`) or, in `worker_ms`,
summed over the workers, so a `worker_ms.decode` above `total_ms` means several workers decoded at once.
[docs/output-format.md](output-format.md) defines every field. `total_us` includes formatting, flushing and
publication and excludes writing the summary. In this mode failures emit a typed `error` line and JSON profile
summaries a typed `profile` line; text profile summaries are rejected. The libzpq, service and Lambda responses carry
the same row counters and time objects.

Exit status is 0 for success, 1 for read/decode/write failure, 2 for invalid or unsupported options/queries, and 141
when a consumer closes stdout. Disk-full errors remain failures. Named local outputs use an exclusive sibling
temporary file and atomic rename: failures remove the temporary and preserve the old target. Input/output aliases,
including hard links, are rejected. This guarantees atomic visibility, not crash durability. S3 success follows PUT
or multipart completion; network errors can leave completion ambiguous and do not promise POSIX rename semantics.
Stdout and shell redirection cannot roll back: a failed NDJSON stream may end mid-record, JSON may lack its closing
bracket, and Parquet may lack its footer. Check exit status, including `set -o pipefail` in shell pipelines.
