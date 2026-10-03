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
