# Footer inspection

`zpq inspect FILE... [--format json] [--detail]` reports each input without decoding its data values.
Local paths and S3 URLs use the same footer parser as queries. Inspect accepts explicit lists and quoted,
basename-only single-`*` globs, sorted lexically; directory wildcards and recursive patterns are unsupported.
`schema` takes one path/URL. Empty matches are errors. A file that cannot be opened/parsed is reported in place,
the other explicit files are still inspected, and the command exits 1. Invalid options exit 2.

JSON is one object with `schema_version: 1` and a `files` array in input order. A successful file has:

- `path`, `status`, `parquet_version`, `created_by` (null when absent), `rows`, `bytes`, `footer_bytes`;
- `schema`: leaves in schema order, each with a **path segment array**, physical/logical/converted type and repetition;
- `row_groups`: footer order, ordinal, rows, uncompressed bytes, `sorting_columns`, and `columns`;
- `io`: requested range bytes/count, detail bytes and limits. Local I/O uses a read-only mapping and touches only
  the requested metadata ranges; requested bytes are not page-fault or physical-device-byte measurements.

Each column retains its complete nested path, types, value count, codec, footer encoding list, compressed and
uncompressed bytes/ratio, data/dictionary offsets, dictionary presence and fallback evidence, null count, NaN count
(`nan_count`, null when the writer did not record it),
column-order interpretation, min/max, index/Bloom offset-length presence, detail results and evidence-based hints.
An error entry has `path`, `status: "error"`, and a named `cause`; it is not an empty successful footer.

## Statistics and evidence

Bounds carry `state`, `kind`, `value`, `raw_hex`, `exactness` and `source`. Values are strings so JSON consumers
cannot round a 64-bit integer or a scaled 128-bit decimal. Decimal `value` is an unscaled integer with a decimal
exponent (for example `123e-2`), accompanied by its schema's precision/scale. Dates/times/timestamps retain their
integer physical value **and their logical unit/UTC annotation**; no guessed calendar or time-zone conversion.
Binary values are hex; validated UTF-8 STRING values are text. INT96 interpretation is unsupported and retains hex.
Exactness is `exact`, `inexact` or `unknown`, based only on explicit footer flags, never assumed from a bound.
Deprecated min/max are labelled separately. `bounds_usable` states whether the engine can read the declared order;
raw metadata may exist even when a higher-level reader hides statistics from a known problematic writer.
The report describes writer claims, not independently verified extrema or actual sortedness.

Presence states are `absent`, `present` and `malformed` (for invalid pointer/length combinations). Missing lengths
on older Bloom/dictionary metadata remain null, rather than zero. Unknown information remains null or `unknown`;
unimplemented interpretation is `unsupported`. Legacy zero dictionary offsets are retained as malformed evidence,
even though the query parser normalizes them away. `sorting_columns` is currently **unsupported**: the parser does
not retain the field, so inspect never says the file is unsorted. Footer encoding sets do not prove dictionary
fallback. Fallback is reported only when `encoding_stats` counts data-page encodings, otherwise `unknown`.

## Bounded details

Default inspection reads just the leading magic and footer/trailer (at most 64 MiB of footer). `--detail` additionally
reads page-header windows and declared page indexes/Bloom headers, with per-file ceilings of **8 MiB**, **128 range
requests** (including footer requests), and **256 observed pages**. It skips compressed payloads; it does not decode
whole data pages or a Bloom bitset. Page-header windows may include a bounded prefix of compressed payload bytes.
Detail state says `unknown-limit` or `unknown-header-limit` when a ceiling is reached; partial observations are not
presented as a complete encoding census. External column-file/encrypted chunk details are not inspected.

Index validation is structural (parse, vector lengths, page ranges), not a proof that page bounds match data values.
Bloom validation covers a supported header and declared extent, not a proof about the bitset's membership behavior.
Unknown header algorithms are unsupported; malformed framing/ranges are reported separately. Dictionary fallback
can be independently examined in the observed page encodings when detail coverage is complete.

S3 uses bounded Range GETs and checks their status, lengths and Content-Range. Configure region/credentials as for
query (`AWS_*` or `S3_*`; anonymous reads use `S3_NO_SIGN_REQUEST=1`). `io.range_requests` counts logical GET calls;
transport retries can add wire requests, so their count is explicitly unknown. A real S3 server must honor ranges.
The network layer's existing response-body ceiling still applies if an endpoint ignores a range request.
No query scan, dictionary refinement, listing-derived schema inference or data-cache warming is performed by inspect.
