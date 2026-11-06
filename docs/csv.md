# CSV input

zpq writes a CSV file to Parquet whole:

```sh
zpq query data.csv -o data.parquet
zpq query --query "SELECT * FROM 'data.csv'" -o data.parquet
```

Every writer option applies (`--codec`, `--row-group-size`, `--row-group-bytes`, `--bloom-filter`, `-j`,
`--max-memory`, `--summary=json`). Columns are chosen by name, in the order given, with `--columns a,b` or
`SELECT a, b FROM 'data.csv'`; the fields not written are still framed (a malformed one is refused) but not typed.
Filters, expressions, renames, ORDER BY, LIMIT and aggregates are refused: they read the Parquet it writes.

## Which inputs are CSV

A file whose first and last four bytes are `PAR1` is Parquet, whatever its name. Any other local file is CSV when it
is named `.csv`, `.tsv` or `.txt` (any case), or when `--input-format csv` is given; otherwise zpq refuses it. One CSV
input per query; S3 CSV input is not supported yet.

## Dialect

- RFC 4180: a field is quoted with `"`, and a quote inside a quoted field is doubled. A quote anywhere else, text after
  a closing quote, or a quote left open at the end of the file is refused. Backslash escapes are not read.
- Records end in LF or CRLF (mixed is fine); quoted fields may contain both. A last record without a newline is read.
  A blank line holds no record.
- UTF-8; a leading BOM is skipped. UTF-16 and UTF-32 are refused.
- The delimiter is the one of `,` tab `;` `|` that splits the first records into the most fields (`--csv-delimiter`
  overrides it). Every record must have as many fields as the first, or the file is refused.
- The first record is a header unless every field of it fits its column's type and some column is not text
  (`--csv-header`, `--csv-no-header`). Empty names become `columnN`; repeats get `_1`, `_2`.

## Values and types

An unquoted empty field is NULL; a quoted empty field is the empty string. `--csv-null STR` makes an unquoted `STR`
NULL too.

A column's type holds every one of its values exactly, or the column is `VARCHAR`. No value is rounded, truncated or
reinterpreted:

| type | its values |
|---|---|
| BIGINT | `-?(0\|[1-9][0-9]*)` in range: no `+`, no leading zeros (`02134` is text), no padding, no `-0` |
| DECIMAL(p,s) | canonical decimals up to 38 digits, and integers too wide for BIGINT; precision is the data's |
| DOUBLE | exponent, `inf` or `nan` text whose double has the same digits: at most 15 significant digits, no overflow or underflow (`1e-400` is text) |
| DATE | `YYYY-MM-DD`, a valid date |
| TIMESTAMP | `YYYY-MM-DD[ T]HH:MM:SS[.f]`, microseconds; nanoseconds (TIMESTAMP_NS) when a fraction needs them and every value is within 1677-2262 |
| TIMESTAMP WITH TIME ZONE | the same with `Z` or `±HH:MM` on every value, stored as UTC instants |
| BOOLEAN | `true`, `false`, any case |
| VARCHAR | anything else in UTF-8 |
| BLOB | a column with bytes that are not UTF-8 |

Types are sampled from the start of the file and checked on every value. A later value its column's type cannot hold
stops the write; the whole file is classified, the column widens to the least type holding everything, and the write
starts again, so the file is the one a whole-file sample writes. `--csv-strict` refuses instead, and
`--csv-types COL:TYPE,...` pins a column's type, refusing a value that does not fit it.

## Refusals

A refused file exits 1 and leaves no output. The message says where: the line (counted from 1, newlines inside quoted
fields included, as an editor counts them), the byte offset (from 0) and the column.

## Memory and speed

A cold file is read ahead of the parse through the local reader (`O_DIRECT` io_uring where the file system allows it);
a file already in the page cache is read through its mapping. Chunks of the file are parsed in parallel, each finding
its first record from the exact quote state at its first byte, and written through the same encoder as a Parquet
rewrite. Bytes in flight are drawn from `--max-memory`; a write the budget refuses runs again on fewer workers.
Row groups are cut at a sixteenth of the budget at most, which below a 1 GiB budget makes them smaller than the
default target.
