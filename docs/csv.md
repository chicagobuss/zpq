# CSV input

zpq reads a CSV file directly. It can write it to Parquet, whole or filtered and projected, or aggregate it:

```sh
zpq query data.csv -o data.parquet
zpq query --query "SELECT a, b * 2 AS c FROM 'data.csv' WHERE a > 0" -o out.parquet
zpq query --query "SELECT k, count(*) AS n, sum(v) AS s FROM 'data.csv' GROUP BY k"
```

What a CSV query can do today:

| query | over a CSV |
|---|---|
| the whole file to Parquet | yes |
| columns by name (`--columns a,b`, `SELECT a, b`) | yes, in the order given |
| filters, expressions, renames (`--filter`, `--select`, `WHERE`, `SELECT b * 2 AS c`) | yes, row by row as the file is parsed |
| aggregates, with or without `GROUP BY`, with `HAVING`, `FILTER (WHERE ...)` and `DISTINCT` | yes, folded as the file is parsed |
| `ORDER BY`, `LIMIT`, `OFFSET` | refused |
| joins, more than one input, S3 input | refused |

A refused query says so by name, and the way round it is to write the Parquet first and query that:
`zpq query data.csv -o data.parquet`, then the query over `data.parquet`.

## Which inputs are CSV

A file whose first and last four bytes are `PAR1` is Parquet, whatever its name. Any other local file is CSV when it
is named `.csv`, `.tsv` or `.txt` (any case), or when `--input-format csv` is given; anything else is read as Parquet
and refused as Parquet when it is not. One CSV input per query; S3 CSV input is not supported yet.

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
stops the query; the whole file is classified, the column widens to the least type holding everything, and the query
starts again, so the answer is the one a whole-file sample gives. `--csv-strict` refuses instead, and
`--csv-types COL:TYPE,...` pins a column's type, refusing a value that does not fit it.

A query that does not bind to the sampled types (a column NULL throughout the sample, say, that a filter compares with
a number) is bound again to the types of the whole file before it runs; one that still does not bind is refused as it
would be over Parquet.

## Filters, expressions and columns

Columns named with `--columns` or a plain `SELECT a, b` are written in the order given. The fields not written are
still framed (a malformed one is refused) but not typed.

Filters, expressions and renames are bound as they would be over the Parquet the file writes to, and run on each
parsed chunk: only the fields they read are typed. A DECIMAL keeps the file column's type, past 18 digits as well.

## Aggregates

An aggregate over a CSV is bound as it would be over the Parquet the file writes to, and folded as the file is parsed:
each chunk parses only the fields the query reads (its calls, keys and filter) and folds its rows into its own
aggregate states, which then merge in file order. Nothing is written. The answer is the one the same query gives over
that Parquet, and the same at every `-j`: the chunks a file is cut into, and the order their states merge in, do not
depend on the number of workers. A floating-point result (a sum of DOUBLEs, a variance) can differ from the one over
Parquet in its last digits, since its values are added in a different order.

- Functions: `count`, `count(DISTINCT)`, `sum`, `min`, `max`, `avg`, `var_samp`, `var_pop`, `stddev_samp`,
  `stddev_pop`, `approx_count_distinct`, `quantile_cont`, `string_agg`, `arg_max`, `arg_min`, as over Parquet.
- Exact sums: integer sums are exact, and a DECIMAL's sum is exact to 38 digits (`DECIMAL(38, s)`); `avg` of a
  DECIMAL adds up to four digits past its scale, from the data's precision, as over Parquet.
- `GROUP BY` takes columns and expressions, groups come out in the order they first appear (unless `--unordered`),
  and `HAVING` filters them.
  `SELECT DISTINCT` is a `GROUP BY` of its columns.
- `count(*)` alone still frames every record (a malformed one is refused), and parses only the first field.
- `--explain` prints the plan of the fold instead of running it: the fields each chunk parses and their types, the
  number of chunks and workers, and the budget. Its types are the sample's, unless binding settled them; a later value
  can still widen one when the query runs.

`ORDER BY`, `LIMIT` and `OFFSET` over an aggregate's answer are refused, as are joins.

## Refusals

A refused file exits 1 and leaves no output. The message says where: the line (counted from 1, newlines inside quoted
fields included, as an editor counts them) and the byte offset (from 0) of the field or record at fault, and its
column where a field is: the malformed field, the value that does not fit its type, or, for a record with the
wrong number of fields, the first field missing or the first past the header's (named `columnN` there, 0-based). An
empty or UTF-16 file is refused as a whole: the message names the file, and no line, byte or column.

A query shape a CSV input does not run (above) is refused before the file is read, by name
(`CsvQueryNotSupported`).

## Memory

A cold file is read ahead of the parse through the local reader (`O_DIRECT` io_uring where the file system allows it);
a file already in the page cache is read through its mapping. Chunks of the file are parsed in parallel, each finding
its first record from the exact quote state at its first byte.

Everything a CSV query holds is drawn from `--max-memory`: the bytes in flight, a write's row groups, and an
aggregate's chunk states, group tables, count(DISTINCT) sets, string min/max values and answer. A write the budget
refuses runs again on fewer workers. An aggregate the budget refuses (more groups or distinct values than it holds) is
refused by name (`ExceededMemoryBudget`), not grown past it. Row groups are cut at a sixteenth of the budget at most,
which below a 1 GiB budget makes them smaller than the default target.
