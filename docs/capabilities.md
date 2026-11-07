# What zpq can do

Moved from the README for 0.5.0: what each part of zpq reads, filters, computes, aggregates and writes.

**Read** standard Parquet — V1 + V2 data pages; PLAIN / RLE / RLE_DICTIONARY / DELTA_BINARY_PACKED /
DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY / BYTE_STREAM_SPLIT encodings; SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED
codecs, plus the deprecated LZ4 codec in each container writers used for it (parquet-mr's Hadoop framing, a bare LZ4
block, an LZ4 frame); all physical types including FIXED_LEN_BYTE_ARRAY; flat, struct, and LIST/MAP nested schemas.
Row output and filters refuse repeated columns (LIST, MAP, legacy REPEATED); copies keep them. Bytes print losslessly:
text when UTF-8, else `{"base64":...}`; UUIDs and INTERVALs in their own forms
([docs/binary-output.md](binary-output.md)).
Encrypted Parquet is refused: zpq does not decrypt.

**Filter** with `= != <> < <= > >=`, `IN` / `NOT IN`, `[NOT] BETWEEN x AND y`, `[NOT] LIKE`, `[NOT] ILIKE`, `IS [NOT] NULL`, `AND` /
`OR` / `NOT` and grouping parentheses (nested at most 32 deep), plus row-group stat pruning and Hive-partition pruning
before any bytes are fetched. Keywords and parentheses inside a quoted literal are the literal's; a quote inside one is
doubled (`'O''Reilly'`), and an unterminated quote is an error. NOT is three-valued: a row whose operand is NULL
passes neither `p` nor `NOT p`. `ILIKE` takes a literal ASCII pattern and folds ASCII letters (and the two characters whose lower case is
`k` and `i`: KELVIN SIGN and I WITH DOT ABOVE) as DuckDB's `lower` does; a pattern with another non-ASCII character is refused
(`IlikePatternNotAscii`). DATE / TIME / TIMESTAMP columns take quoted literals
(`'2024-01-01'`, `'2024-01-01 12:00:00.25'`, `'12:00:00'`) compared exactly: a literal finer than the column's unit is
not rounded to it. In SQL a literal may be typed and a constant folded: `DATE '1998-09-02'`, `TIMESTAMP '1998-09-02
10:00:00'`, `CAST('1998-09-02' AS date)`, `'1998-09-02'::DATE`, `CAST('0.055' AS DECIMAL(15,2))` (0.06: ties round away
from zero, as in DuckDB), `DATE '1998-12-01' - INTERVAL '90' DAY` (DuckDB adds an interval to a DATE as a TIMESTAMP,
the months first with the day clamped to the month's length), `.06 - 0.01`, `24 + 1`. A typed literal compares with a
column of its own kind: DATE and TIMESTAMP with DATE and TIMESTAMP columns (a TIMESTAMP against a DATE keeps its time of
day, as DuckDB compares them as TIMESTAMPs), DECIMAL with a numeric column. Against any other column it is refused
(`TypedLiteralColumn`), as is an INTERVAL anywhere but added to or subtracted from a DATE or TIMESTAMP literal, a
fraction of a whole-number unit, a cast to another type, and `/` or `%` in a constant. Constants keep DuckDB's types and
ranges, and what DuckDB refuses is refused by name, never wrapped, clamped or aborted on: an integer constant has the
narrowest of INTEGER, BIGINT and HUGEINT that holds it after its sign (`-2147483648 - 1` overflows INTEGER), an INTERVAL
component fits its field (32-bit months and days, 64-bit microseconds) before any other can cancel it, a TIMESTAMP stays
within year 294247 (a DATE plus an INTERVAL included), a decimal stays within 38 digits, and a DATE or TIMESTAMP
literal that does not convert to a column's timestamp type (`TemporalOutOfRange`: TIMESTAMP_NS stops at year 2262)
compares with nothing, and a constant nested more deeply than the thread's stack allows folding (the budget is read from
the thread, not fixed) is refused (`ConstantTooDeep`).
In SQL a WHERE comparison may also set two expressions against each other (`l_commitdate < l_receiptdate`,
`l_extendedprice * (1 - l_discount) > 50000`, `abs(a - 25) < z`). Numbers of any lane compare exactly unless one side is
DOUBLE, strings byte by byte, and times only with times in the same unit. A FLOAT column, or abs, coalesce, least,
greatest, floor, ceil or CASE of FLOATs alone, compares as FLOAT; FLOAT arithmetic (`f + i`, `round(f)`) is refused,
since SQL rounds each of its steps to FLOAT and zpq computes in DOUBLE. A row where either side is NULL passes neither
the comparison nor its NOT. A side is evaluated only over the rows earlier conjuncts kept, so `z > 0 AND b / z > 3`
divides no zero. No statistic prunes such a comparison yet. LIKE, IN and BETWEEN still take literals on their value
side, as do FILTER clauses and CASE conditions.

**Set operations** (`zpq query --query`): `UNION [ALL]`, `INTERSECT` and `EXCEPT` of two or more SELECTs, each an ordinary
query of its own (a scan, an aggregate, a GROUP BY, a DISTINCT, `SELECT *`), grouped as DuckDB groups them (INTERSECT
first, then UNION and EXCEPT left to right, parentheses honoured), with the trailing ORDER BY, LIMIT and OFFSET applying to the whole
result and a parenthesized SELECT keeping its own. The result is named by the first SELECT. NULLs are equal in UNION,
INTERSECT and EXCEPT, as are all NaNs and the two zeros (which one is printed is arbitrary, as in DuckDB). Each SELECT runs
to a scratch Parquet file in its own directory beside the output (in `/tmp` for stdout, and when a branch reads from the
output's directory), a pass renames and casts its columns, and the operator is a query over those files; the scratch
directory is removed when the query ends or fails, and each branch releases its memory before the next starts. The output
may not be a file any branch reads, as for a plain query (`BadArgs`, a hard link or symlink to it included). The
two sides' column types must be equal, or integers of any width (BIGINT), or an integer with a DOUBLE (DOUBLE): any other
difference is `SetOperationTypes` (a FLOAT with a DOUBLE, DECIMALs of different scales, a DATE with a TIMESTAMP, text with a
number), a different column count is `SetOperationColumns`, and a BOOLEAN, FLOAT, unsigned-integer or non-microsecond TIMESTAMP column that is
nullable on one side only, or of another unsigned width, is `SetOperationNullability` or `SetOperationTypes`. `INTERSECT ALL`, `EXCEPT ALL` and `UNION BY NAME` are refused
(`SetOperationAll`, `SetOperationByName`), as are an ORDER BY or LIMIT before an operator without parentheses
(`SetOperationOrder`) and a parenthesized combination with its own ORDER BY or LIMIT (`SetOperationNesting`). `--explain`
is not available for them, the in-process SQL requests (libzpq, the service, Lambda) do not carry them yet, and
`--summary=json` describes the last query. Integers of different widths come out as INT64.

**Project** by column name, nested-aware: `events` resolves to every `events.list.element.*` leaf.

**Compute** new columns via `--select`: arithmetic on numerics with promotion (`i32 + f64 → f64`), string concat (`||`),
`coalesce(a, b, ...)` over any expressions, `nullif(col, literal)` (NULL where the column equals the literal; a CASE, so
a column and a literal) and `abs(x)` of an integer, DOUBLE or DECIMAL expression (the magnitude in the argument's type:
the minimum of an integer column's SQL type, `abs(-2147483648)` of an INTEGER, is an overflow error, as DuckDB's),
`CAST(x AS TINYINT | SMALLINT | INTEGER | BIGINT | DOUBLE | DECIMAL(p, s))` and `x::TYPE` of a number (a DECIMAL
rounds half away from zero to an integer or to fewer places, a DOUBLE half to even to an integer and as `round(x * 10^s)`
to a DECIMAL; a value that does not fit stops the query with a conversion error, as DuckDB does, but only in rows that
are read and not NULL; the result is written as INT64 for every integer type), `TRY_CAST(x AS type)` of every cast
here (NULL where `CAST` would stop the query, so the result can be NULL; a TIMESTAMP_MS before 290309-12-22 (BC) stays
a TIMESTAMP in it, as DuckDB's does), a text read as TINYINT, SMALLINT, INTEGER, BIGINT or DOUBLE by `CAST` or
`TRY_CAST` exactly as DuckDB reads it (surrounding whitespace, one `+`, `0x` and `0b` integers, a `_` between digits, a
decimal point and an exponent in an integer, rounded half away from zero; `inf`, `infinity` and `nan` in any case; text
that is no such number is `ConversionInvalid`, NULL for `TRY_CAST`; a text to DECIMAL, DATE or TIMESTAMP is refused by name),
`CAST(x AS VARCHAR)` (also `TEXT`, `STRING`, `VARCHAR(n)` and `x::VARCHAR`) of an integer, BOOLEAN, DECIMAL (at its scale),
DOUBLE, FLOAT column, DATE or TIMESTAMP as DuckDB 2.0 prints it (a DOUBLE by its shortest round-trip digits, fixed from
`0.0001` to `1000000000000000.0` and `1e+16` beyond, `nan`, `inf`, `-0.0`; a FLOAT the way DuckDB's formatter settles on
its digits, which is not always the shortest; a year before 1 as `0001-12-31 (BC)`; a TIMESTAMP with its fraction cut of
trailing zeros in the unit's own 3, 6 or 9 digits, a UTC-adjusted one with `+00`), in a SELECT, an aggregate argument or a
GROUP BY key (a NaN prints with its sign, `-nan` as well as `nan`, and `-x` of a DOUBLE flips the sign bit, a zero's and a
NaN's too; a TIMESTAMP_MS whose microseconds overflow, and a TIMESTAMP that is not UTC-adjusted below 290309-12-22 (BC),
are conversion errors, as in DuckDB, where ±9223372036854775807 milliseconds are the infinite timestamps); an expression computed from a FLOAT, which DuckDB evaluates in single precision, and an
INT96 timestamp are refused by name,
`CAST(t AS DATE)` and
`CAST(x AS TIMESTAMP)` (also `::` and `DATETIME`) between a DATE and a TIMESTAMP of any unit (a time's day is its floor,
before 1970 too; a TIMESTAMP_NS reaches microseconds to the nearest, a tie away from zero, in `date_trunc` and
`date_part` as in a cast; a DATE past the TIMESTAMP range, a TIMESTAMP_MS past the range of microseconds (as a DATE
too), and a TIMESTAMP_MS before 290309-12-22 (BC) as a TIMESTAMP, are conversion errors; a UTC-adjusted value is read
in UTC, as the date parts read it, so DuckDB's answer differs under another session time zone; a UTC-adjusted
TIMESTAMP_NS, which DuckDB keeps as a type of its own, casts to a DATE from its nanosecond and has no `date_trunc`,
`date_part`, `date_diff` or cast to TIMESTAMP here: those are refused by name), `floor`, `ceil`/`ceiling`, `sqrt` and
`round(x [, n])` of a number (a DECIMAL floors and ceils to DECIMAL(p, 0) and rounds half away from zero, in the
precision DuckDB gives it; an integer or a DOUBLE floors and ceils to a DOUBLE; `sqrt` is a DOUBLE and NaN below zero;
`n` is a constant, and an integer is not rounded to tens or up), `least` and `greatest` over numbers (NULLs skipped,
NULL where all are, NaN above every number), `year`, `quarter`, `month`, `week`, `day`/`dayofmonth`, `hour`, `minute`,
`second`, `dayofweek`, `isodow` and `dayofyear` of a date or timestamp (spellings of `date_part`), `ifnull(a, b)` (a
two-argument `coalesce`), `strlen(s)` (bytes; NULL for NULL), the string functions `length`,
`substring`/`substr` (also `substring(x FROM a [FOR b])`), `left`, `right`, `upper`/`lower`, `trim`/`ltrim`/`rtrim`
(spaces), `replace`, `lpad`/`rpad` and `concat(a, b, ...)` (a NULL is the empty text, unlike `||`; the arguments must be
text), and searched `CASE WHEN cond THEN v ...
[ELSE v] END`, parens, unary minus. A CASE evaluates a branch only on the rows that take it, and coalesce an argument
only on the rows every earlier one left NULL, so a fault where no row needs the value is no error; the simple form
`CASE c WHEN v ...` is the searched form `WHEN c = v` for a column and literals. DECIMAL arithmetic is exact (see below).
Integer arithmetic is typed and checked as DuckDB does: `+`, `-`, `*`, `%` and `//` of two integers are of the type
DuckDB resolves for the pair (`SMALLINT + TINYINT` is a SMALLINT; a literal takes the other operand's type when it
fits, so `tinyint_col + 0` is a TINYINT), and a result outside that type is an overflow error, in rows that are read
and not NULL. `%` takes the sign of the dividend; `//` truncates toward zero; `/` of two integers is a DOUBLE division
(`7 / 2` is 3.5: an earlier release truncated it, which `//` now spells). A zero divisor of `%` or `//` of integers or
DECIMALs is an error, and so is the minimum of a signed type over -1; a DOUBLE `%` is C's `fmod` (`x % 0` is NaN), and
`//` of anything but two integers is `/` with a zero-divisor error. `%` of DECIMALs is exact: a DECIMAL of the larger
integer part and the larger scale, or a DOUBLE where that passes 38 digits. `%` and `//` over aggregates, and in a
WHERE predicate, are refused by name. The string functions count and cut text in characters (UTF-8 code points, as DuckDB does: a combining mark is a
character); `upper` and `lower` map one code point to one with DuckDB's own table, so `ß` upper-cases to `ẞ` and
there is no final-sigma form. Their other arguments are constants (a column there is refused; an empty `lpad`/`rpad`
pad is an error only where padding is needed, as in DuckDB). `split_part(s, sep, n)` takes the `n`th field (1-based, a
negative `n` from the end, an empty `sep` splitting into characters, `''` where there is none) and
`make_date(year, month, day)` (any integers; a month or day the calendar has not, or a date past DuckDB's DATE, is an
error) and `date_diff(part, start, end)` / `datediff` (BIGINT boundaries of the unit between two dates or timestamps:
a month between 01-31 and 02-01, a week the day count over 7 truncated, a TIMESTAMP_NS rounded to microseconds first,
NULL for an infinite time; DuckDB keeps UTC-adjusted microseconds, nanoseconds and UTC-adjusted nanoseconds apart, so
a pair of two of them is refused, and any UTC-adjusted nanosecond argument; `year` to `microsecond` plus `decade`, `century` and `millennium`, a constant) are the date
functions beside them, as is `strftime(t, format)` of a DATE or TIMESTAMP (the format a constant of DuckDB's
specifiers: `%Y %y %m %d %H %I %M %S %j` and their `%-` unpadded forms, `%f %g %n %p %a %A %b %B %h %w %u %U %W %V %G
%Z %z %c %x %X %T %%`; years print four digits from 0000 to 9999 and as many as they have beyond; infinite times print
`infinity`; `%Z` is empty for a TIMESTAMP and `UTC` for one with a time zone, which is read in UTC; an empty or unknown
format, such as `%e`, and a TIMESTAMP_NS or INT96 argument are refused), and
`regexp_extract(s, pattern [, group [, options]])` one group of the first match (`''` where there is none, group 0 the
whole match, groups 0 to 9, DuckDB's option letters; the form with a list of group names is refused), the pattern, group
and separator being constants. `trim`, `ltrim` and `rtrim` strip Unicode's space
separators (U+0020, U+00A0, U+1680, U+2000 to U+200A, U+202F, U+205F, U+3000), not tabs or line breaks, as DuckDB's do.
A computed date or time (`make_date`, the casts, `date_trunc`, `epoch_ms`, `coalesce` of timestamps) is the DATE or
TIMESTAMP a column of it would be, in its unit and time zone, wherever it leaves: a Parquet type, CSV text, a group key,
a `min` or `max`; JSON and JSONL print the stored integer, as for a column. Each distinct text is computed once, a dictionary-coded
column once per entry. A DOUBLE column and a DECIMAL default (`coalesce(d, 0.0)`) coalesce to DOUBLE, as in DuckDB.
`coalesce` of dates and times of different kinds (a DATE and a TIMESTAMP, two units, UTC-adjusted and not, a time and a
number) is refused by name: DuckDB converts them to a common type, and answering their ticks as they are would mix
clocks. `make_date(y, m, d)` is NULL where any of its three arguments is, a literal year included.
A DECIMAL used as a DOUBLE (an operand of `/`, of a DOUBLE `%` when the DECIMAL remainder would need more than 38
digits, a `CAST AS DOUBLE`) converts to the nearest double, as DuckDB's cast does, for any scale. A block comment nests
(`/* a /* b */ c */` is one comment) and one that is never closed is an error, as in DuckDB; comments are blanked before
any other reading of the query.

**Aggregate** with `sum / count / min / max / avg / avg_state`, `bool_or` and `bool_and` of a BOOLEAN (`max` and `min`
of it: true if any, false unless all, NULL over no values), `var_samp` / `variance`, `var_pop`, `stddev_samp` /
`stddev` and `stddev_pop` of a number (a DOUBLE whatever the argument; a DECIMAL reads as its nearest double; the sample
forms are NULL below two values and the population forms 0 for one; Welford's mean and squared deviations as DuckDB
folds them, so one row group is bit for bit DuckDB's. Row groups combine pairwise in file order, the same at every `-j`;
DuckDB itself varies in the last digits with its thread count and between runs, and zpq's answer is always one it gives.
`DISTINCT`, and a date, BOOLEAN or text argument, are refused), `arg_max(arg, val)` and `arg_min(arg, val)` (also `max_by`,
`min_by`, `argmax`, `argmin`: the `arg` of the row with the greatest or least `val`, rows with a NULL in either ignored,
NULL over none; the first such row of the scan on a tie, the same at every `-j`, where DuckDB's own tie varies with its
threads; `arg` an integer, date or time, DOUBLE or FLOAT, or text, answered in its own type, `val` any of those or a
DECIMAL (compared exactly), a DOUBLE with NaN greatest; a DECIMAL `arg`, a BOOLEAN on either side, an unsigned 64-bit
`arg`, the `n` form and `DISTINCT` are refused), `string_agg(text [, sep])` (also `group_concat` and
`listagg`: the non-NULL values in file, row-group and row order, joined by a constant separator, `,` by default; NULL
over none; empty texts count. The order is the scan order, the same at every `-j` unless `--unordered`; DuckDB's own
varies with its threads. A text argument only; the other types, a non-constant separator and `DISTINCT` are refused. The
texts are held in memory, with four bytes each, against `--max-memory`), `median(x)` and `quantile_cont(x, q)` of an integer or DOUBLE
(a DOUBLE; NULLs skipped, NULL over none; the values sorted with NaN last and the position `(n - 1) * q` interpolated
exactly as DuckDB's x86-64 build does, so the answer is its own bit for bit and the same at every `-j`; the quantile is
a constant from 0 to 1. A FLOAT (a column, or an expression that reads one), DECIMAL, date, BOOLEAN or text argument, a negative or non-constant quantile, and
`DISTINCT` are refused by name. The values are held in memory, 8 bytes each, against `--max-memory`), `approx_count_distinct(col)` (DuckDB's HyperLogLog
register for register, so the estimate is DuckDB's own and the same at every `-j`; a BIGINT, 0 over no values; a bare
column of an integer, DECIMAL, DOUBLE/FLOAT, DATE, timestamp of any unit (a millisecond one hashes as the microsecond TIMESTAMP DuckDB reads it as), BOOLEAN or text type: an expression, a TIME,
UUID or INTERVAL column and an INT96 timestamp are refused, as DuckDB hashes them at a width zpq does not read; each
group's sketch is 1 KB, held against `--max-memory`), conditional `agg(...) FILTER (WHERE ...)`, and
`GROUP BY`. In SQL, GROUP BY keys may be expressions (bound by value, not spelling), SELECT positions or aliases (a
column of the name wins, as in DuckDB); `HAVING` compares aggregates or keys with literals, or with one another (only
TRUE keeps a group; without GROUP BY the one group may be dropped); ORDER BY may rank by aggregates or keys the SELECT
list lacks, or by an expression of the keys (`GROUP BY k ORDER BY k * 2`: one more key, which cannot change the groups).
A SELECT item, a HAVING comparison or an ORDER BY term may be an expression over aggregates: `+ - * /`, unary minus,
`round(x [, n])`, `abs(x)` and `nullif(x, y)` over aggregate calls and number literals (`sum(a) / sum(b)`, `100 * sum(CASE ...) / sum(...)`,
`round(avg(x), 2)`, `HAVING sum(a) > avg(a) * 1000`, `ORDER BY sum(a) / count(*) DESC`). Each is evaluated once per
group after aggregation, never per input row, and typed as DuckDB types it: `+ - *` stay exact over integers and
DECIMALs (a DECIMAL result of at most 38 digits, else an error), `/` is DOUBLE (a zero divisor gives an infinity or
NaN), anything with a DOUBLE is DOUBLE, NULL propagates, `round` goes half away from zero (a non-finite input stays
as it is; a DECIMAL rounds to the same precision, one digit wider for a whole number rounded to tens and up, and a
result past 38 digits is an overflow error), and an AVG reads as DOUBLE from its exact sum and count (never from the
rounded DECIMAL mean zpq answers a bare AVG of a DECIMAL column with). That DOUBLE is DuckDB's to the last bit, which
decides a ROUND at a cent and a HAVING: DuckDB divides the sum by the count times the double 10^scale in `long double`
inside a GROUP BY or a FILTER, and casts the sum to a double before dividing in a query with neither. Integer results are checked in the SQL type DuckDB gives the operation, not in 128
bits: COUNT is BIGINT, SUM HUGEINT, MIN and MAX their column's own type, a literal takes the other operand's type when
its value fits it, and a result outside the type is an overflow error (`count(*) + 9223372036854775807`,
`max(int32_col) + 2147483647`, `max(tinyint_col) + max(tinyint_col)`); a product of two DECIMALs that each fit 18 digits
keeps 18, with an overflow error past 10^18, as DuckDB's does. Arithmetic on the MIN or MAX of an expression or of a
FLOAT column is refused (`PostAggTypeUnknown`): its SQL type is not one zpq resolves. The rest (`%`,
`||`, other functions, CASE, a string in the expression) is refused by name (`PostAggNotSupported`), and a column beside
an aggregate in one is refused (`ColumnMustBeGrouped`) as it always was. A HAVING term is evaluated only for the groups
it keeps, and a SELECT item before the ranking reads it. `SELECT DISTINCT a, b`
is `GROUP BY a, b` (NULLs one group), `GROUP BY ALL` groups by the SELECT items that are no aggregate (a constant is no key
of its own: constants alone keep one row per input row, and a constant beside only aggregates is refused by name), and `ORDER BY`
takes `NULLS FIRST` or `NULLS LAST`, NULLs last by default in either direction as in DuckDB. `DISTINCT` with an aggregate,
a GROUP BY or `*`, and `DISTINCT ON`, are refused by name. Plain
rows take `ORDER BY` columns with a `LIMIT`, keeping the first rows by those keys, ties by input position.
As in SQL, `sum / min / max / avg` over no non-null values are NULL (JSON `null`, a null cell in `-o` parquet;
avg_state's pair becomes `{"sum": null, "count": 0}`) while `count` is 0.
`avg(x)` is the mean, one value in every format: DOUBLE for integer and floating `x` (the sum, its integers summed
exactly, divided once by the count), DECIMAL for a DECIMAL `x` (below). `avg_state(x)` is the same average as mergeable
state for fan-out: `{"sum": .., "count": ..}` in JSON and NDJSON, `<name>__sum` and `<name>__count` columns in CSV and
Parquet (a DOUBLE or exact DECIMAL(38, s) sum, a non-null INT64 count), so per-file answers merge exactly with
`sum(<name>__sum) / sum(<name>__count)`; you cannot average means. A double avg is the avg_state sum over its count,
bit for bit.
`-o out.parquet` writes the result as parquet: one row, or one row per group with the same columns and values as the
JSON (avg_state as `<name>__sum` / `<name>__count`).
`count(*)` is answered straight from row-group metadata; `min / max / sum` are computed by decoding the data unless you
opt into trusting file statistics — see [Statistics: trust is opt-in](statistics-trust.md). `--max-memory`
is a ceiling on what an aggregate or a write holds at once: one budget (`src/core/memory.zig`) that the GROUP BY tables,
decoded row groups, a local reader's read-ahead, the merge, the answer and its encoding, and a write's encoded output in
flight all draw from before they allocate, whatever `-j` is. A fixed slack sits outside it: the binary, thread stacks,
the mapped input, each worker's first 2 MiB of decode memory, each worker's codec state (about 0.1 MiB reading; writing,
0.2 MiB more for snappy, 0.1 MiB for gzip, 1.3 MiB for zstd, or 3.5 MiB for zstd with `--compress-priority speed`),
16 MiB for a GROUP BY's merge and answer past its tables, and a 32 MiB budget of its own for one decode, a refused
query's last retry or a write's row group sliced on its writer. A query the budget refuses runs again on narrower paths
(fast paths off, partitions, one worker, then small slices) and, if none fits, fails with an error rather than taking
more memory. It also bounds a query's working memory on every path (aggregate, print, write, byte copy), over S3 and
locally alike, so a scan larger than memory completes:
- **Fetched bytes:** S3 inputs past a quarter of it (between 64 MiB and 1 GiB; `ZPQ_S3_WINDOW_KIB` sets it) are
  fetched just ahead of the readers and released behind them (Linux; elsewhere still up front). A print with
  `--limit` fetches row group by row group and stops once it has its rows.
- **Decoded rows:** half of it is for decoded row groups, shared by the workers. A row group too big for a worker's
  share is decoded, filtered and aggregated, printed or written a slice of rows at a time, holding one page per
  column beside the slice; a slice the budget refuses is decoded again at half its rows. Slicing never changes what a
  write writes (see "Output layout" below). A row group that cannot be sliced (a nested column) and does not fit is
  refused (`RowGroupTooLarge`) before anything is decoded, rather than run out of memory.
- **Output:** a write holds at most a quarter of it encoded and waiting. A row group whose rewrite could take more (a
  codec that expands, such as `--codec uncompressed`, or one written in slices) goes straight to the output in its
  turn, a page or a slice at a time; S3 uploads keep at most an eighth of it in flight.

It is a working budget, not a hard cap: the binary, page buffers and output in flight come on top. Aggregation state,
DISTINCT sets and string arenas draw from it; a query that does not fit retries narrower paths or refuses cleanly.
For a hard ceiling, run zpq under a
cgroup `memory.max` or `ulimit -v`. Scans, filters, printed rows, writes, S3 streaming and low- to
medium-cardinality aggregation stay within the budget. Its default
(`src/core/system.zig`) is three quarters of the memory size on Lambda; on Linux, 80% of available memory, but no more
than three quarters of a cgroup v2 `memory.max`; on macOS, half of physical memory; and 512 MiB where none of these
can be read. Give it as bytes, or as a number (fractions allowed) followed by `K`, `M`, `G` or `T`, with or without a
`B`, in any case, all powers of 1024: `--max-memory 2GB`, `--max-memory 1.5g`, `--max-memory 1073741824`. A space,
an exponent (`1e9`) or a `MiB` spelling is refused (exit 2).

Diagnostic path and cache layout overrides are documented in [execution overrides](execution_knobs.md).

Add `--explain` to an aggregate query to print its plan as one JSON object and exit before scanning. It accepts SQL
`--query` and the `--aggregate` / `--group-by` flags, and reports the chosen grouping strategy, the reasons for it,
the column passes, conditional fallbacks and upper bounds on the number of distinct keys, without reading data
pages. Locally it may read dictionary pages to tighten those bounds; over S3 it reads only footers. Every strategy
is exact, and memory estimates stay estimates: execution enforces the budget. `ZPQ_NDV_UNION=0` turns off the
optional dictionary-union probe, and `ZPQ_PARTAGG=1` forces an eligible plain GROUP BY through shared tables.

**DECIMAL is exact.** DECIMAL columns, and numeric literals with a decimal point (`0.05` is DECIMAL(3,2), as in SQL),
compute in scaled 128-bit integers, never through a double. Result types follow Trino's (Athena's) rules: `a + b` /
`a - b` take the larger scale and one more integer digit, both sides first brought to their common type (the larger
scale, every integer digit either side has, at most 38 digits; a value that does not fit it is an error, as the cast
is there), `a * b` adds scales and precisions (capped at 38 digits), an integer operand counts as DECIMAL(10,0) or
DECIMAL(19,0), and a DOUBLE operand gives DOUBLE. Two places follow DuckDB instead: `/` gives DOUBLE, and an `avg`'s
sum may pass 38 digits. DuckDB agrees on every scale but keeps some results at 18 digits where Trino and zpq widen
(the product of two DECIMAL(15,2) is DECIMAL(18,4) in DuckDB, DECIMAL(30,4) here); values are the same.
`sum` is DECIMAL(38, s); `min` / `max` keep the column's type. `avg` over DECIMAL(p, s) is DECIMAL(q, s + q - p) with
q = min(38, p + 4): up to four more fractional digits and never fewer integer digits, so DataFusion's (p + 4, s + 4)
for p <= 34, the input's own type at p = 38 as in Trino, and DECIMAL(38, 37) for DECIMAL(36, 35). The mean is exact,
divided from the exact sum and rounded half away from zero once, so it always fits (it is never larger than the
largest value). The JSON prints every digit of the scale (`3344516955134.66`) and `-o` writes DECIMAL columns.
avg_state's sum is exact at any width in JSON, but `-o`'s `__sum` column is DECIMAL(38, s), so an avg_state whose sum
passes 38 digits answers in JSON and errors with `-o`; avg itself never does. A value that needs more than its type's
digits stops the query with an error rather than wrapping or rounding, but only in rows the filter keeps. Filters
compare a DECIMAL column with a literal exactly, at the column's scale. Arithmetic over a NULL is NULL, for every type,
as in SQL: `x * 2` is NULL where `x` is, aggregates skip it, and a NULL row raises no overflow or division by zero.

**Parquet version.** `--parquet-version v1|v2` chooses a value-encoding profile; v2 is the default.
v2 writes a version 2 footer, V1 data pages, PLAIN dictionary pages and RLE_DICTIONARY dictionary data pages;
it uses DELTA encodings where selected. v1 writes a version 1 footer and V1 data pages, with PLAIN or
PLAIN_DICTIONARY values and no DELTA or RLE_DICTIONARY. v1 is DuckDB-V1-like, not a strict Parquet 1.0 file:
codecs, logical annotations and optional metadata are independent. Untouched copied chunks keep their input
pages and encodings, regardless of the requested footer version. JSON requests use `"parquet_version": "v1"` or `"v2"`.

**Write** with SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED output (`--codec lz4` means LZ4_RAW; zpq never compresses
to the deprecated LZ4 codec, though a plain copy keeps an LZ4 chunk as it is); RLE_DICTIONARY for low-cardinality byte
arrays (a quarter or fewer distinct, and a dictionary within a quarter of the values' bytes, at least 1 MiB and at most one page); DELTA_BINARY_PACKED
or PLAIN for INT32 / INT64, whichever is smaller on a compressed first-page sample; other strings PLAIN, unless a sample of the chunk's first page, compressed with the chosen codec, shows
DELTA_BYTE_ARRAY at half PLAIN's size or less, or DELTA_LENGTH_BYTE_ARRAY smaller at all (FIXED_LEN_BYTE_ARRAY has no
DELTA_LENGTH_BYTE_ARRAY).
DECIMAL columns re-encode losslessly: INT32 / INT64 / FIXED_LEN_BYTE_ARRAY backings carry the unscaled integer, never
a DOUBLE. Direct projection copies preserve page indexes; re-encoded chunks get their own.

**Output layout.** A write that re-encodes (a filter, `--select` or a computed column) cuts its row groups from the
rows it writes, in order, not from the input's row groups: one closes once it holds `--row-group-size` rows (default
131,072) or its values take `--row-group-bytes` (default 64 MiB) at their PLAIN size, a fixed-width value its
width and a byte array 4 bytes plus its length. Each column chunk is split into data pages targeting 1 MiB of
values or 20,000 rows. Dictionary pages can hold more rows within their encoded-index and decoded-value byte budgets;
string pages also retain their string-byte budget. Each chunk gets a page index (ColumnIndex and OffsetIndex, written
after the row groups, column by column). The bytes written depend only on the inputs and the options: the same file at every `-j`, on every machine.
An explicit `--max-memory` caps row groups at a sixteenth of it, so there the file can depend on that value (never
on `-j`). Holding a row group's decoded rows until it is encoded is the write's largest fixed cost: with the default
budget a machine needs a few times `--row-group-bytes` of memory, or an explicit `--max-memory`. Plain copies and row
groups a filter's statistics prove fully matching are copied as they are, keeping their source's row groups. Computed
columns are evaluated only for the rows the filter keeps.

**Copies share blocks.** A copied run of input bytes (a whole row group, or one projected column chunk) is placed at
the same offset modulo 4 KiB as in its input, after up to 4,095 zero bytes no footer offset points at, whenever the
run holds at least one whole 4 KiB block; a run that holds none goes where it is. When the output is a local file
zpq made on the inputs' filesystem and that filesystem shares blocks (btrfs, XFS with reflink, ZFS block cloning, an
NFS 4.2 or SMB server that clones), which zpq asks it by cloning one block into the still-empty output, each run goes
by `copy_file_range`: its whole blocks are shared with the input instead of written, in almost no time and no new
space, and a plain copy maps its inputs rather than reading them ahead. Anywhere else (ext4, another filesystem, a
pipe, stdout, S3) the runs are read and written as before, the same bytes. `ZPQ_SHARE_BLOCKS=0` turns the sharing off.
The layout is a fixed 4 KiB whatever the filesystem, so a copy's bytes are the same wherever it is written. Its cost
is at most 4,095 bytes per run and always fewer than the run's whole blocks; the summary's `byte_copy` reports it.

**Concatenation.** Several inputs (a glob or a file list) with no filter, `--select` or computed column are one byte
copy: each input's row groups are copied whole, in input order, under one footer. The inputs' schemas must agree
column for column (names, physical and logical types, repetition, nesting and DECIMAL scale and precision); the first
that differs is refused, naming the file and the column where it parts from the first input's. Inputs that declare
different column orders are re-encoded instead. File key-value metadata (an embedded Arrow schema among it) is
carried by no write but UPDATE, so it never decides a concatenation.

**UPDATE.** `zpq query --query "UPDATE 'f.parquet' SET col = literal [, col = NULL ...] WHERE predicate"` sets the
matching rows' values by rewriting only the data pages that hold them; every other byte of the file is copied as it
lies, and shared on a filesystem that shares blocks. A rewritten page keeps its own encoding and codec (PLAIN,
DELTA_*, BYTE_STREAM_SPLIT, RLE booleans, or dictionary indices, the value appended to the chunk's dictionary when it
holds none), its version (V1 or V2) and its CRC, recomputed. A page whose size changes carries a pad in its header,
PageHeader field 20 (binary, short form, before the STOP byte), sized so the pages after it keep their offsets modulo
4 KiB; every reader of the 0.6.0 reader survey skips it. The statistics move with the edit: page header statistics,
the ColumnIndex entry (a boundary order the edit breaks becomes UNORDERED), the OffsetIndex sizes, the chunk's null
count and bounds (widened, never narrowed; a bound whose value was edited away is no longer marked exact), size
statistics and level histograms. The new value goes into the chunk's Bloom filter, whose old values stay (a false
positive is allowed), and a row group's `sorting_columns` is dropped when an edited column is one of its keys. A rewritten
page keeps its own length whenever it fits there, so nothing after it moves: the page indexes, Bloom filters and
everything else up to the footer stay where they lie, shared, overwritten only in the bytes an edit changed. A chunk
one of whose pages outgrows its length is copied whole after them, at its offset modulo 4 KiB, so its other pages stay
shared and no other chunk moves. (When an index entry cannot keep its length, every byte after the edit moves instead
and the page indexes and filters are written after the row groups.) The footer keeps every field the source wrote
(key-value metadata among them), and the bytes it shares with the source's are copied from it. In place, the result is staged beside the file, synced and renamed over it,
keeping its mode, and refused if the file changed while it ran; `-o` writes it elsewhere and leaves the file as it was.

- The literal is cast as a WHERE comparison casts it, and must be a value the column's type holds exactly.
- `WHERE` is required; to rewrite a whole column, write a `SELECT` with `-o`.
- Any number of rows may match; the cost grows with the pages they touch. A statement that changes no value writes
  nothing.
- Refused, exit 2, naming the reason: repeated (LIST/MAP) columns, INT96 and FLOAT16 columns, encrypted files, S3
  targets, a page zpq cannot rewrite exactly, STRUCT fields (a leaf under a group), a value outside an 8- or 16-bit
  integer type's range, a glob, a second UPDATE of the same file while one runs, and every option but `-o`, `--fsync`/`--no-fsync`, `--max-memory` and
  `--summary=json`. A file replaced, removed or rewritten while an UPDATE runs is refused (`InputChanged`), and
  `--max-memory` bounds what it holds as it bounds a SELECT.
- On ext4 and other filesystems that share no blocks, the copy is written in full: time and transient disk grow with
  the file. On btrfs or XFS with reflink they grow with the pages touched.

`--codec` sets the output codec. Without it, re-encoded output is SNAPPY and a plain copy keeps each column chunk's own
codec; with it, a plain copy recompresses any chunk stored in another codec. The `codec` field of the write summary names
the codec every written chunk uses, or is `"MIXED"` when a plain copy without `--codec` keeps chunks in more than one
(for example, inputs written with different codecs).

`--compress-priority` (the Lambda's `"compress_priority"`) sets how hard zstd works. `size`, the default, compresses
every page at level 3. `speed` is an opt-in that trades size for CPU: each page may instead be stored as raw blocks
(for pages nothing compresses, such as bit-packed dictionary indices), as entropy-coded literals with no match search,
or at level 1, each a standard zstd frame every reader decodes. The form is the one that costs least when an output
byte is worth 100 cycles, its size estimated by compressing four slices spread over the page, and it is kept only if
the whole page passes three checks: the frame is within 10% plus 1 KiB of the size the sample predicted, a raw or
literals-only page has no more than that to gain from a fast full-page match search, and a raw page no more than that
from entropy coding. A page that fails is compressed at level 3 too, and the smaller frame kept. Every step depends on
the page's bytes alone, so the output is the same at every `-j` and on every run. Pages under 16 KiB are compressed at
level 3.

`speed` has no bound relative to level 3. The checks search less than level 3 does (a 512 KiB window and matches of 6
bytes or more, where level 3 looks back 1 to 2 MiB for matches of 5), so a page that compresses only through matches
they miss is stored as raw blocks: its own size plus about 0.01%. An 8 MiB page repeating 600,000 random bytes, for
example, is written in 8,388,809 bytes under `speed` and 610,006 at level 3, 13.75 times smaller.
`--profile-summary` reports the priority and, under `speed`, how many pages took each form and how many failed their
checks.

**Order** is the input's: every output that lists rows (print, `--format`, `-o` writes and byte copies, several files,
S3 and Lambda) keeps files in the order given, then row groups, then rows, at any `-j`. `GROUP BY` lists groups in the
order they first appear in the input (before 0.5.0, in the byte order of their serialized keys), and every aggregate,
floating-point sums and averages included, comes out with the same bits at every `-j` and on every run: for float
results each fixed block of row groups folds into its own table and the blocks merge in input order, which can take up
to about twice the GROUP BY memory of `--unordered` (`--max-memory` counts it). `--unordered` (the Lambda's
`"ordered": false`) gives both up for speed: groups in no set order, float aggregates free to differ in their last
bits.

**Read local files** through the page cache or explicitly: on Linux, an aggregate over files mostly not in the page
cache reads just the column chunks it needs, row group by row group, with io_uring (`O_DIRECT`) or `pread`, in a
bounded window; see [Local reads](local_io.md).

**Stream** S3-to-S3 with in-flight memory bounded independently of file size: multipart upload as the encoder
produces bytes, a sliding row-group window for re-encode backpressure, and parallel fetch across files.

```bash
zpq query data.parquet -o out.parquet \
  --filter "int8 BETWEEN -10 AND 10" \
  --select "name, qty * price AS revenue, coalesce(notes, '') AS notes"
```
