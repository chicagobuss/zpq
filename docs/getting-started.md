# Getting started

Every example below runs as written against `tiny.parquet`, the three-row file that ships with zpq, and shows its
exact output. The file holds:

| id | group | value |
|---:|---|---:|
| 1 | a | 10 |
| 2 | a | 20 |
| 3 | b | 30 |

Point `f` at it first:

```sh
# In an extracted release archive (run from the directory you extracted it into):
export PATH="$PWD/bin:$PATH"; f=share/zpq/tiny.parquet
# Homebrew:      f="$(brew --prefix)/share/zpq/tiny.parquet"
# Source build:  export PATH="$PWD/zig-out/bin:$PATH"; f=packaging/fixtures/tiny.parquet
zpq schema "$f"
```

```
rows: 3, row_groups: 2, codec: SNAPPY
id: INT64 (OPTIONAL)
group: BYTE_ARRAY (OPTIONAL) [Logical: STRING]
value: INT64 (OPTIONAL)
```

## 1. Filter rows to JSON

Rows print as NDJSON by default, one object per line, keys in column order. `--format json` prints one array instead.

```sh
zpq query "$f" --filter 'value > 10' --columns id,value
zpq query "$f" --filter 'value > 10' --columns id,value --format json
```

```
{"id":2,"value":20}
{"id":3,"value":30}
[{"id":2,"value":20},{"id":3,"value":30}]
```

A filter that matches nothing prints nothing in NDJSON and `[]` in JSON, and exits 0.

## 2. GROUP BY with avg

```sh
zpq query "$f" --group-by group \
  --aggregate 'count(*) AS n, sum(value) AS total, avg(value) AS mean, avg(id) AS mean_id'
```

```
{"group":"a","n":2,"total":30,"mean":15,"mean_id":1.5}
{"group":"b","n":1,"total":30,"mean":30,"mean_id":3}
```

Groups come in the order they first appear in the input. `avg(x)` is the mean as a plain number. For an integer or
floating `x` it is a DOUBLE, and a DOUBLE with no fractional part prints without one (`15`), so read it as a number,
not by its spelling. Without `--group-by` the answer is a single row.

The same query in SQL:

```sh
zpq query --query "SELECT \"group\", count(*) AS n, avg(value) AS mean FROM '$f' GROUP BY \"group\""
```

```
{"group":"a","n":2,"mean":15}
{"group":"b","n":1,"mean":30}
```

To combine averages across files or Lambda invocations, you cannot average the means. Ask for `avg_state(x)` instead,
which gives the sum and count:

```sh
zpq query "$f" --group-by group --aggregate 'avg_state(value) AS state'
```

```
{"group":"a","state":{"sum":30,"count":2}}
{"group":"b","state":{"sum":30,"count":1}}
```

The overall mean is the sum of the `sum`s divided by the sum of the `count`s. In CSV and Parquet, `avg_state` comes out
as two columns, `state__sum` and `state__count`.

## 3. Computed columns to Parquet

`--select` takes expressions. A named `-o` writes Parquet and leaves stdout empty. The file is written to a temporary
name and renamed into place, so a failed query leaves any earlier file as it was.

```sh
zpq query "$f" --filter "group = 'a'" \
  --select 'id, value * 1.5 AS scaled, value + id AS value_plus_id' -o out.parquet
zpq query out.parquet
```

```
{"id":1,"scaled":15.0,"value_plus_id":11}
{"id":2,"scaled":30.0,"value_plus_id":22}
```

`1.5` is a DECIMAL literal, so `scaled` is an exact DECIMAL(21, 1) column, and `value + id` stays INT64. Dividing by
a DECIMAL (`value / 4.0`) gives a DOUBLE. Any Parquet reader can open the output, for example
`duckdb -c "SELECT * FROM 'out.parquet'"`.

## Next

- [output-format.md](output-format.md): JSON, NDJSON and CSV answer shapes, and how each type prints.
- [binary-output.md](binary-output.md): how bytes, UUID, INTERVAL and BSON values print.
- `zpq --help` or `man zpq`: every option. Exit codes: 0 means success, 1 a read, decode or write failure, 2 an invalid
  query or option, and 141 a closed stdout pipe.
- `--max-memory 2GB` caps what one query holds at once. A query that does not fit fails with an error instead of
  growing past the cap.
