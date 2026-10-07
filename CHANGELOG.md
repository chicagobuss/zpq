# Changelog

## 0.5.0 (unreleased)

### Behaviour changes

- `GROUP BY` lists groups in the order they first appear in the input (files in the order given, then row groups, then
  rows), at every `-j`; before, groups came in the byte order of their serialized keys. Floating-point sums and
  averages come out with the same bits at every `-j` and on every run. `--unordered` (the Lambda's `"ordered": false`)
  gives both up for speed.
- Float `min` / `max` order NaN above every number, as DuckDB, Postgres and Spark do: `max` is NaN if any value is,
  `min` skips NaN unless every value is NaN, and an all-NaN input answers NaN (it answered `Infinity` / `-Infinity`
  without `GROUP BY`). Of two equal zeros, `-0` is the lesser. Every path agrees, whatever the order rows and partial
  results meet in: plain and grouped, dictionary-coded or not, and answers from statistics, where a `max` is taken only
  from a chunk known to hold no NaN.
