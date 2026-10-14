# Changelog

## 0.4.1

**Fixes incorrect results from 0.4.0. Upgrade if you use `zpq query --query` with `NOT`.**

zpq 0.4.0 could return incorrect results, with no error, for a SQL `WHERE` clause (or an aggregate `FILTER (WHERE ...)`)
that applies `NOT` to a parenthesized `AND`/`OR`. For example, `WHERE NOT (x > 5 AND y < 3)` was evaluated as
`WHERE (NOT x > 5) AND y < 3`. The `--filter` flag was not affected, and neither was SQL without `NOT` over a group.

What to do: upgrade to 0.4.1, and re-run any 0.4.0 `--query` whose `WHERE` or `FILTER` applies `NOT` to a parenthesized
group. Until you can upgrade, rewrite such a predicate without the outer `NOT` (`NOT (a AND b)` is `NOT a OR NOT b`;
`NOT (a OR b)` is `NOT a AND NOT b`).

In 0.4.1, `NOT` is pushed down to the comparisons, keeping SQL's NULL semantics. Since the filter grammar is a flat OR
of ANDs, a predicate that still needs grouping after that (e.g. `NOT ((a AND b) OR c)`) is refused with an error
instead of evaluated differently.

0.4.1 also refuses SQL conditions that 0.4.0 misread and could answer incorrectly. Each condition must now be
`column op literal`, `LIKE` (without `ESCAPE`), `IS [NOT] NULL`, or `IN`/`BETWEEN` over literals. Refused are: `NULL`
as a value (0.4.0 compared against the string `'NULL'`, so `s NOT IN ('a', NULL)` returned rows where SQL returns
none), another column, an expression, `CAST` or `COLLATE` as a value (0.4.0 compared against their text), and a string
literal or column name containing a quote, ` AND `, ` OR ` or ` BETWEEN ` (or a comma, inside an `IN` list).
`x NOT BETWEEN lo AND hi` now works in SQL, and `a OR (b OR c)` is accepted instead of refused.
