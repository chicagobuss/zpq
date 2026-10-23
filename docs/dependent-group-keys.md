# Group keys determined by other keys

A GROUP BY expression adds no equality information when the retained keys fix
its value. The function need not be injective: grouping on both `k` and `k*k`
produces the same groups as grouping on `k`. zpq can remove the redundant key
from row evaluation and the hash key, then reconstruct its output per group.
Binding still precedes the rewrite, preserving column order, labels, aliases,
output types, and hidden ORDER BY columns. Computed integer outputs stay INT64,
including when their base column is INT32 or BOOLEAN.

The current proof accepts only integer `+`, `-`, and `*` trees over bare retained
column keys and integer literals. Every intermediate is bounded using physical
INT32/INT64/BOOLEAN ranges and the UINT32 annotation; no footer statistics are
used. An interval that exceeds INT64 rejects the rewrite, even if later operations
would cancel it. A potentially faulting tree, such as `k-1` for a full-range INT64
key, keeps per-row evaluation, including groups a LIMIT will discard. All column
references must have retained bare keys; bare keys themselves are never removed.
NULL arithmetic preserves the existing propagation. Calls, CASE, division,
UINT64, float and decimal expressions retain their established grouping path.
The whitelist excludes volatile/nondeterministic functions, including future
functions unless they gain an explicit proof. Unordered execution also retains
its established path.

Float aggregates keep their canonical row blocks and merge order. Only key
evaluation and representation change; row membership and first appearances are
the same. Canonical decode-error projection ranks use the original expressions.
Inline integer/string/MSD executors decline reconstructed output columns until
they support this output binding; the general aggregate path materializes them.

`ZPQ_GROUP_KEY_FD=0` forces legacy grouping. The query context reads this once;
other values enable the proof. This is a semantic rewrite, independent of cache
sizes, and never changes row-group/page or float reduction boundaries. Execution
knobs are listed in [hardware execution](hardware-execution.md).

The comparable is DuckDB's [redundant group optimizer](https://github.com/duckdb/duckdb/blob/main/src/optimizer/remove_duplicate_groups.cpp),
which likewise reconstructs removed expressions above the aggregate. zpq's
integer interval proof additionally ensures that delaying evaluation cannot
change checked-error precedence. This proof needs no allocator; compacted query
bindings live in the query arena and are covered by allocation-failure tests.

`tools/group_dependency_check.py CANDIDATE BASE` compares values, float bits,
checked failures, decode-error choices and Parquet bytes under both forced paths,
V1/V2 pages, nullable signed/unsigned inputs, workers 1 through 16, budgets and
forced decode windows. Strict PyArrow and DuckDB reads validate output files.
