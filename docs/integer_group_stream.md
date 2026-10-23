# Full integer GROUP BY output

The Parquet-only COUNT(*) + SUM(integer) path uses inline integer records when one bare signed INT64 key,
one bare signed INT64 SUM argument, and the three selected output columns form the entire answer. It accepts
NULL keys and values and column permutations. Filters, computed expressions, unsigned arguments, other aggregates,
ORDER BY, LIMIT, OFFSET, HAVING and hidden output columns retain the general executor. A small stored-dictionary
estimate also retains the coded fold. Compression can make that estimate low; it is a cost decision, never a
key-identity proof.

`integer_groups.State` reserves a conservative peak from the query ledger before decoding starts. Its quote depends
on row count, with headroom for at most 64 workers and bounded output windows. Refused admission takes the existing
path without consuming input. A later decode-memory refusal releases the state and enters the existing exact
restart chain with this executor disabled. Source descriptors, decoded pages and dictionary codes never escape their
usual lifetimes: each record owns the key, integer value, dense input ordinal and NULL flags.

Workers append records to private coarse-partition block chains, collecting fine-partition counts at the same time.
A second radix level places equal keys in one of
2,048 fine partitions, then one worker owns each fine table. Full integer equality and a separate NULL identity
resolve collisions. Packed 24-byte records share the 32-byte-per-row final-state allocation: folding backwards and
placing groups from each partition's end cannot overwrite unread records. This avoids a separate final-state
allocation and record-buffer teardown. COUNT is exact and SUM remains i128 until checked narrowing. Since admitted row counts are below
2^31, neither count nor the exact sum can overflow their internal representation; the high bits of the count and first
ordinal can carry SUM-presence and NULL-key flags. An out-of-range final i64 SUM is rejected after every decode has
completed and before the writer opens the output. Decoder failures therefore retain precedence over narrowing.

Each final group publishes its reference at its minimum input ordinal. A winner bitmap and prefix ranks recover
first-appearance order without sorting or copying full group slots. Output windows contain at most four fixed row
groups. Their typed integer columns use the same narrowing and ownership rules as general typed finalization; workers
gather disjoint output rows directly. Exact input presence flags let a window omit definition arrays where NULLs
are impossible; output schema nullability remains unchanged. No full answer of typed columns or generic
output-value unions is retained.

The aggregate writer uses the normal writer's defaults: 131,072 rows per row group and at most 20,000 rows per data
page (the byte target can cut a page earlier). Row-group boundaries depend on output row positions. Input row-group
boundaries, worker count, scheduling and memory-driven encoder waves do not affect the file. Small answers remain
one row group. Values and row order are unchanged from the general executor.

`consumer.AggregateFile` accepts complete fixed row groups in bounded windows and owns only footer metadata after an
append returns. Its optional integer workspace retains a bounded wave of encoder arenas across windows; the entire
scratch bound remains charged until the file is torn down, including pages between appends. If its reservation does
not fit, ordinary per-wave reservations remain available. Writes and encode errors are consumed in column order
after all started tasks join. The one group key is provably unique among present values, so the encoder can skip a
dictionary attempt that would decline; the resulting bytes are unchanged.

The scan owns the integer state until the output is written or abandoned. The engine frees it while its query ledger
is still alive. Direct core callers must likewise keep their supplied ledger alive through result destruction. The
state, file workspace and partially built windows unwind on allocation or sink failure. Spawn failure falls back to
claiming unstarted work, preserving the same result and ownership.
