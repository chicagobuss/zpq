# Integer aggregate finalization

A full grouped Parquet answer owns typed output columns. For selected integer COUNT, SUM, MIN and MAX outputs,
`ColumnBuildTask` reads integer values directly from compact group records or existing scalar states into those
columns. It avoids expanding compact records into the scalar accumulator union and then into an output-value
union merely to narrow them back to integers. Other aggregate outputs retain the general row builder.

Key decoding and ownership remain the general builder's: fixed key images are normalized into serialized framing,
and each string key acquires an owned copy. All source tables remain live through column construction. The
integer getter retains i128 sums, presence flags, unsigned extrema and SUM/AVG shared-field presence.

Workers claim contiguous blocks of the already ordered group references. They visit each row's selected outputs
in SELECT order, record narrowing errors without interrupting later materialization, and stop a block on a
materialization failure. The global builder returns the earliest materialization error by row; otherwise it
returns the first narrowing error by column and row. A once-seen in-place COUNT still passes the copying merge's
signed-count bound before output narrowing. Allocation and cleanup use the existing typed-answer ownership and
query reservation; no new retained buffers are introduced.

Tests compare typed values with the general row builder across compact/generic states, in-place/single tables,
nulls, signed integers and thread counts. They cover spawn failure, every allocation failure, owned string keys,
shared SUM/AVG fields and materialization-versus-narrowing error precedence. The external exactness sweep also
checks unsigned keys/extrema, float keys, canonical float aggregates, DECIMAL, dictionary/plain pages, multiple
files and tight budgets against the preceding executable.
