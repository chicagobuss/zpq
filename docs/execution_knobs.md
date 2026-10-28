# Execution overrides

These environment variables are for diagnosis and testing. They choose among execution paths that give the same
answer; they never change query semantics or bypass the checks on query shape, types and memory budget. Leave them
unset in ordinary use. The [hardware execution catalog](hardware-execution.md) lists every override.

Eligible fast paths are on by default. For the path switches (`ZPQ_INTEGER_RECORDS`, `ZPQ_BYTE_RECORDS`,
`ZPQ_MSD_RECORDS`), only the literal `0` turns a path off; zpq then uses its general executor.

`ZPQ_MSD_CACHE_ROWS` sets the scratch size, in records, of the retained-page COUNT top-k path. Unset, it is sized from
the smallest per-worker L2 cache over the process's CPUs (640 KiB when the cache size cannot be read): a 512 KiB
budget gives 2048 records and 640 KiB gives 4096. `0` selects a fixed 4096. A positive value is clamped to
16–16384 and rounded down to a power of two; a malformed value uses the default. The size chosen is reserved from the
memory budget and reported as `cache_rows` on the `msd_fold` profile event.

The [packed byte-key SUM path](byte_topk.md) sizes its partitions and decode windows from the shared L2 and L3 caches;
its overrides are in the catalog too.

None of these settings changes group identity, output order, output bytes or floating-point summation order.
