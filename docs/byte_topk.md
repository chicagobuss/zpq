# Packed byte-key SUM selection

Unfiltered single-key `GROUP BY` with one signed INT64 SUM and `ORDER BY` that SUM under a bounded LIMIT can
keep complete owner partitions and a small answer. The executor accepts at most 15 decoded key bytes, leaving
the low byte of a big-endian u128 for the exact length. The distinct NULL identity uses low byte 255. Empty,
embedded-zero and variable-length keys therefore remain distinct under one integer equality comparison.
Decoded bytes establish every identity; metadata or an optional first-page sample may only decline the path.

Each owned record contains the packed identity, signed contribution, input ordinal, value-NULL bit and cached
hash. Private buffered blocks partition these records once. A worker folds a complete partition into reusable
key/i128-SUM/first-ordinal scratch, then retains its best H groups in a prepaid heap. H includes OFFSET. Every key
lives in one partition before selection, so the union of worker heaps contains the global top H. Negative sums
are valid; no incomplete group's partial sum is pruned. NULL ranks last in either direction and ties use minimum
input ordinal. The surviving rows reach the existing stable result sorter in first-appearance order.

No global per-input group array, winner array, bitmap, all-group merge or string ownership per group is needed.
Only final answer keys are converted back from integers and copied. JSON retains the ordinary i128 value
representation; Parquet applies the existing checked output conversion. The path admits relaxed GROUP BY order
because its deterministic answer also satisfies that option. Other aggregates, predicates, secondary order keys,
unsupported types, wider keys and larger cuts retain the established executor.
LIMIT 0 also declines this layout, including with a nonzero OFFSET. Outputs reconstructed from removed
dependent GROUP BY keys use the established executor; packed output reconstruction handles only bare keys.

A key wider than 15 bytes discovered after admission abandons the attempt and replays the established executor
from the beginning. A late long key can therefore waste nearly a full scan before replay. Footer bounds and the
bounded first-page sample can decline early, but never replace the check on every decoded key.

The upfront ledger quote covers buffered records and worker tail fragmentation, bounded heaps, worker stacks,
all concurrent scratch high-water marks, and owned output descriptors/keys/names. Group scratch uses at most
40 bytes per input row; rounded u32 hash slots use fewer than another 16 bytes per row. Each worker's scratch
maximum belongs to a different complete partition, so the concurrent maxima sum to at most the input row count.
Allocation failure before consumption declines. A later decoder or scratch refusal releases the complete
attempt and uses the existing serial/mapped retry chain. Admission requires all inputs to be rescannable, and
no decoded view outlives its owner. Every record worker retains the query's complete projection and shared
canonical error-probe context.

Producer windows use the shared minimum per-worker L2 budget, counting views, integer/code/level lanes and
record work. The global forced decode-window option takes precedence. Partition sizing starts with the count
needed for worst-case group/slot scratch to fit L2, then bounds active producer buffers by
`hw.l3BytesPerActiveWorker(workers)`. Each partition needs one 16-record block and three u32 metadata entries
per producer; the L3 share also leaves room for the decode window and cache headroom. A smaller count may put
fold scratch in L3 instead of L2, but must still fit that L3 share. Unknown L3, ordinary unbounded decode windows
or incompatible phase budgets retain the L2-based count. Skew can exceed these cost estimates without changing
correctness. All execution overrides are documented in [hardware-execution.md](hardware-execution.md); cache
choices never change key identity, exact sums, ties or output layout.
Partition counts contribute to the reservation, so cache sizes can change admission near a memory-budget
boundary. Different hosts may choose the owned-record layout or the established executor for the same query
and budget; decoded values, row order and output bytes remain identical.

`src/core/byte_topk.zig` owns packing, reservation, buffered partitions and complete-group heaps. The scan owns
shape admission and copying the final answer, and the ordinary consumer routes decoded batches through its
indirect record handle. The full-output short-key executor remains separate.
