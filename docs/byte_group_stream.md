# Streaming full short-key COUNT/SUM answers

The full Parquet answer for one bare BYTE_ARRAY key, COUNT(*) and SUM of one bare signed INT64 column can use
owned inline records. Selected columns may be permuted. Filters, ordering, limits, hidden columns and other
aggregate shapes retain the established executor. Small inputs and low dictionary-cardinality estimates decline
before allocation. Statistics can decline long keys, but never certify a key's width: every decoded key is checked.
A key longer than 16 bytes releases the attempt and restarts the general executor, without changing query semantics.
Only rescannable inputs admit this executor. Remote windows retain the general path; local reader retries use the
retained mapping. A parallel decode-memory refusal retries the record layout once with one worker before giving it
up. Width refusals skip that retry. When chunk bounds are absent, a bounded first-page sample can cheaply decline
long keys; it never certifies the remaining keys or changes decode errors. A long key later in the input can still
cost a refused scan. EXPLAIN reports metadata eligibility, reservation and the conditional runtime guards.

Each 32-byte record owns its key, value, input ordinal, exact key length and NULL flags. A single hash chooses one
of 2048 partitions; its low 30 bits also supply the initial table bucket. Admission limits input to fewer than 2^27
rows, so even the largest table needs at most 28 bucket bits. Hashes never establish identity: equality compares
NULL presence, exact length and all inline bytes. Empty keys, embedded zero bytes and NULL keys remain distinct.

Workers append to private partition runs of 16 records per block. Private block claims amortize the shared cursor;
partition tails and unused claims are included in the reservation. A worker owns and finishes each partition once,
reading all workers' runs. There is no second full record array or scatter copy. Reusable slots free before growing.
The 40-byte group keeps an exact i128 sum, count, key, minimum ordinal and presence flags. The row bound proves
both count and exact sum capacity. Output SUM narrowing happens only after all input decoding has succeeded.

A winner map indexed by minimum input ordinal, a bitmap and prefix counts recover first-appearance output order.
No all-group sort or boxed row answer is needed. At most four normal row groups of typed columns exist at once.
Each window owns contiguous key bytes and slice views into that storage; it makes no allocation per key. Encoding
uses the normal 131072-row groups and 20000-row pages. Proven unique keys can skip a dictionary attempt that
would decline. The output layout and bytes do not depend on worker count or on which admitted executor ran.

The query ledger reserves 96 bytes per input row plus 192 MiB before records are allocated. This conservatively
covers block capacity and fragmentation, full group capacity, reusable slots, winner/order structures, worker stacks
and bounded output windows. Decode and encoder workspaces retain their ordinary separate reservations. Memory
refusal releases the state and retries the established path. Allocation-failure and spawn-failure tests exercise
partial state, windows and ledger cleanup. Unit fixtures check first order, NULLs, empty/zero/16-byte keys and
exact sums at 1, 2, 4, 8 and 16 workers; release fixtures also compare both Parquet readers and frozen output bytes.
