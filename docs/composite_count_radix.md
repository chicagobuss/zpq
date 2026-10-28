# Retained-page radix COUNT top-k

An unfiltered `GROUP BY string_key, signed_int32_key ORDER BY count(*) DESC LIMIT k` can avoid an all-group table.
The executor retains one owned image of decompressed string pages and emits one buffered record per input row.
Each complete partition is refined by exact integer metadata and cached eight-byte string digits. Only its best H
completed groups survive, where H includes OFFSET. Final output uses the ordinary count ordering and first-input
ordinal tie break. Intern IDs, hashes, worker scheduling and partition numbers never define identity or output order.

`src/core/msd_groups.zig` owns this kernel. `src/core/retained_pages.zig` owns payload storage. The consumer's record
sink connects them to ordinary scan workers without widening the ordinary worker's indirect record handle.

## Admission and ownership

Admission checks the exact query shape, source types, encodings, row/reference bounds and rescannability. Expansion
encodings, unsigned secondary keys, unsupported query forms and insufficient capacity use the established executor.
Metadata is used only to decline or reserve storage; it never establishes string equality or answers an aggregate.
`--unordered` is admitted: the caller permits any group order, and deterministic first-appearance output remains valid.

Before consuming any rows, one reservation covers all retained page capacity, records and buffered tails, block
indices, worker caches and stacks, bounded heaps, output credit and decoder workspace. Payload capacity uses the existing
chunk inflation ceiling plus compressed chunk bytes. The capacity is reserved virtually; only actual decoded pages
are written. First dictionary headers bound decoder workspace. Reader probes are registered before startup and
awaited before their bytes are inspected. Probe/header failures decline. Reader-backed inputs never use an unstarted
planning mapping as a substitute for ready bytes.

The output bound funds H copies of the largest retained chunk capacity plus the output row descriptors. A key is
contained in one such chunk; dictionary entries can win for several secondary keys, so unique payload bytes alone
are not a sufficient bound. Decoder workspace is dead before surviving keys are copied, and the reservation uses
the maximum of those phase credits. Output never requests another parent draw after consumption.

Decoder workers draw from a child ledger funded by that reservation. MSD makes no incremental parent draw during
consumption; reader-window draws remain the established reader's separate behavior. A refused initial reservation
or failed retained allocation declines before rows are consumed. A decoder-ledger refusal, allocation failure after
admission, unsupported retained encoding, or an internal retained-ownership violation releases owners, abandons the
reader, and retries the established path
from its original mapping. Admission requires rescannability for this retry; it never reuses released window bytes.
The ordinary whole-column-to-sliced decode adaptation can rewind its retained cursor only before emitting records.
It preserves column decode precedence and never rereads a released row group.

Both compressed and uncompressed payloads become owned retained pages. Slice/view arrays may then be recycled;
record references remain within their retained chunk. Retained owners outlive all refinement and all surviving-key
copies. Output keys are copied before owners are freed. Library calls release every allocation, including partial
initialization and partial output allocation failures. The existing transparent huge-page advice may improve fault
and release costs where its policy permits it; it changes no admission rule or ownership guarantee.

## Exact refinement

Production records are 28 bytes: cached big-endian u64 word, full payload pointer, u32 length, signed i32 secondary
key, and u32 input ordinal with two NULL flags. Rows are bounded below 2^30 before admission. This supports empty,
embedded-zero and long keys and every signed INT32 secondary value; it has none of the standalone probe's 255-byte
or 0–999 restrictions.

Private 16-record blocks feed 4096 initial f/length buckets. Hashing is unnecessary: every equal pair necessarily
has the same initial bucket, and different pairs sharing it are separated by full metadata and byte-digit coverage.
An owner gathers and compacts its bucket's existing blocks. Large buckets partition in place through compact block
indices. Small buckets use two reusable arrays sized from a per-worker cache budget; at 32 records or fewer,
insertion on cached words finishes
the next split. Metadata includes length and both NULL distinctions. Tail words are compared first; equal tails
advance through all remaining head words. Each decoder derives a common prefix from its retained keys, shortening
it against every non-NULL key. After decoding joins, refinement uses the common prefix of those verified local
prefixes. Its length therefore depends only on the input bytes; NULL keys are excluded and an empty key forces
zero skipped bytes. The views remain owned by retained pages. Footer statistics never establish skipped bytes.

The cache-budget integration seam and forced variants are described in [execution overrides](execution_knobs.md).
Shared hardware discovery supplies the minimum per-worker L2 budget across the process affinity mask; unknown
topology retains the 640 KiB fallback and the legacy 4096-record window.

A complete leaf gives its exact count and minimum input ordinal. Each worker keeps a bounded top-H heap. Its Hth
exact count is a safe global lower bound: a bucket containing fewer rows cannot contain a winning group. Pruning
uses strictly smaller counts; equal-count buckets remain, preserving every boundary tie. A pair is complete in one
partition before selection. The union of worker heaps therefore contains the global top H. Iterating the largest
child and recursing into the others bounds stack depth even for adversarial skew.
The radix fold runs on reserved worker stacks. If the root worker cannot start, an iterative in-place heapsort
over the same block-indexed run compares complete tuple identities, then folds adjacent equal records. It needs
no additional allocation or recursive radix frames on a library caller's stack. First ordinals and bounded top-H
selection remain exact; this exceptional fallback trades throughput for bounded stack use.

## Verification

Focused kernel checks cover full byte-tuple identity, NULL/empty/embedded-zero and long keys, signed metadata,
flat/skewed counts, ties, worker counts and spawn/allocation failures, including root spawn refusal on a small
caller stack at the maximum admitted H. Scan checks cover encoded/plain values,
SNAPPY/uncompressed pages, all five worker counts and tight/ample budgets. A destructive-window reader zeros
released source bytes; a separate check consumes the parent's remaining credit at first consumption. Both require
the admitted path to finish from owned pages and prepaid decoder storage.

External fixtures include keys differing only inside the proposed skipped prefix, ties spanning sixteen actual
workers, permuted output columns and OFFSET. They compare values, order and JSON output with the previous build, require invariant bytes
across workers and budgets, and fully decode Parquet with PyArrow and DuckDB. Real long-string inputs receive the
same checks. This path does not change filtered expression evaluation, integer composite grouping, or the frozen
single-key full-output and SUM top-k kernels.

The whole-column decoder reservation excludes retained payload capacity because it has already been funded. Its
workspace quote retains a per-worker arena-rounding margin. Counting payloads twice would force interleaved sliced
decoding. Decode failures follow the shared [canonical page order](decode-errors.md), including input/row-group
order and projection ties. The record worker receives the complete output projection. Cross-mode regressions cover
an early secondary-column page against a late key page, and integer projection ties, at all five worker counts,
whole/64/256-row windows, admission decline and mapped replay, with complete ledger cleanup.
An unordered admission regression checks the retained MSD path as well as both existing inline record layouts;
relaxing order preserves path selection, exact values and the ordered candidate's deterministic output.
Runtime page headers are checked before retained copying, even when footer encoding lists omit an expansion
encoding. A late unsupported page retries the ordinary decoder instead of reporting the retained capacity as
input corruption. Regressions cover that replay, a refusal after an earlier row group emitted records, upfront
output-credit decline, failed retained allocation, and checked block exhaustion in optimized builds.
