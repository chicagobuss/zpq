# Retained-page radix COUNT top-k

An unfiltered `GROUP BY string_key, signed_int32_key ORDER BY count(*) DESC LIMIT k` can avoid an all-group table.
The executor retains one owned image of decompressed string pages and emits one buffered record per input row.
Each complete partition is refined by exact integer metadata and cached eight-byte string digits. Only its best H
completed groups survive, where H includes OFFSET. Final output uses the ordinary count ordering and first-input
ordinal tie break. Intern IDs, hashes, worker scheduling and partition numbers never define identity or output order.

`src/core/msd_groups.zig` owns this kernel. `src/core/retained_pages.zig` owns payload storage. The consumer's record
sink connects them to ordinary scan workers without widening the ordinary worker's indirect record handle.
The standalone alternatives and physical-work measurements are in [composite_count_probe.md](composite_count_probe.md).

## Admission and ownership

Admission checks the exact query shape, source types, encodings, row/reference bounds and rescannability. Expansion
encodings, unsigned secondary keys, unsupported query forms and insufficient capacity use the established executor.
Metadata is used only to decline or reserve storage; it never establishes string equality or answers an aggregate.
`--unordered` is admitted: the caller permits any group order, and deterministic first-appearance output remains valid.

Before consuming any rows, one reservation covers all retained page capacity, records and buffered tails, block
indices, worker caches, bounded heaps, fixed output credit and decoder workspace. Payload capacity uses the existing
chunk inflation ceiling plus compressed chunk bytes. The capacity is reserved virtually; only actual decoded pages
are written. First dictionary headers bound decoder workspace. Reader probes are registered before startup and
awaited before their bytes are inspected. Probe/header failures decline. Reader-backed inputs never use an unstarted
planning mapping as a substitute for ready bytes.

Decoder workers draw from a child ledger funded by that reservation, with no incremental parent draw during input
consumption. A refused initial reservation declines. An admitted query does not switch to a second executor midway.
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
indices. Small buckets use two reusable 4096-record arrays; at 32 records or fewer, insertion on cached words finishes
the next split. Metadata includes length and both NULL distinctions. Tail words are compared first; equal tails
advance through all remaining head words. A common 20-byte URL prefix is skipped only if every decoded non-NULL
key verified it. Any mismatch simply selects coverage from byte zero. Footer statistics cannot enable the skip.

A complete leaf gives its exact count and minimum input ordinal. Each worker keeps a bounded top-H heap. Its Hth
exact count is a safe global lower bound: a bucket containing fewer rows cannot contain a winning group. Pruning
uses strictly smaller counts; equal-count buckets remain, preserving every boundary tie. A pair is complete in one
partition before selection. The union of worker heaps therefore contains the global top H. Iterating the largest
child and recursing into the others bounds stack depth even for adversarial skew.

## Verification

Focused kernel checks cover full byte-tuple identity, NULL/empty/embedded-zero and long keys, signed metadata,
flat/skewed counts, ties, worker counts and spawn/allocation failures. Scan checks cover encoded/plain values,
SNAPPY/uncompressed pages, all five worker counts and tight/ample budgets. A destructive-window reader zeros
released source bytes; a separate check consumes the parent's remaining credit at first consumption. Both require
the admitted path to finish from owned pages and prepaid decoder storage.

External fixtures include keys differing only inside the proposed skipped prefix, ties spanning sixteen actual
workers, permuted output columns and OFFSET. They compare rc4 values/order and JSON output, require invariant bytes
across workers and budgets, and fully decode Parquet with PyArrow and DuckDB. Real long-string inputs receive the
same checks. This path does not change filtered expression evaluation, integer composite grouping, or the frozen
single-key full-output and SUM top-k kernels.

The whole-column decoder reservation excludes retained payload capacity because it has already been funded. Its
workspace quote retains a per-worker arena-rounding margin. Counting payloads twice would force interleaved sliced
decoding and could change which of two corrupt columns fails first. A PLAIN-page regression checks that a late key
failure still precedes an early secondary-column failure, at all five worker counts, with complete ledger cleanup.
An unordered admission regression checks the retained MSD path as well as both existing inline record layouts;
relaxing order preserves path selection, exact values and the ordered candidate's deterministic output.
