# Group lookup layout

Fixed-width normalized keys use eight-byte slots: a projected 32-bit hash and a 32-bit group id. The hash only
rejects candidates. A match always compares the complete normalized key, including its null markers. Group ids
index separate key, first-position and aggregate buffers. The full 64-bit hash remains the routing value for
partition, census and certified top-k decisions.

A probe returns either the existing id or the first empty slot. Insertions reuse that slot unless capacity growth
changes the mask. Rehashing uses the cached projected hash. The load factor stays below three quarters, so a
probe always finds an empty slot. Variable-width keys use an adapted map insertion carrying the already-computed
hash; only growth needs a preliminary lookup. A new slot temporarily borrows its key and is removed on failure.
The owned key and id are published after all buffers have capacity.

Fixed keys are packed and hashed in batches of 64 rows before lookup. Larger tables prefetch the initial slot.
Consumption remains in input row order, preserving first appearance, canonical float-key normalization, error
propagation and aggregate reduction order. Variable-width keys retain the existing resolver and string memo.

A useful local reduction can clear its fixed table while retaining allocated buffers. The retained capacity
remains charged to the shared budget; no group ids or owned aggregate values survive the clear. Abandoned and
final flushes release the buffers, including final flushes whose table is already empty. Variable-key tables
continue to release their owned keys on reset.

COUNT top-K uses one certificate with hot counters, cell bounds and hash-class bounds as state choices. An input-wide
sample chooses integer hot membership; a complete pass counts those lanes exactly and counts every cold row into
its bound. Late, NULL and out-of-range values remain cold. Saturated bounds cannot certify an exclusion.

When cells do not separate the cut, owner partitions collect four-byte hash fragments in 512-byte frontiers. The
partition supplies eight additional identity bits. A collision only raises a bound: the residual groups complete
keys and certifies only when its exact cutoff exceeds every excluded bound. The COUNT singleton case may instead
prove that enough complete singleton groups first appear before every excluded tie. Cell and identity candidates
share the residual scan and verification; discarded count records are released before that scan. Legacy
`ZPQ_SKETCH_*`/census controls select producers within this certificate, rather than separate COUNT executors.

For unfiltered short byte keys, COUNT-ranked payload cuts can collect identity bounds during the first key pass,
then fold payload state only for admitted full keys. COUNT alone proves the cut, so signed integer SUM is valid;
integer AVG keeps its exact sum and non-NULL denominator. MIN/MAX and first positions retain their ordinary
semantics. Fallible payload expressions, DECIMAL overflow and order-sensitive float folds keep their established
eligibility exclusions. Payload columns still decode through the normal paths, including losing-group rows;
a small candidate row count does not imply sparse payload pages or suppress their required decode errors.
