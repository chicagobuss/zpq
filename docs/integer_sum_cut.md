# Bounded selection after inline integer aggregation

One bare signed INT64 key and one SUM of a bare signed INT64 column can use the inline radix executor for
ORDER BY that SUM with a positive LIMIT. There is one order key, no filter, HAVING or hidden columns, and
H = OFFSET + LIMIT is at most 1024. NULLs, ascending/descending order, and either selected column order work.
Other shapes use the established result cut. Admission and decode-memory restart use the same upfront query
ledger reservation and refusal flag as full integer output.

Each input record owns its key, value, ordinal and NULL flags. The two radix levels send all records of an exact
key to one of 2048 partitions. Each partition is folded completely by one worker before any group enters selection.
An equal hash never substitutes for full integer/NULL-key equality. Exact i128 sums cannot overflow for the admitted
row count below 2^31. Each group retains its minimum input ordinal.

Unlike full output, the fine array contains only 24-byte records. A worker reuses its own group/slot buffers for one
partition at a time, then discards that partition's groups after selection. Buffers free before growing: the sum of
workers' largest partitions is bounded by all input rows, and no old/new growth overlap exceeds the reservation.
The complete fine array, retained scratch and bounded heaps fit within the existing conservative quote. All partial
buffers and heaps unwind on allocation failure; unstarted tasks remain claimable after spawn failure.

A worker keeps top-H across its completed partitions. This subsumes retaining each partition's top-H separately:
if a group is outside its worker's top-H, at least H complete groups precede it, so it cannot be in the global top-H.
The global winners are therefore contained in the union of worker heaps, independent of scheduling or worker count.
The total comparison is SUM presence (NULL last in either direction), exact sum and direction, then first appearance.
No winner map, bitmap, full first-order group sequence or full typed answer is built.

The final combine sees at most workers times H rows. Retained rows enter the ordinary stable result program in
first-appearance order, which applies ORDER BY, OFFSET and LIMIT as usual. Only those rows are materialized, with
i128 SUM intact. JSON can print a wide sum. Parquet narrows selected output as usual; a wide losing sum causes no
error. All input decoding has completed before output narrowing. The small owning row storage remains charged to
the query ledger after record/scratch teardown, just like the general GROUP BY answer.

Kernel tests deliberately put equal final sums in different key/arrival/first orders, with duplicates spread across
workers and NULL keys/sums. They check hand-known winners at 1, 2, 4, 8 and 16 workers, including H larger than the
group count, allocation failure and wide winning/losing sums. Scan tests compare with the established partition cut
and inject spawn failure. The release verification additionally uses a tie-heavy multi-row-group file against rc4,
checking JSON bytes, Parquet decoded values/order and invariant candidate bytes across workers and budget fallback.
