# Bloom filters and dictionaries

Moved from the README for 0.5.0. The full reference is [bloom_filters.md](bloom_filters.md).

Statistics cannot prune an `=` or `IN` on a column whose values span its whole range in every row group, such as an
ID, a UUID or a trace key. Two structures can: a column chunk's split-block Bloom filter, and the dictionary of a chunk
whose every data page is dictionary-encoded. zpq reads both, for the row groups statistics keep and the literals they
leave open, and skips a row group that neither could contain. Neither has false negatives, so a skipped row group never
held a match; `--scan-all` turns both off along with statistics.

On S3 the filters are fetched while the read is planned, in as few requests as a cost model over the connection pool
finds fastest, as whole filters or as just the 32-byte blocks the literals select. When the first eight row groups'
filters prune too few to pay for the rest, the rest are left unread. A warm Lambda keeps whole filters, keyed by the
object's ETag, so repeated lookups read none.

Writing them is opt-in, per column:

```bash
zpq query events.parquet -o compacted.parquet --bloom-filter trace_id --bloom-filter user_id:0.001
```

Each filter is sized from the chunk's values at the false-positive probability given (default 0.01) and placed after
the row groups, as the spec lays them out. A byte copy keeps the filters its source already has, for every column it
copies; a column dropped or re-encoded loses its filter unless `--bloom-filter` asks for one. See
[docs/bloom_filters.md](bloom_filters.md).
