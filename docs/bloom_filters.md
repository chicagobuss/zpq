# Bloom filters and dictionaries

How ZPQ uses split-block Bloom filters (SBBF, parquet-format `BloomFilter.md`) and dictionary pages to skip row groups
on equality, and how it writes filters. Code: `src/core/parquet/bloom.zig` (format, probe, builder),
`src/core/filter/membership.zig` (what to probe and how to read it), `src/core/filter/prune.zig` (where the answer is
used).

## What prunes

A row group is skipped when an `=` leaf of the filter cannot match there. Statistics decide first; a leaf they leave
open (neither ruled out nor proven to match every row) is then tested against the chunk's Bloom filter and, if that
says "maybe", against the chunk's dictionary. Either can only add a skip: a leaf statistics prove fully matching keeps
its literal everywhere, so neither is consulted for it. `IN` is an OR of `=` leaves and `NOT IN` an AND of `!=` leaves,
so `pruneRowGroup` composes them with AND, OR and NOT as before; `!=`, ranges and `LIKE` never use membership.

`membership.load` runs once per query before any row group is planned and records each literal proven absent on its
chunk (`ColumnChunk.absent`). `pruneRowGroup` reads that record, so the S3 planner, the scan workers and the write path
all reach the same decision from the same footer.

## Which literals are probed

Only literals the binder (`filter/parser.zig`) marks `stored`: values that are exactly a stored value of the column's
physical type, so their PLAIN bytes are what a writer hashed and what a dictionary holds. That covers INT32, INT64,
FLOAT, DOUBLE, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY of the column's width, including DATE, TIME and TIMESTAMP literals
that land exactly on a stored tick and unsigned integers. It excludes DECIMAL (compared after decoding), INT96, FLOAT16,
BOOLEAN, NaN, repeated columns, encrypted chunks and chunks kept in another file. A float zero is probed as both signed
zeros, since `=` treats them as equal while their bits hash differently.

The hash is XXH64 with seed 0 over the PLAIN bytes, BYTE_ARRAY without its length prefix. The upper 32 bits pick a block
(fastrange); the lower 32 bits give one bit per word of it. That mask depends on the literal alone, so it is computed
once per literal: each row group then costs a multiply, a 32-byte load and an 8-lane vector AND/compare.

A filter whose header names another algorithm, hash or compression, or a size that is not a whole number of blocks,
reads as "no filter", never as an error.

## Reading them

Reads go through `membership.Source`: a mapping for local files (slices, nothing copied), the S3 range fetcher
(`engine.S3Membership`), and later any other reader. Each source states its `Cost`: a request's fixed cost as the bytes
the medium moves in that time, the requests in flight at once, and the longest request it serves whole.

- Only row groups statistics keep, and only columns with an open `=` literal, are read.
- Ranges merge across the gap a makespan estimate over the source's connections finds cheapest, and large spans split
  so every connection stays busy. Writers lay a file's filters out row group by row group, so one column's filters are
  rarely adjacent; merging across the others' can still beat a request per filter.
- When the length is known (`bloom_filter_length`), just the 32-byte blocks the literals select can be read instead
  of whole filters. Blocks are located through the header every known writer emits; they count only once a real
  header from the same file matched that shape. Blocks must save a quarter of the estimated time to be chosen, since
  whole filters can be kept warm and blocks cannot.
- On a remote source, the filters of the first eight row groups are read first. If they prune fewer than a quarter of
  them, the rest are left unread, which bounds what filters cost a query they cannot help.
- Whole filters are kept in the warm Lambda's metadata cache under the object's ETag, revalidated with the footer.

Dictionaries are read only for literals the filters left open, in row groups still kept, through the same source and
the same first-wave rule (on a mapping the wave bounds decode time instead). A dictionary covers its chunk when
`encoding_stats` list a dictionary page and only dictionary-encoded data pages. Without `encoding_stats`, only
parquet-java's v1 signature (PLAIN_DICTIONARY beside nothing but level encodings) is trusted; a v2 chunk may have
fallen back to PLAIN pages without saying so.

The query summary's `membership` object reports what was probed, read and pruned.

## Writing them

`--bloom-filter COL[:FPP]` (Lambda: `bloom_filters`) asks for a filter on an output column, at false-positive
probability FPP (default 0.01). A filter is sized for every present value of the chunk being distinct, filled, then
halved for as long as its estimated false-positive probability, measured from the bits actually set, stays within FPP.
With a power-of-two block count, halving by OR-ing adjacent block pairs gives exactly the filter those values would
have filled at half the size, so the result is the size their distinct count calls for. Sizes are powers of two from 32
bytes to 128 MiB, which every reader accepts.

Filters are written after the last row group, row group by row group in column order, with `bloom_filter_offset` and
`bloom_filter_length` both set. They are held until then: about 1.2 bytes per distinct value per chunk at 0.01.

A byte copy keeps each copied chunk's filter, whatever probability it was written at; a requested column whose source
chunk has none is decoded to build one. A re-encoded row group gets filters only for requested columns. Dropped columns
lose theirs. BOOLEAN, INT96, nested and FIXED_LEN_BYTE_ARRAY DECIMAL columns take none. The encoder also writes
`encoding_stats`, so its dictionary-encoded chunks can be pruned on their dictionaries by any reader.

## Checking it

- `bloom.zig` tests the probe against the spec's scalar `block_check` and `parquet-testing`'s `bloom_filter.xxhash.bin`,
  and that folding is exact.
- `membership.zig` pins the binder's `stored` decision to hand-computed PLAIN bytes for each type, fuzzes values of
  every type through the writer and back through the reader's keys, and covers the S3 planning choices against a
  simulated remote.
- `tools/bloom/check.py` (CI, via `tools/cli_smoke.sh`) checks with an independent Python SBBF that every value of
  pyarrow- and zpq-written chunks tests present, and that zpq's lookups match pyarrow's counts.
- `tools/bloom/interop.py` checks that DuckDB and Hardwood read zpq-written filters and prune with them, and that zpq's
  lookups on files they and pyarrow wrote match DuckDB and `--scan-all`.
