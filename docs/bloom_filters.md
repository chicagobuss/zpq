# Bloom filters and dictionaries

How ZPQ uses split-block Bloom filters (SBBF, parquet-format `BloomFilter.md`) and dictionary pages to skip row groups
on equality, and how it writes filters. Code: `src/core/parquet/bloom.zig` (format, probe, builder),
`src/core/filter/membership.zig` (what to probe and how to read it), `src/core/filter/prune.zig` (where the answer is
used).

## What prunes

A row group is skipped when an `=` leaf of the filter cannot match there. Statistics decide first; a leaf they leave
open (neither ruled out nor proven to match every row) is then tested against the chunk's Bloom filter or, for a
chunk without one, against its dictionary. Either can only add a skip: a leaf statistics prove fully matching keeps
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
- Bytes the retained suffix already holds are sliced from it and are not planned. What remains goes through the same
  dispatch planner as column data: the in-flight cap, the 8 MiB split, the latency cap, twice the selected bytes, and
  the fetch window charged to each file. A column whose filters touch is one span when that span's buffers fit. A
  row-group-major run keeps a filter's own requests when the gap between them fails those caps.
- The S3 source assumes Lambda's in-region costs (about 25 ms a request, 80 MB/s a connection). A successful footer
  request's time to first byte — a cold suffix, a stale 200/206, or a 304 — is that sample. A slower one means a
  distant link, and then the plan uses that round trip and the bandwidth the first filter reads achieved. Until one
  has been timed, and in-region, the calibration stands: the first reads also open connections, which a few hundred
  kilobytes cannot tell apart from slow bandwidth.
- When the length is known (`bloom_filter_length`), just the 32-byte blocks the literals select can be read instead
  of whole filters. Blocks are located through the header every known writer emits; they count only once a real
  header from the same file matched that shape. Blocks must save a quarter of the estimated time to be chosen, since
  whole filters can be kept warm and blocks cannot.
- On a mapping, every header page and then every block page a probe needs is requested at once (`MADV_WILLNEED`),
  so a cold page cache costs two rounds of parallel reads rather than one disk read per filter.
- On a remote source, lengths the footer omits are read first, a window at a time, and those header bytes are released
  before any filter body is admitted. A contiguous run is then one pass only when the one buffer it allocates fits
  the charged window and one round trip of bandwidth (8 MiB at most). Split requests and a suffix tail write into
  that buffer, so the charge is the buffer and not a second copy. A cache hit is copied into the same buffer for the
  wave and released with it; a hit that vanishes before the copy is a miss, and the GET fills that same buffer. A
  filter that does not fit the window is not read, wherever the file stores it; its row group stays, and
  `bloom_skipped_oversize` counts it. Each
  wave plans its candidate filters once and admits a prefix of that plan; the planner's arrays live in the wave and
  are released with the bodies. What does not fit is the next wave, planned once there. A larger run, and a
  row-group-major run, reads a first wave of at most eight row groups and stops when they prune fewer than a quarter;
  what follows is window-sized. Dictionaries keep the first-wave rule either way.
- Whole filters are kept in the warm Lambda's metadata cache under the object's ETag, revalidated with the footer. The
  cache may hold a sixteenth of the function's memory, at most 256 MiB, and a query's default `max_memory` is what it
  leaves.

Dictionaries are read only for chunks without a filter, in row groups still kept, through the same source and the
same first-wave rule (on a mapping the wave bounds decode time instead). Where a filter says "maybe", the literal is
almost always there; its false positives are too rare to pay for reading the dictionary too. A dictionary covers its chunk when
`encoding_stats` list a dictionary page and only dictionary-encoded data pages. Without `encoding_stats`, only
parquet-java's v1 signature (PLAIN_DICTIONARY beside nothing but level encodings) is trusted; a v2 chunk may have
fallen back to PLAIN pages without saying so.

The query summary's `membership` object reports what was probed, read and pruned.

## Writing them

By default, every fully dictionary-encoded output chunk gets a split-block filter at false-positive probability
0.01, built from its dictionary entries rather than by hashing every row. A re-encoded dictionary contains exactly
the distinct present values. A copied dictionary may also contain unused entries, which only add false positives;
the copy path checks every page header for dictionary encoding before building a filter, without decoding data pages.
A chunk that falls back to non-dictionary pages gets no automatic filter. Existing source filters are preserved.
Automatic filters are initially sized from the dictionary entry count; their retained bytes, like explicitly requested
filters, are outside `--max-memory`.

`--no-bloom-filters` (JSON: `"bloom_filters_enabled": false`) disables automatic creation. Explicit
`--bloom-filter COL[:FPP]` specifications still apply, including to columns without dictionary encoding. The opt out
also applies to aggregate and limited Parquet output. BOOLEAN columns are not dictionary encoded.


`--bloom-filter COL[:FPP]` (Lambda: `bloom_filters`) asks for a filter on an output column, at false-positive
probability FPP (default 0.01). A filter is sized for every present value of the chunk being distinct, filled, then
halved for as long as its estimated false-positive probability, measured from the bits actually set, stays within FPP.
With a power-of-two block count, halving by OR-ing adjacent block pairs gives exactly the filter those values would
have filled at half the size, so the result is the size their distinct count calls for. Sizes are powers of two from 32
bytes to 128 MiB, which every reader accepts.

Filters are written after the last row group, one column's after another, with `bloom_filter_offset` and
`bloom_filter_length` both set. Other writers go row group by row group, so an equality on one column finds its
filters scattered among the others'; in a zpq-written file they are contiguous and a reader fetches them in one ranged
read (split for parallelism). Readers locate each filter through its own offset, so the order is invisible to them.
Memory, per requested column at 0.01: while a chunk is encoded its filter is first sized as if every present value
were distinct, about 1.2 bytes per present value rounded up to a power of two (so up to 2.4, and at most 128 MiB), and
folded once the chunk is filled. The folded filter is held until the file closes: 1.2 to 2.4 bytes per distinct value
per chunk, so a write with a filter on a unique key holds up to 2.4 bytes per row of that column until it closes. This
is outside `--max-memory`. A copied chunk's filter is copied out of the input when its row group is written (a
windowed S3 input releases the row group before the footer) and held until the file closes, also outside
`--max-memory`: its stored size, whatever probability it was written at.

A byte copy keeps each copied chunk's filter, whatever probability it was written at; a requested column whose source
chunk has none is decoded to build one. A re-encoded row group gets automatic dictionary filters and any explicitly requested filters. Dropped columns
lose theirs. Explicit specifications reject BOOLEAN, INT96, nested and FIXED_LEN_BYTE_ARRAY DECIMAL columns.
Automatic generation also covers copied nested dictionaries and INT96 dictionaries; INT96 entries hash as their
12 PLAIN bytes, without timestamp conversion. Noncanonical INT96 times of day decline automatic generation: a
reader can normalize them to a different byte representation of the same instant. The encoder also writes
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
