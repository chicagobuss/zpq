# Ordered encoder page segments

`encoder.Plan` decides a whole column chunk's dictionary, statistics and page boundaries before building pages.
`page` reads only that plan and its inputs, so callers may build independent pages in any order on separate arenas.
`finishSegments` retains their buffers in file order: dictionary first, then data pages. It calculates the same
chunk sizes, relative page offsets, page-index bounds and Bloom filter as the contiguous `finish` interface.
`finish` uses that same metadata construction and joins the segments for callers that require a contiguous chunk.

Segments borrow page buffers. The plan, its dictionary and every page arena must remain alive until placement
returns. The output consumer keeps this storage in its existing charged column wave; only cloned footer and
page-index metadata survive a wave reset. Placement advances the output offset only after a successful chunk
write. A sink error propagates immediately through the existing cancellation/publication path.

This removes the final whole-column copy. Header/payload joins inside each page and encoded level/value staging
remain. The dictionary limits, compression, page and row-group boundaries, output ordering and statistics flags
are unchanged. Dictionary coverage is a property of the complete plan: an automatic Bloom uses its final entries
only when every page uses that dictionary. An explicit Bloom probability keeps precedence.

The [Parquet chunk contract](https://parquet.apache.org/docs/file-format/data-pages/columnchunks/) requires the
single dictionary page, when present, to precede its data pages. A [Bloom filter](https://parquet.apache.org/docs/file-format/bloomfilter/)
describes a whole column chunk. Segment ownership and placement do not weaken either requirement.
