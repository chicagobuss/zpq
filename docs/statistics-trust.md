# Statistics: trust is opt-in

When zpq answers from Parquet statistics, and when it decodes instead.

Parquet files carry per-column statistics (min / max / null counts). The spec says they must be accurate, but
real-world writers ship inaccurate ones: a `parquet-mr 1.8.2` file in the test corpus records a `min` of `2.00` for a
column that contains `1.00`. A reader that trusts those statistics returns an incorrect answer without an error.

By default, `min / max / sum` decode their values instead of taking the answer from statistics. Filters can still
prune data using corroborated statistics. The metadata shortcuts have separate trust boundaries:

- **`count(*)`** is answered from the row group's `num_rows` (structural, not a statistic).
- **A join's row-group skips** use corroborated footer statistics, on by default, on the same trust boundary as the
  row-group pruning below. An inner or semi join on one integer or DATE key skips the probe row groups wholly outside the
  keys the filtered dimension kept. The top-K cut (`... JOIN ... ORDER BY f.id LIMIT n`) reads only the probe row groups
  whose bounds can still reach the n-th key. Internally consistent but false bounds can change the result, as for row-group pruning. `--scan-all`
  turns both off, and `ZPQ_JOIN_TOPK=0` turns off the top-K cut.
- **Row-group pruning** uses footer/page statistics that pass type/order and consistency checks. Internally
  consistent but false bounds can still skip matching rows and change the result; `--scan-all` ignores statistics.
  See the [validation contract](output-format.md#validation-and-skipped-data).

Floats compare in total order, as they sort: NaN equals NaN and lies above every number, so `x > 5` and `NOT (x < 5)`
keep NaN rows. Float bounds leave NaN out, so a chunk whose max is below 5 can still hold rows passing `x > 5`. zpq
prunes such a chunk only when NaN is proven absent: by a `nan_count` of zero in the chunk's statistics or its page
index (a file declaring IEEE_754_TOTAL_ORDER must write one), or by the NaN probe, which reads the float chunk alone and
tests its values (its dictionary, when every page is coded) before any row group is planned. A missing count means NaN
may be present. zpq writes `nan_count` for every float chunk it encodes; a chunk it byte-copies keeps its source's
statistics, count or no count. The probe runs for row output, writes and S3 reads; aggregates over local files use it
only for uncompressed chunks, where it costs less than the scan it replaces.

If you know your writer's statistics are accurate, `--trust-stats` answers `min / max / sum` from metadata in
microseconds, without reading a data page:

```bash
zpq query data.parquet --aggregate "min(price), max(price)" --trust-stats
```

`--trust-stats` also reads a float chunk's missing `nan_count` as zero, so `>` and `>=` prune on its bounds as if it
held no NaN; a NaN row in such a chunk is then lost.

`--trust-stats` trades correctness on files with bad statistics for speed, per query. `--scan-all` uses no
statistics at all: row-group pruning is off and every filter is evaluated on decoded values (`count(*)` still comes
from the row counts).

For supported flat OPTIONAL primitive columns that contain no nulls, `--fast-levels` can skip materializing definition
levels. The check reads the encoded level stream rather than trusting writer statistics, and falls back to the normal
decoder unless one RLE run proves that the entire page is present. Aggregate scans use this proof automatically;
`ZPQ_LEVEL_RUNS=0` selects their legacy plain-value level handling.
Projection/write paths still require `--fast-levels`.
