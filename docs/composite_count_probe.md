# Composite COUNT top-k: physical work budget

Initial table before implementation, 2026-10-23. Unfiltered SELECT k,f,count(*) AS c GROUP BY k,f ORDER BY c DESC
LIMIT10 only. No expression or filtered-shape changes. Baseline c1a6ee2. Physical INT64 or byte-string k, INT32 f.
Exact values, NULL tuples and first input ordinal define identity/ties; only ten rows are emitted. No all-group order,
full answer materialization, string-per-pair ownership, or cross-owner key merge is semantically required.

| Dataset | Rows | Distinct k | Observed pairs | k payload | k/f compressed | k/f uncompressed Parquet |
|---|---:|---:|---:|---:|---:|---:|
| lstr1m |20M|1M|19,801,944|2,400,446,317B|719,358,250B|2,506,457,422B|
| lstr100k |20M|100k|18,125,804|2,405,318,545B|707,892,140B|2,510,425,574B|
| zipf15 |30M|129,231|1,356,241|240MB|78,953,165B|91,439,054B|
| card10 |30M|10|10,000|240MB logical; retain codes|52,968,405B|52,951,509B|

Strings max200B, average120B. f logically80MB/120MB. Outputs roughly1.5KB JSON long keys, a few hundred bytes for ints.
Measured exact counts/bytes out/q15-metrics.json, fresh cold/warm profiles out/profiles-q15-before; no competitor floor.

Hardware: six P cores/twelve SMT timed, local1.25MiB L2/core,30MiB L3,65.71GB/s read,58.91GB/s payload read+write,
3.45ns L2 /98.51ns dependent DRAM. Hardware probe methodology in SPEED-OF-LIGHT.md. Granite supplied read41GB/s,
read+write23GB/s,79ns DRAM,16threads one CCD,~1MiB L2/core. Write-only W/23 and mixed(R+W)/23 are alternative
conservative accounting views, not independent resources to add. RFO, page faults, zeroing and codec work are extra.

| Work | Local optimistic bound / attainable evidence | Granite-class writes23GB/s | Baseline warm ms |
|---|---|---|---:|
| Long read/decode | (0.72GB compressed+2.48GB decoded payload)/58.91=54ms bandwidth only; string slice views can add320MB writes. Fresh min/max(k,f) proxy398/384ms includes extra compare/aggregation and teardown. |2.48GB decoded payload writes108ms, potential320MB views14ms, compressed read18ms. Codec cycles not modeled away. |lstr1m/lstr100k attributed188/184; scalar-proxy398/384|
| Long identity + sparse pairs | Hash all decoded bytes once, copy owned key once per distinct k (about120MB/12MB, not2.4GB per-pair).24B pair records480MB. Fold one completed partition in L2; two dependent cache accesses/row give23ms across6cores plus instructions. Target50–100ms fold; identity must be measured separately. |480MB pair writes21ms, pair read+write960MB42ms. Unique key ownership5.2/0.52ms writes. Avoid2.4GB duplicate key image104ms writes or209ms mixed. |aggregation2135/2209|
| zipf15 read/decode |79MB compressed; coded inputs retain codes. Logical360MB would be7.5ms mixed minimum; scalar proxy20ms. |Unnecessary expanded INT64 f alone240MB10ms writes; preserve physical INT32/codes. |23 attributed|
| zipf15 grouping |L2-sized preaggregation suppresses repeats, flush sparse24B pairs, abandon if reduction<2. One full unreduced720MB image is an upper design budget, not required minimum. Target30–70ms plus decode. |Full image31ms writes/63ms mixed; reduction should remove most of it. |261 aggregation,~88 census/merge|
| card10 decode/group |10×1000 observed code product; exact histogram+first ordinal fits~80KB. Fresh scalar proxy13ms; grouping target5–15ms, no30M tuple scatter. |Only observed histogram entries emitted, not720MB intermediate. |9 decode,20 aggregation,3 merge;42 total|
| Top-H/output |Complete each owner partition, worker heap H=limit+offset, combine<=workers×H, first-ordinal tie;1–5ms CPU hypothesis long/zipf, less for card10. |Bounded cache-sized heaps and ten output rows; negligible bandwidth. |22–24 long merge,3 card10|
| Whole |Initial long hypothesis0.5–0.9s, zipf0.08–0.18s, card10~0.025–0.045s; these are falsifiable targets, not achieved floors. |Measure on target hardware; do not scale entire local wall by59/23. |2414/2489 long,414 zipf,42 card10|

Profile phase shares overlap. Long attributed decode is below isolated proxy wall because proxy has extra string
min/max compares and different memory lifetime; neither is a pure decoder measurement. The progress table's first
bandwidth bound is **54ms**, not a throughput figure.

Design to measure: exact sharded byte interner with owned stable entries and cached full hash; group bytes by shard
inside bounded decode batches, one lock per nonempty shard/batch. IDs are internal stable entry pointers; full bytes
establish equality, fingerprints never do. Translate stored dictionary entries once per decoded batch where offered;
plain values use the same exact interner. Sparse24B(key-token,f,count,ordinal,hash) records in4096private buffered
partition chains, no second full record image. Cache-local24B pair groups plus32-bit slots, one owner per completed
partition. At20M pairs, average partition group+slots about180KB,~360KB/SMTpair, leaving cache headroom for input.
Only H surviving keys are copied into ordinary result rows, sorted by ordinal before ordinary stable result program.
Long key interning is an explicit extra synchronization/random-access cost; compare against copying bytes inline if
it dominates. Integer keys bypass interner, retain physical/coded f. Bounded local preaggregation handles skew; a
small coded Cartesian domain uses exact histogram/first positions. Capacity refusal returns all owned state before
old-path restart. Fixed record/worker/heaps reserve up front; unknown interner growth separately charged to ledger.

Grounding: pinned Zig0.17 PriorityQueue/HashMap/ArenaAllocator and charged allocator inspected; existing GroupTable,
agg_coded and partition-run implementations; DuckDB radix_partitioned_hashtable.cpp and Polars streaming group_by.rs
local source snapshots. Primary references: https://duckdb.org/pdf/ICDE2024-kuiper-boncz-muehleisen-out-of-core.pdf
and https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoding . Earlier HASHAGG-REPORT and probe show
inline keys beat random long-key pointers; canonical exact IDs avoid that random access entirely during pair fold.
This differs from carrying raw string views into final aggregation. Existing executor already cuts complete generic
groups before materialization; a new standalone TopN is not the missing work. Dense entire key×f domain is rejected
for long keys (1B or100M combinations), but small proven coded domains are useful. No unproved partial-count pruning.

Approved admission refinement: interning receives its entire parent-ledger reservation before worker consumption,
not incremental parent draws. Admit only string encodings whose stored uncompressed key-chunk bytes bound unique
owned payload (PLAIN, dictionary/RLE and DELTA_LENGTH_BYTE_ARRAY); DELTA_BYTE_ARRAY prefix expansion declines.
At most N used distinct strings: N fixed24B entry slots, owned payload capacity B=sum uncompressed key chunks,
plus prepaid bounded map growth. Reserve80N+B+64MiB for interner in addition to64N+128MiB pair/worker storage.
Unused dictionary entries are not interned. A declined initial draw leaves the scan untouched. No parent ledger
borrowing during interning. Full byte equality establishes each stable internal token; tokens never rank output.

## Standalone decision: cached MSD, before an engine port

Kernel inputs are the **real**, fully decoded lstr1m/lstr100k k+f rows, not a synthetic string generator. Each has 20M rows, 2.40GB owned key payload and a 160MB compact row image. Inputs are validated and prefaulted outside timing. Allocation, worker startup, grouping and ordered top ten are inside timing. All runs are pinned to the same six P cores/twelve SMT, under a 12GiB limit, serialized with other measurements. Five interleaved repetitions per configuration; every exact result matches the full-byte DuckDB oracle including minimum input ordinal. These times cannot be added to an assumed 54ms decoder: 54ms was a bandwidth bound, not a decoder measurement.

| Dataset | Kernel | ms min / median / max | Peak RSS MiB | Notes |
|---|---|---:|---:|---|
|lstr1m|MSD, cutoff16, verified prefix19|152.28 / **154.43** / 160.40|2755.8|Exact top ten|
|lstr1m|MSD, cutoff32, verified prefix19|151.72 / **152.64** / 160.56|2755.8|Exact top ten|
|lstr1m|Exact interner,64 shards|663.54 / **677.11** / 685.78|2947.6|Exact top ten|
|lstr1m|Exact interner,256 shards|838.83 / **848.51** / 862.69|2962.1|Exact top ten|
|lstr1m|32MiB CMS,u8, **oracle T**|274.58 / **281.22** / 311.74|2478.8|2,374,475 survivor rows; exact verification/witness selection excluded|
|lstr1m|32MiB CMS,u16, **oracle T**|287.92 / **314.41** / 370.20|2478.8|9,528,336 survivor rows; exact verification/witness selection excluded|
|lstr100k|MSD, cutoff16, verified prefix19|139.81 / **152.99** / 155.08|2760.8|Exact top ten|
|lstr100k|MSD, cutoff32, verified prefix19|139.98 / **146.80** / 158.36|2760.6|Exact top ten|
|lstr100k|Exact interner,64 shards|428.21 / **429.03** / 444.04|2794.7|Exact top ten|
|lstr100k|Exact interner,256 shards|609.68 / **622.54** / 650.28|2797.5|Exact top ten|
|lstr100k|32MiB CMS,u8, **oracle T**|285.30 / **298.33** / 339.42|2483.6|142,540 survivor rows; exact verification/witness selection excluded|
|lstr100k|32MiB CMS,u16, **oracle T**|273.89 / **291.88** / 326.87|2483.6|1,598,084 survivor rows; exact verification/witness selection excluded|

A later balanced word-order cohort uses one binary for every mode (so compare within that cohort). The actual verified chunk prefix is **20** bytes, `https://example.org/`; the initial19-byte sweep left its trailing slash in the cached word. This corrects the earlier19-byte shorthand. Prefix validity is checked against every row inside timed scatter, never inferred as truth from stats.

|Dataset|Word order / prefix|Total ms min / median / max|Scatter median ms|Fold median ms|
|---|---|---:|---:|---:|
|lstr1m|prune,prefix19|170.96 / **176.48** / 181.01|85.65|87.87|
|lstr1m|prune,prefix20|157.49 / **161.68** / 165.13|82.19|73.47|
|lstr1m|tail,prefix20|151.12 / **152.70** / 155.95|84.97|64.21|
|lstr1m|tailfull,prefix20|176.97 / **181.85** / 190.13|86.90|88.25|
|lstr100k|prune,prefix19|152.64 / **158.83** / 165.31|82.62|72.99|
|lstr100k|prune,prefix20|148.63 / **154.25** / 155.01|85.13|64.49|
|lstr100k|tail,prefix20|137.73 / **143.81** / 146.17|82.96|56.62|
|lstr100k|tailfull,prefix20|235.38 / **262.60** / 264.81|82.04|175.32|

Choose **cached MSD with length/f partitioning, tail word first, then complete head-word coverage, small-bucket cached-word insertion, and exact-leaf lower-bound pruning** for the long-string shape. It avoids the interner's ownership map and shared identity locks, and does not need a probabilistic candidate selector. This is a measured kernel decision, not a landed query-speed claim.

## Memory traffic and phase floors

Uncore counters cover only the kernel interval, enabled after validation/prefault and disabled before reporting/freeing. Both memory-controller counters are summed. Calibration with eight1GiB sequential passes returns approximately8GiB total reads, and eight1GiB non-temporal writes returns approximately8GiB writes (each controller supplies about half). Counters are system-wide and include background traffic, allocation zeroing, RFO and writeback; they are not per-process logical-store counts. The clean five-run cohort excludes the earlier cohort overlapping a compiler. Both raw cohorts are retained. Do not infer exact per-level DRAM bytes from cache-store counters.

|Dataset|Kernel|Measured DRAM reads GB|Measured DRAM writes GB|Write-only budget at23GB/s ms|Mixed budget at23GB/s ms|Measured total ms|
|---|---|---:|---:|---:|---:|---:|
|lstr1m|msd|5.300|0.838|36.4|266.8|151.76|
|lstr1m|intern|24.071|2.726|118.5|1165.1|675.84|
|lstr1m|cms8|8.910|1.213|52.7|440.1|336.08|
|lstr100k|msd|5.059|0.819|35.6|255.6|145.42|
|lstr100k|intern|11.777|2.142|93.1|605.2|421.91|
|lstr100k|cms8|8.934|1.204|52.3|440.8|326.46|

The mixed column is a traffic budget using the observed local traffic on a23GB/s memory system, **not a prediction**: cache topology, prefetch and other workloads change the traffic. Write-only and mixed bounds are alternative accounting views and must not be added. A32MiB sketch exceeds this machine's30MiB LLC and consumes almost the whole32MiB CCD cache on the supplied desktop, leaving no working space for decoding or input. Its dirty random counters demonstrably reach DRAM. Private32MiB sketches per thread would be a much larger DRAM image.

|Phase|Semantic / layout floor|Measured decoded-input evidence|Granite-class23GB/s writes|
|---|---|---|---|
|Decode / own bytes|Read707–719MB compressed and decode2.51GB stored payload. Only ten output keys need independent ownership.|Baseline attributed188/184ms decode; isolated min/max proxy398/384ms includes extra comparisons. Neither is a pure decoder wall floor.|About109ms if2.51GB decoded buffers are written once; another2.4GB copy adds104ms write budget or209ms mixed.|
|Partition image|One16B record per row:320MB. f/length histogram reads160MB row image in this standalone layout.|Initial hist~4ms; final tail-first scatter~83–85ms, including20B prefix verification and cached-word loads.|320MB logical image14ms writes,640MB image read+write28ms; allocation and RFO are additional.|
|Cache grouping|One owner per completed f/length bucket. Read cached words, partition or finish in L2, reload source only for surviving equal-word runs.|Final tail-first fold64ms/57ms; no all-group table or order. Same-binary full leaf enumeration costs88ms/175ms fold.|Two reusable scratch arrays; initial max top bucket11,302 records,~353KiB combined per worker. Two SMT workers~706KiB before stack/histogram overhead; measure target cache pressure.|
|Prune / top ten|Only prune a bucket with size **strictly below** a proven tenth count from completed exact leaves. Equal-count candidates remain.|Worker heaps union into at most120 survivors; combine~2microseconds. No global key merge, output group image or sorting19M groups.|Cache-sized heaps and~1.5KB output: negligible write traffic.|
|End to end|One decode, one record image, local refinement, bounded output. Retained decoded bytes are still required for later word reloads.|Engine remains baseline2414/2489ms warm. A fused path has a0.4–0.7s hypothesis pending real decode/ownership measurement; standalone153/144ms is not end-to-end.|Port must retain existing decompressed pages or charge any extra owned copy. Avoid a second expanded key image.|

## Passes, levels and cache finishing

The initial sweep is70 runs across prefix0/19, cutoffs0/16/32/64,8/11-bit fanout, and pruning/full grouping. Without skipping the verified head, medians185/171ms. Prefix19 with cutoff32 gives154/144ms; continuing radix recursion instead of finishing small cached-word buckets gives303/179ms. Full leaf enumeration is189/255ms. These are separate cohorts from the later word-order sweep.

The top level has2000 f/high-length buckets rather than a hash partition. Each is copied once into reusable local scratch and split by the remaining length bits; cached radix then handles key identity. In one retained prefix19/cutoff32 run, both datasets send20M records through one cached radix pass; small buckets finish by cached-word insertion. No entire320MB ping-pong image is created. The worker's two scratch arrays alternate without copying back; only the top-ten leaves escape.

|Dataset|Top max bucket|Radix rows|Cached-word reloads|Insertion records|Insertion16B moves|Pruned rows|
|---|---:|---:|---:|---:|---:|---:|
|lstr1m|11,302|20,000,000|8,161,941|20,305,933|44,171,187|19,974,248|
|lstr100k|11,216|20,000,000|6,293,404|16,763,906|39,831,726|19,989,318|

Per-depth counters include calls, logical record/reload/move bytes, exclusive work nanoseconds summed over workers, and the log2 bucket-size histogram. Most work is at cached depths19 and27. Deep equal leaves still verify the remaining bytes; singleton/pruned buckets never do. Those busy-thread times are not additive wall phases. Detailed per-level and bucket distributions are in `out/q15-kernel/{msd-runs,tail-runs}.jsonl`; initial bucket histograms show the majority of recursive calls at sizes1–15. Logical insertion moves and scratch writes are not all DRAM writes; the separate uncore experiment measures that difference.


## Encoding and string-identity alternatives

Actual page headers, not footer encoding lists, were walked for **every** string column chunk in the listed files.
Per-chunk RG/column, encoding/value count, dictionary entries/bytes, compressed/uncompressed size and stats-prefix
bytes are retained in `out/q15-string-pages/results.json` (6,328 hits chunks plus the study files). A separate full
Arrow decode verified the suggested study prefixes against every value and measured length distributions.

| Input / string columns | Dictionary data pages / values | PLAIN data pages / values | Consequence |
|---|---:|---:|---|
|lstr1m k,20 chunks|20 /184,320|2,155 /19,815,680|99.08% of values are PLAIN; codes do not replace whole-query identity.|
|lstr100k k,20 chunks|20 /184,320|2,155 /19,815,680|Same encoding mix; intern dictionary entries once where useful.|
|sstr10m k,29 chunks|116 /2,066,656|1,401 /27,933,344|6.89% coded; length-tagged u128 packing handles the PLAIN majority.|
|sstr100k k,29 chunks|203 /3,529,152|1,342 /26,470,848|11.76% coded.|
|plainsstr100k k,29 chunks|0 /0|1,516 /30,000,000|All PLAIN.|
|hits,28 byte columns,226 RGs|6,387 /2,468,915,102|25,230 /331,014,814|Most columns wholly dictionary-coded; five mixed columns below.|

Study dictionaries use RLE_DICTIONARY; hits uses PLAIN_DICTIONARY. The mixed hits columns, dictionary/PLAIN values:
Title24,785,920/75,211,577; URL19,477,504/80,519,993; Referer12,385,280/87,612,217;
SearchPhrase63,968,152/36,029,345; OriginalURL48,355,815/51,641,682. The other23 byte columns are entirely
PLAIN_DICTIONARY (99,997,497 values each). No surveyed data page uses DELTA_BYTE_ARRAY. Its adjacent-equality
shortcut therefore saves nothing on these inputs; on such pages an empty suffix plus a prefix equal to the **previous
full value length** proves adjacent k equality, still requiring f equality for a composite pair. Dictionary codes
are chunk-local: translate each dictionary entry by full bytes once; never assume equal numeric codes across chunks.

Every long chunk suggests the same20B prefix, verified for all20M values per dataset with zero mismatches.
Skipping it avoids400MB of key-comparison/hash input, not400MB of required Parquet decode output. The short files
have verified1B prefix `k`, lengths2–11 (seven or five observed lengths). Long keys have161 observed lengths40–200.
Length bucketing is therefore a strong exact digit, without inspecting key bytes; the PLAIN prefix already supplies it.
For keys<=16B, big-endian u128 **plus length** provides exact identity even with embedded zeros; that production
path already exists. It is insufficient for the long shape unless an exact overflow path owns/verifies the rest.

For long comparisons, a discriminating word first can reject unequal values early; equal keys still require all
bytes. This host exposes AVX2, not AVX-512, so a64B AVX-512 throughput claim cannot be measured here. The exact
interner benchmark uses the engine's existing full-byte equality routine. The tail-first MSD experiment measures
an alternative without full-byte hashing: in its paired cohort, fold falls73.47→64.21ms and64.49→56.62ms versus
prefix20 head-first. There is no claim that a URL's last word is always the best digit: this is measured for these
actual keys. Full-mode group totals and byte-tuple fixtures verify that the changed word order leaves no byte unchecked.

## Sketch distribution and exactness costs

The exact full-pair distributions are measured, not inferred from NDV:

|Shape|Groups|True tenth count / max|Exact frequency evidence|
|---|---:|---:|---|
|lstr1m|19,801,944|3 /4|19,605,260 count1;195,318 count2;1,360 count3;6 count4|
|lstr100k|18,125,804|5 /7|16,372,206 count1;1,638,975 count2;108,884 count3;5,509 count4;225 count5;4 count6;1 count7|
|zipf15|1,356,241|11,767 /11,857|Highly concentrated counts; u16 needed for a useful bound.|
|card10|10,000|3,172 /3,231|Every pair's true count exceeds u8 saturation.|

The independent distribution probe evaluates8/16/24/32MiB,2/4 rows,u8/u16, retaining exact pair frequencies.
Its mixed hashes model collision inflation; these are not runtime Wyhash timings. At32MiB/depth2/u8 and an oracle
T, lstr1m retains about2.38M rows and lstr100k142k. An optimistic final-estimate heap of64 witnesses only obtains
T=2 for lstr1m (about9.80M survivor rows), T=4 for lstr100k (816k). Even this selector is an oracle over final
estimates, not proof that a small online heap finds them. At16MiB the lstr1m witness threshold is1, so nothing can
be excluded. Real full-byte runtime hashing at32MiB/u8 and true T produces2,374,475/142,540 survivor rows, close to
the independent collision model. The oracle sketch time already exceeds exact MSD before witness counting or
survivor identity is paid.

For zipf15,32MiB/u8 saturation retains about23.56M rows; u16 retains118,115 rows at oracle T. For card10,u8 retains
all30M rows; u16 retains31,869. All saturated counters mean **infinity**, not255/65535, or the sketch could
underestimate. Collisions and fingerprint collisions may only enlarge the set verified by full bytes and f.
Flat/adversarial distributions must fall back if the survivor budget is exceeded. These distribution results do
not establish a fast runtime for integer q15. Card10's exact10×1000 count/first histogram remains the preferred
shape, avoiding a30M-row scatter entirely; zipf15 needs measured cache-local reduction rather than the long-key MSD.

The proposed three logical tasks cannot in general fit into two streaming decode passes: pass1 sketches and selects
witnesses; pass2 counts witnesses exactly, establishing T **at its end**; applying that final T to every other key
then needs retained decoded data or pass3. A valid two-pass variant needs a proven bound established in pass1 or a
certificate/fallback protocol. We did not silently substitute an oracle threshold into a purported exact engine.
Existing COUNT census/CountSketch and owner top-H were inspected; the sketch probe is deliberately only a lower
bound experiment, not a second production implementation. Positive-only SUM bounds need a proof on contributions
and do not cover arbitrary signed SUM. No filtered expressions were changed.

## Ownership boundary identified by the prototype

The prototype could not be inserted as another record sink unchanged. `SliceReader` deliberately
resets byte-page arenas after a page drains, and whole-RG decode storage is reused on the next RG. A cached record
holding an offset into that storage would be invalid at later refinement. Retaining a borrowed pointer would repeat
the lifetime class of the replay bug just repaired. Copying every decoded key into a new2.4GB image would be correct
only with an upfront reservation, but adds104ms write-only /209ms mixed traffic budget on the desktop class.

The next design seam is **prepaid retained decompressed key pages**: cache initial digits while those pages are hot,
retain only their owned payload until refinement completes, discard slice/value-view arrays, and avoid a second
per-row key copy. A contiguous bounded byte region permits u32 offsets; a segmented design needs an equally exact
compact reference. Stored uncompressed bytes bound PLAIN/dictionary page payload but not arbitrary prefix-expanded
strings or allocator overhead. Both must be bounded before admission and checked at runtime; no answer is derived
from metadata. All required memory is reserved before consuming input, and non-replayable inputs decline. The
existing engine remains the fallback, including NULL/out-of-domain keys, unsupported encodings and tight budgets.
The integration must preserve ordinary decode error precedence and release retained pages only after all reloads.
The production ownership implementation is described in [composite_count_radix.md](composite_count_radix.md).
The standalone probe itself retains its simpler, fully owned decoded-input interface.

Sources: cached radix/string sorting [Bingmann et al.](https://panthema.net/2013/parallel-string-sorting/);
[COUNT-MIN original paper](https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf);
[Parquet encodings](https://parquet.apache.org/docs/file-format/data-pages/encodings/);
[Linux counter definitions](https://raw.githubusercontent.com/torvalds/linux/master/tools/perf/pmu-events/arch/x86/alderlake/uncore-memory.json).
The measurements and raw artifacts above, rather than competitor timings, choose this path.

A retained tail-first run gives this per-depth work table (depth columns are cached eight-byte slots, starting at20;
row visits include recursive revisits, and work time is summed across workers):

|Dataset|Slot|Row visits|Radix records|Reloaded words|Insertion moves|Busy ms|
|---|---:|---:|---:|---:|---:|---:|
|lstr1m|20|59,138,015|20,111,670|697,238|69,550,659|419.54|
|lstr1m|28|1,351,943|0|52,324|788,571|8.01|
|lstr1m|36|82,528|0|21,600|33,023|0.64|
|lstr100k|20|57,710,429|20,113,296|177,275|67,445,105|381.92|
|lstr100k|28|337,167|0|19,324|249,193|2.03|
|lstr100k|36|26,158|0|11,985|10,371|0.19|

Tail-first trades more cache-local insertion moves for far fewer source-word reloads, which matters more when a
random miss is79ns. The traffic table above measures the original prefix19/head-first kernel; it is not labeled as
a tail-first traffic measurement. The selected version's traffic is checked separately before any port claim.
