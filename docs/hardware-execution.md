# Hardware-dependent execution

`core/hw.zig` discovers cache topology once per process. `hw.current()` and
`hw.forCpu(logical_cpu)` expose L1 data, L2 and L3 sizes and sharing CPU sets
for diagnostics. Execution policies use `hw.l2BytesPerWorker()`: the minimum
per-worker budget across the process affinity mask, captured once. This is
stable across scheduling and conservative on heterogeneous CPUs. Each cache
is divided by its sharing logical CPUs, reserving a share for an idle sibling.
Callers subtract fixed state and choose headroom for all live inputs,
temporaries, selection and group IDs.
`hw.l3BytesPerActiveWorker(workers)` divides the smallest whole L3 in the
same affinity snapshot by the query's active worker count. It assumes all
workers compete in that domain and conservatively ignores extra domains.
Unknown remains zero; this hint is also independent of the caller's CPU.

CPUID gives sizes and maximum sharing for the executing x86 CPU. Linux sysfs
supplies the other logical CPUs and concrete sharing sets, including separate
L3 domains. Missing data stays unknown (zero bytes / an empty set); consumers
must use a conservative path. Linux CPU IDs beyond its fixed 1024-bit affinity
mask are unknown. Discovery does not change affinity and retains no open files.
CPU hotplug or affinity changes after discovery are outside this snapshot.
An unknown allowed CPU makes the execution budget zero. Platforms without
affinity/cache discovery, including macOS arm64, return zero;
`hw.l2BytesPerWorkerOr(default)` lets each caller supply an explicit fallback.

`hw.l1dBytesPerWorker()` uses the same affinity minimum and sharing rule.
`hw.cacheLineBytes()` returns the widest discovered L1 data-cache line in that
snapshot, with Zig's target cache-line estimate when unknown. Integer COUNT
owners reserve a worker-local exact preaggregation cache bounded by one
sixteenth of the L2 share and an L1-scale allowance (the larger of half the L1
share and one quarter of existing owner headroom). Unknown L1 uses one sixteenth of the L2 share. Capacity rounds
down to whole cache lines and then a power of two of 16-byte entries; less
than one line disables the cache. NULL uses the ordinary cache with a separate
identity bit. Evictions retain the exact count and minimum source ordinal,
so cache capacity changes work and memory only, including first-row ties.
The record reservation includes every worker's cache before scanning any rows;
ordered run owners omit it because they allocate no cache. COUNT owner fold
pointer tables start within the L2 share and grow from the exact groups seen,
without clearing a rows-sized table for a hot partition. The worst-case group
array remains reserved, with only discovered groups initialized.
Each input source tries two cache-line-widths of probes per slot. If fewer
than one record per slot was eliminated, the worker flushes exact partials
and bypasses the cache until the next source; later skew can still benefit.

Tests can set the thread-local `hw.test_cpu` fixture without affecting other
threads. Process-wide environment overrides are read at discovery; sizes
accept decimal bytes or uppercase `K` / `M` binary suffixes:

| Knob | Meaning |
| --- | --- |
| `ZPQ_HW_L1D_BYTES` | Replace discovered L1 data sizes, in bytes |
| `ZPQ_HW_L2_BYTES` | Replace discovered L2 sizes, in bytes |
| `ZPQ_HW_L3_BYTES` | Replace discovered L3 sizes, in bytes |
| `ZPQ_GROUP_KEY_FD` | Fault-free dependent integer GROUP BY keys; `0` forces legacy grouping |
| `ZPQ_EXPR_BATCH_ROWS` | Force computed DOUBLE batches; `0` selects the legacy full-row-group evaluator |
| `ZPQ_DECODE_BATCH_ROWS` | Unset selects cache-sized decode for admitted float trees; `0`, invalid or overflowing values keep ordinary memory admission; positive values force the window path with a row cap |
| `ZPQ_LEVEL_RUNS` | Aggregate level runs: unset chooses run-dominated pages, `1` forces streaming, `0` keeps legacy expansion |
| `ZPQ_INTEGER_RECORDS` | Eligible integer inline aggregation; `0` selects the established executor |
| `ZPQ_BYTE_RECORDS` | Eligible short-key inline aggregation; `0` selects the established executor |
| `ZPQ_BYTE_TOP_PARTITIONS` | Short-key SUM top-H partitions; unset costs L2 fold scratch and L3 producer buffers, `0` keeps 2048, positive values force a power of two in 64–16384 |
| `ZPQ_BYTE_TOP_BATCH_ROWS` | Short-key SUM top-H decode rows; unset uses shared cache headroom, `0` keeps ordinary decode admission, positive values force a power of two in 64–16384 |
| `ZPQ_MSD_RECORDS` | Eligible retained-page COUNT top-k; `0` selects the established executor |
| `ZPQ_MSD_CACHE_ROWS` | Retained-group scratch rows; unset uses shared L2, `0` keeps 4096, positive values force a bounded power of two |

A positive `ZPQ_DECODE_BATCH_ROWS` caps windows at that many rows, rounded down
to a multiple of 64 with a minimum of 64. Ledger admission and its measured
64-row pilot may select a smaller window. The pilot resizes only between
windows, after the current window has been consumed; live vectors never resize.
The knob is parsed once from the query context as an unsigned
decimal integer. `ZPQ_EXPR_BATCH_ROWS=0` also disables automatic cache decode.

Sharing sets remain real under size overrides. Zero means unknown, letting a
caller test its conservative fallback. Invalid values leave discovery intact.
Execution-specific `ZPQ_*` forced knobs belong in this table as they land;
zero selects the legacy executor. Cache choices may change evaluation batches
or partition counts, never result bytes, checked-error precedence, row-group
boundaries, page sizes, float summation blocks or merge order. A cache size
must never reach any choice that changes bytes or float bits.
Retained-group admission, scratch headroom and override bounds are described in
[aggregation execution overrides](execution_knobs.md).

`python tools/hw_execution_check.py CANDIDATE [BASE]` checks q1-like scalar
and grouped float answers byte for byte under 256K, 1M and 4M L2 overrides
at j1/3/8/16, L3 overrides of 256K/32M and auto/0/64/256 decode windows.
It includes nullable, empty and long string keys, mixed dictionary/plain
encodings, and forced window execution with sparse and empty selections.
It uses isolated temporary fixtures and requires pyarrow.

Flat level streams are validated eagerly before the value decoder is installed.
The default retains encoded streams when runs of at least 64 levels cover half
the requested page rows, determined solely by encoded headers. Other mixed
pages keep the expanded path to avoid decoding packed values twice. This is a
performance hint; both paths perform the same validation.
`ZPQ_LEVEL_RUNS` recognizes exactly `0` and `1`. Empty strings, `00`, `2`,
signed numbers and whitespace select automatic admission. `--fast-levels`
still permits its existing single-run all-present proof under `0`.
RLE spans contribute weighted counts directly to histogram consumers. Other
consumers expand only their requested row window, without a page-sized level
array. Nested levels keep their established expanded representation.
`python tools/level_runs_check.py CANDIDATE [BASE]` checks both forced variants
on nullable V1/V2 pages, scalar/grouped arithmetic, dictionary histograms,
BOOLEAN filters, and memory-driven row slices.

Expression DAGs count every live input, widened lane, intermediate, null layout,
selection and group ID. Their row budget uses three quarters of each SMT
sibling's L2 share after fixed scratch. Batches align to 64 rows; a positive
forced size rounds down to that alignment, with a minimum of 64. The knob
bypasses cache admission for cross-host experiments. The engine reads it once
from the query context and passes a value to workers; evaluating a row group
does not consult the process environment. Checked integer/DECIMAL
trees retain the general evaluator's error precedence. Grouped DOUBLE folds
carry each group's running sum in row order across batches; scalar sums keep
their canonical 64-row reduction and row-group merge boundaries.

The x86 decoding follows the deterministic cache leaves (4 / 0x8000001d), as
documented by the [Linux cache discovery implementation](https://github.com/torvalds/linux/blob/master/arch/x86/kernel/cpu/cacheinfo.c).

Automatic decode admission requires a computed fault-free FLOAT/DOUBLE tree.
Scalar calls share one expression DAG per decoded window and feed prepared
values to the established typed folds. Grouped calls use the existing coded
fold; partition and owned-record consumers keep their own admission. Checked
integer/DECIMAL expressions and unsigned 64-bit aggregate inputs keep their
established path. The forced decode knob exercises eligible windows even when
cache discovery is unavailable.

The cache quote includes decoded values and levels, every live DAG result,
bare-column widening, filter/null masks, key codes, composite scratch and
aggregate state. It also covers the expanded tree if a window of NULL keys
declines recoding. Byte columns require an encoded-header proof that a single
dictionary precedes only dictionary-encoded data pages. Its payload size bounds
one copied string even if every row repeats the longest entry; dictionary
entry counts bound key-domain products. These facts come from page bytes,
not statistics. Unbounded/plain byte columns, excessive composite state or
unknown cache sizes decline automatic admission. A failed admission allocation
also falls back to ordinary execution. Header facts are memoized once per
column chunk per query and shared across workers and passes. The memo uses one
hash shard per active scan worker, initialized before scanning, so independent
header walks proceed in parallel. A refused shard allocation retains a single
inline memo. The shards retain only
sizes/counts, never pointers into input windows. Refusal to allocate the
optional memo leaves that hint uncached. Optional planning and decode attempts
have separate refusal flags, reset per attempt, so a declined hint cannot
misclassify a later host allocation failure.

The row cap is a multiple of 64 within three quarters of the stable per-worker
L2 share after fixed scratch. Memory admission can shrink it further. Decoded
row vectors stay local to that window; full decompression-page buffers retain
their separate bounded lifetime and can still produce DRAM traffic. Cache
windows never alter the canonical scalar reduction blocks, grouped row order,
row-group merges, or output layout.

Automatic decode also compares the whole-row-group hot-state estimate with
that stable L3 share. It counts encoded codes/presence, plain values/levels,
one decompression buffer and aggregate scratch. If that estimate fits, the
whole-row-group path avoids batching overhead at low concurrency. This L3
comparison is a cost hint; the admitted window's L2 quote remains conservative.
An unknown L3 leaves the L2 policy in place. A positive forced decode size
bypasses the L3 cost choice. Both paths keep identical float blocks and bytes.
