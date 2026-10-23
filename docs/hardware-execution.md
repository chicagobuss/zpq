# Hardware-dependent execution

`core/hw.zig` discovers cache topology once per process. `hw.current()` and
`hw.forCpu(logical_cpu)` expose L1 data, L2 and L3 sizes and sharing CPU sets
for diagnostics. Execution policies use `hw.l2BytesPerWorker()`: the minimum
per-worker budget across the process affinity mask, captured once. This is
stable across scheduling and conservative on heterogeneous CPUs. Each cache
is divided by its sharing logical CPUs, reserving a share for an idle sibling.
Callers subtract fixed state and choose headroom for all live inputs,
temporaries, selection and group IDs.

CPUID gives sizes and maximum sharing for the executing x86 CPU. Linux sysfs
supplies the other logical CPUs and concrete sharing sets, including separate
L3 domains. Missing data stays unknown (zero bytes / an empty set); consumers
must use a conservative path. Linux CPU IDs beyond its fixed 1024-bit affinity
mask are unknown. Discovery does not change affinity and retains no open files.
CPU hotplug or affinity changes after discovery are outside this snapshot.
An unknown allowed CPU makes the execution budget zero. Platforms without
affinity/cache discovery, including macOS arm64, return zero;
`hw.l2BytesPerWorkerOr(default)` lets each caller supply an explicit fallback.

Tests can set the thread-local `hw.test_cpu` fixture without affecting other
threads. Process-wide environment overrides are read at discovery; sizes
accept decimal bytes or uppercase `K` / `M` binary suffixes:

| Knob | Meaning |
| --- | --- |
| `ZPQ_HW_L1D_BYTES` | Replace discovered L1 data sizes, in bytes |
| `ZPQ_HW_L2_BYTES` | Replace discovered L2 sizes, in bytes |
| `ZPQ_HW_L3_BYTES` | Replace discovered L3 sizes, in bytes |
| `ZPQ_EXPR_BATCH_ROWS` | Force computed DOUBLE batches; `0` selects the legacy full-row-group evaluator |

Sharing sets remain real under size overrides. Zero means unknown, letting a
caller test its conservative fallback. Invalid values leave discovery intact.
Execution-specific `ZPQ_*` forced knobs belong in this table as they land;
zero selects the legacy executor. Cache choices may change evaluation batches
or partition counts, never result bytes, checked-error precedence, row-group
boundaries, page sizes, float summation blocks or merge order. A cache size
must never reach any choice that changes bytes or float bits.

`python tools/hw_execution_check.py CANDIDATE [BASE]` checks q1-like scalar
and grouped float answers byte for byte under 256K, 1M and 4M L2 overrides
at j1/3/8/16. It uses an isolated temporary fixture and requires pyarrow.

Expression DAGs count every live input, widened lane, intermediate, null layout,
selection and group ID. Their row budget uses three quarters of each SMT
sibling's L2 share after fixed scratch. Batches align to 64 rows; a positive
forced size rounds down to that alignment, with a minimum of 64. The knob
bypasses cache admission for cross-host experiments. Checked integer/DECIMAL
trees retain the general evaluator's error precedence. Grouped DOUBLE folds
carry each group's running sum in row order across batches; scalar sums keep
their canonical 64-row reduction and row-group merge boundaries.

The x86 decoding follows the deterministic cache leaves (4 / 0x8000001d), as
documented by the [Linux cache discovery implementation](https://github.com/torvalds/linux/blob/master/arch/x86/kernel/cpu/cacheinfo.c).
