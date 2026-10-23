# Hardware-dependent execution

`core/hw.zig` discovers cache topology once per process. `hw.current()` and
`hw.forCpu(logical_cpu)` expose L1 data, L2 and L3 sizes and sharing CPU sets.
`hw.l2BytesPerWorker()` divides L2 by the logical CPUs sharing it, reserving a
share for an SMT sibling even if idle. Callers subtract their fixed state and
choose headroom for all live inputs, temporaries, selection and group IDs.

CPUID gives sizes and maximum sharing for the executing x86 CPU. Linux sysfs
supplies the other logical CPUs and concrete sharing sets, including separate
L3 domains. Missing data stays unknown (zero bytes / an empty set); consumers
must use a conservative path. Linux CPU IDs beyond its fixed 1024-bit affinity
mask are unknown. Discovery does not change affinity and retains no open files.
CPU hotplug after discovery is outside this process-lifetime snapshot.

Tests can set the thread-local `hw.test_cpu` fixture without affecting other
threads. Process-wide environment overrides are read at discovery:

| Knob | Meaning |
| --- | --- |
| `ZPQ_HW_L1D_BYTES` | Replace discovered L1 data sizes, in bytes |
| `ZPQ_HW_L2_BYTES` | Replace discovered L2 sizes, in bytes |
| `ZPQ_HW_L3_BYTES` | Replace discovered L3 sizes, in bytes |

Sharing sets remain real under size overrides. Zero means unknown, letting a
caller test its conservative fallback. Invalid values leave discovery intact.
Execution-specific `ZPQ_*` forced knobs belong in this table as they land;
zero selects the legacy executor. Cache choices may change evaluation batches
or partition counts, never result bytes, checked-error precedence, float
summation boundaries or merge order.

The x86 decoding follows the deterministic cache leaves (4 / 0x8000001d), as
documented by the [Linux cache discovery implementation](https://github.com/torvalds/linux/blob/master/arch/x86/kernel/cpu/cacheinfo.c).
