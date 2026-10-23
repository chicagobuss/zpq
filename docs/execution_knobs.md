# Execution overrides

These diagnostic environment variables preserve query semantics. Admission still checks the query shape, types,
ownership and memory budget; forcing a layout never bypasses those checks. Leave them unset in ordinary use.

| Variable | Unset | `0` | Positive value |
|---|---|---|---|
| `ZPQ_INTEGER_RECORDS` | Eligible integer inline aggregation | Established executor | Enable eligible inline path |
| `ZPQ_BYTE_RECORDS` | Eligible short-key inline aggregation | Established executor | Enable eligible inline path |
| `ZPQ_MSD_RECORDS` | Eligible retained-page COUNT top-k | Established executor | Enable eligible retained-page path |
| `ZPQ_MSD_CACHE_ROWS` | Scratch window from the per-worker L2 budget | Legacy 4096-record window | Force window, rounded down to a power of two after clamping to 16–16384 records |

For the three path switches, only the literal `0` disables the path. For the scratch override, malformed values use
the default. The selected scratch size is prepaid and appears as `cache_rows` on the `msd_fold` profile event.

`msd_groups.l2BytesPerWorker()` is the integration seam for shared hardware discovery. Until that module is wired in,
it returns the existing 640 KiB worker budget: half a 1.25 MiB cache. No detection is implemented in the kernel.
The shared-module integration replaces that stub with `hw.l2BytesPerWorkerOr(640 << 10)`, which supplies the minimum
per-worker L2 budget over the process affinity mask and retains the fallback when discovery is unavailable.
Sizing subtracts local metadata and one radix frame, leaves half the remainder for input and nested frames, then fits
two record arrays. A 512 KiB worker budget chooses 2048 records; 640 KiB chooses 4096. Initial partition counts remain
fixed pending a separate change to their ownership arrays and reservation proofs.

Scratch size changes only where exact refinement copies a bucket into bounded arrays. It changes neither identity
nor first-input ordinals, output ordering or output encoding. The path counts integers; floating-point reductions
remain on their existing execution path and retain their summation rules.
