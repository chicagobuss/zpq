# Execution overrides

These diagnostic environment variables preserve query semantics. Admission still checks the query shape, types,
ownership and memory budget; forcing a layout never bypasses those checks. Leave them unset in ordinary use.

The shared [hardware execution catalog](hardware-execution.md) lists all execution and hardware overrides.
Eligible inline paths are enabled by default. For retained grouping, an unset scratch override uses the per-worker
L2 budget; zero selects the legacy 4096-record window. A positive override rounds down to a power of two after
clamping to 16–16384 records.

For the three path switches, only the literal `0` disables the path. For the scratch override, malformed values use
the default. The selected scratch size is prepaid and appears as `cache_rows` on the `msd_fold` profile event.

Retained grouping uses `hw.l2BytesPerWorkerOr(640 << 10)`, which supplies the minimum per-worker L2 budget over the
process affinity mask and uses a 640 KiB fallback when discovery is unavailable. Detection stays in the shared module.
Sizing subtracts local metadata and one radix frame, leaves half the remainder for input and nested frames, then fits
two record arrays. A 512 KiB worker budget chooses 2048 records; 640 KiB chooses 4096. Initial partition counts remain
fixed pending a separate change to their ownership arrays and reservation proofs.

Scratch size changes only where exact refinement copies a bucket into bounded arrays. It changes neither identity
nor first-input ordinals, output ordering or output encoding. The path counts integers; floating-point reductions
remain on their existing execution path and retain their summation rules.
