# Native architecture checks

```sh
tools/verify/verify arch --rev SHA [--host HOST]...
```

Builds each target at the git revision and `arch.zig` from this tooling checkout,
generates inventory, then ships/runs natively via SSH. Omit `--host` for every
enabled host; repeat it for a subset. Select Apple silicon sparingly.

The default inventory is `/mnt/fast/zpq-out/arch-matrix/fleet.json`;
`--config` or `ZPQ_ARCH_CONFIG` overrides it. Supply prepared `data`, `out`, and
`hosts`. Each host declares `root`, `target`, `duckdb_platform`; optional policies
are `memory_max` (8G), `disk_cap_gb` (20), `nice` (10), `threads`, `hash_only`,
`tiny_perf`, `enabled`, and a `hold` command array. Top-level `notify` receives
hash mismatches. Commands receive literal arguments. Keep host details external.

`zig-out/verify-cache/SHA` holds the archived source, target artifacts and generated
`fleet-*.json`; `ZPQ_VERIFY_CACHE` relocates it. Builds use pinned Zig, baseline CPU,
a shared compiler cache, a 16 GiB user scope and `-j8`. The x86 Linux build host
(`reference_host`, default local hostname) runs first in hash-only mode as the
fixed reference. Prior revision references are discarded. A single selected host
reports `no cross-host comparison` and exits 2 after its native checks.
`--setup-only` builds/caches artifacts without runtime checks or a timing window.

ARM Linux (oci-zt) and Apple silicon (granted-mbp) also run the release CLI smoke.
Linux builds/runs the Lambda bootstrap against the fake runtime API with its
existing anyzig. The Mac needs no Zig. Sources cache beneath `root`; Linux compiler
caches use its parent's `zig-cache` (`/c_data/zig-cache` on oci-zt).

Prepare deterministic inputs once using `arch_prepare.sh --duckdb PATH --study
PATH --lineitem PATH --clickbench PATH --out DIR`. `arch_cases.json` records
scorecard source pointers at `HARNESS`. Official DuckDB 2.0.0 archive and binary
hashes are pinned in `arch_duckdb.json` (linux-amd64, linux-arm64, osx-universal).
Optional Polars uses polars and polars-runtime-32 2.0.0; unavailable support skips
only those rival rows. No pyarrow environment is required on remote hosts.

The runner checks input/binary hashes and exact raw zpq answers against a
same-revision, same-manifest reference. SQL/write arguments stay unchanged.
Rival top-k checks allow equal-count ties; only floats use tolerance. ClickBench
binary keys decode only for rivals. DuckDB validates Parquet with bidirectional
`EXCEPT ALL`; rival writes include file/directory fsync.

The 90-second budget (max 120) covers correctness and fresh-process measurements;
setup/collection are separate. One initial sample precedes three hot samples.
Ratios never stop sampling; missing samples stay null. Hash-only has no timings.

Each run collects only `summary.json` (including host-computed hashes) and small
logs. A hash mismatch fetches only that cell's exact output. Successful remote
answers are deleted; setup stays cached. Exits: 0 passed, 1 failed, 2 incomplete
(including correctness budget exhaustion), 3 raw zpq mismatch (preserve/notify).
