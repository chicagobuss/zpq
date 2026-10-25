# Architecture mini-matrix

`tools/verify/verify arch --host HOST` reuses cached inputs, binaries, the
official DuckDB CLI, and an optional two-package Polars environment. Configure
`ZPQ_ARCH_CONFIG` once. Each invocation creates a new run directory and collects
its `summary.json` and exact answers locally. Successful remote answers are
removed after collection; setup remains cached for the next integration push.
The existing Python entry point also forwards `arch` to the shell launcher.

The measured host process is `arch.zig`, a standalone Zig program. It invokes
the shipping zpq CLI and the official pinned DuckDB 2.0.0 CLI. SHA-256 is computed
by `sha256sum` or `shasum`, with no normalization of zpq output. Python is used
only by `arch_polars.py`, with Polars 2.0.0 and polars-runtime-32 2.0.0. Missing
Polars support skips those rival rows and records the reason.

The runner first checks input/binary hashes, then executes every zpq query and
compares raw output hashes with the reference host. A mismatch returns exit 3
immediately, preserves its output, and invokes the inventory's notification
command. A reference must have the same engine revision and manifest hash.
Zpq receives the scorecard SQL unchanged except source substitution; the two
write cases use the scorecard's original native CLI arguments. No ORDER BY or
tie-break is added. Rival top-k answers are checked against full grouped truth,
allowing equal-count ties. Rival schemas select comparison lanes: integers and decimals compare exactly,
text compares as bytes, and tolerance applies only to FLOAT/DOUBLE columns.
ClickBench binary keys are decoded only for rival comparisons. Raw zpq hashes
remain untouched.

Every Parquet result is decoded by the DuckDB CLI and checked against complete
truth using two-direction `EXCEPT ALL`. Rival file and directory fsync are
included in write times. Zpq uses its default durable output behavior. This mode
uses no pyarrow environment; it follows the CLI-only deployment requirement.

The headline uses each engine's default threads. An inventory may cap a service
host explicitly. The default 90-second budget includes the full hash pass,
validation and measurements; setup and result collection are separate. It cannot
be raised above 120 seconds. One initial sample has uncontrolled file cache;
three hot samples are the target. An input read precedes each hot sample.
Timers include spawn, full answer capture and reap; Polars also includes Python
startup/imports. These are fresh-process ratios, not in-process throughput.

Measurements stop only at the fixed rep target or the wall time budget. The
ratio never controls sampling. Each query records individual samples, rep counts,
medians and a stop reason. Missing hot samples have null medians. No missing
sample is invented or counted as a win. A budget exhausted during required
correctness work is exit 2; after correctness it may end optional perf work with
exit 0 and a budget stop reason. `wall_seconds` records the measured host run.

Exit codes: 0 required checks passed, 1 unexpected failure, 2 incomplete or
invalid invocation, 3 exact zpq hash mismatch. `--hash-only` host policy does not
run rivals or publish query timings. `--tiny-perf` validates all cases and times
only point lookup and PDS-H q6 in the hot loop.

## Prepare once

`arch_cases.json` is a small selection from the scorecard revision pinned by
`HARNESS`, with source pointers per query. Inputs are prefixes of existing
deterministic study data, PDS-H publisher SF1 and ClickBench sub1. Build one set:

```sh
tools/verify/arch_prepare.sh --duckdb /path/to/pinned/duckdb \
  --study /path/to/study --lineitem /path/to/sf1/lineitem.parquet \
  --clickbench /path/to/sub1.parquet --out /path/to/arch-data
```

Build zpq at the engine commit under test, not the tooling branch HEAD:

```sh
zig build cli -j8 -Doptimize=ReleaseFast -Dcpu=baseline -Dstrip=true \
  -Dtarget=x86_64-linux-musl -Drevision=FULL_ENGINE_SHA --prefix /path/to/artifacts
zig build-exe tools/verify/arch.zig -lc -O ReleaseSafe -mcpu baseline \
  -target x86_64-linux-musl -femit-bin=/path/to/binaries/x86_64-linux-musl/arch-runner
```

Also build `aarch64-linux-musl` and `aarch64-macos` when needed. Place each engine
and runner at `binaries/TARGET/{zpq,arch-runner}`. DuckDB official archive and
extracted binary hashes are pinned in `arch_duckdb.json`; setup checks both and
every measured invocation rechecks the binary. The pins come from the official
[2.0.0 release](https://github.com/duckdb/duckdb/releases/tag/v2.0.0).

Inventory example (keep actual host names, paths and coordination IDs outside
the repository):

```json
{
  "revision": "FULL_ENGINE_SHA",
  "data": "/absolute/path/to/arch-data",
  "binaries": "/absolute/path/to/binaries",
  "out": "/absolute/path/to/results",
  "budget_seconds": 90,
  "hosts": {
    "HOST": {
      "root": ".cache/zpq-arch",
      "target": "x86_64-linux-musl",
      "duckdb_platform": "linux-amd64",
      "memory_max": "8G",
      "disk_cap_gb": 20,
      "nice": 10
    }
  }
}
```

Run the first host without `reference`; after it passes, set `reference` to its
collected summary path. `--setup-only` caches setup without a timing window.
Supported DuckDB platforms: `linux-amd64`, `linux-arm64`, `osx-universal`.
Host options include `threads`, `hash_only`, `tiny_perf`, `enabled`, and an
optional `hold` command array. A hold receives start/end messages around the
timing window. The optional top-level `notify` command array receives hash
mismatch notifications. No command is evaluated through an extra shell.

Host snapshots include uname, CPU details, uptime/load and top before the run,
plus load afterward. MemoryMax is applied where a user systemd manager exists;
macOS uses the fixed small input set without a systemd scope. Disk caps are
checked before setup/run. The launcher never changes power profiles, stops
services, or assumes one CPU topology.
