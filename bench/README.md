# bench

## Quick integration checks

Use a same-host candidate/base mini-matrix for integration pushes, in idle gaps.
The default budget is 90 seconds; **both builds share a hard maximum of 120 seconds per host**.
A flagged cell gets a short targeted check. A full release matrix runs once, after content and
architecture checks are settled, with release-owner approval; obtain approval before starting
an estimate longer than an hour.

~~~sh
python -m bench quick --config /configured/fleet.json --host HOST \
  --candidate /configured/bin/candidate --base /configured/bin/base \
  --out /configured/results/new-run --budget-seconds 90
~~~

The command checks configured full source/binary hashes and matching target/CPU/optimization/
strip/toolchain pins; it does not build engines. For remote HOST, the CLI binary paths identify
local files to stage. Keep the inventory and host paths outside the repository:

~~~json
{
  "data": "/configured/mini-inputs",
  "manifest_sha256": "FULL_64_HEX_MANIFEST_DIGEST",
  "budget_seconds": 90,
  "reps": 3,
  "noise_pct": 0,
  "candidate": {
    "path": "/configured/bin/candidate",
    "sha": "FULL_40_HEX_SOURCE_SHA",
    "sha256": "FULL_64_HEX_BINARY_DIGEST",
    "target": "x86_64-linux-musl",
    "cpu": "baseline",
    "optimize": "ReleaseFast",
    "strip": true,
    "toolchain": "0.17.0"
  },
  "base": {
    "path": "/configured/bin/base",
    "sha": "FULL_40_HEX_BASE_SHA",
    "sha256": "FULL_64_HEX_BASE_BINARY_DIGEST",
    "target": "x86_64-linux-musl",
    "cpu": "baseline",
    "optimize": "ReleaseFast",
    "strip": true,
    "toolchain": "0.17.0"
  },
  "hosts": {
    "HOST": {
      "enabled": true,
      "root": "/configured/quick-cache",
      "lock": "/configured/shared-host-timing.lock",
      "memory_max": "8G",
      "nice": 10
    }
  }
}
~~~

Replace the digest placeholders. Per-host candidate/base objects override top-level builds for
other targets. Roots and the existing timing-lock parent must exist. Use the same lock as other
benchmarks on that host. Output directories must be fresh. Linux uses a user systemd scope;
optional cpus and threads apply to both engines. Linux idle admission defaults to at most 0.5
busy cores plus counter-skew allowance. Native foreign CPU is also checked. On other platforms,
configure idle_command as an argv array returning JSON with quiet: true. macOS keeps the fixed
small-input policy and does not claim a systemd memory cap. No services are stopped.

Prepare the existing 15-query, approximately 44 MiB catalog once:

~~~sh
bash bench/arch_prepare.sh --duckdb CLI --study STUDY --lineitem INPUT \
  --clickbench INPUT --out /configured/mini-inputs
~~~

This preserves the original query definitions and pins every file's size and SHA-256.
Small/prefix inputs have their own identities. The optional only list selects case IDs before
admission; selected and unrequested catalog cases remain in the request. A sealed request-specific
manifest can retain native zpq_args, environment, SQL, expected outcomes and source-input pins.

SSH/rsync setup and collection are recorded separately from host execution. Input objects are
keyed by full digest and hard-linked into manifest views; only absent objects transfer. Binaries
use content-addressed paths. Existing incorrect bytes fail verification. Transfers take the same
host lock so they cannot overlap another timing window. Remote evidence is retained for explicit
cleanup decisions.

The deadline starts before admission and covers input/binary verification, all answer checks,
warming, optional rivals and timed work. The outer watchdog terminates only its own child sessions
and retains an incomplete record. Teardown/collection cannot add query time. Hash agreement for
all first answers precedes performance sampling. Processes are fresh; each hot sample reads its
input first. The first pass has uncontrolled file cache; these quick checks do not claim verified
OS-cold or mincore-certified residency. Build order alternates by repetition. Sampling ends on the
fixed target or deadline, never on a favorable ratio. Partial pairs have no selected half or median.

Written Parquet outputs need complete DuckDB value/multiplicity comparison and PyArrow readback
inside the budget. Configure a duckdb object with path and sha256, and an existing pyarrow_python
executable, globally or per host. JSON-only A/B uses the standard library; measured runs install
no readers or packages. Existing validators can instead supply parquet_validators argv arrays
keyed by duckdb and pyarrow; {output}, {input} and {sql} substitute individual arguments without
a shell. Their exit status must represent full validation. Missing readers cannot yield green.

Raw byte identity stays literal. A Parquet format change requires an explicit per-case
parquet_byte_difference explanation and both value validators. Refusals use expected.all or
per-engine expected native_exit, stdout_sha256, stderr_sha256 and outcome: refusal. Unexpected
transitions fail. An engines: ["candidate"] case can compare retained outcomes without passing an
unsupported budget flag to the base. Refusal latency has no speed ratio; candidate-only outcome
coverage does not itself establish a performance merge gate.

The optional reference path names another host's hashes.json. It binds source revisions, input
manifest and query digests, then checks each build's exact output identity. Binary hashes remain
target-specific. A first host establishes a reference and leaves other architectures unmeasured.
No scheduler or fleet geometric mean is inferred.

report.json uses zpq-bench-result/v1 with result_kind: quick-ab. request.json, cells.jsonl,
immutable attempts.jsonl, final selection.json and hashes.json retain pins, exclusions and complete
denominators. Medians need all fixed reps. Positive median changes are flagged by default; any
noise_pct allowance is declared before execution and recorded, not inferred as significance.
Exit 0 means the requested host work completed within that allowance, 2 means deferred/incomplete,
and 3 means an answer failure or performance flag. Release disposition remains separate.

The command python -m bench arch aliases the same A/B backend. The portable standalone arch.zig
runner retains optional DuckDB 2.0.0 / Polars 2.0.0 checks and the corrected exact-number comparator.
Set rivals: true with already provisioned arch_runner and duckdb path/SHA-256 objects; optional
polars_python enables that retained adapter. This mode needs the full mini catalog and a baseline
CPU/ReleaseFast/stripped build. It uses only time left after A/B, with missing rival reps incomplete.

Named tooling checks:

~~~sh
python -m unittest bench.tests.test_quick bench.tests.test_export
zig test bench/arch.zig -lc -O ReleaseSafe
zig fmt --check bench/arch.zig
bash -n bench/arch_prepare.sh bench/arch_remote.sh
~~~

The standalone runner cross-builds with zig build-exe, -lc, -O ReleaseSafe, -mcpu baseline and
-target x86_64-linux-musl, aarch64-linux-musl or aarch64-macos. Configure build/test cache and
temporary directories explicitly.

## Full scorecard

The scorecard: zpq against DuckDB 2.0 and Polars 2.0 on ClickBench, the study matrix, PDS-H, read shapes, writes and
votes (`suites.py`, 464 cases), with the previous zpq build as a same-host control. Every answer is judged by
`zpq-check compare`, the comparer the correctness gates use, against DuckDB 2.0; only judged answers are timed.

```sh
zig build check-runner -Doptimize=ReleaseSafe          # zig-out/bin/zpq-check
python -m bench run --host HOST --lane cold --binary zig-out/bin/zpq --prev PATH/TO/PREVIOUS/zpq
python -m bench score ~/bench/runs/RUN...             # re-score; writes RUN/score.json
python -m bench replay OLD_RUN --out DIR              # an old scorecard-v1 run under these rules
```

One command per host and lane. On another host, `run` ships `bench/`, `zpq-check` and both zpq builds over ssh,
runs there and copies the run directory back. Missing inputs fail the plan; coverage never shrinks silently.

## Lanes

- **cold**: a fresh process per query, inputs evicted (`POSIX_FADV_DONTNEED`) and verified out of cache (`mincore`:
  at most 0.1% and 8 MiB resident) before every rep. Wall time from spawn through reap.
- **hot**: a fresh process per query on verified-resident inputs, after one warm-up rep.
- **engine**: engine time inside one session, a warm-up then N queries: one DuckDB CLI session (`.timer`, 1 ms
  resolution) and one Polars process (planning through `collect()` or the sink). zpq stays a fresh process: its
  start-up is a few milliseconds, against DuckDB's ~50 ms and Polars' ~140 ms, which the fresh-process lanes include.

Every engine gets the same thread count: the CPUs of cpu0's last-level cache domain, where engines run; the
controller and the oracle run on the other CPUs. Engines run inside one systemd user scope (`MemoryMax`, no swap)
through `spawn.py`, so the scope's pressure counters cover engine work only. Writes go to tmpfs.

## Stalls, re-runs, quarantine

Each rep records host and scope PSI (`io`, `memory`) deltas, bytes read, and the timing CPUs' foreign CPU.
`stalls.py` calls a rep stalled when it is more than max(1 s, 2x) slower than its cell's fastest rep *and* the scope
stalled for at least half the excess without reading more, or when host `io full` grew by more than 1 s. A busy host
before the rep, foreign CPU during it, or an unverified eviction also disturbs it. Reps run rep-major, so one episode
touches one rep of many cells. A disturbed case is re-run whole (every engine and rep) at the end of the lane, at
most twice; if still disturbed it is quarantined: no median, listed with its evidence.

## Score

`score.py`: a case's engine median needs every rep checked. Ship bar: zpq beats each rival on a majority of cases,
at most 20% behind the best rival, none over 1.5x. The denominator is every planned case; a zpq error, timeout or
incorrect answer is a failed cell (behind and over 1.5x); a rival's failure counts as zpq beating it; quarantined cases
are neither. The lane is INCOMPLETE when a cell has no result or more than 2% of cases are quarantined, UNCERTIFIED
without PSI (or for `--smoke`), else COMPLETE (exit 0, 2, 3). The control's geomean is reported, never scored.

## Records

`RUN/header.json` pins the SHA and whether `bench/` and `tools/check` are clean, the bench tree, host, kernel, CPU
placement, every engine's sha256 and version, zpq-check's sha256, the cases' hash and each input's size and mtime.
`RUN/journal.jsonl` has one record per rep: timings, counters, the answer's hash and verdict. Answers are not kept.
A rep whose answer hashes like a validated answer of the same case and engine takes its verdict; anything else is
judged again. `--resume RUN` continues a run whose pins are unchanged.

zpq has no views, so its ClickBench SQL is the query in DuckDB's fair form: create.sql's view and macro written in
place (`EventDate` as `make_date(EventDate)`, `toDateTime(t)` as `epoch_ms(t * 1000)`); DuckDB and the oracle read
through the view itself. Polars answers keep `EventTime` as epoch seconds only when it is still a timestamp
(ClickBench q18 groups on a derived minute of that name). Writes use each shape's codec; zstd at level 3 where the
engine takes a level.

`python -m unittest discover -s bench/tests` checks the rules (the `bench-self` stage of the commit tier).

## Requests and saved-run exports

Prepare an offline request packet, optionally with a metadata-only input dependency plan:

```sh
python -m bench request request.json --out packet.json
python -m bench request request.json --catalog catalog.json --inventory host.json --out packet.json
python -m bench export request.json --out export-directory
```

These commands do not run engines, judge answers, change source records or access the network. The exporter reads
native `header.json` + `planned.json` + `journal.jsonl`, or legacy `manifest.jsonl` + `results.jsonl`. Legacy manifests
must contain their locked configuration. An unsupported source schema refuses. The existing scorer determines native
quarantine and pending reruns; the exporter preserves every planned case, including failures and missing engines.

A request is ordinary JSON:

```json
{
  "schema": "zpq-bench-request/v1",
  "id": "candidate-comparison",
  "requester": "operator",
  "runner": null,
  "host": "benchmark-host",
  "purpose": "Compare the candidate and baseline on the selected cases",
  "candidate": {"sha": null, "binary_sha256": null},
  "baseline": {"sha": null, "binary_sha256": null},
  "targets": ["study/card10/q09_grp_cd"],
  "controls": [],
  "lanes": ["cold"],
  "expected_engines": ["zpq", "base"],
  "correctness_receipt": null,
  "corpus_manifest_sha256": null,
  "query_set_sha256": null,
  "runs": [{"id": "main", "path": "/path/to/saved-run", "lane": "cold"}]
}
```

Supply full source SHA and binary SHA-256 values when available; null keeps missing evidence explicit. A correctness
receipt supplies `path`, `sha256`, `source_sha` and `tree_clean: true`. Packet completeness records supplied fields;
it does not validate the receipt's contents or approve scheduling. A requester and the execution runner are separate
identities. The source binary/source pins are checked against the packet wherever the source format records them.

When several saved runs plan the same case and lane, choose the complete case cohort explicitly:
`"selection": {"cold": {"study/card10/q09_grp_cd": "confirmation"}}`. The choice applies to every engine; the exporter
never selects the fastest attempts or mixes engines from different confirmation cohorts. Warmups, excluded attempts
and replaced cohorts remain in `attempts.jsonl`. Missing requested cases remain pending. Optional `stored_summaries`
lists legacy composite JSON files; their rival medians retain their source run, lane and metric field and are labeled
stored. They do not replace an existing measured outcome or certify a missing measurement.

The export directory contains `request.json`, `report.json`, complete `cells.jsonl`, audit `attempts.jsonl`,
`tracker.json` (a document bundle) and `export.json` (file digests). The bundle has one request and report document,
plus bounded per-lane JSONL documents. Publish it using the document store's existing client: compare existing content,
create missing documents, and update changed ones with its normal version/lease checks. Retrying an identical export
is a no-op; conflicting output refuses before any file is replaced. No export timestamp enters deterministic content.

`byte_identical` concerns candidate/base raw output bytes only when every selected output has a raw digest. Canonical
answer hashes and semantic comparer acceptance do not prove raw equality. `oracle_byte_identical`, method compliance
and release decisions stay unknown unless separate evidence establishes them. Historical source contracts are preserved
in the report; export does not recertify a run under a newer method.

The input planner accepts a `zpq-corpus-catalog/v1` with `objects` (`sha256`, `bytes`, `source_paths`) and
`case_dependencies` (`case`, `objects`), plus a host inventory with `host` and `inputs` entries containing `files`
(`path`, `bytes`). Shared objects are counted once. A matching local size means verification is required, not a trusted
copy. Missing/wrong-size bytes are reported separately. This command neither hashes large datasets nor fetches or
releases them; preparation and verification must finish before timing/cache conditioning.
