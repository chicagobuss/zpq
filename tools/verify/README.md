# verify

Install the pinned gate dependencies, then run from the repository root:

```sh
uv pip install --python .venv/bin/python -r tools/verify/requirements.txt
. .venv/bin/activate
python tools/verify/verify.py --help
python tools/verify/verify.py quick
python tools/verify/verify.py commit
python tools/verify/verify.py full
python tools/verify/verify.py stage differential
python tools/verify/verify.py soak --hours 10
python tools/verify/verify.py perf --help
```

No arguments and `--help` show every stage, tier, requirement and exit code.
`quick` runs every core test in ReleaseSafe, with safety checks and unchanged
test scales. `commit` uses the same core tests, builds the CLI, runs the CLI
contracts and independent reader checks, and runs zpq-check.
`full` adds integration, ABI, robustness, ClickBench and release checks.
Release checks block on non-native targets unless `--arch-summary PATH` supplies
matching-SHA native evidence (repeat for each target). The Intel macOS runtime
is explicitly deferred and accepted by name in the stage command.
`full`'s unit tests stay in Debug, including the environment-clearing and SIGPIPE
handler identity ABI tests. No test or exhaustive sweep is moved or omitted
from quick/commit; the core optimization mode is their only coverage change.
Each unit stage records `optimize` beside its exact command in summary.json.
For separate compile/run measurements, `zig build test-core-bin -Doptimize=MODE`
installs `zig-out/bin/zpq-core-test`; run it from the repository root.
Versioned Parquet corpora must match the existing `just fetch-corpus` pin;
a mismatch stops before gates and its revision is recorded in provenance.
Native builds default to
`--cpu native`, using the host CPU. `--cpu` remains an explicit override.
Provenance records both the requested setting and the resolved Zig CPU model.

Each run writes `zig-out/verify/<run-id>/summary.json` (override with `--out`).
Stages retain status, duration, command, counts, logs and binary hashes.
The record includes the engine source tree, working-tree state, host and exact
Python dependency versions. A dependency mismatch exits 2 before any build or
host probe; align the environment with `tools/verify/requirements.txt` first.
Run directories are immutable. Reruns use a new directory and `--rerun-of ID`.
Exit 0 means required checks passed or matched their declared refusals;
1 means failure; 2 means a missing prerequisite or incomplete run.
Build/test stages use a 16 GiB systemd scope, low priority and at most `-j8`.

Answers are checked by `zpq-check` (`tools/check/README.md`): the engine in-process, built
with `-Doptimize=ReleaseSafe`, against DuckDB 2.0. `commit` uses the oracle cache and
`full` asks DuckDB afresh; both run `--self-test` first, which proves the runner rejects
wrong values, undeclared or misnamed refusals, diverging axis points and empty runs.

The CLI's answers, text and written files are zpq-check cases (`tools/check/cases/cli_*.json`, goldens under
`tools/check/goldens`); what needs the operating system staged around the process is `tools/check/contracts.py`, the
full tier's `contracts` stage. Its filesystem-fault contracts mount loop devices and device-mapper targets, so no
tier runs them: they skip by name unless `--privileged` is given, on a dedicated host with passwordless sudo.
`tests/libzpq/run.sh` retains CLI/libzpq/Lambda byte parity, C ABI, ASan, Python/service, SQL-session and RSS
checks.
`tests/handoff/run.py` also remains: zpq-check does not replace its Python
handoff coverage.
The `repro-skspill` stage expects exit 1 and the declared memory-budget
message. Missing fixtures, runtimes and oracles are never successful checks.


`perf` calls the clean scorecard worktree pinned in `tools/verify/HARNESS`;
it preserves plan/run/summary semantics. `--suites` and `--cases` declare the
filter; missing suites make the result blocked. Cold scoring requires a
majority of wins against each rival, at most 20% behind the best rival, and
no cell slower than 1.5x. The same-host control is reported separately.
The in-process profile is a separate metric. `--plan-only` only plans;
`--resume RUN_DIR` continues the prior sealed run in a new labelled record.
Timing holds the benchmark lock before changing the host power profile and
restores that profile before releasing the lock.

Generate fixtures with `.venv/bin/python tools/zpq-gen SCENARIO --out DIR`.
`--help` lists the recipes and the few scenario-specific options. The recipes
retain the former scripts' values, seeds, page layouts and footer edits.
`packed_count` regenerates the indexed nullable composite-COUNT fixture;
its bytes match the original packed-count generator.
`--rows` is available for descending Top-N and sparse files; `--target-size`
sets the sparse logical size. Uploading a nested fixture requires an explicit
`--upload-bucket`. Existing binary fixtures and profiling tools stay.

Test stages require a nonempty parsed result with passed + skipped = total.
`skip_policy.json` declares each tier's budget through named, fixture-dependent
allowances (including explicitly missing CPU features). `-Dobserve-tests=true` forwards the stock Zig protocol and records
every skipped test. An unnamed or undeclared skip blocks the stage, even when
the process exits 0. Summary counts retain each test and its reason, the active
budget, and a top-level `skipped` map. Installing a missing fixture or supplying
the required CPU feature removes its allowance. Unknown CPU capabilities do not
activate a waiver. Soak unit rounds use the same policy.
