# verify

Run from the repository root with a Python that has PyArrow and DuckDB:

```sh
python tools/verify/verify.py --help
python tools/verify/verify.py quick
python tools/verify/verify.py commit
python tools/verify/verify.py full --base /path/to/previous-zpq
python tools/verify/verify.py stage differential
python tools/verify/verify.py soak --hours 10
python tools/verify/verify.py perf --help
```

No arguments and `--help` show every stage, tier, requirement and exit code.
`quick` runs core tests. `commit` builds the CLI, checks its argument parsing
and JSON/JSONL/Parquet formatting, and runs the full differential referee.
`full` adds integration, ABI, robustness, ClickBench and release checks.
`--base` is the previously integrated comparison binary; it is required for
`full` and is recorded without a default. Native builds default to
`--cpu raptorlake`; select the appropriate CPU explicitly on other hosts.

Each run writes `zig-out/verify/<run-id>/summary.json` (override with `--out`).
Stages retain status, duration, command, counts, logs and binary hashes.
The record includes the engine source tree, working-tree state and host.
Run directories are immutable. Reruns use a new directory and `--rerun-of ID`.
Exit 0 means required checks passed or matched their declared refusals;
1 means failure; 2 means a missing prerequisite or incomplete run.
Build/test stages use a 20 GiB systemd scope, low priority and at most `-j8`.

The referee runs the engine in-process, built with `-Doptimize=ReleaseSafe`.
Its JSON cases live in `tools/runner/cases/`; `oracle_compare.py` owns numeric
comparison policy. Decimal digits remain exact through JSON, canonicalization
and decimal means. Written Parquet is checked by strict PyArrow readers;
wire and value checks also use DuckDB. SQL refusals declare exact query and
error kind in `tests/known_divergences.json`; other refusal cases declare
`expected_error` beside the expected diagnostic. Registry metadata cannot
waive a failed value comparison. Refusal counts survive into the summary.
Dense cases marked `skip_duck` explain their narrower byte-parity contract;
other dense cases check independent values. Empty or limited runs cannot pass.

The real CLI tier and `tests/libzpq/run.sh` retain CLI/libzpq/Lambda byte
parity, C ABI, ASan, Python/service, SQL-session, RSS and write-sync checks.
`tools/triangulate.py` and `tests/handoff/run.py` also remain: the referee
does not replace their Hardwood and Python handoff coverage.
The `repro-skspill` stage expects exit 1 and the declared memory-budget
message. Missing fixtures, runtimes and oracles are never successful checks.

For harness regression checks, build the referee, then run
`tools/runner/test_harness.py` with `DUCKDB` naming the DuckDB CLI.
`tools/runner/coverage.py LEGACY_CHECKOUT OUTPUT.json` instruments every
counted check in the retired a01ec00 differential and compares its inventory
with the JSON cases. The legacy checkout needs its matching binary/fixtures.

`perf` calls the clean scorecard worktree pinned in `tools/verify/HARNESS`;
it preserves plan/run/summary semantics. `--suites` and `--cases` declare the
filter; missing suites make the result blocked. Cold scoring requires a
majority of wins against each rival, at most 20% behind the best rival, and
no cell slower than 1.5x. The same-host control is reported separately.
The in-process profile is a separate metric. `--plan-only` only plans;
`--resume RUN_DIR` continues the prior sealed run in a new labelled record.
Timing holds the benchmark lock before changing the host power profile and
restores that profile before releasing the lock.

The fixture-generator rewrite, generator deletion and broad directory moves
remain separate work. Existing fixture generators and profiling tools stay.
