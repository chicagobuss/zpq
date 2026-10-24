# verify

One entry point for the 0.5.0 gate set, the seeded soak, and the scorecard ship bar.

```sh
python tools/verify/verify.py
python tools/verify/verify.py quick
python tools/verify/verify.py commit -- --only sql_pred
python tools/verify/verify.py full --base /path/to/previous-zpq
python tools/verify/verify.py soak --hours 10
python tools/verify/verify.py perf
```

No arguments prints the tiers, every stage, and the exit codes. A run writes
`summary.json` under `~/code/zpq-wt/verify-v2-out/<run-id>/`. The writer refuses
to open a directory that already has a summary. A rerun is a new directory;
`--rerun-of` records the earlier id and leaves that file alone.

`repro-skspill` is a declared refusal: exit 1 and the message
`the query needs more memory than --max-memory allows` (`-j 8 --max-memory 500MB`)
is status `expected`, not a failure.

Native ReleaseFast builds pass `-Dcpu=raptorlake`. On the seriousbusiness
i7-13700HX, Zig's `raptorlake` model enables AVX2 and AVX-VNNI and leaves
AVX-512 off, which is what that chip can run. The flag is `host.zig_cpu` in
the summary. aarch64 and macOS cross builds do not take that model.

Compile and test stages run under `systemd-run --user --scope -p MemoryMax=20G`
and `nice -n 10`, with at most `-j12`. `tools/verify/zigw` is `ZIG` for scripts
that call `zig build` themselves, so they pick up the same cap.

`full` requires `--base`, the previously integrated binary the candidate is checked
against. For the e4c6878 equivalence run that is `~/bench/zpq-fair/zpq-588a80c`.
In normal use it is the integ tip before the change. The path is stored in
`summary.json` under `parameters.base`. It is not defaulted.

## Scorecard

`tools/verify/HARNESS` pins forgejo `bench/scorecard-inproc` at `2d8b109`.
That commit fast-forwards `bench/scorecard` at `c2aba06`. The harness stays a
separate clean worktree (`~/code/zpq-wt/scorecard-inproc`). `verify perf` runs,
with that worktree as cwd:

```sh
python bench/scorecard.py plan --config CONFIG --run-dir SEALED
python bench/scorecard.py run --run-dir SEALED
python bench/scorecard.py summary --run-dir SEALED
```

`plan` refuses a directory that already contains `manifest.jsonl`. `--resume`
calls `run` again on that directory. Ship-bar text (`shipbar.txt`) and host
provenance stay in the verify run directory. The cold ship bar is: majority
against DuckDB 2.0 and Polars 2.0, at most 20% of cells behind, no cell over
1.5x. `zpq_prev` (default `~/bench/zpq-fair/zpq-588a80c`) is reported as a
same-host control and is not part of that boolean. `--profile in-process` is a
different metric and is not folded into the cold geomean.

The harness keeps its own CPU placement (timing 0–7 and 16–23, housekeeping
8–15). This machine has those logical CPUs. perf does not add a 20G scope
around the harness, and it does not apply the granite Zen affinity string.

`--suites` and `--cases` are the declared filter. With no `--suites`, the
profile's full list is the filter (cold: clickbench, study, pdsh, reads-sf1,
writes, votes-local). Each suite is checked on its own. A suite whose inputs
are missing is recorded under `suites_skipped` with the reason, and it is not
planned. The suites that resolve are planned together, so one missing dataset
does not throw away the rest. A skipped suite makes the perf stage `blocked`.
The harness pins its own `.venv` as an engine dependency. That tree is not
created here. When the pinned interpreter is absent, the emitted config points
it at the python that is running verify. The harness worktree stays untouched.

On this host the proof is `verify perf --suites clickbench`. The other suites
live on granite and [redacted] and are not copied here. The same entry point runs
the full matrix there once those inputs exist.

## What this branch does not do

The outside review's `.ztest` language, `zpq-gen` rewrite, second benchmark
runner, and re-pinned uv environment are not in this branch. Existing check
scripts stay the stage bodies.

| Retired invocation | Reason |
| --- | --- |
| Separate `write_sync_check.py` process | `tests/libzpq/run.sh` already runs it on Linux when `strace` is present |
| sol1's extra `aarch64-linux-musl:lambda` skip | `release-smoke` follows lead-gates: skip the two macOS runs, run the Linux ones |
| Merging or copying `bench/` onto `integ/050-perf` | Histories diverged at `16b02572`. The pin is the join |
| `chain-e4c.sh` service stop/restore | This host is shared. perf does not stop other campaigns |
| Granite `0-7,16-23` affinity as a config override | That string is a Zen CCD layout. The harness default placement is unchanged |

`fd` is blocked until `--base` names a comparison binary. ClickBench smoke uses
`~/bench/zpq-fair/data/clickbench/sub{1,16}` and the SQL repo at
`~/bench/zpq-fair/repos/ClickBench` (shallow clone `b113475`). The scorecard's
own cold ClickBench case still wants `data/clickbench/hits.parquet`, which is
not on this host; `plan` fails closed when a suite's inputs are missing.
