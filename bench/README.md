# bench

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
