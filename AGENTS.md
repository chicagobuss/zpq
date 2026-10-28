# AGENTS.md — Working norms for this repo

Short, durable instructions for AI coding agents working on ZPQ.

## Ground every non-trivial decision in three things

Before writing more than ~50 LoC of new code or making a load-bearing design choice, take five minutes for each of:

1. **Zig 0.17.0 stdlib & language features.** The release reorganised large parts of std (`std.posix`, `std.fs`,
   `std.Io`, `std.time`). Read the relevant module before re-implementing. Look for new primitives that match what
   you're about to build — `std.MultiArrayList`, `std.bit_set`, `std.Io.Threaded`, `std.Io.Group`, `std.Io.Queue`, etc.
   — and either use them or have a *specific* reason not to. Don't guess at APIs; inspect the pinned Zig stdlib from
   your local toolchain install.

2. **Comparables in `references/`.** Hardwood (Java), DuckDB (C++), Polars (Rust), parx (Rust). They've all hit the
   problem before. For a tricky case (concurrency model, nested decoding, predicate pushdown, footer thrift quirks) it
   is *almost always* faster to read 200 lines of one of them than to figure it out from spec. The hardwood pattern for
   nullable decode (Page → IntPage with parallel `definitionLevels` + `values`) is a recent example — ported in a few
   hours instead of designed from scratch.

3. **Light web research for best practices.** Especially for wire-format edge cases (parquet thrift IDL types vs runtime
   types, RLE/bit-packed-hybrid framing, definition-level semantics for nested), and for AWS-side behaviour (Lambda
   seccomp, S3 retry classes, SigV4 path encoding). The format docs are usually authoritative; mailing-list threads and
   parquet-mr issue tracker conversations fill in the "why is it this way" part. Don't trust LLM training-data recall on
   these — they drift.

If a change is "just plumbing" the three-step grounding can be collapsed; if it's a load-bearing design choice (new
primitive, new wire-format support, allocator restructure, parallelism model) it's mandatory.

## Don't trust correctness without verifying it

Wire-format engines are easy to get *almost right* in a way that passes lenient readers and fails strict ones. ZPQ
shipped a broken `IntType.bitWidth` encoding for months because Polars was lenient about it; pyarrow and DuckDB both
rejected the output.

So:

- **Validate every parquet output with at least pyarrow + duckdb before claiming success.** "bytes_out > 0" is not the
  same as "valid parquet."
- **Run `apache/parquet-testing` corpus regularly** (wired into CI conformance). It surfaces unknowns you wouldn't have
  thought to test.
- **Never end a session with passing wallclock numbers but unvalidated outputs.** That's the trap that costs us the
  most.

### Test tiers (fastest first)

The gates are build.zig steps (tools/check/README.md); each stage leaves a receipt, and a tier passes only on clean
receipts for HEAD with nothing failed or skipped. Don't skip the full tier before a release.

- **edit — `zig build check -Dtier=edit [-Dtest-filter=X]`** (`just test-core`): the core unit tests in ReleaseSafe.
  Run constantly as you work. Checks our logic against *hand-known outputs* — fast, but self-referential by nature.
- **commit — `zig build check -Dtier=commit`** (`just check`): before every commit. Fixtures, the core units, the
  runner's self-test, and every case against the cached DuckDB oracle, written files read back by PyArrow. Mirrors CI.
- **full — `zig build check -Dtier=full`** (`just full`, or `tools/check/fleet.sh` across the fleet): on a cadence
  and before releases. Debug units, integration, every case uncached, the pinned `apache/parquet-testing` corpus with
  a declared outcome per file, ClickBench, path parity, OS contracts, libzpq, and the architecture check. This is
  what catches the "almost right" wire-format bugs above — a self-round-trip will not.

## Documentation discipline

- Keep repository docs durable: design/reference notes, capability findings, measurements, and test truth.
- Avoid adding status snapshots, session journals, or forward-looking roadmap checklists to the public tree. If a note
  will go stale as soon as priorities change, keep it out of the repo.
- Other docs (`docs/<topic>_design.md`) are reference material edited when the underlying thing changes.

## Performance tracking

`python -m bench run --host H --lane cold|hot|engine --prev ZPQ_PREV` (bench/README.md) runs zpq against DuckDB 2.0
and Polars 2.0, with the previous build as a same-host control, and counts only answers `zpq-check compare` validates.
Re-run it **before AND after** a perf change on the same host and keep both records; don't change suite or case
filters between the two runs. A lane that is INCOMPLETE (a stalled case quarantined past 2%) or UNCERTIFIED (no PSI)
proves nothing: re-run it on a quiet host. AWS-side noise is real: if a single remote cell jumps and the others don't,
suspect the network before the diff.

## Profiling

Frame pointers are on in both binaries (`omit_frame_pointer = false` in `build.zig`); cost is in the noise. `just
flamegraph <zpq args>` records a perf trace with `--call-graph=fp -e cycles:u -F 4999`, runs it through Brendan Gregg's
stackcollapse + flamegraph (cloned in `references/FlameGraph/`), and writes `prof/flame-<ts>.svg`.

```bash
just flamegraph query data/benchmark_100mb.parquet \
  --aggregate 'sum(int64_sorted) AS s, count(*) AS n'
```

Sub-100 ms runs are too short to sample — wrap in a 50× shell loop or use a multi-file glob to get hundreds of samples.
Open the SVG in a browser; click frames to zoom. `just flamegraph-bpf <secs>` attaches via `profile-bpfcc` for
long-running processes (needs sudo).

When picking a perf target, the flamegraph is the verdict.

## Tone and scope reminders

- The Tier 1 "grumpy elitism" mindset is real: skepticism over optimism, honesty over salesmanship, narrow primitive
  over bloated framework. When critiquing our own work, go ruthless — it's how we find what's actually broken.
- ZPQ is a **sharp narrow primitive** ("taco bell programming"). Filter + project + S3-to-S3 passthrough on parquet. We
  don't compete with DuckDB on joins/aggs; we don't compete with Polars on dataframe ergonomics. We compete on
  cold-start, warm-pool throughput, and binary size *for our specific workload*.

## Code style

- Execution heuristics must not contain dataset-, benchmark-, or value-specific constants. Derive content
  properties from input and verify them before using them to skip semantic work.
- Size execution batches and partitions from runtime cache capacity where possible, with forced-path controls
  for coverage on every host. Hardware choices must preserve output bytes; floating-point reductions retain
  their canonical summation order. Document fixed-layout exceptions and the remaining adaptation work.

- Line width is **120 columns** for Zig, Python, and shell. Enforced by convention only — `zig fmt` does not wrap lines
  and will not reflow or complain, so it is on you and on review.
- The 120 limit governs *source*. Human-facing output — `--help` text, error and diagnostic messages — wraps at **80**
  so it renders in a default terminal. `src/cli/help.txt` (embedded as `usage_text`) is deliberately narrow for this
  reason; do not widen it to match the source limit.
- Machine-readable output is exempt from both limits. Never insert line breaks into the JSON status/summary format
  strings — a break there corrupts the output.
- Comments carry the *why*, not the *what*. If a comment restates what the next line plainly does, delete it. Keep the
  things a reader cannot recover from the code: thread-safety and lifetime invariants, why a value or approach was
  chosen over the obvious alternative, determinism guarantees, and one-line notes on bugs the code actively guards
  against.
- Prefer a dense two-line comment over a nine-line essay. Long blocks are justified when they encode several distinct
  invariants a reader could each break independently.

## Git / PR conventions

- Don't add `Co-Authored-By` trailers unless the user asks.
- Commit message body explains *why*, not *what* — the diff has the what.
- Don't push or open PRs without explicit user approval.
