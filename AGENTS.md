# AGENTS.md — Working norms for this repo

Short, durable instructions for any agent (Claude / Codex / etc.)
working on ZPQ. Read first; the tier docs in `.agent/rules/` go
deeper on what the project is and where it's going.

## Ground every non-trivial decision in three things

Before writing more than ~50 LoC of new code or making a
load-bearing design choice, take five minutes for each of:

1. **Zig 0.16.0 stdlib & language features.** The release
   reorganised large parts of std (`std.posix`, `std.fs`,
   `std.Io`, `std.time`). Read the relevant module before
   re-implementing. Look for new primitives that match what
   you're about to build — `std.MultiArrayList`, `std.bit_set`,
   `std.Io.Threaded`, `std.Io.Group`, `std.Io.Queue`, etc. —
   and either use them or have a *specific* reason not to. Path
   to read the local install:
   `/home/joshua/.zvm/0.16.0/lib/std/`. Don't guess at APIs;
   grep the stdlib.

2. **Comparables in `references/`.** Hardwood (Java),
   DuckDB (C++), Polars (Rust), parx (Rust). They've all hit
   the problem before. For a tricky case (concurrency model,
   nested decoding, predicate pushdown, footer thrift quirks)
   it is *almost always* faster to read 200 lines of one of
   them than to figure it out from spec. The hardwood pattern
   for nullable decode (Page → IntPage with parallel
   `definitionLevels` + `values`) is a recent example —
   ported in a few hours instead of designed from scratch.

3. **Light web research for best practices.** Especially for
   wire-format edge cases (parquet thrift IDL types vs runtime
   types, RLE/bit-packed-hybrid framing, definition-level
   semantics for nested), and for AWS-side behaviour
   (Lambda seccomp, S3 retry classes, SigV4 path encoding).
   The format docs are usually authoritative; mailing-list
   threads and parquet-mr issue tracker conversations fill in
   the "why is it this way" part. Don't trust LLM training-data
   recall on these — they drift.

If a change is "just plumbing" the three-step grounding can be
collapsed; if it's a load-bearing design choice (new primitive,
new wire-format support, allocator restructure, parallelism
model) it's mandatory.

## Don't trust correctness without verifying it

Wire-format engines are easy to get *almost right* in a way that
passes lenient readers and fails strict ones. ZPQ shipped a
broken `IntType.bitWidth` encoding for months because Polars was
lenient about it; pyarrow and DuckDB both rejected the output.

So:

- **Validate every parquet output with at least pyarrow + duckdb
  before claiming success.** "bytes_out > 0" is not the same as
  "valid parquet."
- **Run `apache/parquet-testing` corpus regularly** (Phase A in
  `docs/roadmap.md`). It surfaces unknowns you wouldn't have
  thought to test.
- **Never end a session with passing wallclock numbers but
  unvalidated outputs.** That's the trap that costs us the most.

## Journaling discipline

- `docs/journal/<YYYY-MM>.md` is a running log. Append entries
  with HH:MM timestamps. Capture decisions, surprising bugs,
  benchmark results.
- `docs/roadmap.md` is the plan, edited only as we finish phases
  or genuinely re-prioritise. **Don't restate roadmap content in
  the journal**; link to it.
- Other docs (`docs/<topic>_design.md`,
  `docs/COMPARISON_TO_HARDWOOD.md`) are reference material edited
  when the underlying thing changes.

## Tone and scope reminders

- The Tier 1 "grumpy elitism" mindset is real: skepticism over
  optimism, honesty over salesmanship, narrow primitive over
  bloated framework. When critiquing our own work, go ruthless —
  it's how we find what's actually broken.
- ZPQ is a **sharp narrow primitive** ("taco bell programming").
  Filter + project + S3-to-S3 passthrough on parquet. We don't
  compete with DuckDB on joins/aggs; we don't compete with
  Polars on dataframe ergonomics. We compete on cold-start,
  warm-pool throughput, and binary size *for our specific
  workload*.

## Git / PR conventions

- Don't add `Co-Authored-By` trailers unless the user asks.
- Commit message body explains *why*, not *what* — the diff has
  the what.
- Don't push or open PRs without explicit user approval.
