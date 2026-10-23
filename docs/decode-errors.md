# Canonical aggregate decode failures

Aggregate scans choose one decode failure independently of worker scheduling,
memory budgets and whole-row-group or sliced execution. A page is the failing
unit. Resource-limited probes use the bounded fallback below. Compare confirmed
failures by:

1. Input order, then row-group order within that input.
2. The first row of the failing page, within its row group.
3. The input column's rank in the result projection.
4. The decoder's established check order within that page.

This order applies when execution observes a decode failure. An exact sketch/census/admit proof can omit
irrelevant data without observing corruption there. Such proofs use decoded, validated keys and predicates,
never the skipped aggregate payload; all gated inputs are excluded from these multipass plans.
The separate footer/page-statistics trust boundary still applies: corroboration cannot detect every
internally consistent false claim. See [validation and skipped data](output-format.md#validation-and-skipped-data).

For flat columns, preceding data-page counts locate a page's first row.
Dictionary pages before data and failures before a page is installed belong
to row zero. Page ordinals include dictionary pages. A computed output's
underlying leaves keep schema order; columns used only by predicates follow
the projected columns. Repeated aggregate inputs remain unsupported.

Every worker failure participates in selection. A restartable memory refusal
is located by its failing work item's input and row group, even before a
decoder installs a site; a previous row group's decoder marker cannot locate
that refusal. A refusal in an earlier or the same row group wins over a decode
failure and triggers replay. An unlocated refusal also wins conservatively.
A known later refusal cannot hide an earlier decode failure. Worker index and
completion order do not choose the result. The two-worker regression forces
both completion orders and both worker assignments, then verifies replay
reports the earlier row group's corruption.
An abandoned optional census discards its partial decode errors and retries
the exact scan from the start. A located failure or refusal in an ordered
block stops later blocks while allowing all earlier blocks to finish.

After a decode failure, the scan releases its worker arenas, scratch, group
tables and partitions before the coordinator replays the full query projection
in input/row-group order through the chosen row group. The replay ignores optional
count-pass projections and admit-pass item masks; only the query's ordinary,
option-dependent statistics pruning applies. Valid files pay no replay cost. A
local reader releases its input window and the probe reads its mapping. An
unrescanable stream retains its failed source unit until that worker unwinds
its arena and scratch, then takes a query-wide probe lock before returning the
source bytes. Thus probes never run concurrently or stack on failed decode
arenas. Probes use a bounded allocator charged through the query ledger.
Before stream workers or tables start drawing, the query prepays a probe quota
from the largest projected stream-column probe quote, capped by
`min(max_memory / 8, lean_decode / 2)` (at most 16 MiB). The quote uses chunk
uncompressed sizes, row counts, a 64-row window and reader/planning headroom;
byte windows allow repeated copies of a long dictionary entry. No projected
payload means no reservation. Size metadata is a cost hint, never a skip proof. The serialized
stream probe uses only that funded quota, so concurrent worker usage cannot
change what it can verify. Unused funding returns after the workers stop.
The funding is held on valid runs too; tight budgets can refuse an otherwise
valid query before decoding starts. The plan-derived quote avoids reserving
the maximum for small chunks. Resource-limited probes retain the fallback below.
Mapped inputs probe after scan teardown within `max_memory`, after local input
windows release their charges.

Columns are probed in fixed schema order. Only pages starting at or before the
best confirmed failure's first row need checking, and those pages are checked
completely: a late invalid value in an earlier page wins over a fault in a
later page. At every page boundary, page buffers and levels are released before
the next unit; the dictionary alone remains for the column. Columns release
all their storage before the next column. A nullable column consumed only by
COUNT keeps its level-only semantics. A worker assigned only some aggregate
calls still checks the full query projection. Optional filter-cost samples
decline corruption and leave failure selection to that full scan; they use
a reader's mapping rather than its not-yet-filled window. No lower-row unit
is skipped by an early-stop rule: a column stops only before pages starting
strictly after an already confirmed failure in the same row group. The prefix
replay stops after the first row group with confirmed evidence.

If a probe cannot allocate within its bound, that column is left unverified and
the other columns are still tried. Report the minimum canonical key among
confirmed failures found so far; if none can be reproduced, keep the original
decode diagnostic. If error-path planning itself cannot allocate, it also keeps
the original diagnostic. A resource refusal from probing never replaces
corruption.
An unreproduced execution marker has unknown row/projection ranks, sorts after
confirmed sites and never bounds their search. The bounded fallback is part of
the error contract; it can retain less evidence under a smaller memory option.

Validation of levels and value-decoder installation retain their existing
order within each page. Flat column/row-group count agreement is checked while
opening metadata. Slice readers enforce the chunk's remaining value budget
before installing pages, including REQUIRED pages. Parser/binder errors and
resource refusals outside probing remain separate from payload decode failures.
A memory-refused attempt never saves a decode diagnostic that could mask a
later exact retry's failure. These checks and projection ranking run only on
decode errors.

`python tools/decode_error_check.py CANDIDATE` creates corrupt multi-column,
multi-page V1/V2 fixtures. It asserts known winners and byte-identical error
output for scalar and grouped queries, reversed projections, multiple inputs,
whole-row-group execution, tight memory, forced 64/256-row decode windows,
and every worker count from 1 through 16. Tight 64K/128K/256K/1M cases also
compare mmap, pread and io_uring input paths. `ZPQ_DECODE_BATCH_ROWS` is documented
with the other execution overrides in [hardware-execution.md](hardware-execution.md).

Core regressions force a count pass to encounter a later corrupt key after
omitting an earlier corrupt argument, and replay through row groups absent
from the failed worker's work list. They also fill every unreserved ledger
byte before a stream probe, reverse worker assignments for residual errors,
and verify optional hint/refusal flags and memo allocation-failure cleanup.
