# Structured aggregate plan v1

The coordinator and native worker negotiate before sending query inputs. A request
`{"operation":"capabilities"}` has no inputs. The worker acknowledges
`structured_plan_versions: [1]` and `prepare_aggregate_version: 1`. A missing or
incompatible acknowledgement refuses dispatch. An older worker's successful
unrelated response is insufficient.

Preparation uses `operation: "prepare_aggregate"`, `plan_version: 1`, inputs and
the caller's original aggregate, optional group_by and filter fields. The native
fragment grammar parses these fields. Footer-only opening and the execution
planner validate the query without running a scan. The response contains:

- `inputs` and `row_group_counts`, in input order;
- `plan_version: 1` and `plan`, the schema-independent tagged syntax;
- `aggregates`, with each merge function and original output name;
- `groups`, the original group output names in key order.

COUNT, SUM, MIN, MAX and AVG merge. DISTINCT and other holistic aggregates refuse
preparation. Worker aliases `_aN`/`_gN` and AVG partial-state mode are separate
fields; the coordinator constructs no aggregate or GROUP BY SQL.

A worker query carries inputs, row_groups, plan_version and plan. The worker binds
the syntax against its own footer through the common native semantic builders.
Column references retain quoted path semantics. Numeric and predicate literal
tokens stay strings in JSON, preserving precision through coordinators. No bound
column indices, physical-type proofs, regex handles or membership tables cross
the wire. Expression and predicate depths are bounded during binding; JSON
nesting is checked before recursive typed decoding. Unsupported versions and
mixed plan/text fields refuse instead of silently falling back.

Legacy text requests remain supported. The coordinator caches an acknowledgement
for the life of its process only when the worker is a published (immutable) version,
`name:7` or a version ARN, keyed by account, region, name and version, and sends
preparation and every partial to that same version. An alias, `$LATEST` or an
unqualified name is negotiated on every query, before any input is sent. With the
optional coordinator `counts` (and no `*` pattern among the inputs) the plan comes
from preparing the first input alone; the full preparation of every input, with its
footer and schema checks, runs concurrently with the scatter, and the query answers
only if both succeed. A refusal from the full preparation refuses the query and its
partial answers are discarded. Counts remain trusted for scheduling; a range past a
footer still fails that worker.
Query output ordering and LIMIT apply after merging.

## Partial aggregate (0.5.2)

For inputs that are not `*` patterns the coordinator makes no call before the scatter. Each worker receives
`operation: "partial_aggregate"`, `plan_version: 1`, and a `partial` object holding that worker's inputs, fractional
`row_groups`, and the caller's aggregate, group_by and filter. The worker builds the same mergeable plan
`prepare_aggregate` builds, reads its own footers, and answers with `partial_version: 1`, `schema_fingerprint`,
`plan_digest`, `aggregates` and `groups` beside its partial `agg`. The coordinator merges only when every answer
carries `partial_version: 1` and the same fingerprint, digest, aggregates and groups, and otherwise refuses naming
the inputs.

A worker that predates `partial_aggregate` receives the envelope but refuses it before it opens an input: 0.5.1
rejects the unknown `operation` (`stringToEnum` fails) and 0.5.0 finds no top-level `inputs`, `s3_url` or `sql`
(`MissingField`). The inputs ride only inside `partial`, which those parsers ignore. `*` patterns keep the v1 path
above, because expanding a pattern still has to list the inputs before they can be split. Supplied row-group counts
only balance the fractions; a count of 0 is still one whole input, so every footer is checked. An exact range past
a footer still fails that worker.
