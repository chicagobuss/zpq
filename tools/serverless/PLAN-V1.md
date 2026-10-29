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

Legacy text requests remain supported. The optional coordinator `counts` argument
can override scheduling counts for diagnostics, but native preparation still
validates the footers. Query output ordering and LIMIT apply after merging.
