# Canonical aggregate decode failures

Aggregate scans choose one decode failure independently of worker scheduling,
memory budgets and whole-row-group or sliced execution. A page is the failing
unit. Compare failures by:

1. Input order, then row-group order within that input.
2. The first row of the failing page, within its row group.
3. The input column's rank in the result projection.
4. The decoder's established check order within that page.

For flat columns, preceding data-page counts locate a page's first row.
Dictionary pages before data and failures before a page is installed belong
to row zero. Page ordinals include dictionary pages. A computed output's
underlying leaves keep schema order; columns used only by predicates follow
the projected columns. Repeated aggregate inputs remain unsupported.

After a decode failure, the failing column is checked with the ordinary
slice reader to recover its unit, including a resumed page's ordinal. Other
projected columns are checked through pages starting at or before that row.
Those pages are checked completely: a late invalid value in an earlier page
wins over a fault encountered sooner in a later page. Validation of levels
and value-decoder installation retain their existing order within each page.
A nullable column consumed only by COUNT keeps its level-only semantics.
Flat column/row-group count agreement is checked while opening metadata. Slice
readers enforce the chunk's remaining value budget before installing pages. A
REQUIRED page cannot escape validation by claiming more rows than the footer
while a consumer stops early.

These checks and projection ranking run only after a decode failure. They
retain one column reader, its page/dictionary buffers, and bounded row scratch;
they do not aggregate or retain whole decoded columns. A worker assigned only
some aggregate calls still checks the full query projection. Workers choose
among failures by input/row-group/unit order, and a failed ordered block leaves
earlier blocks eligible to decode. Parser/binder errors and resource refusals
are separate from payload decode failures.

`python tools/decode_error_check.py CANDIDATE` creates corrupt multi-column,
multi-page V1/V2 fixtures. It asserts known winners and byte-identical error
output for scalar and grouped queries, reversed projections, multiple inputs,
whole-row-group execution, tight memory, forced 64/256-row decode windows,
and every worker count from 1 through 16. `ZPQ_DECODE_BATCH_ROWS` is documented
with the other execution overrides in [hardware-execution.md](hardware-execution.md).
