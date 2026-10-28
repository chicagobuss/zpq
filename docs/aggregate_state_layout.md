# Integer group-state records

A `GroupTable` keeps normalized key bytes and the minimum input position for each group. Its aggregate payload is
one of two representations: the scalar `Accumulator` array, or a record of integer/count fields described by
`expr/agg_state.zig`. Private partition runs keep the same record layout as their local and destination tables.

The scalar union remains the interchange format for merging and finalization. Records own no pointers. Decimal,
float, DISTINCT and string extrema retain the union representation, as do tuples beyond the record descriptor's
call limit. A lone SUM currently retains the union: its ranking merge's unpacking cost outweighed the scan savings.
The planner's strategy and the canonical floating reduction order are unchanged.

| Payload | Record bytes per group |
|---|---:|
| COUNT | 8 |
| COUNT + integer SUM | 32 |
| COUNT + integer SUM/AVG/MIN/MAX of the same argument | 56 |

Integer sums retain all 128 bits. Extrema store the argument's 64 raw bits, interpreted using its signedness when
read. A presence mask distinguishes an empty reduction from zero. AVG retains an exact integer sum and a separate
non-null count. SUM can borrow that pair only for the same bound argument and filter, with no rewritten offset.
The AVG must precede SUM or immediately follow it, so sharing cannot delay the first argument evaluation past another
call's error. The SUM's presence then comes from AVG's non-null count rather than an independent mask bit.

Record buffers participate in the existing table admission and private-run `held`/`settle` contract. Capacity is
reserved before publishing a group; a failed allocation leaves its key unpublished and its owned buffers available
for cleanup. First-position minima, full key equality, null selection and checked output narrowing retain their
existing behavior. Unsupported layouts use the established representation rather than approximating state.

Rows read compact fields when materializing an aggregate output. They do not first expand every field into a
stack tuple, which would add unpacking and spill traffic to full grouped answers.
