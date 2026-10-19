# Admitted private runs for long composite keys

The cost-selected partitioned hash executor normally folds groups into 32 shared
partition tables. A count over multiple keys, including strings, with a combined
estimated width of at least 32 bytes, can instead use private runs when there
are at least 65,536 input rows and several workers. Float reductions and
DISTINCT retain their existing executors. Complete normalized keys still decide
equality.

The first nonempty local flush observes the ratio of groups to selected rows.
Only a window at least as large as the local probe, with at least 90% groups,
can admit private runs. A reducing first window retains the original shared
partition array and returns the prepaid reservation. All workers pass the one
admission decision under a lock before accessing any partition. An admitted
choice replaces only empty partitions. Workers with useful local reduction can
still flush into shared tables while others append private runs for those same
partitions.

Before workers start, the candidate prepays a run ceiling from the query ledger:
twice the estimated key, count, position and offset bytes over the input rows,
plus run/partition metadata. The initial reservation is at most half the query
budget and must be granted beside existing query holders. Width is a cost estimate, never a
proof about subsequent keys. Dictionary strings use the dictionary payload per
entry, including length and presence framing, rather than encoded column bytes
per input row. This admission estimate leaves the other planner cost rules alone.
Other key components retain their original estimate to preserve decode headroom.
Run capacity growth transfers prepaid credit to that run's charge; temporary
old/new buffer overlap borrows credit too. Any shortfall first draws the exact
missing bytes from the shared ledger; exhausting the estimate alone never forces
a rescan. A concurrent credit change relinquishes that draw before retrying. Unused
credit returns before the partition owners fold the runs. A query with no
surviving rows returns its whole reservation without ever choosing private runs.
Tables and decode continue to draw their actual allocations from the same
ledger.

Selection, routing, expression and group-id scratch is reused at each grouping
window. Decoded and materialized column values retain row-group lifetime; group
tables and runs own every key and aggregate state that escapes a window. Keeping
window scratch until row-group teardown needlessly competes with prepaid credit
and can make parallel worker progress decide whether the scan must restart.

Before reserving runs, the plan also funds decoder floors from at most a quarter
of the query ledger (and no more than its decode limit), split evenly between
workers. Whole row groups may borrow additional room from the same query ledger;
the parent holds the larger of each floor and its live allocations. A refused
whole-row-group admission selects sliced decode, whose buffers use protected
room, so one worker cannot consume another decoder's floor. Local group tables
and run growth continue to share the query ledger. A reducing sample returns
unused floors immediately; live allocations retain their charge and subsequent
decode uses the parent's available room. If both initial reservations cannot be
funded, the plan keeps shared partitions before consuming any rows. Regions and
side allocations are freed before remaining funding returns, before the run fold.

Local sample tables also receive bounded floors, transferred from the prepaid
run credit rather than charged again. Otherwise whole decoders can consume the
ledger before any table reaches the first flush, while the entire run ceiling
is still unused. Each table returns its floor when it is abandoned or finally
flushed; a cancelled sample returns all unused floors while keeping live bytes
charged. If the run ceiling cannot fund those samples, private runs decline
before consuming rows. Allocation-failure checks exercise sample tables with a
full parent ledger and verify that transferring credit adds no charge.

A refused admission leaves every local accumulator available to the shared path.
If later growth or folding refuses the budget, the whole attempt is freed and
rescanned through shared partitions with the other cost rules preserved, before
the existing narrower retries. It never continues a fallback from partially moved accumulators. The bounded retry
chain has at most seven scans. Private runs are only offered for rescannable
inputs. Allocation and spawn failures preserve cleanup and exact answers.

Every partition combines complete counts and the minimum first input position.
Thus final ranking, ties and first-appearance order match the shared executor;
private-run order and worker scheduling are invisible in the answer.
