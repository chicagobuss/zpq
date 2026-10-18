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
plus run/partition metadata. The ceiling is at most half the query budget and
must be granted beside existing query holders. Width is a cost estimate, never a
proof about subsequent keys. Run capacity growth transfers prepaid credit to
that run's charge; temporary old/new buffer overlap borrows credit too. Unused
credit returns before the partition owners fold the runs. A query with no
surviving rows returns its whole reservation without ever choosing private runs.
Tables and decode continue to draw their actual allocations from the same
ledger.

A refused admission leaves every local accumulator available to the shared path.
If later growth or folding refuses the budget, the whole attempt is freed and
rescanned through shared partitions before the existing narrower retries. It
never continues a fallback from partially moved accumulators. The bounded retry
chain has at most seven scans. Private runs are only offered for rescannable
inputs. Allocation and spawn failures preserve cleanup and exact answers.

Every partition combines complete counts and the minimum first input position.
Thus final ranking, ties and first-appearance order match the shared executor;
private-run order and worker scheduling are invisible in the answer.
