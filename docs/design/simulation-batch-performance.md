# Simulation batch performance experiments

These experiments used a cached Grand Genève case with 43,054 demand units,
two scenarios, two outer steps, two search batches and four Polars threads.
Public transport and road congestion were excluded. Times are individual
measurements, not controlled comparisons; nested operation times overlap.

## Retained: reachable plan IDs

History compaction reuses eligible target IDs from allocation when origins and
IDs match, and otherwise finds eligible IDs without calculating probabilities.
Per-unit pruning uses a different ID space and falls back to the eligibility
calculation. A two-scenario control matched candidate IDs, allocations,
utilities and random states exactly at both steps. Tests cover the behavior
scopes, infeasible origins, stay-home and revision probabilities zero and one.

The first measurements removed three probability calculations (14 to 11), but
whole-run times varied: an initial comparison took 173.6 s before and 158.9 s
after, while a later warm comparison took 77.0 s before and 87.1 s after.
Neither comparison establishes a speedup.

## Deferred: sorted allocation and candidate slots

A sorted-target allocation prototype was slower on a captured batch: 0.162 s
versus 0.091 s for the existing pair calculation in a warm replay. The capture
had only four allowed targets per occupied origin on average. It also produced
small floating-point differences and needed further work for extreme utilities
and per-unit floors. The result does not rule out gains for denser contexts.

A candidate-slot replay reconstructed two completed pool snapshots, but did
not test producer-side changes or avoid timing and scoring work. In the same
case, new memberships reached 9,894 of 9,897 contexts. After eligibility
filtering, roughly 47–48% of batch-2 transition pairs belonged to contexts with
the same captured pair inputs. Allocation took 1.45 s across both scenarios in
an instrumented batch, including capture work. These observations do not justify
adding either a slot structure or an allocation cache for this workload.

## Deferred: completed plan scores

In consecutive batches, 78.2% of distinct timed-plan/profile combinations in
batch 2 had appeared in batch 1. A completed-score cache replay matched
context-level outputs exactly. Two-call comparisons took 3.93 s without the
cache versus 4.00 s with it, and 4.48 s versus 3.70 s in reverse order; peak
memory was higher with the cache. An earlier larger-run experiment also failed
to establish an end-to-end improvement. The existing section-score cache remains
in use; the completed-plan cache was not retained.

## Deferred: radiation probability experiments

Both destination samplers remain supported. In the measured step, the existing
per-profile probability cache missed because consecutive batches requested
different utility profiles. Their shared OD/mode rows had different costs, so
sharing probabilities across them would change the model.

Materializing common radiation inputs took 1.70 s and peaked at 4.37 GiB,
versus 1.36 s and 1.94 GiB for the existing query. Joining opportunities
before expanding uncertainty offsets took 1.37 s. Both returned the same
300,610 destination keys, but probabilities differed at floating-point
precision; even repeated baseline runs had small differences. Neither
experiment demonstrated a gain or established equivalent subsequent draws.
