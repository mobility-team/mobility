# Simulation design review

One replication has one random seed and can contain several scenarios.
Those scenarios share compatible candidate generation, while each keeps its
own allocations, costs and modeled state. A simulation step advances those
states. Search batches within a step explore additional candidates against
the same starting state and costs.

The simulation runner handles a group of requested results together. It can
reduce metrics as steps finish and stores requested raw tables in temporary
files owned by the returned result. Checkpoints are optional saved restart
points, not the default storage for every intermediate table.

## Review order

1. Execution, input identities, saved state and checkpoint restart.
2. Shared survey schedules, candidate generation and stable plan identities.
3. Scenario coordination, adaptive search, congestion and resource lifetime.
4. Modeled timing, factored plan representation and shared section scoring.
5. Result requests, table lifetime, diagnostics and user-facing parameters.

Review model changes separately from computation reuse: schedule sampling,
search effort and pruning can change model results. Check that section scoring
matches expanded scoring, and that an interrupted run matches a continuous run.
The iteration-to-step parameter rename is a breaking change; old parameter
names produce an error that identifies their replacement.

## Checkpoint scale check

A cache-only Grand Genève probe used 43,054 demand units, two scenarios,
two steps and two search batches, with two draws each for activities,
destinations and modes. It excluded public transport and road congestion.
The initial run took 92.1 s and reached a sampled process-tree peak of
5.04 GiB. A fresh process resumed the final step in 28.8 s with a sampled
peak of 5.40 GiB, including preparation of initial costs. Saved allocations,
utilities and random states matched the original in both steps; final trip
counts matched within floating-point tolerance. This is one measurement,
not a full Grand Genève validation or an isolated checkpoint timing.

After sharing the candidate storage code, the same two-step probe produced
exactly the same candidate IDs, allocations, utilities and random states.
It took 102.4 s and sampled 5.44 GiB; unit tests ran concurrently, so this
single comparison cannot establish whether the refactor affected performance.

After replacing output comparisons with input-based reuse, the same probe
again matched candidate IDs, allocations, utilities and random states at both
steps. It took 78.1 s and sampled 5.13 GiB. These individual runs used the
same model inputs but different background workloads, so they do not establish
a precise performance improvement.

After the restart and coordinator changes, the same probe again matched
candidate IDs, allocations, utilities and random states exactly at both steps.
The fresh run took 81.9 s and sampled 5.22 GiB. A fresh process resumed at
step 2, loading one state per scenario, in 29.0 s with a 5.31 GiB sampled
peak; final trip counts matched within floating-point tolerance. The largest
sampled process RSS during a scoring join was 3.12 GiB. Public transport and
road congestion were excluded, and these single measurements are not a
full-scale performance comparison.
