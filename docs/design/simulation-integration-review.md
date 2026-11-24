# Simulation integration review

The integration starts from the current simulation checkout at commit
`3ad2dca`, including its local code, tests and diagnostic scripts as captured
in commit `d72ed04`. The local GTFS input database stays outside Git. The
asset foundation from #427 and the latest main branch were merged afterward.

The separate `mobility-simulation-reorg` worktree also contains local changes.
Its routing handoff cleanup has been included. Its experiment that removes
shared-asset disk spilling is not included: it changes memory and disk use,
and needs a comparison at representative workloads. Its changes remain in
the separate local worktree.

## Review order

1. Execution, input identities, saved state and checkpoint restart.
2. Shared survey schedules, candidate generation and stable plan identities.
3. Scenario coordination, adaptive search, congestion and resource lifetime.
4. Modeled timing, factored plan representation and shared section scoring.
5. Result requests, table lifetime, diagnostics and user-facing parameters.

Review model changes separately from computation reuse: schedule sampling,
search effort and pruning can change model results. Check that section scoring
matches expanded scoring, and that an interrupted run matches a continuous run.
The iteration-to-step parameter rename also needs a compatibility decision.
