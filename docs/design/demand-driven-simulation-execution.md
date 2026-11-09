# Demand-driven simulation execution

Status: proposed. This design replaces the current split between result planning
and whole-run coordination. It does not change the transport model or its effort
settings.

## Problem and goal

The result execution plan selects metrics, tables and steps, but obtaining a
step currently triggers `MultiScenarioRunAsset` to execute the complete run.
The coordinator retains references to historical state assets, including shared
candidate data, before the result plan can reduce the requested outputs. Some
diagnostic paths also retrieve the full history. Consequently, selecting only
the final result does not imply that earlier state can be released.

Make the requested outputs and the model's future state the roots of one
execution plan. Compute, retain and write only what those roots require. Release
each intermediate after its last consumer. Preserve the model's results,
including its scenario sharing and reproducibility. Prefer existing asset
resolution and Polars operations over building a general-purpose query engine.

## User contract

- Users define the experiment, its scenarios, time steps, effort or manual
  sampling settings, and replications as today. They request metrics and raw
  tables, with steps and filters where supported. A batch of requests is the
  unit of execution and sharing.
- An experiment's scenario and sensitivity cohort is a model input. Its members
  share candidate generation within a replication; replications are independent
  lanes. A filter on requested results or a cached metric must not silently
  change that cohort or its candidate pool.
- Requested metrics are reduced as their inputs become available. Requested raw
  tables include only the requested scope and columns. They may be large; the
  plan must not require keeping the complete simulation history in memory.
- Raw tables are transient by default. Do not automatically keep a persistent
  disk cache for every requested table: large outputs can fill the project disk
  quickly. A caller may explicitly save a table for reuse or export it. Metrics
  need not cause raw tables to be saved.
- A raw result may be streamed to temporary disk storage to avoid a large
  in-memory allocation. Its storage has an explicit owner and lifetime: the
  result remains readable while its owner is open and is removed when it closes.
  A bare lazy scan must not outlive its backing files; design the public result
  handle and its close/context-manager behaviour together rather than returning
  an unowned scan. A caller may collect a result in memory or explicitly save it
  for longer use. Validate the API and default storage choice with representative
  table sizes.
- A later request for something not retained is a new execution, unless a valid
  explicitly requested checkpoint or result cache can satisfy it. No hidden
  promise to retain every intermediate for future queries.
- `explain()` describes the cohort and replication lanes, requested outputs and
  steps, cache hits, required computations and planned storage. It does not run
  the simulation. Estimates, if available, are identified as estimates.

The exact public method names need not preserve the existing results API. Direct run
and results entry points must use the same execution engine and declare their
requested outputs; a request for a full final state is permitted, but must be
explicit about its larger retention needs. Avoid introducing a second way to
run the same model with different lifetime rules.

## One plan per execution batch

There is one execution owner for a batch, not a process-wide singleton and not
a planner nested inside another coordinator. Extend or replace the current
result execution plan so it owns scheduling, result consumers and the lifetime
of simulation intermediates. Refactor the existing coordinator into operations
used by that plan. Continue to use the asset graph and resolver for input
dependencies, cache identity and shared assets; there should be no competing
simulation DAG or duplicate cache system.

Build the plan backwards from:

1. Uncached requested outputs and their selected steps, columns and filters.
2. State required to advance each active cohort member to those steps, including
   costs, allocations, stable identifiers, random-generator state and shared
   candidate generation.
3. Inputs needed by requested reducers, reference-scenario comparisons and
   cross-replication aggregation, including their pairing requirements.
4. Explicitly requested checkpoints or diagnostics.

Operators declare the inputs and columns they need, and when their outputs are
consumed. Unknown dependencies remain conservative: keep them until they are
made explicit. Push projections, filters and aggregation down only where the
operator contract proves equivalence. For example, a request for car results
cannot remove other modes before mode choice. Removing candidates or cohort
members changes the model and is not a query optimization.

A cached output eliminates its own computation, not a scenario required by
another output. If every output is validly cached, return it without running
the simulation, after the usual input and cache validity checks. A selected
earlier step does not by itself prove that shortening the configured run is
equivalent: only stop early when the model contract and tests establish that
the configured horizon cannot change that result.

## Execution and lifetimes

For each replication lane, initialize the cohort and advance it step by step.
Each step generates the shared candidates and evaluates the cohort members
using their own scenario inputs. Preserve the fixed pre-step state across all
search batches within a step. Once all consumers for a step have run, retain
only the state needed for the next step, requested output fragments and any
explicit checkpoint. Release references to previous state, expanded candidate
tables and per-scenario evaluation intermediates at their actual last use.

Reducers consume each selected step while its data is live and retain only
their partial aggregates. Comparisons and replication aggregates retain the
minimum matching partials needed for their semantics. Raw table consumers
project and stream selected fragments to their result files. Compute final
post-update costs and final plan steps whenever a requested result or model
dependency needs them; omit them only when the dependency closure proves they
are unused. Diagnostics are consumers, not an unconditional reason to retain
all state.

Lanes can initially run sequentially to bound memory. Parallel scheduling is a
separate resource decision and must not alter seeds, sharing, ordering or model
semantics. Shared assets remain live until the last lane or consumer using them
finishes. Temporary spill stays in the existing project data folder. Temporary
intermediates are cleaned on success, failure or cancellation; files backing
returned results remain until their owner closes. Explicitly saved outputs are
published atomically. Never delete valid shared caches or files still backing
open results as part of execution cleanup. Track temporary disk use and enforce
a configurable limit for files Mobility manages; if it is reached, fail clearly
and clean up rather than silently filling the disk. Check whether Polars and
other libraries expose their spill usage and controls before claiming the limit
covers them. Report any untracked spill explicitly. Streaming a large result
does not make its disk footprint small.

## Identity and reproducibility

Cache identity must cover all inputs that affect the result: the complete
cohort, scenarios and sensitivity values, replication seeds, effort and manual
settings, model and input versions, relevant run horizon, and the output's
step, projection and filter. Use the existing asset identity and invalidation
mechanisms, including explicit versions where meanings change. Do not assume
that a path alone tracks changed file contents.

Preserve stable plan identities, ordering where it affects random draws or
results, seed-to-replication mapping and random-generator state. A checkpoint
contains all state needed for a faithful continuation, including shared cohort
state; reuse or extension is allowed only when its identity and horizon
semantics are verified. Existing legacy simulation state need not be migrated.
Changing retention must not change the model's stochastic path.

## Implementation and acceptance

1. Inventory result and model consumers, including direct run access,
   diagnostics, transitions, comparisons, final costs and checkpoints. Give
   each explicit dependencies and last-use ownership. Document any conservative
   fallback before removing historical state.
2. Introduce the unified execution owner and step-wise cohort operations. Move
   result reduction into the step loop and remove the historical-state lists
   and competing coordination path once all consumers are migrated. Keep
   changes incremental, but do not maintain a permanent legacy mapping layer.
3. Add safe raw-result streaming, storage ownership, cache identity, cleanup and
   `explain()`.
   Apply column and row pruning only to operators whose contracts establish it.
4. Compare against the current model on small deterministic fixtures: selected
   versus all steps, final costs, multiple scenarios and sensitivity cases,
   independent seeds, filters, raw tables, diagnostics, cached outputs and
   checkpoint continuation. Check candidate identities and RNG evolution as
   well as outputs; use exact equality where promised and justified numerical
   tolerances otherwise. Include failures, cancellation and lazy reads after
   execution. A simple release experiment previously changed aggregates, so
   memory reduction alone is not sufficient evidence of correctness.
5. Measure live state and intermediates, peak process memory, spill and wall
   time separately. Avoid summing shared buffers or nested timings as if they
   were independent. On the real Grand Genève model, repeat the two-scenario
   iteration comparison first, then compare scenario counts and seeds only if
   tractable. Report candidate-pool size and identify the limiting operation;
   stop and discuss before an impractical next run. Allocator-retained RSS is
   not by itself proof that live model data is still referenced.

The acceptance criterion is equivalent requested results with bounded
historical retention, not a promised speedup or a particular RAM number. The
live candidate pool and current cohort state may still grow as the model runs;
measure those separately before choosing further optimizations.

## Handoff and first milestone

Before changing code, read `AGENTS.md`, inspect the current Git state and keep
any existing uncommitted work intact. This document is a proposed direction,
not evidence that the implementation already behaves this way.

Start with a small end-to-end slice: one replication lane, its complete scenario
cohort and a final-step metric. Show that the metric is consumed inside the
step loop, that old state is no longer retained unnecessarily, and that the
model's candidate identities, RNG evolution and results remain equivalent.
Then extend the same path to other consumers and replications. Do not build a
generic optimizer or a new public result-handle abstraction before the slice
demonstrates what is needed. Keep intermediate changes reviewable and run the
relevant unit tests before broader integration tests.

For context, an earlier Grand Genève probe with two scenarios and one seed
measured approximately 243 seconds and 11.27 GiB peak process memory for five
steps, and 545 seconds and 22.27 GiB for ten. The candidate pool grew to about
52,000 and 99,000 respectively. These are observations of that configuration,
not performance targets; shared buffers, allocator retention and total project
disk usage must not be confused with uniquely attributable live memory or spill.
An experimental release of earlier state changed aggregate results, so do not
reuse it as a correctness-preserving fix without identifying the cause.
