# Demand-driven simulation execution

Status: implementation in progress; representative correctness validation is
still outstanding. This design replaces the split between result planning and
whole-run coordination. It does not change the transport model or its effort
settings.

## Problem and goal

The current result runner selects metrics, tables and steps, but obtaining a
step currently triggers `MultiScenarioRunAsset` to execute the complete run.
The coordinator retains references to historical state assets, including shared
candidate data, before the result runner can reduce the requested outputs. Some
diagnostic paths also retrieve the full history. Consequently, selecting only
the final result does not imply that earlier state can be released.

Make the requested outputs and the model's future state guide one simulation
runner. Compute, retain and write only what those outputs and state require. Release
each intermediate after its last consumer. Preserve the model's results,
including its scenario sharing and reproducibility. Prefer existing asset
resolution and Polars operations over building a general-purpose query engine.

## User contract

- Users define the experiment, its scenarios, time steps, effort or manual
  sampling settings, and replications as today. They request metrics and raw
  tables, with steps and filters where supported. Requests made together are
  the unit of execution and sharing.
- An experiment's scenario and sensitivity cohort is a model input. Its members
  share candidate generation within a replication; replications are independent
  lanes. A filter on requested results or a cached metric must not silently
  change that cohort or its candidate pool.
- Requested metrics are reduced as their inputs become available. Requested raw
  tables include only the requested scope and columns. They may be large; the
  runner must not require keeping the complete simulation history in memory.
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

## One runner for requested results

One simulation runner owns each group of requests. It is not a process-wide
singleton or a runner nested inside another coordinator. Extend the current
runner so it owns the order of work, result consumers and the lifetime
of simulation intermediates. Refactor the existing coordinator into operations
used by the runner. Continue to use the asset graph and resolver for input
dependencies, cache identity and shared assets; there should be no competing
simulation DAG or duplicate cache system.

Determine the work needed by starting from:

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
2. Introduce the unified simulation runner and step-wise cohort operations. Move
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

## Current consumer inventory

This describes the existing implementation, not the proposed runner. The
resolver may retain or spill values independently of these Python references.

| Consumer | Inputs and current lifetime |
| --- | --- |
| Next outer step | Previous `RunState`, including current plans and steps, candidate history, saturation, stable indices and RNG state. Keep through the next step's candidate search and behavior update. Search batches use the same pre-step state. |
| Scenario cohort | The coordinator generates shared candidates and attaches scenario-specific inputs to iteration assets until their final use. |
| Checkpoints | Iteration state and RNG state, plus a matching cohort manifest. Recorded transition files are also fingerprinted when enabled. Keep until checkpoint writing finishes; continuation depends on the complete cohort and its identity. |
| Plan steps and costs | Scoped tables resolve run outputs at selected live steps. Final plan steps additionally use post-update costs. |
| Metrics and comparisons | Per-step metrics consume scoped tables and often demand groups; comparisons and replication aggregation need matching scopes. Fallback metrics resolve dependencies after the per-step loop. |
| Diagnostics | Iteration metrics consume plans, plan steps and saturation from each state. Adaptive-search rows accumulate during the run; MNL flows use the last inner estimate. |
| Transitions | Transition output can use persisted step events and may trigger the coordinated run. Preserve its persistence and reconstruction requirements. |
| Direct run access | `Run.get()` requests the full final state; `iteration_table()` and `Run.results()` request tables when an analysis needs them. Analyses needing several tables can request them together. |

The former coordinator cached `_cached_result` on the experiment after the
asset resolver closed. That cache has been removed: later requests do not reuse
handles whose temporary spills have disappeared. The coordinator no longer
retains a history list. Performance traces start when the runner actually
advances a cohort; reading a cached result does not create a run trace.
Directly constructed runs now create a single-member
cohort and use the same runner; direct reads no longer reconstruct a discarded
iteration independently. A later request for an unretained result should run
the complete cohort again, unless a valid checkpoint or
explicitly saved result satisfies it. Compare the new
runner with the current model while inputs are live, including candidate IDs,
RNG state and requested results. The car-only integration fixture compares
independent executions for two scenarios and two seeds. It also checks a
two-scenario checkpoint continuation and a four-member sensitivity cohort.
A separate small-fixture comparison with the previous implementation is
described below; a representative full-model comparison remains outstanding.
Run and checkpoint identities also fingerprint external timetable and road
geometry files selected for the scenario, its configured steps and final costs.
Shapefile sidecars are included. A different future input can therefore
conservatively prevent checkpoint reuse when a run is extended. Road and
public-transport graph identities also change when their selected external
geometry or timetable files change.
Requests retain the input identity declared when their experiment was built.
If one of those files changes afterward, execution and `explain()` ask for a
new experiment and request instead of returning a result under the old identity.

The independent-execution fixture exposed a difference in the project
scenario's step-2 utility despite matching candidate IDs and RNG states. Step-1
destination occupation was summed in Float32 and varied with row order; it is
now summed in a stable order using Float64. Run and checkpoint identities were
updated because this changes numerical results. The two-scenario fixture passed
twice after this change; broader reproducibility and checkpoint coverage remain
necessary when comparing the new runner with the existing model.

An isolated comparison with the pre-change Git implementation used two
scenarios, two steps and seed 108. Both implementations produced the same 35
and 70 candidate IDs at steps 1 and 2, identical random states, matching plan
keys, and exactly equal final costs. The largest step-2 plan-weight differences
were 0.000870 and 0.001926 represented people in the default and project
scenarios; each scenario's total remained 169,111. Applying only the stable
Float64 saturation sum above to the old implementation made all captured
per-step plan and plan-step tables match the new runner at relative tolerance
1e-9 and absolute tolerance 1e-8. This isolates the observed difference on this
fixture; it does not establish equivalence for every model configuration.

The runner owns the outer step loop and calls the coordinator's cohort operations.
It clears coordinated inputs after the current step's consumers and the next step
have finished using them. Unknown result dependencies still use a conservative
result-resolution path until they declare their needs. Supported requests in
the same batch continue to share their step-wise cohort execution; an unknown
dependency may still require separate work when it is resolved.
The inner-search allocations and detailed result steps are released as soon as
each scenario's flow totals are computed. Search convergence uses reachable
behavioral allocations; the optional fresh-MNL flow diagnostic is calculated
separately after the final search batch. Each scoring evaluation is released after its update.
When a batch may be the last, its candidate history is compacted before retaining
the evaluation while the other scenarios are scored.
The retained step table contains only columns needed downstream. If neither
fresh-MNL flows nor finite transition distances are requested, it contains
steps only for occupied or reachable plans.
During restricted behavior phases, intermediate batches that cannot yet end
the search score only reachable plans. Contexts with infeasible occupied plans
retain every candidate for forced reassignment, and the last batch scores the
complete pool so that candidate history and the optional MNL output remain complete.
When candidate steps are factored, the reachable filter runs on context-plan
memberships before physical steps are attached.
For shared transition probabilities, the outer update creates new demand-unit
plan IDs only for occupied origins and reachable targets. Existing IDs and the
full candidate history are retained for later steps. Transition distances are
calculated on context-level plans before target IDs are assigned.
The expanded evaluation is also released before the resulting candidate steps
are compacted for the next step.
When another search batch is selected, the preceding batch's evaluation and
allocation tables are released before the next pool is built; the candidate
catalogue, indices and allocation summaries needed for that batch remain live.
Active candidate history uses a streaming membership join before deduplication,
so multiple matching plan rows do not expand the history being collected.
Compact candidate history is reduced by plan variant before shared content is
rebuilt; occupied plans are filtered by demand unit before their steps are
expanded, without sorting them before an unordered deduplication. The full-table
fallback remains for history that cannot be compacted without loss.
When transition distances are disabled, possible plan steps are collected after
the transition calculation instead of being held during it. Distance-based
transitions still collect the steps before computing distances.

## Handoff and first milestone

The live consumer currently supports one or more uncached requests at selected
steps across sequential replication lanes. Supported metrics include standard trip metrics,
trip counts by demand group, immobility, opportunity occupation, activity
duration distributions and activity time series. Filters, normalization,
external survey references and comparisons with another scenario are supported
where the metric already provides them.
Plan steps, costs, demand groups, transitions, step metrics, adaptive-search
diagnostics and final MNL flows can be requested in the same execution. A
request needing only demand groups or opportunities, including a custom metric
whose simulation inputs are those initial-state tables, reads the initial state
without advancing the cohort.
When transition-event recording is enabled, the recorded step files are scanned
without collecting the complete event history in memory. Event files are
published atomically, and missing completed-step files raise an error instead
of silently returning a partial history. Raw table fragments are
streamed to temporary files owned by the returned result. A result and its
views keep those files available; `close()` removes them immediately and they
are otherwise removed when the last reference is released. Filtering, selecting,
sorting and grouped aggregation preserve that ownership. Saving a result
publishes a separate Parquet file atomically; failure or cancellation preserves
an existing export and removes the incomplete file. Metric cache files are also
replaced only after a complete Parquet file has been written. An optional project limit applies
while Mobility writes result tables, shared state, destination and mode-sequence
spills, mode-search files, road flow inputs and extracted GTFS tables. Sequence
spills publish their rows and indexes together only after every file is written;
initial and iteration state spills likewise use unique staging folders and
preserve an existing spill. Limit failures and cancellation remove incomplete
spills. Path-cost and shared congestion output
written by R is checked after the tool finishes; other external-tool scratch files
and Polars spill are not fully tracked. Temporary map-animation frames are
included in managed usage and checked after each render finishes.
If the Python mode-search subprocess is interrupted, the runner stops its worker
processes before removing their temporary files.
When a checkpoint is rejected and a step is recomputed, its saved state and
recorded events are rewritten before a new cohort manifest is published.
Checkpoint files are checked again at execution start, and checkpoint-save
flags are reset when the same experiment runs again.
An interrupted checkpoint rewrite removes its old completion marker, so a
partially written state cannot be selected for resume.
The default is unlimited until `MOBILITY_TEMPORARY_DISK_LIMIT_BYTES` is set.
The complete experiment cohort still runs. The runner reduces the requested
metrics during each selected step and releases preceding state after its
next-step consumer. Later requests run again rather than reuse discarded history.
When every declared request selects only earlier steps, the full configured
cohort still runs, but final post-update costs and final flow diagnostics are
omitted. Explicit step tables, step diagnostics and inner-flow diagnostics can
also use the last step without building post-update costs. Unknown consumers and
direct final-state requests remain conservative. Cached outputs do not cause
final costs or flow diagnostics to be built for another request.
Cached derived metrics return without rerunning their cached inputs; a transient
table requested alongside them still runs each required lane once.
The last inner-flow diagnostic is also computed only when requested or needed by
an unknown consumer.
The resolver preserves existing path-cost caches. Newly computed path costs use
execution-owned files that are removed after their final consumer or on cleanup.
Focused resolver checks cover shared consumers and failure before the last use.
A standalone `run.get()` requests the final state through the step-wise runner
and releases earlier states. Standalone `run.table()` and `run.iteration_table()`
also request their selected outputs through the runner. The legacy `run.results()`
helper defers table requests until an analysis uses them. Its per-person metric
requests plan steps, costs and demand groups together, including a reference
scenario in the same cohort, and the trip-count loss helper batches plan steps
with iteration diagnostics. Step-based emissions comparisons batch their
selected plan steps with the final costs they use, across each scenario cohort.
Other analyses can still
resolve their inputs separately. A nested direct read owns
its execution and does not release values held by the caller's resolver.
Direct-run step diagnostics request plan allocations and plan steps through the
same cohort runner instead of resolving historical state assets independently.
Their multi-step and comparison helpers batch the selected steps and related
scenario runs by cohort, then release their transient tables after the analysis.
The cohort checks cover paired replications and compare sensitivity-case
results with values from their live states.
Raw tables and the supported metrics can also process multiple replication lanes
in sequence, including mixed requests, missing groups, population-normalized
metrics, shares, paired scenario comparisons, opportunity occupation, activity
diagnostics, immobility and their available external references. Earlier-step
trip metrics also use the selected step for survey references. Other unsupported
requests still use the conservative existing path. A custom metric whose
declared dependencies are scoped tables from the same results object and
independent file-backed inputs can share the lane execution: those tables are
streamed to temporary files and released
after the metric is computed. If the same table was requested as an output,
the metric reads that result's files and the caller keeps ownership.
The declared tables also determine whether final costs or inner-flow
diagnostics are needed; inconsistent declarations remain conservative.
Metrics that only depend on independent file-backed inputs are resolved without
running the cohort. Declared metric-to-metric dependencies are ordered before
their consumers, so missing intermediate metrics can share the requested
cohort execution and cached chains can be returned without running it.
Their declared dependencies also determine whether final costs or inner-flow
diagnostics are needed; cached dependencies do not force those outputs for
another request. A derived metric can combine another metric with scoped
tables in the same execution, including demand-group-only chains that do not
advance the simulation.
Other undeclared or unsupported dependencies
remain conservative. `results.explain()` reports requested scopes, dependency
tables, cache status, storage, and whether final costs and inner-flow diagnostics
are planned for each lane without executing the simulation. It also lists the
main cohort work and the requested outputs at each selected step. It does not
describe every individual model computation or estimate temporary storage use.

The focused integration regression uses two independent cohort executions.
The result API tests also interrupt a later replication lane after earlier
fragments have been written and check that those temporary files are removed.
An attempted controlled Grand Geneve comparison used the committed baseline,
two scenarios, seed 0 and a two-step horizon. It recorded 127,967 and 281,119
candidate IDs in the first and second steps respectively, but the 20 GiB memory
guard interrupted the baseline during the second step's behavior transition
after 261.9 seconds. The traced process peak reached 23.36 GiB before the
native operation returned and the interruption was handled. This attempt did
not establish equivalence or produce a complete state comparison; the old
baseline also does not support the current managed temporary-disk limit. The
opt-in `tests/manual/compare_grand_geneve_iterations.py` records candidate and
state fingerprints for a future comparison under a suitable resource limit.
With the same two-step horizon and seed, a current run stopped before step 2
matched the baseline's first-step fingerprint for all 127,967 candidate IDs.
Subsequent first-step captures for both scenarios matched all 218,044 plan keys,
plan IDs and RNG-state fingerprints. Individual plan weights and utilities did
vary: two runs of the same current implementation differed by up to 2.96e-5
represented people per plan and 1.72e-5 in utility. Initial OD costs matched
exactly; initial activity summary differences were at most 3.02e-14.
The home-night utility summed Float32 step durations and travel times before
converting the result. After summing those inputs in Float64 in both scoring
and candidate pruning, two more first-step runs retained identical candidate
IDs, plan IDs and RNG fingerprints. Their maximum differences fell to 6.47e-13
people per plan and 2.85e-13 utility. This improves numerical reproducibility;
it does not promise bitwise equality. Run and checkpoint identities were
updated because the numerical calculation changed. The observed first-step trace peaks were 11.55 GiB for the
baseline and 9.30 GiB for the current run; their wall times were 61.8 and
70.8 seconds respectively. These single observations are not a controlled
speed comparison or evidence of equivalence beyond the recorded fingerprints.
Stopping immediately after the second candidate search allowed a direct
two-step-horizon comparison without running the high-memory transition. The
current runner produced 281,167 candidate IDs, while the baseline produced
281,119. Applying the Float64 home-night and destination-saturation changes to
the isolated baseline did not change its candidate count or fingerprint. The
candidate sets therefore did not meet the identity acceptance criterion. The
baseline in that comparison reused a congestion graph calculated before the
precision changes, so it did not fully reflect those changes. In a first-step state comparison, the
compact candidate membership and steps, destination and mode sequence indexes,
and time-sequence steps matched exactly. Plan-step timings and durations also
matched, while plan weights and saturation values differed only at floating-point
rounding scale. The measured search peaks were 14.99
GiB for the current runner and 19.44 GiB for the adjusted baseline. These are
single observations, and neither run completed the second behavior update.
A subsequent baseline with a freshly calculated congestion graph still showed
different mode choices. Car search costs differed by at most 2.7e-14 in their
original units. Searching 29,014 captured location chains with those saved
inputs produced 398 different mode sequences on each side. An experiment
rounding search inputs to 1e-12 in the original cost units made those captured
results match. However, in a fresh end-to-end comparison with the same
experimental rounding applied to both implementations, two candidate IDs on
each side still differed in the first batch of step 2 (205,477 IDs per side).
The remaining candidates exchanged modes between repeated trips. The
experimental rounding also changed some model choices, so it was not retained.
The complete identity criterion remains unverified.
An additional check on the captured first-step plan rows showed that shuffling
identical rows changed 957 of 13,975 aggregated OD-mode flows, by up to
9.09e-13 people, because the summation order changed. Person flows now sum
Float64 weights in a stable order, and both person and vehicle flow tables use
a stable OD order before traffic assignment. The same captured rows then
produced identical flow tables after shuffling. This removes one source of
round-off variation; it does not remove differences already present in plan
weights or establish full-model candidate identity. Run, state and checkpoint
identities were updated for the numerical change.
A fresh bounded comparison after ordering the OD tables produced 205,609 and
205,613 candidates in the first step-2 batch, with 429 and 433 IDs unique to
the current and baseline runs. Their car search costs differed by at most
1.49e-14 in original units. This comparison used a direct `sort().sum()`
expression: later inspection showed Polars can optimize away that sort. The
flow and per-plan utility sums now retain the sorted values through the sum;
focused tests exercise reordered inputs. A further bounded two-scenario run
using the corrected sums in both implementations matched the first-step
candidate and RNG fingerprints. In the first batch of step 2 it produced
205,596 and 205,610 candidates, with respectively 520 and 534 IDs unique to
each run; the maximum car search-cost difference was 5.40e-14 in original
units. This does not establish equivalence. The baseline trace included a
19.65 GiB sampled peak before stopping at that batch, close to the guard; list
aggregation can affect memory and the larger transition was not run.
Further first-step captures isolated an earlier source of variation: the
population-weighted survey summaries and opportunity capacities used unordered
floating-point sums. Two fresh initial-state captures now have identical
activity and home-night summaries, opportunity tables and costs after sorting
their rows. With the same stable sums in an isolated baseline, all 218,044
first-step plan utilities matched exactly; 42,345 person allocations still
differed by at most 1.07e-14. After making the transition-probability and
allocation sums stable too, another bounded comparison matched every captured
plan utility and allocation exactly for both scenarios, as well as the
candidate and RNG fingerprints. The full second-step candidate search and
behavior update have not been verified with these latest changes.
Stopping before mode search in a subsequent two-scenario comparison kept the
candidate batch from being built. The two implementations produced exactly
matching destination sequences and all 1,341,481 captured mode-cost rows,
including car costs. Four of 1,104 generation-request population weights still
differed, by at most 8.19e-12; their effort, reason and priority matched in
that capture. Population and allocation-effect sums in the adaptive scheduler
now use stable ordering as well, because those weights can affect later search
decisions. The captured-input comparison predates this last scheduler change.
The sampled trace peaks for the bounded input captures were 10.19 GiB and
12.21 GiB; these are not full candidate-search peaks.
A later bounded capture continued through the first scenario's mode search,
with the stable adaptive sums applied to both implementations. All 1,104
generation requests per scenario, 157,067 captured destination rows,
1,341,481 mode-cost rows and 388,608 mode-sequence rows matched exactly after
sorting. The captured trace peaks were 10.39 GiB and 11.89 GiB. The diagnostic
stopped before assembling the shared candidate pool, so this does not establish
identity for that pool, the other search batches or the behavior update.
The shared pool now releases source candidate frames immediately after their
content and per-run history have been merged, before attaching the merged pool
to each scenario. A lifetime regression checks those source frames are no
longer referenced at attachment. A guarded current-run probe reached the first
step-2 batch with 205,606 candidate IDs and a 10.29 GiB sampled trace peak; it
stopped before the behavior update. The earlier comparable current trace
peaked at 9.24 GiB, so these single observations do not demonstrate an RSS
reduction. That probe did not yet include an updated baseline pool comparison.
A later baseline probe, with the same retention-only cleanup and a 16 GiB
sampled RSS guard, stopped while merging shared candidate content. The monitor
observed 16.70 GiB, while the operation trace recorded 19.21 GiB before the
native operation returned; no second-step batch result was produced. The guard
is therefore not a hard memory ceiling. A separate local experiment merged
three copies of 863,945 captured candidate-step rows with constructed sequence
keys. Eager and streaming Polars merges produced the same output fingerprint,
but their sampled process peaks were 1.09 GiB and 2.04 GiB respectively in
these single runs. Streaming was not retained as a memory optimization; this
experiment does not measure the full model's live memory.
An unguarded two-scenario, two-step comparison subsequently completed using
the same seed and cached inputs. The baseline used a diagnostic streaming
merge for the shared candidate content; on the real captured merge inputs,
its output matched the original eager merge. In these single runs, the baseline
took 603 seconds and peaked at 30.27 GiB sampled RSS; the current runner took
352 seconds and peaked at 18.00 GiB. All three second-step candidate batches,
their captured structural steps, and the random-generator states matched. The
requested first-step rows, final cost rows and projected final plan steps also
matched within the comparison tolerance. This is not full-state equivalence:
each scenario had 27 different detailed plan keys, and 210 common plan weights
differed by more than 1e-9, with a largest difference of 2.87 represented
people. The projected rows do not include demand-group identities. A later
five-step comparison remains necessary.

The pruning calculation was found to sum incoming transition weights in their
arrival order before ranking targets. Rounding can reverse the order of equal
targets and bypass the existing plan-key tie-break, changing which plan receives
the pruned weight. A capture of three affected groups from the real second step
confirmed that reversing input rows changed four pruning destinations with the
old calculation, including destinations involved in the observed discrepancy.
The stable sums produced identical mappings in both orders. This establishes a
cause for part of the discrepancy, but does not establish that every difference
between the earlier complete runs had the same cause.
A fresh unguarded two-step comparison with the stable pruning calculation in
both implementations matched every detailed plan key, plan ID, weight and
utility exactly at both steps for both scenarios. Candidate IDs and captured
candidate steps matched exactly in all three second-step batches, as did the
random-generator state fingerprints. The requested first-step rows, final costs
and projected final plan steps matched at relative tolerance 1e-10 and absolute
tolerance 1e-8. The diagnostic-only baseline streaming merge was retained for
this comparison. Sampled process peaks were 28.93 GiB for the baseline and
17.98 GiB for the current runner; wall times were 404 and 318 seconds. These
single observations are not a controlled performance benchmark or proof of
equivalence for other horizons and settings.
For more detailed search-input and score comparisons, run the opt-in diagnostic
`pixi run python -m pytest --local --use-truststore tests/manual/inspect_cohort_reproducibility.py`.

The first end-to-end slice has since been extended to the consumers described
above. The Grand Geneve comparison and broader validation still need to show
that the implemented behavior meets the acceptance criteria.

For context, an earlier Grand Genève probe with two scenarios and one seed
measured approximately 243 seconds and 11.27 GiB peak process memory for five
steps, and 545 seconds and 22.27 GiB for ten. The candidate pool grew to about
52,000 and 99,000 respectively. These are observations of that configuration,
not performance targets; shared buffers, allocator retention and total project
disk usage must not be confused with uniquely attributable live memory or spill.
An experimental release of earlier state changed aggregate results, so do not
reuse it as a correctness-preserving fix without identifying the cause.

On 10 November 2026, a cache-only probe adapted the current Grand Genève scenario
script to the step API and requested five steps for the default and carbon-tax
scenarios, one seed. It used the script's current settings, including adaptive
search, so it is not a controlled comparison with the earlier numbers above.
Step 1 completed in 57.8 seconds with 11.58 GiB peak Python-process RSS. Step 2's
inner search took 174.1 seconds and peaked at 18.44 GiB; the process reached
23.49 GiB while applying behavior transitions, and the memory guard interrupted
the run after 264.4 seconds. The interruption arrived after the 20 GiB guard
threshold because the active Polars operation had to return first. No final
results were produced. These RSS peaks are process measurements, not sums of
unique live buffers. The completed search batches in step 2 took 48.3, 68.2 and
57.1 seconds; candidate search and behavior transitions both warrant attention.
Candidate-pool sizes were not recorded by that attempt. The opt-in diagnostic
`tests/manual/inspect_grand_geneve_execution.py` now prints them as each search
finishes. This guarded attempt predates the later unguarded comparison above.

After releasing inner-search intermediates earlier, the same guarded probe
completed steps 1 and 2, with candidate pools of 127,967 and 281,216 IDs.
Their process RSS peaks were 10.58 and 21.78 GiB respectively; the earlier
attempt reached 11.58 and 23.49 GiB at those steps. Step 2 took 185.2 seconds.
The guard fired and the run stopped during step 3 after 299.0 seconds overall,
while creating the shared candidate pool. The interrupt can be delayed until a
native operation returns, so the 20 GiB threshold is not a hard peak limit.
These are two observations, not proof of a fixed reduction, and the five-step
comparison remains unfinished.

After the streaming history join and delayed plan-step collection, another
guarded probe again produced 127,967 and 281,216 candidate IDs in the first two
searches. Step 1 completed in 57.1 seconds at 10.91 GiB peak RSS. Step 2's
search completed in 167.8 seconds at 18.23 GiB, but the guard interrupted its
behavior update at 20.23 GiB while compacting candidate steps. The step and run
did not complete, so their elapsed times are not comparable to completed runs.
Matching counts do not establish that candidate identities or results match.
Candidate-step compaction now uses Polars streaming for its deduplication and
count checks; its effect on the Grand Genève peak has not yet been measured.
The probe now reports estimated sizes of individual live state and catalogue
tables, plus sampled project-wide managed temporary-disk usage. The estimates
are not added together because those tables may share buffers, and the project
disk measurement can include other open results.

A subsequent probe with those measurements completed steps 1 and 2 with the
same candidate counts. Their process RSS peaks were 9.53 and 18.26 GiB; step 2
took 175.9 seconds. At the end of step 2, the candidate catalogue's step table
was estimated at 45,370,689 bytes, and each reported scenario's compact
candidate-membership table at 328,717,604 bytes. These estimates may share
buffers and are not added together. Sampled project-wide managed temporary usage
peaked at 1,851,222,951 bytes; it can include other open results and excludes
untracked library spill. The guard fired during step 3 while expanding compact
candidate history, with process RSS peaking at 22.16 GiB before the native
operation returned. The five-step run again did not finish.

After reducing compact history before reconstruction, the same guarded probe
completed steps 1 and 2 at 9.30 and 18.32 GiB peak RSS, with 127,967 and
281,216 candidate IDs respectively. Step 2 took 169.6 seconds. Step 3 passed
the previous history-expansion interruption but reached the guard during scoring
context construction; its interrupted peak was 21.20 GiB, and the overall run
stopped after 397.7 seconds. Sampled project-wide managed temporary usage peaked
at 1,851,223,415 bytes. The five-step run remains incomplete. A small fixture
compared compact history with the full-table fallback and matched candidate IDs,
random states and requested outputs; checkpoint continuation also passed.

The unguarded five-step probe subsequently completed for the same two scenarios
and one replication, requesting trip counts at every step and adaptive-search
diagnostics. Its candidate pools contained 127,967, 281,227, 438,159, 587,633
and 632,245 IDs. The run took 1,535 seconds; its traced process RSS peaked at
44.25 GiB during behavior transition probabilities, and sampled managed
temporary files peaked at 1,979,423,421 bytes. That transition calculation
started at 24.04 GiB RSS and ended at 24.18 GiB after the temporary peak; this
does not identify how much of the peak was live model data. The longest simulation
step took 513 seconds, including 423 seconds in its inner loop. These times are
nested and must not be added. At the end of step 5, each scenario's compact
candidate-membership table was estimated at about 677 MB; estimates can share
buffers and do not explain all process RSS. This single run establishes that
the configuration completes and identifies a high-memory operation; it is not
a five-step comparison with the former implementation.

After the stable pruning change, all 456 group-day-trip and runtime unit tests
and all 24 cohort reproducibility integration cases passed. These include
checkpoint continuation and the supported shared-result paths.

## Scoring reuse investigation

A separate diagnostic captured scoring inputs before utility and modeled timing
calculations in step 4, batches 1 and 2, for the default and carbon-tax
scenarios. The run used the same two-scenario, one-replication Grand Geneve
configuration as the five-step probe and stopped during the second batch.

| Input keys | Batch 1 rows / distinct keys | Batch 2 rows / distinct keys |
| --- | ---: | ---: |
| Current scoring key, including plan IDs | 17,244,549 / 17,244,549 | 18,651,750 / 18,651,750 |
| Step inputs without plan IDs | 17,244,549 / 9,880,586 | 18,651,750 / 10,688,674 |
| Travel cost inputs | 17,244,549 / 403,550 | 18,651,750 / 412,280 |
| Activity parameter inputs | 17,244,549 / 61,685 | 18,651,750 / 61,913 |

All distinct first-batch keys in these four groups reappeared in the second
batch. The second batch added 1,407,201 current scoring keys, 808,088 distinct
step inputs without plan IDs, 8,730 travel cost inputs and 228 activity parameter
inputs. Both scenarios had identical input-key sets in batch 1. A key without
plan IDs includes country, CSP, activity, origin, destination, mode, original
timings, duration and utility profile; the travel key includes utility profile,
origin, destination and mode, while the activity key includes country, CSP,
activity and destination.

These counts identify possible reuse, not measured speedup. Matching keys across
scenarios do not establish identical transport costs, profile meanings,
activity parameters or saturation. The captured inputs precede modeled timing
adjustments, which can depend on neighboring plan steps; they do not establish
that final step utilities or entire plans can be reused. Reuse must preserve
feasibility, schedule identities, deterministic sums and the inputs' lifetimes.
The opt-in capture and counting commands are
`pixi run python tests/manual/capture_scoring_inputs.py` and
`pixi run python tests/manual/analyze_scoring_inputs.py CAPTURE_FOLDER`.

The inner loop now keeps scoring contexts and temporary plan indexes for each
scenario within the fixed outer step. A bounded complete-plan cache records the
input plan content, modeled schedule mapping, utility and feasibility. Later
batches rescore changed complete plans, combine their results with matching
plans still in the current pool, and recalculate allocation for the full choice
set. A separate bounded lookup shares fixed travel-cost and activity inputs
across plans and batches. Both caches are scoped to the current inner loop and
scenario; they are not shared between scenarios or written as persistent model
state. The cohort-wide retained-data budgets are 1 GiB for plan results and
512 MiB for scoring lookups. If a lookup cannot fit, later work recomputes it;
the limits do not cover temporary operations or all other simulation data.
When input schedules collapse into one modeled plan, the runner combines their
histories using the candidate catalogue's existing rules: earliest iteration
and first-seen value, and latest seen and active values. Every leg receives
the combined history. This replaces the former arbitrary choice of one input
history; because history affects candidate retention, it can also change later
search and simulation results. Fresh and incremental evaluation use the same
rule, and neither needs to reevaluate plans solely to resolve their histories.
Simulation and checkpoint identities include the new history policy, so saved
states from the former policy are not silently resumed under the new one.
Cross-scenario score reuse remains disabled because
matching plan inputs alone do not prove matching costs and model state. A new
softmax implementation was not introduced: the earlier trace measured roughly
40 seconds for fresh allocation, including work beyond normalization, versus
almost 15 minutes for candidate evaluation. Any future blockwise reduction must
also preserve deterministic numerical behavior.

On the captured step-4 input rows, a focused lookup benchmark with constructed
costs and activity parameters produced matching output fingerprints in all
three methods. The original lookup took 6.13 and 6.80 seconds for batches 1 and
2 and peaked at 9.34 GiB RSS. The narrower lookup took 3.65 and 3.84 seconds,
peaking at 3.78 GiB; retaining that lookup across batches took 3.56 and 3.79
seconds, peaking at 3.73 GiB. This diagnostic does not measure modeled timing,
complete-plan evaluation or an entire simulation.

A fresh two-scenario two-step Grand Geneve run using incremental evaluation
matched the earlier current implementation exactly for the saved detailed plan
states, candidate IDs and candidate steps, and matched the requested tables at
the established tolerance. Candidate and RNG fingerprints also matched. The
earlier run took 318 seconds with an 18.00 GiB traced peak; the incremental run
took 271 seconds with a 20.74 GiB traced peak. These are individual runs, not a
controlled performance benchmark or proof for other horizons or scenarios.
That incremental measurement predates the history safeguard: its step-2
candidate history could not be compacted and is not acceptable for later steps.
With the full-batch safeguard, a diagnostic stopped while saving the first large step-2
history. All 9,632,685 membership rows matched the fresh-evaluation reference
exactly, including null values. The history remained compact; this diagnostic
took 291 seconds and had a sampled process RSS peak of 18.29 GiB. It did not
finish both scenarios or measure later steps, so it is not a whole-run benchmark.
An initial selective-history diagnostic matched the same membership rows in
268 seconds at 20.30 GiB sampled peak RSS. A subsequent regression test exposed
different step metadata, so that version was corrected; the diagnostic's timing
and memory do not measure the corrected version. The corrected implementation
completed a two-step comparison in 289 seconds at 17.90 GiB sampled peak RSS.
Both scenarios' step-2 membership rows matched the reference exactly;
allocations and requested tables matched at the established floating-point
tolerance, and candidate and random-state fingerprints matched. These are
individual runs and do not establish a controlled speedup.
Before the history merge change, an opt-in five-step run completed in 1,487
seconds with a traced peak RSS of 43.92 GiB, compared with 1,535 seconds and
44.25 GiB in the earlier run. The two-scenario trip-count tables matched at
the established tolerance (maximum absolute difference below 5e-9), and the
candidate-pool sizes matched at every step. Both scenarios' candidate histories
remained compact. Total top-level candidate evaluation time was 825 seconds,
compared with 889 seconds previously; these individual runs are not a controlled
benchmark and shared the machine with brief test runs. Peak managed temporary
storage sampled during the new run was 2.91 GB. This does not establish that a
20-step, ten-scenario run fits within available memory.

After adopting the history merge rule, a two-step run took 280 seconds and
peaked at 19.23 GiB sampled RSS; the immediately preceding selective-history
run took 289 seconds and peaked at 17.90 GiB. These individual runs are not a
controlled benchmark. Candidate counts, allocations and requested outputs
matched for both steps at the established tolerance, while 182,923 of the
9,632,685 step-2 candidate memberships had different history fields. A
separate fresh evaluation using the merge rule matched all 9,632,685 saved
incremental history rows exactly. Later search and pruning can still differ
because the history change is intentional.

A five-step run with the merge rule took 1,240 seconds and reached 44.75 GiB
traced peak RSS, compared with 1,487 seconds and 43.92 GiB in the preceding
selective-history run. Candidate evaluation plus fresh allocation totaled
703 seconds versus 897 seconds. All 40 trip-count rows matched within the
established tolerance and all 40 per-batch candidate counts matched; both
scenario histories stayed compact. These are individual runs under different
machine load, not a controlled benchmark. The history fields deliberately
differ, so this result does not imply identical behavior for configurations
where candidate retention depends on those differences.

## Candidate attachment and transition memory

The inner loop now keeps shared physical plan steps separate from their
demand-unit memberships where the input can be represented without loss.
Membership is grouped into scoring contexts before the steps are attached;
conflicting or incomplete representations use the existing row-based path.
The shared and occupied candidate rows are also combined with one final
deduplication instead of sorting and deduplicating intermediate expansions.

On a captured step-2 batch, the factored result matched all 8,269,243 compacted
rows exactly. A complete two-step, two-scenario run took 261 seconds with a
20.32 GiB sampled RSS peak, versus 280 seconds and 19.23 GiB for the preceding
run. Candidate and random-state fingerprints matched, both scenarios' saved
candidate memberships matched exactly, and requested tables and allocations
matched at the established tolerance. These are individual runs, not a
controlled benchmark.

For each replanning scope, origins with the same fixed sequence choices have
the same feasible target set. The maximum target utility is now calculated
before joining origins to targets, allowing the existing utility and gain
filters to run inside the join. Self-transitions and forced reassignment retain
their original rules; a no-transition step needs no maximum calculation. On
captured step-4 Grand Geneve inputs (498,044 current plans and 19,384,212
possible-plan rows), separate
processes produced 3,110,966 transition rows with matching two-seed fingerprints
over every output column. The previous calculation took 5.62 seconds and
peaked at 22.98 GiB RSS; the fused calculation took 1.44 seconds and peaked at
2.58 GiB. These are isolated measurements and do not include the rest of the
simulation's live state.

When transition-event details and transition distances are not requested, the
complete possible-step table stays lazy until it is joined to the resulting
current plans. Event recording and transition-distance calculations still
collect the complete table when they need it.

With these changes, the same opt-in five-step, two-scenario run took 1,053
seconds and reached 38.45 GiB traced peak RSS, versus 1,240 seconds and
44.75 GiB in the preceding run. Candidate evaluation took 444 seconds versus
629 seconds; fresh allocation took 71 versus 74 seconds. Candidate compaction
took 48 seconds versus 216 seconds, while shared-pool generation and
attachment took 308 versus 284 seconds. These spans are nested: compaction is
part of candidate evaluation, and they must not be added together. All 40
per-batch candidate counts matched, and all 40 trip-count rows matched at the
established tolerance. The peak moved to expansion of allocated plan steps in
step 5. These individual runs had different machine load and are not a
controlled benchmark; they do not establish memory requirements for ten
scenarios or twenty steps.

The opt-in lookup benchmark is `pixi run python
tests/manual/benchmark_scoring_lookup.py CAPTURE_FOLDER METHOD`.
