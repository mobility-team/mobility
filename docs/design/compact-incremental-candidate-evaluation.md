# Compact incremental candidate evaluation

Status: proposed. This design concerns the internal execution of candidate
evaluation and inner allocations. It builds on
[demand-driven simulation execution](demand-driven-simulation-execution.md).
Measurements and prior experiments are in
[Grand Geneve performance notes](grand-geneve-performance-notes.md).


## Update: allocation floor

The proposed physical-plan-wide floor in this document was superseded. The
implemented `min_plan_persons` threshold is applied separately to each plan
and demand subgroup, after summing allocations from all origins. Its default
is one represented person; zero disables it. Smaller mobile allocations go to
the same subgroup's stay-home plan. The floor starts at
`plan_pruning_start_step` and is skipped on no-transition steps. It replaces
the previous retained-probability-share rule. Saved transition events retain
both the chosen plan and the final target after reassignment. References below
to an optional global floor or the former retained-share policy describe the
earlier proposal and experiments, not the implemented policy.

## Goal and model contract

Keep physical plan steps, eligibility, scores and allocations separate through
the inner loop. Evaluate new physical timing inputs and new context-plan scores
once per outer step, rather than recreating a wide table of every physical step
for every scoring context in every batch. Recalculate choices and flows over
the current choice set after each batch.

The compact representation must preserve the existing model when its existing
pruning policy is selected: candidate generation, scenario sharing,
feasibility, transition restrictions, allocations, flows, adaptive search,
random seeds, saved state and outputs. Sampling and convergence strategy
changes are separate proposals.

An earlier version also proposed a scenario-wide physical-plan volume floor.
The implemented floor instead applies separately to each demand subgroup, as
described above. The remaining proposal and measurements are kept for context.

## What exists today

- `PlanCatalogue` stores physical candidate steps separately from sequence
  variants and run membership/history. Physical identity is currently two
  content hashes. A `variant_id` is local to the catalogue and is rebuilt on
  merge; it is not a persistent experiment-wide ID. The catalogue is usually
  discarded when the inner search finishes.
- `ScoringCandidateSteps` and `CompactCandidateSteps` separate physical steps
  from memberships. `ScoringContexts` groups demand units with equivalent
  scoring inputs. The factored path checks that a local plan key has one
  complete physical representation; otherwise it falls back to expanded rows.
- The shared pool merges scenario candidates within one replication,
  translates local sequence IDs, restores occupied plans and applies retention
  rules. `collapse_factored_step_rows` then joins physical steps to contexts
  before timing and scoring. The same candidate/context rows are revisited in
  later batches even though the pre-step inputs are fixed.
- The existing scoring lookup reuses some OD/mode and activity calculations,
  but not complete modeled timings or plan scores. The allocation code already
  works at context level when per-unit low-mass pruning is disabled. When it
  is enabled, it expands the relevant transitions to demand units, prunes, and
  collapses allocations back to contexts.
- The OD/mode flow helper joins allocated plan weights to step rows. The
  final behavioral update, fresh-MNL diagnostic, transition events and result
  consumers also rely on the current evaluation representation.

The temporary cache placed inside the existing row-expanded pipeline did not
improve the high-effort run. Across steps 1–4, evaluation increased from
320.97 to 360.15 seconds and traced Python peak from 24.00 to 27.65 GiB. It
still built expanded rows and added wide joins and retained tables. That result
does not rule out a compact data path, but it does rule out presenting that
cache experiment as evidence of a gain.

## Terms and data shapes

A **physical plan** is an ordered activity, timing, destination and mode
sequence. Plans start and end at home: the final destination identifies home,
the first origin is home, and each later origin is the preceding destination.
Thus a complete ordered destination sequence contains the origins. Stay-home is
a special alternative and keeps the current zero-ID behavior.

A **variant** preserves the survey reference and sequence choices needed by
transition restrictions. Different reference variants can become the same
modeled physical plan. Their references and merged history must survive.

A **scoring context** is the existing group of demand units with equivalent
home zone, country, socioeconomic category, allowed-activity profile and
utility profile. A **run** is a scenario/sensitivity case within a replication
lane. Runs share physical candidates in that lane, while costs, feasibility,
scores and history can differ. Replication lanes remain independent.

An **inner batch** is one candidate-generation and hypothetical-allocation
pass. No person changes their real plan between batches: each allocation starts
from the same pre-step state. The actual behavioral transition occurs once,
after the search stops.

The following are logical tables, not a requirement to introduce classes or
persist each table separately. Reuse existing representations where possible.

| Table | One row per | Contents |
| --- | --- | --- |
| Physical plans | physical plan | shared physical identity and component identities |
| Physical steps | physical plan and step | activity, destination, mode and reference timings |
| Variants | physical plan and meaningful reference | survey reference and choices needed by restrictions |
| Run membership | run and variant | availability and first/last seen/active history |
| Context membership | run, context and variant | eligibility to score and choose the variant |
| Modeled timings | run, physical plan, time inputs and step | modeled arrival, departure, duration and schedule identity |
| Plan scores | run, context and modeled plan/variant | utility, feasibility, missing/invalid-cost counts |
| Origins | run, context or demand unit and occupied plan | fixed pre-step people and previous utility |
| Allocation | run, context or demand unit and target plan | represented people after the hypothetical transition |

If `P` is physical plans, `L` their total step rows, `R` eligible context-plan
pairs and `U` demand units, the aim is step storage proportional to `L`, timing
work proportional to distinct physical-plan/travel-time-input combinations,
and scores proportional to `R`. Do not require a persistent wide table of
`R × steps per plan`. Choice and transition work can still depend on the
number of origin-target pairs; this design does not assume choice sets are
small.

## Identity and history

Use stable integer identities that are meaningful through the shared pool's
lifetime. The desired physical key combines **shared** activity, time,
destination and mode sequence IDs. Combining today's run-local IDs does not
provide a shared key. Complete typed sequence content, ordering and the
existing whole-second timing normalization define equality. A collision-free
shared index must compare actual canonical keys; hashes used internally by
Polars are fine because actual equality is checked.

Use the existing indexing machinery where it fits. Sort newly discovered keys
before assigning IDs and retain the mapping. Sequential IDs are reproducible
for a fixed execution and discovery order, including checkpoint/resume; they
are not automatically invariant to changing scenario or batch order. Do not
promise immutable, order-independent sequential IDs or IDs shared between
unrelated experiments without a separate design.

The current coordinator has one lane per replication. The first target is a
shared namespace across scenarios and sensitivity cases **within a lane**;
an experiment-wide reference can include lane plus integer ID. A single
mutable index shared by independent replications would introduce coordination
and restart dependencies. Do not conflate local IDs from different lanes.

The compact-path prototype may initially carry the existing two physical
content keys. That preserves their current collision properties; it is not an
exact collision-free index. Measure an exact component index before replacing
the existing identity system. Maintain reference variants even when physical
contents coincide. Avoid constructing another full-plan fingerprint after
context expansion.

History is pool bookkeeping, not a behavioral restriction. When observations
merge, preserve the existing earliest/latest rules: minimum `iteration` and
`first_seen_iteration`, maximum `last_seen_iteration` and
`last_active_iteration`. Maintain run/variant history as needed and the
physical retention summary across scenarios within the replication. Protect
occupied plans. Apply inactivity pruning at the current outer-step boundary
and warm-up point. `max_shared_candidates` is a separate optional size cap,
defaults to `None`, and can remove non-active plans between batches; the
Geneva script does not set it.

## One outer step

1. **Freeze inputs.** Hold pre-step allocations, history, demand units,
   scenario costs, saturation, activity settings and behavior phase fixed
   across inner batches. Preserve each scenario's resolved inputs and RNG
   state. Physical content can be shared without assuming scores or timings
   match across scenarios.
2. **Start the pool.** Restore candidate membership and actual occupied plans,
   apply normal outer-step retention and establish scoring contexts. Keep
   reference variants and the original origin distribution.
3. **Generate a batch.** Use the existing scheduler, effort settings, seed
   progression and samplers. Add new physical plans, variants and eligible
   memberships; report plan-level history refreshes and any removals. A
   surviving plan's scoring inputs are fixed in the inner loop, so do not
   compare every old and new step to rediscover changes. Apply the optional
   size cap with the current deterministic ordering when configured.
4. **Evaluate additions.** Update timings for new physical-plan/time-input
   combinations and score newly eligible run/context/plan combinations. Carry
   existing timings and scores forward. Preserve infeasibility and cost-row
   diagnostics. When modeled identities converge, use the current reference,
   choice-deduplication and history-merge rules.
5. **Estimate choices and flows.** Recompute allowed alternatives and choice
   probabilities over the complete current choice set. Apply existing
   population weighting and pruning, then aggregate allocated plan weights to
   OD/mode flows using physical steps. Retain only the allocation summaries
   needed by scheduling and convergence.
6. **Continue or stop.** Preserve the existing flow and scenario-to-base gap
   convergence checks, consecutive-pass requirement, batch limit, adaptive
   requests and scenario cohort. A result filter or cached output must not
   silently remove a scenario from the shared candidate pool.
7. **Commit outer state.** Apply the actual behavioral transition once to the
   original pre-step allocation; map allocated plans to demand-unit IDs,
   update history and destination saturation, write requested outputs and
   checkpoints, and release inner-only tables.

For `no_transitions`, generate no activity, destination or mode alternatives
and do not advance the sampling RNG. Refresh occupied plans' costs and
feasibility; retain feasible occupied plans. An infeasible origin uses feasible
alternatives already in the shared pool under the existing forced-reassignment
rules, with stay-home available as fallback. Apply normal pool retention.
Behavior phases are currently global across the experiment's scenarios at a
given step.

## Timings, utility and transitions

Reproduce the existing modeled arrival, departure and duration calculations,
activity-specific arrival rigidity, duration floor, whole-second rounding and
stable schedule mapping. A timing result depends on more than a physical plan
when scenario networks or resolved profiles produce different travel times.
Start conservatively with run/physical-plan/utility-profile timing inputs;
share more broadly only when identical time inputs are established. Timing and
score reuse lasts only for the fixed outer step.

Calculate context-plan utilities with the current step cost and activity
parameters, destination shadow-price behavior, sorted aggregation, home-night
term, feasibility and missing/invalid-cost counts. Timing can be shared without
implying that utilities are shared. Do not change aggregation order, approximate
utility or discard low-probability plans as part of the representation change.

The current transition rules remain the contract:

- Preserve full, destination-only, mode-only and no-transition scopes;
  restricted phases use the reference timing sequence where they do today.
- Retain infeasible plans as origins, exclude them as targets, and preserve
  forced reassignment and stay-home fallback.
- Preserve utility-window and minimum-gain filters, protected self-transition,
  revision probability, optional plan-distance threshold and friction.
- Preserve stable softmax normalization per origin, sorted sums, tie breaking,
  and behavior at zero or small logit scales.
- Preserve existing per-unit low-mass target pruning when the legacy policy is
  selected. It happens after transition mass is calculated and treats stay-home
  and mobile targets separately. The optional global floor below deliberately
  changes this behavior and must not be applied in addition to legacy pruning.
- Preserve the distinction between transition-based inner allocation and the
  optional fresh-MNL diagnostic, calculated after the final batch when needed.

Probabilities can be shared for origins with identical context and transition
inputs. Under the legacy policy, apply unit-specific target pruning before
combining units whenever configured. Under the optional global floor, apply the
single plan-level decision and stay-home remapping described below. Keep data
required by the optional distance model and transition events; do not
specialize the algorithm to the Geneva settings.

## Optional global volume floor and stay-home reallocation

The purpose is to avoid retaining physical plans that carry negligible total
allocated volume. This is allocation bookkeeping, not a statement that
stay-home has higher utility. It intentionally approximates behavior: moving
small allocations to stay-home removes their trips and can change future
origins, convergence, sampling and scenario comparisons. It is a separate
model change from the compact representation.

### Configuration and migration

Use a non-negative floor in **represented persons**, not sampled rows. When
the global-floor policy is selected, propose **1 represented person per
physical plan** as the initial default, pending representative validation. A zero floor disables the threshold.
The existing pruning start step remains configurable; use the same start-step
semantics as the current setting during comparison, whose default is step 2.

Only one low-mass allocation policy applies in a run: existing per-unit
retained-share pruning, the global floor, or neither. Selecting the global
policy replaces the legacy policy; it does not apply both in sequence.
Existing configurations retain their current behavior until explicitly
migrated, so the proposed value does not by itself enable a behavioral
change. Once the new rule is validated and a modeling decision is made,
remove or deprecate the old policy in a separate change rather than maintaining
two engines indefinitely. The Geneva script currently uses 0.95 retained
share starting at step 2; switching it to the global rule changes results.

### Floor calculation

For one scenario, sensitivity case and replication at one allocation:

1. Calculate the normal transition probabilities and represented allocation
   from the fixed origin states. Apply the existing feasibility, scope,
   utility-window, gain and revision rules first.
2. Sum allocated represented persons by **modeled physical plan** across all
   demand units and scoring contexts in that run. Combine reference variants
   only when they represent the same physical plan; retain the separate
   reference/transition records required by the model. Use deterministic
   ordering and summation. Do not pool volumes across scenarios, sensitivity
   cases or replications.
3. Identify mobile plans whose total is **strictly less than** the configured
   floor. A total exactly equal to the floor is retained. Stay-home is never
   removed. The decision is made once from the pre-remapping allocation; do
   not repeatedly prune or recalculate the floor after redistribution.
4. Redirect every removed plan's allocated volume to the stay-home plan for
   the **same demand unit**. Sum redirected volumes with any existing
   stay-home allocation. Keep the source origin attribution where transition
   events require it. Ensure the demand unit has its existing stay-home ID
   and utility, and use the stay-home target keys and zero-trip steps.
5. Every demand unit's represented population and the scenario total must be
   conserved. Dropped plan volume must equal the added stay-home volume, apart
   from the existing numerical tolerance. A removed physical plan can remain
   in the candidate pool under its normal inactivity rules; the floor prunes
   **allocations**, not candidate generation or pool history.

The floor operates after allocation. Consequently, in ordinary replanning
phases it may redirect an otherwise allowed mobile target to stay-home even
where a normal mode- or destination-replanning transition would not choose
stay-home. It can also redirect a small self-allocation, including people who
would otherwise not revise their plan. These are deliberate consequences of
this approximation and must be measured, not described as preserving all
transition restrictions. Forced-reassignment feasibility is evaluated before
this post-processing. Stay-home is always available for the redirect.

A `no_transitions` step is the explicit exception: skip the floor for that
step so a feasible occupied plan remains unchanged, as agreed for that phase.
An infeasible origin still follows the normal forced-reassignment and
stay-home fallback rules. Do not generate new plans or advance sampling RNG.

### Inner loop, final state and diagnostics

Apply the selected global floor to each hypothetical inner allocation **after**
normal transition probabilities and before producing OD/mode convergence
flows and allocation summaries. Calculate its plan totals using all eligible
context/plan weights; where probabilities are shared, aggregate context/origin
weights without expanding every candidate step to every demand unit. Preserve
the information needed to credit redirected mass to the correct demand units
at the final boundary. The floor can change flow convergence and adaptive
sampling, which is part of the explicitly selected model behavior.

After the inner search, calculate the actual outer transition from the fixed
pre-step state, apply the floor again to that allocation, then write the final
state and requested transition events. The inner allocation is hypothetical;
its redirected volumes are not applied a second time to people. The final
fresh-MNL output remains the current unpruned diagnostic, rather than silently
applying this behavioral allocation floor to it.

Report per scenario, replication, step and inner batch: configured floor,
number of physical plans below it, redirected represented persons and their
share of the scenario population. For the final transition, separately report
the actual redirected total. Verify mode totals, OD flows, total trip loss and
results for small groups as well as aggregate totals. The sum of many
individually small plan volumes can be significant.

Transition events must describe the **actual post-floor target** and moved
people, not falsely claim that the original mobile target was retained. When
redirected events are requested, identify them as floor reassignment and retain
the original chosen target separately; keep pre-floor choice probability and
distance fields identified as such rather than mislabeling them as the
probability or distance to stay-home. Populate stay-home target utility and
keys consistently. If the current event schema cannot represent this without
ambiguity, extend and version it and its cached outputs.

### Comparison and acceptance

Compare three policies on the same seed, scenarios and effort: current
retained-share pruning, no low-mass pruning, and the explicitly selected global
floor. Include later outer steps: inner allocation time, number of retained
allocations and candidate plans, per-step and peak memory, redirected mass,
OD/mode flows, convergence and final outputs. This is a behavioral comparison,
not a demand for equality between policies. Check whether the proposed 1-person floor gives acceptable runtime,
distributional and trip impacts before changing the Geneva script or making
the global policy the default. The compact representation
must also be tested with the legacy policy to separate representation speedup
from a model-policy change.

## Allocation, flows and outputs

For inner OD/mode flows, aggregate allocation weights by physical plan and
join once to its narrow physical `(from, to, mode)` steps. A plan that repeats
an OD/mode contributes once per actual step; do not collapse repeated trips.
Stay-home contributes no trips. Preserve deterministic summation and current
population weighting.

Expand to demand units where per-unit pruning, transitions, events or final
state require them, and expand detailed steps only for allocated plans and
requested outputs. Preserve final behavioral allocation, optional MNL flows,
transition events, raw tables, output filtering and demand-driven result
lifetimes. Inner estimates are not final behavioral allocations.

## Lifetime, checkpoints and reproducibility

The runner/coordinator owns the shared pool, indexes, modeled timings,
memberships and scores for an outer step. Carry them across its inner batches
and release obsolete intermediates after their last consumer. Do not retain a
wide timed context-step table alongside compact physical and score tables.
Measure peak RSS, temporary joins and shared buffers; stored table estimates
alone do not prove a lower peak. Do not introduce an automatic memory limit.

Keep an explicit mapping among physical IDs, modeled timing IDs, reference
variants, contexts and persistent demand-unit `plan_id`s. Complete evaluations
may be shared across scenarios only when all relevant inputs and references
match; matching physical content alone is insufficient. Preserve zero-ID and
stay-home behavior.

Current checkpoints save each run's allocations, candidate history, plan and
sequence indexes, modeled schedules and RNG state. If saved state references
new shared IDs, save their index and namespace as part of the same coherent
cohort checkpoint and restore them before use. Preserve deterministic ID
assignment, RNG state and resumed results. Update relevant asset/checkpoint
versions when their content or semantics change. If the first implementation
converts to the existing per-run state at the outer boundary, keep the existing
checkpoint format until an actual migration requires changing it. Do not add a
process-wide mutable index or couple independent replications.

## Implementation sequence

1. **Make the compact path explicit.** Adapt the existing catalogue, factored
   steps and scoring contexts so eligibility does not immediately expand
   physical steps by context. Preserve occupied plans, variants and history;
   remove redundant conversions rather than adding another permanent copy.
2. **Expose plan-level pool changes.** Carry the existing shared identity
   through a batch and report new memberships, history refreshes and optional
   removals from the pool. Do not add a generic step-by-step diff or replay
   mechanism for normal inner iterations.
3. **Evaluate compact additions.** Store modeled timings by physical/time
   inputs and scores by run/context/plan. Evaluate additions once, preserving
   the existing modeled-schedule collision and history rules.
4. **Calculate choices and flows compactly.** Preserve transition and legacy
   pruning semantics when that policy is selected, then aggregate plan weights
   before joining narrow physical steps. Retain paths needed by distance, MNL
   and events. Implement the opt-in global volume floor and its stay-home
   remapping as a separately selectable policy, applied to both hypothetical
   and final allocations as specified above; do not silently change existing
   configurations.
5. **Connect the outer boundary.** Produce existing behavioral state and
   requested outputs without rebuilding all unallocated context-step rows.
   Remove temporary compatibility bridges when their consumers are migrated.
6. **Evaluate exact shared indexes separately.** Replace content fingerprints
   only when exact component indexes, namespace and checkpoint handling are
   implemented and their memory/time tradeoff is measured. The compact-path
   experiment need not wait for that migration.

These are milestones, not a mandate to maintain two complete simulation
engines. Temporary comparison tools should stay outside the ordinary CI suite
and be removed when they no longer answer a question.

## Validation and performance decision

Run focused unit tests first. Cover sequence identity and ordering, reference
variants converging to one modeled plan, history merge, occupied and new
plans, optional pool eviction, missing/invalid costs, eligibility, repeated
OD/mode steps, stay-home, differing travel times and scenario/replication
namespaces. For the optional global floor, also cover: zero/disabled floor;
volume below, exactly at and above the floor; multiple units and reference
variants sharing one physical plan; separate scenarios and replications;
multiple dropped plans moving to one stay-home target; exact per-unit and
scenario population conservation; no candidate-pool eviction; no-transition
steps; forced-reassignment origins; and transition events that distinguish
original choices from floor redirects. Verify the fresh-MNL diagnostic remains
unpruned and invalid simultaneous pruning configurations are rejected.

Compare old and compact calculations on the same fixed inputs: modeled
timings and mappings, IDs, candidate history, utilities, feasibility,
origin-target probabilities, the selected pruning policy, actual allocations,
OD/mode flows and scheduler/convergence decisions. Compare the legacy policy
for equivalence; compare the global floor against an independently calculated
floor/remapping reference, not against legacy results. Exercise all behavior scopes,
infeasible origins, stay-home fallback, optional plan distance, MNL output and
transition events. Preserve deterministic ordering and numerical behavior;
differences that change ranking or pruning cannot be dismissed as harmless
noise. Compare RNG state and checkpoint/resume with an uninterrupted run,
including multiple scenarios and more than one replication.

Use a small representative automated integration test; high-effort simulations
and artificial replay checks do not belong in CI. First benchmark complete
compact evaluation, allocation and flows on captured actual consecutive
batches. Then compare an unmodified baseline and the new path using the same
current Geneva setup: one scenario, five outer steps, 6 activity draws,
2 destination draws, 6 mode draws and up to 3 inner batches. The older
high-effort reference is directly comparable only for steps 1–4 because the
no-transition fifth step has since changed. Record per-step and per-batch
times, candidate/context-plan counts, Python and process-tree peak RSS, CPU,
output equivalence, RNG/checkpoint results and machine/load conditions.
Exclude capture/profiling overhead from performance comparisons.

The objective is a substantial improvement in evaluation and a meaningful
improvement in inner-loop/step time without a material memory regression, not
just a faster isolated hash or utility expression. Use repeated measurements
when practical and report variance; the current traces do not justify a
promised percentage. If the isolated compact calculation improves but the full
run does not, profile attachment, choice, flows and table lifetimes before
adding complexity. After the high-effort check, measure longer runs and
scenario scaling; one five-step run does not establish that 20 steps or ten
scenarios fit.
