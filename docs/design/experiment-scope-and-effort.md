# Experiment scope, effort and simulation steps

**Status:** Proposed. Preset values are initial choices to evaluate, not validated accuracy targets.

## Purpose

Make a normal result request do the work it describes. Let users select a familiar level of computational effort without learning every sampling parameter, while retaining full manual control. Use distinct names for progression through the model and repeated candidate search within a step.

The current setup declares all possible scenarios and sensitivity cases. The first run nevertheless creates a coordinated cohort containing all of them. Selecting two scenarios in `results(...)` filters outputs but does not restrict that cohort. This can make a small request unexpectedly expensive. Because cohort members share candidates, the cohort is also part of the model calculation, not merely a scheduling detail.

## Public workflow

The setup declares the model and available scenarios. It does not select an experiment or start a simulation. A normal result request defines an experiment from its selected scope:

```python
parameters = mobility.GroupDayTripsParameters(
    effort="medium",
    run=mobility.GroupDayTripsRunParameters(n_steps=10),
)
population_trips = mobility.PopulationGroupDayTrips(
    population=population,
    modes=modes,
    activities=activities,
    surveys=surveys,
    scenarios=scenarios,
    parameters=parameters,
)

results = population_trips.results(
    "weekday", scenarios=["default", "carbon_tax"], replication=0,
)
outputs = results.get_all(
    trips=results.metrics.trip_count(by_variable="mode"),
    travel_time=results.metrics.travel_time(by_variable="mode"),
)
```

This experiment contains the two selected scenarios, the base sensitivity case, and replication 0. Result and metric declarations do not execute the simulation. `get_all(...)` is the batch execution boundary; a request's `.get()` remains the one-output convenience. The existing default result selection remains the default scenario, base sensitivity case and all configured replications; `replication` or `replications` narrows the selected lanes. `run(...)` is shorthand for a one-member experiment with the requested scenario, sensitivity case and replication.

For inspection and reuse, provide an explicit experiment API. `results(...)` on the setup is shorthand for creating the same experiment and returning its results:

```python
experiment = population_trips.experiment(
    "weekday", scenarios=["default", "carbon_tax"], replications=[0, 1],
)
print(experiment.describe())  # Scope and resolved settings; no simulation.
results = experiment.results()
```

The experiment is immutable once created. Its result views may filter scenarios, cases, replications or output steps without changing its members. A filtered view of the two-scenario experiment uses its two-scenario search. Constructing a new one-scenario experiment creates a different search; its baseline can differ. Multiple result batches from the same experiment keep that scope even when they request different metrics. A new scope creates a separate experiment, not a mutation of an existing one. Declarations and results must be independent of the order in which users access views or request metrics.

Scenario comparisons and reference metrics must have their reference member in the experiment. If a reference is missing, raise a clear error naming it and explaining how to include it; never silently add a member or compare results from different cohorts. Sensitivity analysis explicitly constructs an experiment containing its selected scenarios and requested cases. Validate unknown scenarios, sensitivity cases and replications before execution.

## Effort levels

`GroupDayTripsParameters(effort=...)` accepts `"low"`, `"medium"` or `"high"`; the default is `"low"`. Resolve presets into ordinary model parameters before constructing run assets. The following values are provisional:

| Resolved setting | Low | Medium | High |
| --- | ---: | ---: | ---: |
| `k_activity_sequences` | 1 | 3 | 6 |
| `k_destination_sequences` | 1 | 3 | 5 |
| `k_mode_sequences` | 1 | 3 | 5 |
| `max_search_batches` | 3 | 5 | 8 |
| `n_replications` | 1 | 3 | 5 |
| Default seeds | `[0]` | `[0, 1, 2]` | `[0, 1, 2, 3, 4]` |

Activity draws are per eligible activity profile and home zone; destination draws are per sampled schedule and utility profile; mode search considers up to the configured number of combinations per destination sequence. Draws can repeat or be infeasible, so these are not counts of distinct plans. Three low-effort batches allow the existing two-consecutive-pass convergence rule an opportunity to pass, but do not guarantee convergence. A search can stop earlier when no work remains, or reach its limit without converging.

Explicit values override the preset field by field, including values in partially supplied nested parameter objects. Distinguish an omitted field from an explicitly supplied value, including `k_activity_sequences=None` (all eligible schedules). A user may explicitly set all effort-controlled parameters; the result is a fully manual configuration. Other existing parameter defaults remain unchanged. Invalid preset names and invalid resolved values produce clear validation errors.

An explicitly supplied `seeds` list determines `n_replications` when the count is omitted, including when the preset would otherwise supply a different count. If both are explicitly supplied, require their lengths to match. When a count is supplied without seeds, use deterministic seeds `0` through `n_replications - 1`; preserve an explicitly supplied single `seed` for a single replication, and reject ambiguous or conflicting single-seed and multi-replication settings. Reject invalid seeds according to the existing constraints. Resolve and display the actual seed for every lane.

Effort is a computational budget, not a guarantee of accuracy or runtime. The sampling dimensions interact, so high can cost much more than medium. Outer simulation steps, population sample size, behavioural assumptions, convergence tolerances and scenario definitions do not change with effort. Users can inspect the preset and the complete resolved parameters before execution; record those values and seeds with experiment results and logs. Cache identity uses resolved settings, not the preset label alone. Changing a preset in a later release must not cause old results to be reused for different resolved parameters.

## Simulation steps and search batches

A **simulation step** advances the model through the configured sequence of scenario values, cost updates and behaviour phases. It does not imply a fixed calendar duration. The default remains one step for a fast first result; users choose the number needed to represent their study. `n_steps` replaces `n_iterations` as the preferred public run parameter.

A **search batch** generates and evaluates candidates within a simulation step. `max_search_batches` replaces `max_inner_iterations`; the existing adaptive stopping rules and their meanings are unchanged. Report whether search stopped by convergence, by exhaustion of work, or at the limit. Do not describe reaching the limit as convergence.

Use `step` consistently for the outer index and `search_batch` for the inner index in public output columns, diagnostics, trace labels, logs, documentation and examples. Introduce step-based public names for behaviour phase starts, scenario-by-step parameter values, result selectors, checkpoint settings and any other iteration-facing API. Preserve one-based indexing and the existing meaning and ordering of every step. Inner optimization algorithms with their own iterations keep their own precise names where appropriate.

Warn once per experiment, before simulation, when a configured scenario change or behaviour phase relevant to the selected members starts after the final step. Identify the change and its start step; do not warn about unselected scenarios. A warning does not alter or extend the run. Show the selected number of steps in the execution summary.

## Compatibility and migration

Retain existing input names such as `n_iterations`, `max_inner_iterations`, `start_iteration`, `by_scenario_and_iteration`, and iteration selectors as deprecated aliases during a documented transition. Alias and new name have identical semantics; reject supplying both for the same setting rather than choosing silently. Emit actionable deprecation warnings and update examples to the new names. Existing code that explicitly sets effort-controlled fields retains those values, but code relying on previous defaults will now use low effort; document this intentional default change prominently.

Public result columns change from `iteration` to `step`, and inner-batch columns use `search_batch`. Provide a documented compatibility option for the old column names during the transition, with an actionable deprecation warning; never return two competing columns by default. Apply the same policy to persisted result schemas and trace consumers, and document how downstream code migrates. Old caches are reused only when identity and schema are demonstrably compatible; otherwise rebuild them. Do not rewrite or delete existing user data as part of migration.

## Computation, identity and lifecycle

Construct the complete selected member set before creating coordinators. A coordinator contains the selected scenarios and sensitivity cases for exactly one replication and seed. Its candidate pool, adaptive search and stopping rules are local to that lane; state and costs remain local to each member. Replications do not contribute candidates to one another. Lanes may execute sequentially initially; concurrency is a separate resource policy and must not affect results.

Canonicalize member identity independently of selection order. The lane identity includes the day type, full selected scenario/case cohort for that lane, resolved model and search inputs, seed, and relevant algorithm/cache versions. It must not change merely because another replication was selected. The experiment identity describes its selected lanes. Simulation-dependent tables and metrics inherit the lane identity; filtering outputs does not shrink the cohort identity. A single cached metric cannot remove a member from a lane that must otherwise execute. If all requested durable metrics are valid and no transient output needs execution, no simulation is required.

Checkpoint identity includes the lane cohort and resolved parameters. Checkpoint files as well as completion manifests must be isolated by cohort: a manifest alone cannot prevent two cohorts overwriting the same member's checkpoint. Resume only a complete, compatible lane checkpoint, preserving candidate history and RNG state. Keep completed-step checkpoints reusable when extending the requested run length if all other inputs and the lane cohort match. A changed cohort, seed, sampling setting or algorithm version must not resume an incompatible state. Keep temporary and spill files in the configured project data folder and clean up only files owned by the execution.

The result planner remains responsible for batching requested outputs and managing their lifetimes. Do not introduce a second competing asset graph. Shared upstream assets may be reused when their identities match, but sharing must not make experiment results depend on access order. Log or expose the selected members, seeds, resolved effort, step count, cache/checkpoint reuse and search stop reasons.

## Validation and rollout

1. Add unit coverage for preset resolution, explicit nested overrides, `None`, seed inference and conflicts, invalid values and old-name aliases. Check that preset labels with the same resolved inputs have the same computational identity.
2. Test experiment scope, stable ordering and separate immutable experiments; verify that references cannot silently expand a cohort and that result filtering preserves it. Confirm requested sensitivity cases and replications only, one coordinator per seed, and no cross-lane candidate sharing.
3. Test cache and checkpoint isolation across cohorts, seeds and resolved effort settings; verify repeated access and reordered selections are reproducible. Test complete-cohort resume, incomplete checkpoints and extension to additional steps, including RNG state. Test result-column compatibility and warnings for changes beyond the final step.
4. Run relevant unit tests before integration tests. Update the user guide, API reference and examples, including the intentional change to default effort and scenario selection. Preserve the existing Grand Genève script and project data.
5. After correctness checks, repeat real-model scaling measurements with fixed seeds and one dimension changed at a time. Start with the previously successful two-scenario configuration at five steps, then ten only if tractable. Compare two and five scenarios, then additional seeds. Record wall time, peak memory, temporary spill and candidate-pool size per step where available, identify the limiting operation, and stop to discuss before an impractical run. Evaluate whether added search and replications change results before adjusting presets.
