# Run Parameters

## Computational effort

`GroupDayTripsParameters(effort="low")` is the default. Choose `"medium"` or
`"high"` to increase candidate sampling and the number of independent seeded
replications. These initial settings are computational budgets, not guarantees
of accuracy or runtime. Explicit parameters override the selected effort level.

| Setting | Low | Medium | High |
| --- | ---: | ---: | ---: |
| Activity schedule draws | 1 | 3 | 6 |
| Destination draws | 1 | 3 | 5 |
| Mode combinations | 1 | 3 | 5 |
| Maximum search batches | 3 | 5 | 8 |
| Replications | 1 | 3 | 5 |

Default replication seeds are consecutive integers starting at zero. An explicit
seed list sets the replication count unless a count is also supplied, in which
case the two must agree. Effort does not change the number of simulation steps.
Simulation steps advance scenario and behaviour changes; search batches explore
candidate plans within a step. `n_steps` and `max_search_batches` are accepted
as names for the corresponding run and adaptive-search settings.

`GroupDayTripsParameters` controls how `PopulationGroupDayTrips` runs.

For a first run, keep the parameters small. The first objective is to check that the model can go from inputs to results. Once this works, you can increase the number of simulation steps, add replications, and describe how people are allowed to change their plans across steps.

## Minimal First Run

```python
parameters = mobility.GroupDayTripsParameters(
    run=mobility.GroupDayTripsRunParameters(
        n_steps=1,
    ),
    mode_sequences=mobility.GroupDayTripsModeSequenceParameters(
        mode_sequence_search_parallel=False,
    ),
)
```

This is useful for a quick technical check on a small area. Use a larger configuration before reporting project indicators.

## Project Run

A project run usually needs more steps. The model runs the configured number of simulation steps, even when behavior appears stable; the steps represent the model's progression. Use diagnostics to understand how results change. Replications are useful when you want to estimate how much results depend on sampling and random seeds.

`n_steps_per_cost_update` controls how often travel costs are refreshed during the loop. Setting it to `0` disables congestion feedback. If congestion is part of the study question, document this value with the other supply assumptions.

`plan_update.enable_opportunity_congestion` controls whether occupied
opportunities affect activity utility and destination sampling. It defaults
to `True`; setting it to `False` disables both effects while still reporting
opportunity occupation. This is independent of transport congestion.

```python
parameters = mobility.GroupDayTripsParameters(
    run=mobility.GroupDayTripsRunParameters(
        n_steps=16,
        n_steps_per_cost_update=1,
        n_replications=2,
        seeds=[0, 1],
    ),
    periods=mobility.GroupDayTripsPeriodParameters(
        simulate_weekend=False,
    ),
    mode_sequences=mobility.GroupDayTripsModeSequenceParameters(
        k_mode_sequences=6,
        mode_sequence_search_parallel=True,
        use_rust_mode_sequence_search=True,
    ),
)
```

`simulate_weekend=False` means that only weekday runs are enabled. Set it to `True` if the study needs weekend results.

During full replanning, Mobility samples three survey schedules per activity profile in each home zone by default. An activity profile is a set of allowed activities shared by demand groups. Each profile draws with replacement from its compatible schedules, using survey weights. For example, a zone with two profiles makes six draws with the default `k_activity_sequences=3`. Draws can repeat a schedule, so the number of distinct schedules can be smaller. The default `activity_support_threshold=0.02` includes an activity in a CSP's allowed set when at least 2% of its survey-weighted schedules include it. Set `k_activity_sequences=None` to use every eligible schedule.

Destination sampling makes `k_destination_sequences` draws per sampled schedule and utility profile present in the home zone (three by default). A utility profile groups demand units with identical resolved transport-cost parameters, including population-segment overrides. Each profile uses its own expected travel costs across modes to rank destinations and check the legs around each stop. Identical destination chains drawn by several profiles are stored once and shared with compatible demand units. Mode search then evaluates destination chains using each utility profile's costs.

Each activity's `activity_time_rigidity` ranges from 0 (flexible) to 1 (rigid). When a trip's modeled duration changes, the share taken from the preceding activity is `(1 + destination rigidity - origin rigidity) / 2`; the destination activity absorbs the remainder. Equally rigid activities share the change equally. Home defaults to 0; other activities default to 1 for anchors and 0 otherwise. Users can override these values in the activity parameters. This replaces the earlier `arrival_time_rigidity` parameter, whose meaning was different.

Set `destination_sequences.sampling_method` to `"utility"` to try an alternative to the default `"radiation"` sampler. It samples anchors first, then other activities using the known next anchor as the onward destination. For each stop it compares fastest and cheapest travel options on both legs, estimating activity utility with activity time rigidity, destination effects, and typical activity durations averaged across people with the same travel-cost profile. When the next stop is the anchor, it also estimates the effect on the anchor's activity utility. Full evaluation still calculates individual context utilities, feasible mode chains and complete plan timing; the guide may combine modes that cannot be used together. A preliminary cost-and-capacity cutoff limits the destinations considered, so this method is a sampling guide, not an exact choice model. The utility method uses `dest_prob_cutoff` for this shortlist; `alpha`, `cost_uncertainty_sd`, and activity `radiation_lambda` apply only to radiation sampling.

When a non-anchor activity is immediately followed by its known anchor, the utility sampler skips a destination if even the fastest available travel on both legs would make the activity finish before it starts. The check accounts for activity time rigidity and allows for time rounding. If no destination in the shortlist passes, that destination-sequence draw is dropped; other draws and existing plans remain available.

The guide also estimates how an earlier incoming departure changes the preceding activity's utility, or home-night utility for the first trip. Where the preceding trip's locations are known, it estimates arrival using its fastest and cheapest travel options. During anchor-first sampling, an unknown intervening destination is approximated by the last known location. Full plan evaluation later determines the actual mode chain and arrival times.

During candidate evaluation, identical plan steps with the same country, CSP, and utility profile share cost and activity-value calculations. Fresh inner-loop allocation shares are calculated once for each identical set of feasible plans and utilities, then multiplied by each demand unit's population. Units with different alternatives or utilities keep separate shares. Behavioral transitions still use each unit's current plan. Scored step queries stay lazy until allocation needs their step details.

By default, from step 2, a mobile plan allocated to fewer than one represented
person in a demand subgroup is removed from that subgroup's current allocation.
Its people are allocated to the subgroup's stay-home plan. This limits small
allocations but changes model results; the threshold is not a probability.
Set `plan_update.min_plan_persons` to another number of people, or to zero to
disable it. `plan_update.plan_pruning_start_step` changes the first step where
it applies. A no-transition step does not apply the floor.

## Coordinated Search Limits

Each simulation step can run up to `adaptive_search.max_search_batches`
candidate-generation and allocation batches (five by default). Each batch
requests at most one utility profile per home zone in each run. Diagnostics
estimate the requested mode-sequence alternatives from the activity,
destination and mode draw counts, including duplicate or infeasible draws.

In full replanning, the first batch requests a utility profile in each home
zone, which gives each allowed-activity profile there a generation attempt.
Later batches prioritize activity profiles without a feasible non-stay-home
plan, then missing activity-profile and utility-profile combinations, recent
allocation changes, differences from the base scenario, and exploration. In
mode-only or destination-only replanning, coverage targets are limited to
activity profiles with active plans. The search stops at the search-batch
limit, when it converges, or when there is no generation work left. The
`adaptive_search_diagnostics` table reports requested effort, coverage counts,
and the stop reason for each batch.

Request `results.tables.adaptive_search_diagnostics()` to also calculate
`flow_change_index` for successive search batches and completed steps. It is
the sum of absolute changes in represented trips by origin, destination, mode
and activity, divided by the sum of trips in the two flow tables. It is zero
when flows match, one when they do not overlap, and null when no previous
batch or step is available. Multiply by 100 for a percentage. Batch values
compare estimates from the same pre-step state; outer values compare completed
behavioral allocations. This diagnostic does not change the search stopping
rule, which uses the separate `flow_change` measure. Calculating the index
requires extra flow aggregation; request only the diagnostic columns needed
to avoid this work when the index is not needed.
`flow_change_by_mode` and `flow_change_by_activity` contain the current trip
totals and the same index within each category, including on the first batch
or step when the change is not yet available.

To trace simulation time and RAM, set `MOBILITY_SIMULATION_TRACE` to a JSONL output path before running Python. For example, in PowerShell: `$env:MOBILITY_SIMULATION_TRACE='D:/data/mobility/simulation-trace.jsonl'`. Tracing is disabled when this variable is unset. Each coordinated run appends records with a unique run id. Records identify the nested operation, scenario, simulation step, search batch, wall time, starting/ending RSS, and sampled peak RSS. The `operation` field names the measured operation; `step` and `search_batch` identify the simulation step and search batch. An `asset_step` can identify an asset associated with a different step. RAM is sampled every 100 ms for the Python process, including its native threads; child processes are excluded. Short spikes may be missed. Parent times include child steps, so do not add all recorded durations together. RAM changes measure retained process memory, not bytes allocated by that step alone. Cached result reads do not create another run trace.

## Behaviour Change Phases

Behaviour change phases describe which plans can change during each part of the run.

If no phases are provided, all steps use `FULL_REPLANNING`. If phases are provided, steps before the first `start_step` also use `FULL_REPLANNING`.

```python
parameters = mobility.GroupDayTripsParameters(
    run=mobility.GroupDayTripsRunParameters(
        n_steps=16,
    ),
    behavior_change=mobility.GroupDayTripsBehaviorChangeParameters(
        phases=[
            mobility.BehaviorChangePhase(
                start_step=5,
                scope=mobility.BehaviorChangeScope.MODE_REPLANNING,
            ),
            mobility.BehaviorChangePhase(
                start_step=9,
                scope=mobility.BehaviorChangeScope.DESTINATION_REPLANNING,
            ),
            mobility.BehaviorChangePhase(
                start_step=13,
                scope=mobility.BehaviorChangeScope.FULL_REPLANNING,
            ),
        ],
    ),
)
```

The current scopes are:

- `MODE_REPLANNING`: keep activities and destinations fixed, but allow mode changes,
- `DESTINATION_REPLANNING`: keep activities fixed, but allow destination and mode changes,
- `FULL_REPLANNING`: allow activity, destination, and mode replanning.

For a first model, you can leave these phases aside. They become useful when a scenario assumption is introduced after some warm-up steps, or when you want to separate mode replanning from destination replanning in the interpretation. The order of phases is a modelling assumption: mode-only, destination, then full replanning tells a different story from full replanning followed by restricted replanning.

## Keep A Parameter Report

For each project, keep a parameter report:

```python
parameters_report = weekday_run.parameters_dataframe()
```

This helps you explain later how a result was produced, which parameters changed, and which assumptions were held constant.

## Vary Mode Costs Between Population Segments

Define population segments once on the population, then refer to
their names from generalized-cost parameters. A segment can select on
`country`, `csp`, `home_zone_id`, `city_category`, and `n_cars`.

```python
population = mobility.Population(
    transport_zones,
    sample_size=1_000,
    population_segments=[
        mobility.PopulationSegment(
            name="pupils",
            csp="8a",
        ),
        mobility.PopulationSegment(
            name="localist_pupils",
            csp="8a",
            share=0.30,
        ),
        mobility.PopulationSegment(
            name="zone_30_pupils",
            csp="8a",
            home_zone_id=30,
        ),
    ],
)

parameters = mobility.GroupDayTripsParameters(
    demand_groups=mobility.GroupDayTripsDemandGroupParameters(
        max_persons_per_demand_subgroup=50,
    ),
)

pupil_value_of_time = mobility.ParameterValue.by_population_segment(
    default=mobility.ParameterValue.by_step({1: 20.0, 5: 24.0}),
    segment_values={
        "localist_pupils": mobility.ParameterValue.by_scenario(
            default=10.0,
            school_policy=8.0,
        ),
        "zone_30_pupils": 6.0,
    },
)

car = mobility.CarMode(
    transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_constant=0.0,
        cost_of_time=mobility.CostOfTimeParameters(
            intercept=pupil_value_of_time,
            max_value=pupil_value_of_time,
        ),
        cost_of_distance=0.1,
    ),
)
```

The most specific matching segment value is used. In this example,
`zone_30_pupils` takes precedence over `localist_pupils` for pupils living in
zone 30. Mobility raises an error when two matching values are equally
specific and neither selector contains the other.

`share=0.30` creates a 30% subgroup and a 70% complement in every matching
demand group. The share split happens before
`max_persons_per_demand_subgroup`, so both parts can then be split further to
respect the size limit. Population weights are preserved.

Scenario and step values are resolved inside each segment. A segment
value replaces the default value: it does not inherit the default step
curve. Define a step curve explicitly inside the segment value when the
segment should also change over time.

Mobility deduplicates identical coefficient combinations into utility
profiles. The same profile is used to search mode sequences and compute final
plan utility. Destination sampling also considers these profiles. Plans found
for one profile are shared with other compatible populations and scenarios.
Segment shares do not start separate model runs.

During the coordinated inner search, populations with the same home zone,
country, socio-professional category, allowed activities and utility profile
share scoring work. Their population weights are added before estimating plan
shares and OD flows. A mapping retains the original demand-group identities
for the outer behavioral update.

The inner MNL allocation measures the available candidate pool. It does not
replace the population's current behavior. Each outer update starts from the
previous outer allocation and applies the configured transition rules. Main
trip outputs describe the behavioral allocation after the last outer update.
The coordinated run then updates transport costs for that allocation without
another candidate search. To inspect the MNL allocation from the last regular
inner search, request `results.tables.mnl_od_mode_flows()` in
`results.get_all(...)`. That diagnostic uses costs fixed at the start of the
last simulation step, before the behavioral and final transport-cost updates;
it is not an estimate at the final costs. It is available for a coordinated
run's last executed step.

Polars 2 changes the hashes used for seeded sampling and plan identities.
Results from the same seed can therefore differ from runs made with Polars 1.
Mobility uses new cache and checkpoint identities so older simulation states
are not resumed with the new hashes.
