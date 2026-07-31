# Run Parameters

`GroupDayTripsParameters` controls how `PopulationGroupDayTrips` runs.

For a first run, keep the parameters small. The first objective is to check that the model can go from inputs to results. Once this works, you can increase the number of iterations, add replications, and describe how people are allowed to change their plans across iterations.

## Minimal First Run

```python
parameters = mobility.GroupDayTripsParameters(
    run=mobility.GroupDayTripsRunParameters(
        n_iterations=1,
    ),
    mode_sequences=mobility.GroupDayTripsModeSequenceParameters(
        mode_sequence_search_parallel=False,
    ),
)
```

This is useful for a quick technical check on a small area. Use a larger configuration before reporting project indicators.

## Project Run

A project run usually needs more iterations. Use diagnostics to decide whether the iteration count is adequate for the indicators being reported. Replications are useful when you want to estimate how much results depend on sampling and random seeds.

`n_iter_per_cost_update` controls how often travel costs are refreshed during the loop. Setting it to `0` disables congestion feedback. If congestion is part of the study question, document this value with the other supply assumptions.

```python
parameters = mobility.GroupDayTripsParameters(
    run=mobility.GroupDayTripsRunParameters(
        n_iterations=16,
        n_iter_per_cost_update=1,
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

## Behaviour Change Phases

Behaviour change phases describe which plans can change during each part of the run.

If no phases are provided, all iterations use `FULL_REPLANNING`. If phases are provided, iterations before the first `start_iteration` also use `FULL_REPLANNING`.

```python
parameters = mobility.GroupDayTripsParameters(
    run=mobility.GroupDayTripsRunParameters(
        n_iterations=16,
    ),
    behavior_change=mobility.GroupDayTripsBehaviorChangeParameters(
        phases=[
            mobility.BehaviorChangePhase(
                start_iteration=5,
                scope=mobility.BehaviorChangeScope.MODE_REPLANNING,
            ),
            mobility.BehaviorChangePhase(
                start_iteration=9,
                scope=mobility.BehaviorChangeScope.DESTINATION_REPLANNING,
            ),
            mobility.BehaviorChangePhase(
                start_iteration=13,
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

For a first model, you can leave these phases aside. They become useful when a scenario assumption is introduced after some warm-up iterations, or when you want to separate mode replanning from destination replanning in the interpretation. The order of phases is a modelling assumption: mode-only, destination, then full replanning tells a different story from full replanning followed by restricted replanning.

## Keep A Parameter Report

For each project, keep a parameter report:

```python
parameters_report = weekday_run.parameters_dataframe()
```

This helps you explain later how a result was produced, which parameters changed, and which assumptions were held constant.

## Try Complete Destination-Plan Search

The existing step-by-step destination sampler remains the default. To try the
bounded Rust search, enable it explicitly:

```python
from mobility import (
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsParameters,
)

parameters = GroupDayTripsParameters(
    destination_sequences=GroupDayTripsDestinationSequenceParameters(
        use_destination_plan_search=True,
    )
)
```

The search uses `plan_update.transition_logit_scale` when ranking complete
destination plans. The same scale is then used when choosing between plans.
The destination-sequence `alpha` parameter only applies to the legacy
step-by-step sampler.

This search chooses and ranks complete destination chains together. It returns
the best chains found by a bounded search; it does not prove that no better
chain was omitted.

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
    destination_sequences=mobility.GroupDayTripsDestinationSequenceParameters(
        use_destination_plan_search=True,
    ),
)

pupil_value_of_time = mobility.ParameterValue.by_population_segment(
    default=mobility.ParameterValue.by_iteration({1: 20.0, 5: 24.0}),
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

Scenario and iteration values are resolved inside each segment. A segment
value replaces the default value: it does not inherit the default iteration
curve. Define an iteration curve explicitly inside the segment value when the
segment should also change over time.

Mobility deduplicates identical coefficient combinations into utility
profiles. The same profile is used to rank destination plans, search mode
sequences, and compute final plan utility. Segment shares do not start separate
model runs: Mobility resolves each distinct segment-membership combination once
and searches all resulting profiles together.
