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
