# Scenarios

Scenarios are how Mobility compares different assumptions for the same territory.

Most studies start with a reference situation: the current mobility system, or a baseline agreed by the study team for a future year. Then you add one or more project scenarios: a new public transport line, a change in car cost, a speed change, a new land-use assumption, or another assumption you want to test.

In Mobility, a scenario can change parameters by scenario name and by iteration. This lets you run a reference state first, then introduce a change and observe how simulated plans react.

## Declare Scenarios

```python
scenarios = mobility.Scenarios(
    [
        mobility.Scenario(
            name="default",
            title="Reference",
            description="Current state of the mobility system.",
        ),
        mobility.Scenario(
            name="car_distance_cost",
            title="Car distance cost",
            description="Adds a distance cost to car trips.",
            reference="default",
        ),
    ]
)
```

Scenario names appear in output folders and result tables, so short names are easier to use. The title and description can be longer and more readable.

`reference` is optional. When you use it, it should point to another declared scenario, usually `default`. It is metadata for reports and scenario manifests. In metric calls, you still choose the comparison explicitly with `reference=("scenario", "default")`.

## Change A Parameter By Scenario

Use `ParameterValue.by_scenario_and_iteration` when a value changes by scenario:

```python
car = mobility.CarMode(
    transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_constant=3.0,
        cost_of_distance=mobility.ParameterValue.by_scenario_and_iteration(
            default=0.0,
            car_distance_cost={
                1: 0.0,
                5: 0.20,
            },
        ),
        cost_of_time=mobility.CostOfTimeParameters(intercept=5.0),
    ),
)
```

In this example:

- the reference value is `0.0`,
- the `car_distance_cost` scenario also starts at `0.0`,
- from iteration 5, the `car_distance_cost` scenario uses `0.20`.

This pattern is useful when a scenario assumption should appear after a few warm-up iterations. Remember that the model still replans during those warm-up iterations unless the run parameters restrict behaviour-change phases.

## Complete Small Scenario Example

This example declares a reference and a car-cost scenario, changes car distance cost from iteration 5, runs both scenarios, then compares final distance by mode.

```python
scenarios = mobility.Scenarios(
    [
        mobility.Scenario(
            name="default",
            title="Reference",
            description="Current modelled mobility system.",
        ),
        mobility.Scenario(
            name="car_distance_cost",
            title="Car distance cost",
            description="Adds 0.20 euro per kilometre to car distance cost from iteration 5.",
            reference="default",
        ),
    ]
)

car = mobility.CarMode(
    transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_of_distance=mobility.ParameterValue.by_scenario_and_iteration(
            default=0.0,
            car_distance_cost={
                1: 0.0,
                5: 0.20,
            },
        ),
    ),
)
```

Pass `scenarios=scenarios` and the scenario-aware `car` mode to `PopulationGroupDayTrips`.

## Run And Read Several Scenarios

```python
population_trips.run(day_type="weekday", scenario="default")
population_trips.run(day_type="weekday", scenario="car_distance_cost")

results = population_trips.results(
    "weekday",
    scenarios=["default", "car_distance_cost"],
)
```

Then compare indicators:

```python
results.metrics.travel_distance(
    by_variable="mode",
    iterations="last",
    reference=("scenario", "default"),
    output="plot",
)
```

With a scenario reference, the result is a gap by mode. A positive gap means the selected scenario has a higher value than the reference for that row. A negative gap means it has a lower value.

## Practical Advice

Start with a documented reference scenario and make sure it runs correctly.

Then add one project scenario at a time when possible. A short description can work when it states the modelling assumption clearly enough for another modeller to reopen the project later.

Useful scenario descriptions answer four questions:

- what changed,
- when it changed in the model,
- which year or modelling context it represents, when relevant,
- which reference it should be compared with.
