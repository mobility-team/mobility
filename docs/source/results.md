# Results And Indicators

For project work, use `population_trips.results(...)` as the main results entry point.

It can gather one or several scenarios, and one or several replications, for the same day type:

```python
results = population_trips.results(
    "weekday",
    scenarios=["default", "project"],
)
```

The returned object gives access to:

- `results.metrics`,
- `results.diagnostics`,
- `results.tables`.

Use `population_trips.run(...)` when you need to execute or inspect one concrete run. Use `population_trips.results(...)` when you want indicators.

Most result metrics are population-weighted. Trip counts sum the represented people attached to simulated trips. Distance, time, cost, and emissions multiply each trip quantity by the represented-person weight before aggregation. When several replications are selected, metric values are averaged across replications and standard-deviation columns describe the spread across replications.

## Common Indicators

Trip count:

```python
results.metrics.trip_count(
    by_variable="mode",
    iterations="last",
    output="table",
)
```

Travel distance:

```python
results.metrics.travel_distance(
    by_variable="mode",
    iterations="last",
    output="table",
)
```

Travel time:

```python
results.metrics.travel_time(
    by_variable="activity",
    iterations="last",
    output="table",
)
```

Greenhouse gas emissions:

```python
results.metrics.ghg_emissions(
    by_variable="mode",
    iterations="last",
    output="table",
)
```

These indicators can also be read with `output="plot"` when you want a visual check of a table.

Metric tables usually contain:

- `scenario`,
- `day_type`,
- `iteration`,
- optional grouping columns such as `mode`, `activity`, or a zone id,
- one metric column, such as `trip_count`, `travel_distance`, `travel_time`, `cost`, or `ghg_emissions`.

If several replications are selected, the result can also contain uncertainty columns such as standard deviations or replication counts depending on the metric.

Typical units are:

- `trip_count`: represented trips for the selected day type,
- `travel_distance`: passenger-kilometres,
- `travel_time`: passenger-hours,
- `cost`: generalized-cost units defined by the mode parameters,
- `ghg_emissions`: kgCO2e when the mode emission factors are in kgCO2e per passenger-kilometre.

## Group Results

The main grouping arguments are:

- `by_variable`, for dimensions such as `"mode"`, `"activity"`, `"distance_bin"`, `"time_bin"`, or `"csp"`,
- `by_zone`, for `"home_zone"`, `"origin_zone"`, or `"destination_zone"`.

If you need to decode survey categories such as `csp`, see the [definitions page](definitions.md).

Example:

```python
results.metrics.trip_count(
    by_zone="home_zone",
    by_variable="mode",
    iterations="last",
    output="plot",
    inner_zone_residents_only=True,
)
```

`inner_zone_residents_only=True` is useful when the model has a large routing area but the study focuses on residents inside the main perimeter.

## Normalize Results

You can normalize by population:

```python
results.metrics.travel_distance(
    by_variable="mode",
    normalize_by="person_count",
    normalize_scope="study_area",
    iterations="last",
    output="table",
)
```

You can also compute shares:

```python
results.metrics.trip_count(
    by_variable="mode",
    normalize_by="metric_total",
    normalize_scope="study_area",
    iterations="last",
    output="table",
)
```

## Compare A Scenario To A Reference

```python
comparison = results.metrics.travel_distance(
    by_variable="mode",
    iterations="last",
    reference=("scenario", "default"),
    output="table",
)
```

This compares each selected scenario to the `default` scenario. With the default `reference_view="gap"`, values are returned as scenario minus reference. Use `reference_view="values"` when you want model and reference values side by side.

Some metrics can also be compared with survey references:

```python
results.metrics.trip_count(
    by_variable="mode",
    iterations="last",
    reference="external",
    output="plot",
)
```

External references are available for some dimensions and final-iteration results. They are mainly available for survey-comparable indicators such as trip counts, distances, times, immobility, activity-duration distributions, and activity time series. Model-only quantities such as generalized cost usually have no external reference.

## Survey-Comparable Checks

Some checks are especially useful for comparing the model with survey evidence:

```python
results.metrics.immobility(reference="external")
results.metrics.activity_duration_distribution(reference="external")
results.metrics.activity_time_series(reference="external")
```

These checks compare modelled behaviour with the survey reference used by the run. Use them as a first check before local validation.

## Iterations And Replications

Use `iterations="last"` for final-iteration indicators after checking the iteration diagnostics.

Use `iterations="all"` when you want to inspect how an indicator changes during the run:

```python
results.metrics.trip_count(
    by_variable="mode",
    iterations="all",
    output="plot",
)
```

If the setup has several replications, `population_trips.results(...)` selects all of them by default. You can select one replication with `replication=0`, or several with `replications=[0, 1]`.

## First Results Checklist

For a new project setup, start with these checks:

1. `trip_count(by_variable="mode")`: are the main modes present, and do totals match the scale expected from the population sample?
2. `travel_distance(by_variable="mode")`: are distances in the expected order of magnitude for the territory?
3. `ghg_emissions(by_variable="mode")`: are emissions consistent with the modes and emission factors used?
4. `trip_count(by_zone="home_zone", by_variable="mode")`: do maps or zone tables reveal missing areas, boundary effects, or unexpected spatial concentration?
5. `diagnostics.iteration_metrics(iterations="all")`: do indicators behave consistently across iterations?
6. `immobility(reference="external")`: does immobility remain close to the survey reference for the relevant groups?
7. `activity_duration_distribution(reference="external")`: do activity durations keep a credible shape?

These checks are a first screen before calibration, validation against independent observations, sensitivity tests, or scenario interpretation.

## Raw Plan Steps On One Run

When you need to inspect one concrete run, keep the `Run` object returned by `population_trips.run(...)`:

```python
weekday_run = population_trips.run("weekday")
weekday_plan_steps = weekday_run.get()["plan_steps"].collect()
```

This is useful for checking raw simulated plan steps. For indicators, diagnostics, scenario comparison, and project reporting, prefer `population_trips.results(...)`.

## Diagnostics

Diagnostics help check whether the model behaves as expected:

```python
diagnostics = results.diagnostics.iteration_metrics(
    iterations="all",
).collect()
```

Use diagnostics when a scenario result looks surprising, when the model varies across iterations, or when you need to check that a scenario gap is larger than iteration variation.
