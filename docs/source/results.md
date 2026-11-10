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

A request containing only `results.tables.demand_groups(...)` reads the initial
demand groups without running the behavior simulation.

Metric and table methods declare deferred requests. Use `results.get_all(...)` when several outputs should share scenario work. For supported requests, it reduces metrics and writes tables as each step becomes available. Some undeclared dependencies may require separate executions. A request's `.get()` is convenient when you need only that one output; it runs only that request. Metrics are cached, while raw tables remain transient. Selected step numbers must be within the configured run.

For example, request a metric and a raw table together:

```python
import polars as pl

outputs = results.get_all(
    trip_counts=results.metrics.trip_count(by_variable="mode"),
    travel_distances=results.metrics.travel_distance(by_variable="mode"),
    plan_steps=results.tables.plan_steps(
        columns=["scenario", "home_zone_id", "activity", "mode", "from", "to"],
    ),
)

trip_count_table = outputs["trip_counts"]
travel_distance_table = outputs["travel_distances"]
plan_steps = outputs["plan_steps"]
car_steps = plan_steps.filter(pl.col("mode") == "car").collect()
plan_steps.save("plan_steps.parquet")
```

To inspect requests before executing them, pass them to `results.explain(...)`.
The returned dictionary lists each replication's full scenario cohort and seed,
the selected steps and inputs, current cache status, and planned result storage.
For each replication it also describes the main simulation work and names the
outputs requested at each step. It does not start a simulation.

```python
description = results.explain(
    trips=results.metrics.trip_count(by_variable="mode"),
    plan_steps=results.tables.plan_steps(columns=["scenario", "mode"]),
)
```

Use `.get()` for one output:

```python
plan_steps = results.tables.plan_steps().get()
table = plan_steps.collect()
```

Raw table results own temporary files in the project data folder. The files are
removed when the result and its views are no longer referenced; call `close()`
to remove them sooner. `filter`, `select` and `sort` return views that share the
same lifetime. `group_by(...).agg(...)` also returns an owned view, so an
aggregation can run without collecting the full table first:

```python
trips_by_mode = (
    plan_steps.filter(pl.col("activity_seq_id") != 0)
    .group_by("mode")
    .agg(trips=pl.col("n_persons").sum())
    .collect()
)
```

`collect()` returns an independent Polars DataFrame; `save()`
writes a Parquet file that remains after cleanup. Large tables can require
substantial temporary disk space. When requesting specific `columns`, every
name must be present in the selected table; an unknown name raises an error.

To limit Mobility's managed temporary files, set `temporary_disk_limit_gb`
when configuring Mobility, for example `mobility.set_params(temporary_disk_limit_gb=20)`.
The limit is shared by open result tables and other
managed temporary files in the project. Mobility stops a managed write and
cleans up the incomplete result when the limit would be exceeded. There is no
limit unless this setting or the `MOBILITY_TEMPORARY_DISK_LIMIT_BYTES`
environment variable is provided.

Polars may also write its own spill files, which are not covered by this limit.
The shared congestion calculation writes its temporary output from R; Mobility
checks its size after R finishes, so that write can temporarily exceed the limit.
Map-animation frames are checked after each frame is rendered and can likewise
exceed the limit briefly while a frame is being written.
Other external-tool scratch files are not fully tracked. Check available disk
space as well, especially for large tables.

Use `population_trips.run(...)` when you need to execute or inspect one concrete run. Call `run.table("plan_steps")` to get a raw output table as a Polars DataFrame; the table stays transient. `run.results()` creates analysis helpers without running the model; each analysis requests the tables it needs. Use `population_trips.results(...)` when you want to request several indicators or tables together.

Most result metrics are population-weighted. Trip counts sum the represented people attached to simulated trips. Distance, time, cost, and emissions multiply each trip quantity by the represented-person weight before aggregation. When several replications are selected, metric values are averaged across replications and standard-deviation columns describe the spread across replications.

## Common Indicators

Trip count:

```python
results.metrics.trip_count(
    by_variable="mode",
    steps="last",
    output="table",
).get()
```

Travel distance:

```python
results.metrics.travel_distance(
    by_variable="mode",
    steps="last",
    output="table",
).get()
```

Travel time:

```python
results.metrics.travel_time(
    by_variable="activity",
    steps="last",
    output="table",
).get()
```

Greenhouse gas emissions:

```python
results.metrics.ghg_emissions(
    by_variable="mode",
    steps="last",
    output="table",
).get()
```

These indicators can also be read with `output="plot"` when you want a visual check of a table. Plot requests compute and cache the same metric table before rendering it.

Metric tables usually contain:

- `scenario`,
- `day_type`,
- `step`,
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
    steps="last",
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
    steps="last",
    output="table",
).get()
```

You can also compute shares:

```python
results.metrics.trip_count(
    by_variable="mode",
    normalize_by="metric_total",
    normalize_scope="study_area",
    steps="last",
    output="table",
).get()
```

## Compare A Scenario To A Reference

```python
comparison = results.metrics.travel_distance(
    by_variable="mode",
    steps="last",
    reference=("scenario", "default"),
    output="table",
).get()
```

This compares each selected scenario to the `default` scenario. With the default `reference_view="gap"`, values are returned as scenario minus reference. Use `reference_view="values"` when you want model and reference values side by side.

Some metrics can also be compared with survey references:

```python
results.metrics.trip_count(
    by_variable="mode",
    steps="last",
    reference="external",
    output="plot",
)
```

External references are available for some dimensions and final-step results. They are mainly available for survey-comparable indicators such as trip counts, distances, times, immobility, activity-duration distributions, and activity time series. Model-only quantities such as generalized cost usually have no external reference.

## Survey-Comparable Checks

Some checks are especially useful for comparing the model with survey evidence:

```python
results.metrics.immobility(reference="external").get()
results.metrics.activity_duration_distribution(reference="external").get()
results.metrics.activity_time_series(reference="external").get()
```

These checks compare modelled behaviour with the survey reference used by the run. Use them as a first check before local validation.

## Steps And Replications

Use `steps="last"` for final-step indicators after checking the step diagnostics.

Use `steps="all"` when you want to inspect how an indicator changes during the run:

```python
results.metrics.trip_count(
    by_variable="mode",
    steps="all",
    output="plot",
)
```

If the setup has several replications, `population_trips.results(...)` selects all of them by default. You can select one replication with `replication=0`, or several with `replications=[0, 1]`.

## Detailed Travel Costs

Use `tables.costs` when you need the OD-by-mode travel-cost table itself, separate from the aggregated `metrics.cost(...)` indicator:

```python
last_iteration_costs = results.tables.costs(steps="last").get()
one_iteration_costs = results.tables.costs(steps=3).get()
selected_costs = results.tables.costs(steps=[1, 3, 5]).get()
all_iteration_costs = results.tables.costs(steps="all").get()
```

The returned table owns temporary files and can be collected or saved as described above. Pass `columns=[...]` to keep only the columns you need. `results.metrics.travel_costs(...)` is an alias for the same transient table request.

## First Results Checklist

For a new project setup, start with these checks:

1. `trip_count(by_variable="mode")`: are the main modes present, and do totals match the scale expected from the population sample?
2. `travel_distance(by_variable="mode")`: are distances in the expected order of magnitude for the territory?
3. `ghg_emissions(by_variable="mode")`: are emissions consistent with the modes and emission factors used?
4. `trip_count(by_zone="home_zone", by_variable="mode")`: do maps or zone tables reveal missing areas, boundary effects, or unexpected spatial concentration?
5. `diagnostics.step_metrics(steps="all")`: do indicators behave consistently across steps?
6. `immobility(reference="external")`: does immobility remain close to the survey reference for the relevant groups?
7. `activity_duration_distribution(reference="external")`: do activity durations keep a credible shape?

These checks are a first screen before calibration, validation against independent observations, sensitivity tests, or scenario interpretation.

## Raw Plan Steps On One Run

When you need to inspect one concrete run, keep the `Run` object returned by `population_trips.run(...)`:

```python
weekday_run = population_trips.run("weekday")
weekday_plan_steps = weekday_run.table("plan_steps")
```

This is useful for checking raw simulated plan steps. For indicators, diagnostics, scenario comparison, and project reporting, prefer `population_trips.results(...)`.

## Diagnostics

Diagnostics help check whether the model behaves as expected:

```python
diagnostics = results.diagnostics.step_metrics(
    steps="all",
).get().collect()
```

Use diagnostics when a scenario result looks surprising, when the model varies across steps, or when you need to check that a scenario gap is larger than step variation.
