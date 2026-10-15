# Demand-Driven Result Batches

**Status:** Implemented in `perf/demand-driven-table-outputs`

## Problem

`GroupDayTripsResults` has two different ways to request output:

- Metric methods return `Metric` assets. They are queued, and the first metric `.get()` sends the queued metrics through `ResultExecutionPlan` together.
- Table methods such as `results.tables.plan_steps()` call `.get()` before returning. They resolve their run outputs immediately and do not join the metric plan.

As a result, declaring a table can start or resolve the scenario before the caller has declared the metrics they want. A later request can repeat run work when iteration checkpoints are disabled. The results guide's advice to declare all results before the first `.get()` only describes the metric path; it does not describe table methods.

The existing assets already describe most of the work. `ScopedRunTable` knows its run output assets and selected iterations. `Metric` knows its selected iterations and how to calculate one iteration. The planner only accepts metrics today.

## Goals

- Let one explicit call plan metrics and raw tables together.
- Run each needed scenario, replication, and iteration once per batch.
- Keep metric cache checks and metric persistence.
- Keep raw result tables transient and return them to the caller.
- Make declaring a request side-effect free.
- Remove the metrics-only pending queue and execution callback.

## API

`GroupDayTripsResults.get_all(**requests)` resolves named metric and table requests together. Each keyword names one output; each value is a deferred request.

```python
outputs = results.get_all(
    trip_count=results.metrics.trip_count(by_variable="mode"),
    travel_time=results.metrics.travel_time(by_variable="mode"),
    plan_steps=results.tables.plan_steps(
        columns=["scenario", "home_zone_id", "activity", "mode", "from", "to"]
    ),
)

trip_counts = outputs["trip_count"]
travel_times = outputs["travel_time"]
plan_steps = outputs["plan_steps"]
```

The call is the planning boundary. Declarations do no run work. `get_all` returns a mapping with the same names and insertion order as the arguments. Metric values keep their existing Polars result types; raw table values remain `LazyFrame`s. The caller can collect a table when it needs the rows in memory.

A request's `.get()` remains a convenience for one output and uses the same planner with only that request. It does not implicitly include other requests previously declared. Code that needs shared work uses `get_all`.

Raw table methods return deferred requests instead of resolving them. `tables.costs()` continues to represent the persisted `metrics.travel_costs()` result, but returns that deferred request without calling `.get()` during declaration. This keeps detailed costs' current cache behavior while letting them join a batch. `metrics.travel_costs()` remains the direct way to request that result.

Requests passed to one `get_all` call must belong to the same `GroupDayTripsResults` object. Identical requests are planned once and may be returned under more than one output name.

## Execution Plan

Generalize `ResultExecutionPlan` to accept result asset roots, rather than a list of metrics and one requested metric.

1. Validate request ownership and deduplicate identical roots by asset graph key.
2. Check persistent metric roots first. A valid cached metric is returned without loading iteration state.
3. For uncached metrics and requested tables, union their selected iteration numbers and run the plan in iteration order.
4. For each iteration, resolve the run output assets shared by those requests, compute metric reductions, and assemble requested raw table parts.
5. Release run tables after every planned consumer for that iteration has used them. Preserve invariant inputs such as demand groups for later iterations when needed.
6. Persist completed metric results using the current cache rules. Return raw table frames without writing a durable result cache.
7. Release resolver-owned transient values on success or error.

If every requested metric is cached and no raw table is requested, no scenario iteration is run. If a raw table is requested alongside cached metrics, the run executes only for the table, and the cached metrics are returned from their existing assets.

## Architecture

Use the existing result assets as the request objects. A request passed to `get_all` is a `Metric` or `ScopedRunTable` asset. Do not add a parallel request tree or a second copy of the run dependency graph.

Give `Metric` and `ScopedRunTable` the small common planner contract needed to describe their iteration scope, planned run outputs, and per-iteration work. Keep result-specific behavior on each asset: metrics check and write their persistent cache; scoped tables assemble transient Polars frames. The planner coordinates those assets but does not become a generic runtime asset scheduler.

This lets the refactor remove the implicit `_pending_metrics` list and `Metric.execution_plan` callback. `GroupDayTripsResults.get_all` becomes the single entry point for combined planning, while a request's `.get()` delegates a one-item batch to its owner. Metric and table declarations then have the same lifecycle, and there is no hidden queue whose contents depend on call order.

Keep the existing asset graph as the source of truth for cache identity and run dependencies. Planner-only sharing metadata must stay out of metric cache keys. Reuse `ScopedRunTable.run_output_assets` instead of rebuilding output dependencies in the table namespace.

Plotting is a presentation step after a metric has been resolved. Separating plotting from metric declaration would remove the current eager `output="plot"` path, which resolves a metric while it is being declared. That cleanup is useful, but it can be a follow-up if it would make the planner change too broad.

## Data Lifetime

Raw tables remain uncached on disk. The returned `LazyFrame` must own or retain the table data it needs after the planner releases run state. This preserves the current table result contract and avoids adding a temporary-file ownership and cleanup API. Detailed travel costs remain a persisted metric result, as they are today.

Holding several large raw tables can use substantial memory. The planner should only retain tables explicitly requested by the caller and should release unrequested intermediates as soon as possible. If real workloads show that requested outputs exceed memory, add a separate spill-to-disk result-lifetime design based on measured use; do not silently persist raw tables in the long-lived metric cache.

## Documentation and User Guidance

Replace the instruction to declare every result before the first `.get()` with:

- Declare the metric and table requests needed for one analysis.
- Pass them together to `results.get_all(...)` when they should share scenario work.
- Use a request's `.get()` when only that one output is needed.
- A request declared after a completed batch starts a new batch if fetched separately.

The results guide should show a mixed metric and table example, and explain that raw tables are transient while metric outputs are cached.

## Acceptance Checks

- A mixed metric and table batch resolves each selected scenario/replication iteration once, including with checkpoints disabled.
- A table declaration alone does not resolve run outputs.
- Multiple tables sharing run outputs use the same run work.
- A cached metric plus a table request does not recompute that metric and computes run state only for the table.
- A batch containing only cached metrics does not resolve scenario iterations.
- Different iteration selections are combined into one ordered iteration plan, with each request receiving only its selected iterations.
- Duplicate request roots execute once.
- Raw tables still return usable `LazyFrame`s after the asset resolver closes and are not persisted as result caches.
- Request failure releases transient resolver values and does not leave a partial raw table result in the returned mapping.
- Calling one request's `.get()` executes only that request, independent of other declarations.
