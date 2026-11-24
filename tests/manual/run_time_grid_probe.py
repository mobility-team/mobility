"""Run the Geneva probe with a temporary, approximate grid for plan timing.

MOBILITY_TIME_GRID_MINUTES selects the grid (default 5). This changes only the
plan-timing scorer: candidate generation and routing retain their normal time
resolution, and existing schedule IDs still use their normal schema. This is a
hybrid experiment, not a complete minute-based model or production feature.
"""
import os
import runpy
import tempfile
from pathlib import Path

import polars as pl

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset


interval = int(os.environ.get("MOBILITY_TIME_GRID_MINUTES", "5"))
if interval < 0:
    raise ValueError("The time grid must be non-negative; zero selects the baseline.")
original = PlanScheduleUpdater.update_plan_timings
original_advance = MultiScenarioRunAsset._advance_cohort_step


def update_grid(updater, possible_plan_steps, *, arrival_time_rigidity_by_activity, enabled):
    if not enabled:
        return original(updater, possible_plan_steps,
                        arrival_time_rigidity_by_activity=arrival_time_rigidity_by_activity, enabled=enabled)
    schema = possible_plan_steps.collect_schema()
    rigidity = pl.DataFrame({"activity": list(arrival_time_rigidity_by_activity),
                             "_rigidity": list(arrival_time_rigidity_by_activity.values())}).with_columns(
                                 activity=pl.col("activity").cast(schema["activity"]))
    return (possible_plan_steps.join(rigidity.lazy(), on="activity", how="left")
            .with_columns(*[((pl.col(name).cast(pl.Float64) * 60 / interval).round(0) * interval)
                            .cast(pl.Int32).alias("_grid_" + name)
                            for name in ("departure_time", "arrival_time", "next_departure_time", "time")])
            .with_columns(_grid_delta=(pl.col("_grid_time")
                                       - (pl.col("_grid_arrival_time") - pl.col("_grid_departure_time"))
                                       .clip(0, 1440)))
            .with_columns(
                _grid_modeled_departure=(pl.col("_grid_departure_time")
                                         - ((pl.col("_rigidity") * pl.col("_grid_delta") / interval)
                                            .round(0) * interval)).cast(pl.Int32),
                _grid_modeled_arrival=(pl.col("_grid_arrival_time")
                                       + (((1 - pl.col("_rigidity")) * pl.col("_grid_delta") / interval)
                                          .round(0) * interval)).cast(pl.Int32),
            ).sort(PLAN_KEY_COLS + ["seq_step_index"])
            .with_columns(
                _grid_modeled_next=pl.col("_grid_modeled_departure").shift(-1).over(PLAN_KEY_COLS)
                .fill_null(pl.col("_grid_next_departure_time"))
            ).with_columns(
                departure_time=(pl.col("_grid_modeled_departure") / 60).cast(schema["departure_time"]),
                arrival_time=(pl.col("_grid_modeled_arrival") / 60).cast(schema["arrival_time"]),
                next_departure_time=(pl.col("_grid_modeled_next") / 60).cast(schema["next_departure_time"]),
                duration_per_pers=((pl.col("_grid_modeled_next") - pl.col("_grid_modeled_arrival")) / 60)
                .clip(lower_bound=updater.MIN_ACTIVITY_DURATION_HOURS).cast(schema["duration_per_pers"]),
                time=(pl.col("_grid_time") / 60).cast(schema["time"]),
            ).drop("_rigidity", "_grid_departure_time", "_grid_arrival_time", "_grid_next_departure_time",
                   "_grid_time", "_grid_delta", "_grid_modeled_departure", "_grid_modeled_arrival", "_grid_modeled_next"))


def capture_flows(coordinator, iteration, states, **kwargs):
    result = original_advance(coordinator, iteration, states, **kwargs)
    _, next_states, _ = result
    if len(next_states) != 1:
        raise ValueError("This diagnostic expects one scenario.")
    state = next(iter(next_states.values()))
    (state.current_plan_steps.filter(pl.col("mode_seq_id") != 0)
     .group_by("from", "to", "mode", "activity")
     .agg(pl.col("n_persons").cast(pl.Float64).sum().alias("persons"))
     .write_parquet(output / f"flows-{iteration}.parquet"))
    return result


if __name__ == "__main__":
    output = Path(tempfile.mkdtemp(prefix="mobility-time-grid-flows-"))
    print("TIME_GRID_FLOW_FILES", output, flush=True)
    if interval:
        PlanScheduleUpdater.update_plan_timings = update_grid
    MultiScenarioRunAsset._advance_cohort_step = capture_flows
    print("TIME_GRID_MINUTES", interval, flush=True)
    runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
