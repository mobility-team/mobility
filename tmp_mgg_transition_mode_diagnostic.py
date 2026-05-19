import sys

import polars as pl

sys.path.insert(0, r"D:\dev\mobility-grand-geneve\mobility-grand-geneve")

from gtfs import create_RER_gtfs_zip
from helpers import (
    get_group_day_trips,
    get_population,
    get_surveys,
    get_transport_zones,
    setup_mobility,
)
from mobility.trips.group_day_trips import BehaviorChangePhase, BehaviorChangeScope


def as_lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return frame if isinstance(frame, pl.LazyFrame) else frame.lazy()


def plan_mode_expr(column: str) -> pl.Expr:
    text = pl.col(column)
    has_walk = text.str.contains(r"mode: walk(\||<|$)")
    has_bicycle = text.str.contains(r"mode: bicycle(\||<|$)")
    has_car = text.str.contains(r"mode: car(\||<|$)")
    has_pt = text.str.contains(r"mode: walk/public_transport/walk(\||<|$)")
    n_modes = (
        has_walk.cast(pl.UInt8)
        + has_bicycle.cast(pl.UInt8)
        + has_car.cast(pl.UInt8)
        + has_pt.cast(pl.UInt8)
    )

    return (
        pl.when(text.is_null() | (text == "none"))
        .then(pl.lit("stay_home"))
        .when(n_modes > 1)
        .then(pl.lit("mixed"))
        .when(has_pt)
        .then(pl.lit("walk/public_transport/walk"))
        .when(has_car)
        .then(pl.lit("car"))
        .when(has_bicycle)
        .then(pl.lit("bicycle"))
        .when(has_walk)
        .then(pl.lit("walk"))
        .otherwise(pl.lit("unknown"))
    )


def explode_steps(events: pl.LazyFrame, *, side: str) -> pl.LazyFrame:
    step_col = f"steps_{side}"
    return (
        events
        .select(
            [
                "event_id",
                "iteration",
                "n_persons_moved",
                pl.col(step_col).str.split("<br>").alias("step_text"),
            ]
        )
        .explode("step_text")
        .filter(pl.col("step_text").is_not_null() & (pl.col("step_text") != "none"))
        .with_columns(
            seq_step_index=pl.col("step_text").str.extract(r"#(\d+)").cast(pl.UInt8),
            **{
                f"{side}_mode": (
                    pl.col("step_text")
                    .str.extract(r"mode: ([^|<]+)")
                    .str.strip_chars()
                ),
                f"{side}_distance": (
                    pl.col("step_text")
                    .str.extract(r"dist_km: ([^|<]+)")
                    .str.strip_chars()
                    .cast(pl.Float64)
                ),
                f"{side}_time": (
                    pl.col("step_text")
                    .str.extract(r"time_h: ([^|<]+)")
                    .str.strip_chars()
                    .cast(pl.Float64)
                ),
            },
        )
        .drop("step_text")
    )


setup_mobility("WARNING")
transport_zones = get_transport_zones()
surveys = get_surveys()
population = get_population(transport_zones, surveys["ch"])

population_trips = get_group_day_trips(
    transport_zones,
    surveys,
    population,
    n_iterations=10,
    additional_gtfs_files=[create_RER_gtfs_zip()],
    speed_modifiers={},
    behavior_change_phases=[
        BehaviorChangePhase(
            start_iteration=5,
            scope=BehaviorChangeScope.MODE_REPLANNING,
        )
    ],
)

events = (
    as_lazy(population_trips.weekday_run.results().transition_events)
    .filter(~pl.col("is_self_transition"))
    .with_row_index("event_id")
)

plan_switches = (
    events
    .with_columns(
        from_plan_mode=plan_mode_expr("steps_from"),
        to_plan_mode=plan_mode_expr("steps_to"),
    )
    .group_by(["from_plan_mode", "to_plan_mode"])
    .agg(pl.col("n_persons_moved").sum())
    .sort("n_persons_moved", descending=True)
)

from_steps = explode_steps(events, side="from")
to_steps = explode_steps(events, side="to")

leg_switches = (
    from_steps
    .join(to_steps, on=["event_id", "iteration", "n_persons_moved", "seq_step_index"])
    .filter(pl.col("from_mode") != pl.col("to_mode"))
)

leg_switch_summary = (
    leg_switches
    .group_by(["from_mode", "to_mode"])
    .agg(pl.col("n_persons_moved").sum())
    .sort("n_persons_moved", descending=True)
)

car_to_walk_by_distance = (
    leg_switches
    .filter((pl.col("from_mode") == "car") & (pl.col("to_mode") == "walk"))
    .with_columns(
        distance_bin=pl.col("from_distance").cut(
            [0.0, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 1000.0],
            left_closed=True,
        )
    )
    .group_by("distance_bin")
    .agg(
        pl.col("n_persons_moved").sum(),
        pl.col("from_distance").mean().alias("mean_from_distance"),
        pl.col("to_distance").mean().alias("mean_to_distance"),
        pl.col("from_time").mean().alias("mean_from_time"),
        pl.col("to_time").mean().alias("mean_to_time"),
    )
    .sort("n_persons_moved", descending=True)
)

print("PLAN SWITCHES")
print(plan_switches.collect().head(30))

print("\nLEG SWITCHES")
print(leg_switch_summary.collect().head(30))

print("\nCAR TO WALK BY DISTANCE")
print(car_to_walk_by_distance.collect())
