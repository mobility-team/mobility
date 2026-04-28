import pathlib

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset

from .transition_schema import TRANSITION_EVENT_COLUMNS


def build_transition_events_lazy(transitions: pl.LazyFrame, *, iteration: int) -> pl.LazyFrame:
    """Build the core transition-event table from raw plan-to-plan transitions."""
    return transitions.with_columns(
        iteration=pl.lit(iteration).cast(pl.UInt32),
        utility_from=pl.col("utility_from_updated"),
        utility_to=pl.col("utility_trans"),
        utility_prev_from=pl.col("utility_prev_from"),
        utility_prev_to=pl.col("utility_prev_to"),
        tau_transition=pl.col("tau_transition"),
        q_transition=pl.col("q_transition"),
        adjustment_factor=pl.col("adjustment_factor"),
        is_self_transition=pl.col("plan_id") == pl.col("plan_id_trans"),
    ).select(
        [
            "iteration",
            "demand_group_id",
            "activity_seq_id",
            "time_seq_id",
            "dest_seq_id",
            "mode_seq_id",
            "activity_seq_id_trans",
            "time_seq_id_trans",
            "dest_seq_id_trans",
            "mode_seq_id_trans",
            "n_persons_moved",
            "utility_prev_from",
            "utility_prev_to",
            "utility_from",
            "utility_to",
            "tau_transition",
            "q_transition",
            "adjustment_factor",
            "is_self_transition",
        ]
    )


def add_transition_plan_details(
    transition_events: pl.LazyFrame,
    possible_plan_steps: pl.LazyFrame,
) -> pl.LazyFrame:
    """Attach full from/to plan details to transition events."""
    plan_keys = ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]

    plan_details = (
        possible_plan_steps.with_columns(
            step_desc=pl.format(
                "#{} | to: {} | activity: {} | mode: {} | dist_km: {} | time_h: {}",
                pl.col("seq_step_index"),
                pl.col("to").cast(pl.String),
                pl.col("activity").cast(pl.String),
                pl.col("mode").cast(pl.String),
                pl.col("distance").fill_null(0.0).round(3),
                pl.col("time").fill_null(0.0).round(3),
            )
        )
        .group_by(plan_keys)
        .agg(
            trip_count=pl.len().cast(pl.Float64),
            activity_time=pl.col("duration_per_pers").fill_null(0.0).sum(),
            travel_time=pl.col("time").fill_null(0.0).sum(),
            distance=pl.col("distance").fill_null(0.0).sum(),
            steps=pl.col("step_desc").sort_by("seq_step_index").str.join("<br>"),
        )
    )

    from_details = plan_details.rename(
        {
            "trip_count": "trip_count_from",
            "activity_time": "activity_time_from",
            "travel_time": "travel_time_from",
            "distance": "distance_from",
            "steps": "steps_from",
        }
    )
    to_details = plan_details.rename(
        {
            "activity_seq_id": "activity_seq_id_trans",
            "time_seq_id": "time_seq_id_trans",
            "dest_seq_id": "dest_seq_id_trans",
            "mode_seq_id": "mode_seq_id_trans",
            "trip_count": "trip_count_to",
            "activity_time": "activity_time_to",
            "travel_time": "travel_time_to",
            "distance": "distance_to",
            "steps": "steps_to",
        }
    )

    events_with_details = transition_events.join(from_details, on=plan_keys, how="left").join(
        to_details,
        on=["demand_group_id", "activity_seq_id_trans", "time_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"],
        how="left",
    )

    return events_with_details.with_columns(
        trip_count_from=pl.when(pl.col("mode_seq_id") == 0)
        .then(0.0)
        .otherwise(pl.col("trip_count_from"))
        .fill_null(0.0),
        activity_time_from=pl.when(pl.col("mode_seq_id") == 0)
        .then(24.0)
        .otherwise(pl.col("activity_time_from"))
        .fill_null(0.0),
        travel_time_from=pl.when(pl.col("mode_seq_id") == 0)
        .then(0.0)
        .otherwise(pl.col("travel_time_from"))
        .fill_null(0.0),
        distance_from=pl.when(pl.col("mode_seq_id") == 0)
        .then(0.0)
        .otherwise(pl.col("distance_from"))
        .fill_null(0.0),
        steps_from=pl.when(pl.col("mode_seq_id") == 0)
        .then(pl.lit("none"))
        .otherwise(pl.col("steps_from")),
        trip_count_to=pl.when(pl.col("mode_seq_id_trans") == 0)
        .then(0.0)
        .otherwise(pl.col("trip_count_to"))
        .fill_null(0.0),
        activity_time_to=pl.when(pl.col("mode_seq_id_trans") == 0)
        .then(24.0)
        .otherwise(pl.col("activity_time_to"))
        .fill_null(0.0),
        travel_time_to=pl.when(pl.col("mode_seq_id_trans") == 0)
        .then(0.0)
        .otherwise(pl.col("travel_time_to"))
        .fill_null(0.0),
        distance_to=pl.when(pl.col("mode_seq_id_trans") == 0)
        .then(0.0)
        .otherwise(pl.col("distance_to"))
        .fill_null(0.0),
        steps_to=pl.when(pl.col("mode_seq_id_trans") == 0)
        .then(pl.lit("none"))
        .otherwise(pl.col("steps_to")),
    ).select(TRANSITION_EVENT_COLUMNS)


class TransitionEventsAsset(FileAsset):
    """Persisted transition events produced during one iteration."""

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        transition_events: pl.LazyFrame | None = None,
    ) -> None:
        self.transition_events = transition_events
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"transition_events_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pathlib.Path:
        if self.transition_events is None:
            raise ValueError("Cannot save transition events without a lazy query.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.unlink(missing_ok=True)
        self.transition_events.sink_parquet(self.cache_path)
        return self.cache_path
