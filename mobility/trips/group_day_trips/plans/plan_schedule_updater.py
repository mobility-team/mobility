import polars as pl

from .plan_ids import PLAN_KEY_COLS


class PlanScheduleUpdater:
    """Adjust candidate-plan schedules from survey timings and modeled travel times."""

    MIN_ACTIVITY_DURATION_HOURS = 1e-3

    def update_plan_timings(
        self,
        possible_plan_steps: pl.LazyFrame,
        *,
        activity_time_rigidity_by_activity: dict[str, float],
        enabled: bool,
    ) -> pl.LazyFrame:
        """Return iteration-local plan timings updated from modeled travel times."""
        if enabled is False:
            return possible_plan_steps

        activity_dtype = possible_plan_steps.collect_schema()["activity"]
        duration_dtype = possible_plan_steps.collect_schema()["duration_per_pers"]
        time_dtype = possible_plan_steps.collect_schema()["departure_time"]

        rigidity_lookup = (
            pl.DataFrame(
                {
                    "activity": list(activity_time_rigidity_by_activity.keys()),
                    "activity_time_rigidity": list(activity_time_rigidity_by_activity.values()),
                }
            )
            .with_columns(
                activity=pl.col("activity").cast(activity_dtype),
                activity_time_rigidity=pl.col("activity_time_rigidity").cast(pl.Float64),
            )
            .lazy()
        )

        raw_duration = pl.col("next_departure_time") - pl.col("arrival_time")

        return (
            possible_plan_steps
            .join(rigidity_lookup, on="activity", how="left")
            .sort(PLAN_KEY_COLS + ["seq_step_index"])
            .with_columns(
                reference_travel_time=(pl.col("arrival_time") - pl.col("departure_time")).clip(0.0, 24.0),
                previous_rigidity=pl.col("activity_time_rigidity").shift(1).over(PLAN_KEY_COLS)
                .fill_null(activity_time_rigidity_by_activity.get("home", 0.0)),
            )
            .with_columns(
                travel_time_delta=pl.col("time") - pl.col("reference_travel_time"),
                departure_share=(1.0 + pl.col("activity_time_rigidity") - pl.col("previous_rigidity")) / 2.0,
            )
            .with_columns(
                departure_time=(
                    pl.col("departure_time")
                    - pl.col("departure_share") * pl.col("travel_time_delta")
                ).cast(time_dtype),
                arrival_time=(
                    pl.col("arrival_time")
                    + (1.0 - pl.col("departure_share")) * pl.col("travel_time_delta")
                ).cast(time_dtype),
            )
            .sort(PLAN_KEY_COLS + ["seq_step_index"])
            .with_columns(
                next_departure_time=(
                    # Rows of the same plan are adjacent after sorting. Only
                    # use the next row's departure when its plan keys match.
                    pl.when(pl.all_horizontal([
                        pl.col(key).eq_missing(pl.col(key).shift(-1))
                        for key in PLAN_KEY_COLS
                    ]))
                    .then(pl.col("departure_time").shift(-1))
                    .otherwise(pl.col("next_departure_time"))
                    .fill_null(pl.col("next_departure_time"))
                    .cast(time_dtype)
                )
            )
            .with_columns(
                duration_per_pers=(
                    pl.when(raw_duration < self.MIN_ACTIVITY_DURATION_HOURS)
                    .then(pl.lit(self.MIN_ACTIVITY_DURATION_HOURS))
                    .otherwise(raw_duration)
                    .cast(duration_dtype)
                )
            )
            .drop(["activity_time_rigidity", "previous_rigidity", "departure_share",
                   "reference_travel_time", "travel_time_delta"])
        )
