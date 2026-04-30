from __future__ import annotations

import os
import pathlib
from typing import Any

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from mobility.transport.modes.core.mode_values import get_mode_values

from .survey_sequence_index import add_index


class MobilitySurveyPlanSteps(FileAsset):
    """Persist canonical step-level survey plans for one mobility survey.

    This asset turns the raw survey day-trip tables into one normalized
    step table keyed by stable sequence identifiers. The output keeps the
    timed schedule representation needed by the grouped day-trips model
    while removing transient preprocessing-only fields.
    """

    def __init__(
        self,
        *,
        survey: Any,
        activities: list[Any],
        modes: list[Any],
    ) -> None:
        """Initialize the step-level survey-plan asset.

        Args:
            survey: Source mobility survey asset.
            activities: Canonical activity definitions used to map survey
                motives and anchor semantics.
            modes: Canonical mode definitions used to map survey mode ids.
        """
        self.survey = survey
        self.activities = activities
        self.modes = modes
        folder_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "mobility_surveys"
            / survey.inputs["parameters"].survey_name
        )
        cache_path = folder_path / "group_day_trip_plan_steps.parquet"
        inputs = {
            "version": 3,
            "survey": survey,
            "activities": activities,
            "modes": modes,
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        """Read the cached step table from disk.

        Returns:
            A canonical step-level survey-plan table keyed by activity
            and time sequence ids.
        """
        return pl.read_parquet(self.cache_path)

    def _get_survey_plan_mappings(self) -> tuple[dict[str, str], dict[str, str]]:
        """Build raw-to-canonical mappings for activities and modes."""
        activity_mapping = {
            activity_id: activity.name
            for activity in self.activities
            if activity.name != "other"
            for activity_id in (activity.inputs["parameters"].survey_ids or [])
        }
        mode_mapping = {
            mode_id: mode.inputs["parameters"].name
            for mode in self.modes
            for mode_id in (mode.inputs["parameters"].survey_ids or [])
        }
        return activity_mapping, mode_mapping

    def _get_raw_tables(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Load raw survey day and short-trip tables as Polars dataframes."""
        cached = self.survey.get()
        days_trips = pl.from_pandas(cached["days_trip"].reset_index())
        short_trips = pl.from_pandas(cached["short_trips"].reset_index())
        return days_trips, short_trips

    def _get_sequence_index_folder(self) -> pathlib.Path:
        """Return the shared folder used for persisted sequence indexes."""
        return (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "mobility_surveys"
            / "group_day_trip_sequence_indexes"
        )

    def _get_incomplete_sequences(self, short_trips: pl.DataFrame) -> pl.DataFrame:
        """Identify plan sequences with missing intermediate steps."""
        return (
            short_trips.group_by(["individual_id", "day_id"])
            .agg(
                min_step=pl.col("daily_trip_index").min(),
                max_step=pl.col("daily_trip_index").max(),
                n_steps=pl.col("daily_trip_index").n_unique(),
            )
            .with_columns(dense_ok=(pl.col("min_step") == 1) & (pl.col("max_step") == pl.col("n_steps")))
            .filter(~pl.col("dense_ok"))
            .select(["individual_id", "day_id"])
        )

    def _fix_trip_times(self, short_trips: pl.DataFrame) -> pl.DataFrame:
        """Repair trip times so within-day event order stays monotonic."""
        return (
            short_trips.select(["day_id", "individual_id", "daily_trip_index", "departure_time", "arrival_time"])
            .unpivot(index=["day_id", "individual_id", "daily_trip_index"], value_name="event_time")
            .with_columns(is_arrival=pl.col("variable") == "arrival_time")
            .sort(["day_id", "individual_id", "daily_trip_index", "is_arrival"])
            .with_columns(event_time=pl.col("event_time").mod(24.0 * 3600.0))
            .with_columns(
                prev_event_time=pl.col("event_time").shift(n=1).over(["day_id", "individual_id"]),
                next_event_time=pl.col("event_time").shift(n=-1).over(["day_id", "individual_id"]),
            )
            .with_columns(
                day_change=(
                    (
                        ((pl.col("event_time") - pl.col("prev_event_time")) < -12.0 * 3600.0)
                        & ((pl.col("next_event_time") - pl.col("prev_event_time")) < -12.0 * 3600.0).fill_null(True)
                    )
                    .fill_null(False)
                    .cast(pl.Int8())
                )
            )
            .with_columns(n_day_changes=pl.col("day_change").cum_sum().over(["day_id", "individual_id"]))
            .with_columns(event_time_corr=pl.col("event_time") + 24.0 * 3600.0 * pl.col("n_day_changes"))
            .with_columns(event_time_corr=pl.col("event_time_corr").cum_max().over(["day_id", "individual_id"]))
            .pivot(on="variable", index=["day_id", "individual_id", "daily_trip_index"], values=["event_time_corr"])
            .rename({"daily_trip_index": "seq_step_index"})
        )

    def _prepare_survey_plans(self) -> pl.DataFrame:
        """Build cleaned step-level survey plans from raw survey tables."""
        activity_mapping, mode_mapping = self._get_survey_plan_mappings()
        days_trips, short_trips = self._get_raw_tables()
        incomplete_sequences = self._get_incomplete_sequences(short_trips)
        short_trips_fixed_times = self._fix_trip_times(short_trips)

        plans = (
            days_trips.select(["day_id", "day_of_week", "pondki", "city_category", "csp", "n_cars"])
            .join(short_trips, on="day_id")
            .join(incomplete_sequences, on=["individual_id", "day_id"], how="anti")
            .rename({"daily_trip_index": "seq_step_index"})
            .sort(["day_id", "individual_id", "seq_step_index"])
            .drop(["departure_time", "arrival_time"])
            .join(short_trips_fixed_times, on=["day_id", "individual_id", "seq_step_index"])
            .with_columns(
                is_weekday=pl.col("day_of_week") < 5,
                departure_time=pl.col("departure_time") / 3600.0,
                arrival_time=pl.col("arrival_time") / 3600.0,
            )
            .with_columns(activity=pl.col("motive").cast(pl.Utf8).replace_strict(activity_mapping, default="other"))
            .with_columns(mode=pl.col("mode_id").cast(pl.Utf8).replace_strict(mode_mapping, default="other"))
            .filter(pl.col("seq_step_index") < 11)
            .filter((pl.col("departure_time") < 24.0) & (pl.col("arrival_time") < 24.0))
            .with_columns(max_seq_step_index=pl.col("seq_step_index").max().over(["individual_id", "day_id"]))
            .with_columns(
                activity=pl.when(
                    (pl.col("seq_step_index") == pl.col("max_seq_step_index")) & (pl.col("activity") != "home")
                )
                .then(pl.lit("home"))
                .otherwise(pl.col("activity"))
            )
            .with_columns(
                activity_seq=pl.col("activity").str.join("-").over(["individual_id", "day_id"]),
                mode_seq=pl.col("mode").str.join("-").over(["individual_id", "day_id"]),
                travel_time=pl.col("arrival_time") - pl.col("departure_time"),
            )
            .with_columns(
                next_departure_time=(
                    pl.col("departure_time").shift(n=-1).over(["day_id", "individual_id"]).fill_null(pl.col("arrival_time"))
                )
            )
            .select(
                [
                    "day_id",
                    "individual_id",
                    "seq_step_index",
                    "is_weekday",
                    "day_of_week",
                    "city_category",
                    "csp",
                    "n_cars",
                    "activity",
                    "mode",
                    "departure_time",
                    "arrival_time",
                    "travel_time",
                    "next_departure_time",
                    "distance",
                    "pondki",
                    "activity_seq",
                    "mode_seq",
                    "max_seq_step_index",
                ]
            )
        )

        sequences_sup_24 = (
            plans.group_by(["is_weekday", "city_category", "csp", "n_cars", "activity_seq", "mode_seq"])
            .agg(sequence_duration=pl.col("travel_time").sum())
            .filter(pl.col("sequence_duration") > 24.0)
            .drop("sequence_duration")
        )

        plans = plans.join(
            sequences_sup_24,
            on=["is_weekday", "city_category", "csp", "n_cars", "activity_seq", "mode_seq"],
            how="anti",
        )

        return plans.drop("day_of_week")

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build and cache the canonical survey plan-step table.

        Returns:
            A normalized step-level table containing survey provenance,
            segment keys, stable sequence ids, timing fields, and
            per-person durations.
        """
        raw_steps = self._prepare_survey_plans()
        anchors = {activity.name: activity.is_anchor for activity in self.activities}
        mode_values = get_mode_values(self.modes, "other")
        sequence_index_folder = self._get_sequence_index_folder()

        plan_keys = (
            raw_steps
            .with_columns(
                departure_time_seconds=(pl.col("departure_time") * 3600.0).round(0).cast(pl.Int64),
                arrival_time_seconds=(pl.col("arrival_time") * 3600.0).round(0).cast(pl.Int64),
                next_departure_time_seconds=(pl.col("next_departure_time") * 3600.0).round(0).cast(pl.Int64),
            )
            .with_columns(
                step_time_key=pl.format(
                    "{}|{}|{}|{}|{}",
                    pl.col("seq_step_index"),
                    pl.col("activity").cast(pl.String),
                    pl.col("departure_time_seconds").cast(pl.String),
                    pl.col("arrival_time_seconds").cast(pl.String),
                    pl.col("next_departure_time_seconds").cast(pl.String),
                )
            )
            .group_by(["individual_id", "day_id"])
            .agg(
                activity_key=pl.col("activity_seq").first(),
                time_key=pl.col("step_time_key").sort_by("seq_step_index").str.join("||"),
            )
        )
        plan_keys = add_index(
            plan_keys,
            col="activity_key",
            index_col_name="activity_seq_id",
            index_folder=sequence_index_folder,
        )
        plan_keys = add_index(
            plan_keys,
            col="time_key",
            index_col_name="time_seq_id",
            index_folder=sequence_index_folder,
        ).drop(["activity_key", "time_key"])

        canonicalized_steps = (
            raw_steps
            .join(plan_keys, on=["individual_id", "day_id"])
        )
        canonical_plan_weights = (
            canonicalized_steps
            .group_by(
                [
                    "individual_id",
                    "day_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "is_weekday",
                    "city_category",
                    "csp",
                    "n_cars",
                ]
            )
            .agg(plan_weight_mass=pl.col("pondki").first())
            .group_by(
                [
                    "activity_seq_id",
                    "time_seq_id",
                    "is_weekday",
                    "city_category",
                    "csp",
                    "n_cars",
                ]
            )
            .agg(plan_weight_mass=pl.col("plan_weight_mass").sum())
        )

        plan_steps = (
            canonicalized_steps
            .with_columns(
                is_anchor=pl.col("activity").cast(pl.Utf8).replace_strict(anchors),
                duration_per_pers=(pl.col("next_departure_time") - pl.col("arrival_time")).clip(0.0, 24.0),
            )
            .select(
                [
                    "activity_seq_id",
                    "time_seq_id",
                    "is_weekday",
                    "city_category",
                    "csp",
                    "n_cars",
                    "seq_step_index",
                    "activity",
                    "mode",
                    "is_anchor",
                    "departure_time",
                    "arrival_time",
                    "next_departure_time",
                    "travel_time",
                    "distance",
                    "duration_per_pers",
                ]
            )
            .unique(
                subset=[
                    "activity_seq_id",
                    "time_seq_id",
                    "is_weekday",
                    "city_category",
                    "csp",
                    "n_cars",
                    "seq_step_index",
                ],
                keep="first",
            )
            .join(
                canonical_plan_weights,
                on=[
                    "activity_seq_id",
                    "time_seq_id",
                    "is_weekday",
                    "city_category",
                    "csp",
                    "n_cars",
                ],
                how="inner",
            )
            .sort(["time_seq_id", "seq_step_index"])
            .with_columns(
                seq_step_index=pl.col("seq_step_index").cast(pl.UInt8),
                mode=pl.col("mode").cast(pl.Enum(mode_values)),
                departure_time=pl.col("departure_time").cast(pl.Float32),
                arrival_time=pl.col("arrival_time").cast(pl.Float32),
                next_departure_time=pl.col("next_departure_time").cast(pl.Float32),
                duration_per_pers=pl.col("duration_per_pers").cast(pl.Float32),
            )
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        plan_steps.write_parquet(self.cache_path)
        return self.get_cached_asset()
