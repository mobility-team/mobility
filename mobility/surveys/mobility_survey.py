from __future__ import annotations
import os
import pathlib
import polars as pl
import pandas as pd
from typing import Annotated, Any
from pydantic import BaseModel, ConfigDict, Field
from mobility.runtime.assets.file_asset import FileAsset


class MobilitySurvey(FileAsset):
    """
    A class for managing and processing mobility survey data for the EMP-2019 and ENTD-2008 surveys.
    
    Attributes:
        source (str): The source of the mobility survey data (e.g., "fr-EMP-2019", "fr-ENTD-2008", "ch-MRMT-2021").
        cache_path (dict): A dictionary mapping data identifiers to their file paths in the cache.
    
    Methods:
        get_cached_asset: Returns the cached asset data as a dictionary of pandas DataFrames.
    """
    
    def __init__(
        self,
        survey_name: str | None = None,
        country: str | None = None,
        parameters: "MobilitySurveyParameters" | None = None,
    ):
        """Initialize mobility survey asset inputs and cache paths.

        Args:
            survey_name: Survey dataset identifier.
            country: Country code associated with the survey.
            parameters: Optional pre-built pydantic parameters model.
        """
        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=MobilitySurveyParameters,
            explicit_args={
                "survey_name": survey_name,
                "country": country,
            },
            required_fields=["survey_name", "country"],
            owner_name="MobilitySurvey",
        )

        folder_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "mobility_surveys"
            / parameters.survey_name
        )
        
        files = {
            "short_trips": "short_dist_trips.parquet",
            "days_trip": "days_trip.parquet",
            "long_trips": "long_dist_trips.parquet",
            "travels": "travels.parquet",
            "n_travels": "long_dist_travel_number.parquet",
            "p_immobility": "immobility_probability.parquet",
            "p_car": "car_ownership_probability.parquet",
            "p_det_mode": "insee_modes_to_entd_modes.parquet"
        }
        
        cache_path = {k: folder_path / file for k, file in files.items()}
        inputs = {
            "version": "1",
            "parameters": parameters,
        }
        inputs.update(self.get_additional_inputs())
        super().__init__(inputs, cache_path)

    def get_additional_inputs(self) -> dict[str, Any]:
        """Hook for specialized parsers to add extra hashed inputs before cache initialization."""
        return {}
        
    def get_cached_asset(self) -> dict[str, pd.DataFrame]:
        """
        Fetches the cached survey data.
        
        Returns:
            dict: A dictionary where keys are data identifiers and values are pandas DataFrames of the cached data.
        """
        return {k: pd.read_parquet(path) for k, path in self.cache_path.items()}
    

    def _get_survey_plan_mappings(
        self,
        activities,
        modes,
    ) -> tuple[dict[str, str], dict[str, str]]:
        """Return raw-to-canonical survey mappings for activities and modes."""

        activity_mapping = {
            activity_id: activity.name
            for activity in activities
            if activity.name != "other"
            for activity_id in (activity.inputs["parameters"].survey_ids or [])
        }
        mode_mapping = {
            mode_id: mode.inputs["parameters"].name
            for mode in modes
            for mode_id in (mode.inputs["parameters"].survey_ids or [])
        }
        return activity_mapping, mode_mapping

    def prepare_survey_plans(self, activities, modes) -> pl.DataFrame:
        """Return a cleaned step-level survey plan table before aggregation."""

        activity_mapping, mode_mapping = self._get_survey_plan_mappings(activities, modes)

        cached = self.get()
        days_trips = pl.from_pandas(cached["days_trip"].reset_index())
        short_trips = pl.from_pandas(cached["short_trips"].reset_index())

        # About 3 % of the sequences for the swiss MRMT survey are incomplete
        # (missing steps). For now they are filtered out to avoid bugs in later
        # processing steps.
        incomplete_sequences = (
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

        short_trips_fixed_times = (
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

        plans = (
            days_trips.select(["day_id", "day_of_week", "pondki"])
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
            .with_columns(max_seq_step_index=pl.col("seq_step_index").max().over(["individual_id", "day_id"]))
            .filter(pl.col("max_seq_step_index") < 11)
            .filter((pl.col("departure_time") < 24.0) & (pl.col("arrival_time") < 24.0))
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

        # Some rare schedules are still more than 24 hours long at this point.
        sequences_sup_24 = plans.group_by(["is_weekday", "city_category", "csp", "n_cars", "activity_seq", "mode_seq"]).agg(
            sequence_duration=pl.col("travel_time").sum()
        ).filter(pl.col("sequence_duration") > 24.0).drop("sequence_duration")

        plans = plans.join(
            sequences_sup_24,
            on=["is_weekday", "city_category", "csp", "n_cars", "activity_seq", "mode_seq"],
            how="anti",
        )

        survey_plans = (
            plans.select(["individual_id", "day_id"])
            .unique()
            .sort(["individual_id", "day_id"])
            .with_row_index("survey_plan_id")
            .with_columns(survey_plan_id=pl.col("survey_plan_id").cast(pl.UInt32))
        )

        return (
            plans.join(survey_plans, on=["individual_id", "day_id"])
            .drop("day_of_week")
        )

    def get_plans_probability(self, activities, modes):
        """Return raw timed survey plans with segment-level survey frequencies."""

        plans = self.prepare_survey_plans(activities, modes)
        plan_probabilities = (
            plans.group_by(["survey_plan_id", "is_weekday", "city_category", "csp", "n_cars"])
            .agg(
                pondki=pl.col("pondki").first(),
            )
            .with_columns(
                p_plan=pl.col("pondki") / pl.col("pondki").sum().over(["is_weekday", "city_category", "csp", "n_cars"])
            )
            .select(["survey_plan_id", "is_weekday", "city_category", "csp", "n_cars", "p_plan"])
        )

        return (
            plans.join(
                plan_probabilities,
                on=["survey_plan_id", "is_weekday", "city_category", "csp", "n_cars"],
                how="left",
            )
            .drop(["pondki", "mode_seq", "max_seq_step_index"])
        )

class MobilitySurveyParameters(BaseModel):
    """Parameters used to configure a mobility survey asset."""

    model_config = ConfigDict(extra="forbid")

    survey_name: Annotated[
        str,
        Field(
            title="Survey name",
            description="Identifier of the survey dataset folder to load.",
        ),
    ]

    country: Annotated[
        str,
        Field(
            title="Country code",
            description="ISO-like country code used to map surveys to population inputs.",
        ),
    ]

