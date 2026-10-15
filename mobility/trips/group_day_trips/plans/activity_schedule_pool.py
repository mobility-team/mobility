import polars as pl

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from .activity_profiles import (
    get_schedules_by_activity_profile,
    get_survey_schedule_activities,
)


POOL_SCHEDULES_SCHEMA = {
    "country": pl.String,
    "pool_id": pl.String,
    "activity_seq_id": pl.UInt32,
    "time_seq_id": pl.UInt32,
    "plan_weight_mass": pl.Float64,
}
PROFILE_SCHEDULES_SCHEMA = {
    "country": pl.String,
    "activity_profile_id": pl.String,
    "activity_seq_id": pl.UInt32,
    "time_seq_id": pl.UInt32,
    "plan_weight_mass": pl.Float64,
}
ZONE_PROFILES_SCHEMA = {
    "country": pl.String,
    "home_zone_id": pl.Int32,
    "activity_profile_id": pl.String,
    "pool_id": pl.String,
}


def prepare_activity_schedule_pool(
    *,
    survey_plans: pl.DataFrame,
    survey_plan_steps: pl.DataFrame,
    demand_groups: pl.DataFrame,
    is_weekday: bool,
) -> dict[str, pl.DataFrame]:
    """Prepare weighted schedule indices and the fixed activity-profile eligibility maps."""
    demand_units = demand_groups.select(
        ["country", "home_zone_id", "allowed_activities"]
    ).with_columns(
        country=pl.col("country").cast(pl.String),
        activity_profile_id=pl.col("allowed_activities").list.join("|"),
    )
    if demand_units.filter(pl.col("allowed_activities").is_null()).height:
        raise ValueError(
            "Could not derive activity support for one or more demand groups. "
            "Check that the survey and population use matching country and CSP codes."
        )

    zone_profiles = (
        demand_units
        .filter(pl.col("allowed_activities").list.len() > 0)
        .select(["country", "home_zone_id", "activity_profile_id"])
        .unique()
    )
    zone_pool_ids = (
        zone_profiles
        .group_by(["country", "home_zone_id"])
        .agg(pool_profiles=pl.col("activity_profile_id").unique().sort())
        .with_columns(pool_id=pl.col("pool_profiles").list.join("||"))
        .select(["country", "home_zone_id", "pool_id"])
    )
    zone_profiles = zone_profiles.join(
        zone_pool_ids,
        on=["country", "home_zone_id"],
        how="inner",
    )

    profile_definitions = (
        demand_units
        .select(["country", "activity_profile_id", "allowed_activities"])
        .unique()
    )
    # A zone pool accepts schedules supported by any activity profile in that zone.
    pool_support = (
        zone_profiles
        .select(["country", "pool_id", "activity_profile_id"])
        .unique()
        .join(
            profile_definitions,
            on=["country", "activity_profile_id"],
            how="inner",
        )
        .select(["country", "pool_id", "allowed_activities"])
        .explode("allowed_activities")
        .group_by(["country", "pool_id"])
        .agg(allowed_activities=pl.col("allowed_activities").unique().sort())
        .with_columns(activity_profile_id=pl.col("pool_id"))
        .select(["country", "activity_profile_id", "allowed_activities"])
    )

    schedule_activities = get_survey_schedule_activities(survey_plan_steps)
    schedules = (
        survey_plans
        .filter(pl.col("is_weekday") == is_weekday)
        .group_by(["country", "activity_seq_id", "time_seq_id"])
        .agg(plan_weight_mass=pl.col("plan_weight_mass").sum())
        .join(
            schedule_activities,
            on=["activity_seq_id", "time_seq_id"],
            how="inner",
        )
        .with_columns(country=pl.col("country").cast(pl.String))
    )

    pool_schedules = (
        get_schedules_by_activity_profile(
            schedules=schedules,
            profiles=pool_support,
        )
        .rename({"activity_profile_id": "pool_id"})
        .select(list(POOL_SCHEDULES_SCHEMA))
        .with_columns(
            [
                pl.col(column).cast(dtype)
                for column, dtype in POOL_SCHEDULES_SCHEMA.items()
            ]
        )
    )
    profile_schedules = (
        get_schedules_by_activity_profile(
            schedules=schedules,
            profiles=profile_definitions,
        )
        .select(list(PROFILE_SCHEDULES_SCHEMA))
        .with_columns(
            [
                pl.col(column).cast(dtype)
                for column, dtype in PROFILE_SCHEDULES_SCHEMA.items()
            ]
        )
    )
    zone_profiles = zone_profiles.select(list(ZONE_PROFILES_SCHEMA)).with_columns(
        [
            pl.col(column).cast(dtype)
            for column, dtype in ZONE_PROFILES_SCHEMA.items()
        ]
    )

    return {
        "pool_schedules": pool_schedules,
        "profile_schedules": profile_schedules,
        "zone_profiles": zone_profiles,
    }


class ActivitySchedulePoolAsset(InMemoryAsset):
    """Prepare shared survey schedule pools once for the current run."""

    def __init__(
        self,
        *,
        initial_state: Asset,
        is_weekday: bool,
    ) -> None:
        self.initial_state = initial_state
        self.is_weekday = is_weekday
        inputs = {
            "version": 1,
            "initial_state": initial_state,
            "is_weekday": is_weekday,
        }
        super().__init__(inputs)

    def create_and_get_asset(self) -> dict[str, pl.DataFrame]:
        """Prepare schedule eligibility once from the initial run state."""
        state = self.initial_state.get()
        tables = prepare_activity_schedule_pool(
            survey_plans=state.survey_plans,
            survey_plan_steps=state.survey_plan_steps,
            demand_groups=state.demand_groups,
            is_weekday=self.is_weekday,
        )
        return tables
