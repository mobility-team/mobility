import polars as pl


def get_survey_schedule_activities(survey_plan_steps: pl.DataFrame) -> pl.DataFrame:
    """Return the distinct non-home activities in each timed survey schedule."""
    # A zero time sequence marks the stay-home plan, not a sampled schedule.
    return (
        survey_plan_steps
        .filter(
            (pl.col("time_seq_id") != 0)
            & (pl.col("activity").cast(pl.String) != "home")
        )
        .group_by(["activity_seq_id", "time_seq_id"])
        .agg(activities=pl.col("activity").cast(pl.String).unique().sort())
    )


def get_activity_supports(
    *,
    survey_plans: pl.DataFrame,
    survey_plan_steps: pl.DataFrame,
    threshold: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build survey-weighted activity sets for each CSP and country fallback."""
    schedule_activities = get_survey_schedule_activities(survey_plan_steps)
    weighted_schedules = (
        survey_plans
        .group_by(["country", "csp", "activity_seq_id", "time_seq_id"])
        .agg(plan_weight_mass=pl.col("plan_weight_mass").sum())
        .join(
            schedule_activities,
            on=["activity_seq_id", "time_seq_id"],
            how="inner",
        )
    )

    csp_totals = weighted_schedules.group_by(["country", "csp"]).agg(
        total_mass=pl.col("plan_weight_mass").sum()
    )
    activity_by_csp = (
        weighted_schedules
        .explode("activities")
        .group_by(["country", "csp", "activities"])
        .agg(activity_mass=pl.col("plan_weight_mass").sum())
        .join(csp_totals, on=["country", "csp"])
        .filter(pl.col("activity_mass") / pl.col("total_mass") >= threshold)
        .group_by(["country", "csp"])
        .agg(allowed_activities=pl.col("activities").sort())
    )

    country_totals = weighted_schedules.group_by("country").agg(
        total_mass=pl.col("plan_weight_mass").sum()
    )
    activity_by_country = (
        weighted_schedules
        .explode("activities")
        .group_by(["country", "activities"])
        .agg(activity_mass=pl.col("plan_weight_mass").sum())
        .join(country_totals, on="country")
        .filter(pl.col("activity_mass") / pl.col("total_mass") >= threshold)
        .group_by("country")
        .agg(allowed_activities=pl.col("activities").sort())
    )
    return activity_by_csp, activity_by_country


def get_schedules_by_activity_profile(
    *,
    schedules: pl.DataFrame,
    profiles: pl.DataFrame,
) -> pl.DataFrame:
    """Return schedules whose non-home activities all fit each activity profile."""
    profile_activities = (
        profiles
        .select(["country", "activity_profile_id", "allowed_activities"])
        .unique()
        .explode("allowed_activities")
        .rename({"allowed_activities": "activity"})
    )
    schedule_activities = (
        schedules
        .select(["country", "activity_seq_id", "time_seq_id", "plan_weight_mass", "activities"])
        .with_columns(schedule_activity_count=pl.col("activities").list.len())
        .explode("activities")
        .rename({"activities": "activity"})
    )
    eligible_keys = (
        schedule_activities
        .join(profile_activities, on=["country", "activity"], how="inner")
        .group_by(["country", "activity_profile_id", "activity_seq_id", "time_seq_id"])
        .agg(
            n_supported_activities=pl.col("activity").n_unique(),
            n_schedule_activities=pl.col("schedule_activity_count").first(),
            plan_weight_mass=pl.col("plan_weight_mass").first(),
        )
        .filter(pl.col("n_supported_activities") == pl.col("n_schedule_activities"))
    )
    return eligible_keys.select(
        [
            "country",
            "activity_profile_id",
            "activity_seq_id",
            "time_seq_id",
            "plan_weight_mass",
        ]
    )
