"""Build home-zone survey plans without invoking any plan generation assets.

This is an experiment for one scoring context. Its temporary IDs are unique
within that context and are not suitable for saving in simulation state.
"""
import polars as pl

from mobility.trips.group_day_trips.plans.activity_profiles import get_survey_schedule_activities
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.trips.group_day_trips.plans.compact_candidate_steps import MEMBERSHIP_COLUMNS, STEP_COLUMNS
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


def prepare_home_zone_schedules(survey_plans, survey_steps, is_weekday):
    """Prepare the survey schedule and activity sets once for all contexts."""
    return (
        survey_plans.filter(pl.col("is_weekday") == is_weekday)
        .with_columns(pl.col("country").cast(pl.String))
        .group_by("country", "activity_seq_id", "time_seq_id")
        .agg(pl.col("plan_weight_mass").sort().implode().list.sum())
        .join(get_survey_schedule_activities(survey_steps), on=["activity_seq_id", "time_seq_id"])
    )


def build_home_zone_candidates(*, survey_plans, survey_steps, context, opportunities, modes, is_weekday,
                               prepared_schedules=None, max_schedules=None):
    """Expand compatible survey schedules into single-mode home-zone plans."""
    if context.height != 1:
        raise ValueError("The experimental builder expects one scoring context.")
    group = context.row(0, named=True)
    zone, country = group["home_zone_id"], group["country"]
    local_activities = (
        opportunities.filter((pl.col("to") == zone) & (pl.col("opportunity_capacity") > 0))
        ["activity"].cast(pl.String).to_list()
    )
    allowed = sorted(set(group["allowed_activities"]) & set(local_activities))
    if prepared_schedules is None:
        prepared_schedules = prepare_home_zone_schedules(survey_plans, survey_steps, is_weekday)
    schedules = (
        prepared_schedules.filter(pl.col("country") == country)
        .filter(pl.col("activities").list.set_difference(pl.lit(allowed, dtype=pl.List(pl.String))).list.len() == 0)
    )
    if max_schedules is not None:
        schedules = schedules.sort(
            ["plan_weight_mass", "activity_seq_id", "time_seq_id"], descending=[True, False, False]
        ).head(max_schedules)
    schedules = schedules.select("activity_seq_id", "time_seq_id")
    mode_rows = pl.DataFrame({"mode": modes, "_mode_index": range(len(modes))},
                             schema={"mode": pl.String, "_mode_index": pl.UInt32})
    candidates = (
        schedules.join(survey_steps, on=["activity_seq_id", "time_seq_id"])
        .join(mode_rows, how="cross")
        .with_columns(
            demand_group_id=pl.lit(group["demand_group_id"], dtype=pl.UInt32),
            demand_subgroup_id=pl.lit(group["demand_subgroup_id"], dtype=pl.UInt32),
            country=pl.lit(country).cast(context.schema["country"]),
            csp=pl.lit(group["csp"]).cast(context.schema["csp"]),
            **{"from": pl.lit(zone, dtype=pl.UInt16), "to": pl.lit(zone, dtype=pl.UInt16)},
            # One context uses one home zone. The number of steps distinguishes
            # repeated-home destination and repeated-mode sequences here.
            dest_seq_id=(pl.col("step_count").cast(pl.UInt32) + 1),
            mode_seq_id=(pl.col("_mode_index").cast(pl.UInt32) * 256 + pl.col("step_count") + 1),
            iteration=pl.lit(1, dtype=pl.UInt16),
            first_seen_iteration=pl.lit(1, dtype=pl.UInt16),
            last_seen_iteration=pl.lit(1, dtype=pl.UInt16),
            last_active_iteration=pl.lit(None, dtype=pl.UInt16),
        )
        .select(CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS)
    )
    return candidates


def build_home_zone_candidates_batch(*, survey_steps, contexts, opportunities, available_modes, prepared_schedules,
                                     selected_schedules=None):
    """Build direct home-zone plans for a batch of scoring contexts."""
    local = (
        opportunities.filter((pl.col("activity").cast(pl.String) != "home")
                             & (pl.col("opportunity_capacity") > 0))
        .group_by("to").agg(pl.col("activity").cast(pl.String).unique().sort().alias("local_activities"))
        .rename({"to": "home_zone_id"})
    )
    context_rows = (
        contexts.with_columns(pl.col("country").cast(pl.String))
        .join(local, on="home_zone_id", how="left")
        .with_columns(pl.col("local_activities").fill_null(pl.lit([], dtype=pl.List(pl.String))))
    )
    if selected_schedules is not None:
        schedules = context_rows.lazy().join(
            selected_schedules.lazy().select("demand_group_id", "demand_subgroup_id", "activity_seq_id", "time_seq_id"),
            on=["demand_group_id", "demand_subgroup_id"],
        ).join(
            prepared_schedules.lazy(), on=["country", "activity_seq_id", "time_seq_id"],
        )
    else:
        schedules = context_rows.lazy().join(prepared_schedules.lazy(), on="country")
    return (
        schedules
        .filter((pl.col("activities").list.set_difference(pl.col("allowed_activities")).list.len() == 0)
                & (pl.col("activities").list.set_difference(pl.col("local_activities")).list.len() == 0))
        .join(survey_steps.lazy(), on=["activity_seq_id", "time_seq_id"])
        .join(available_modes.lazy(), on=["home_zone_id", "utility_profile_id"])
        .with_columns(
            pl.col("demand_group_id", "demand_subgroup_id").cast(pl.UInt32),
            country=pl.col("country").cast(contexts.schema["country"]),
            **{"from": pl.col("home_zone_id").cast(pl.UInt16), "to": pl.col("home_zone_id").cast(pl.UInt16)},
            dest_seq_id=(pl.col("step_count").cast(pl.UInt32) + 1),
            mode_seq_id=(pl.col("_mode_index").cast(pl.UInt32) * 256 + pl.col("step_count") + 1),
            iteration=pl.lit(1, dtype=pl.UInt16),
            first_seen_iteration=pl.lit(1, dtype=pl.UInt16),
            last_seen_iteration=pl.lit(1, dtype=pl.UInt16),
            last_active_iteration=pl.lit(None, dtype=pl.UInt16),
        )
        .select(CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS
                + ["utility_profile_id"])
        .collect(engine="streaming")
    )


def build_factored_home_zone_candidates(*, survey_steps, contexts, opportunities, available_modes,
                                        prepared_schedules, selected_schedules):
    """Build shared physical steps and context memberships without expanding both."""
    local = (opportunities.filter((pl.col("activity").cast(pl.String) != "home")
                                  & (pl.col("opportunity_capacity") > 0))
             .group_by("to").agg(pl.col("activity").cast(pl.String).unique().sort().alias("local_activities"))
             .rename({"to": "home_zone_id"}))
    choices = (contexts.lazy().with_columns(pl.col("country").cast(pl.String))
        .join(local.lazy(), on="home_zone_id", how="left")
        .with_columns(pl.col("local_activities").fill_null(pl.lit([], dtype=pl.List(pl.String))))
        .join(selected_schedules.lazy().select("demand_group_id", "demand_subgroup_id", "activity_seq_id", "time_seq_id"),
              on=["demand_group_id", "demand_subgroup_id"])
        .join(prepared_schedules.lazy(), on=["country", "activity_seq_id", "time_seq_id"])
        .filter((pl.col("activities").list.set_difference(pl.col("allowed_activities")).list.len() == 0)
                & (pl.col("activities").list.set_difference(pl.col("local_activities")).list.len() == 0))
        .join(available_modes.lazy(), on=["home_zone_id", "utility_profile_id"])
        .join(survey_steps.lazy().select("activity_seq_id", "time_seq_id", "step_count").unique(),
              on=["activity_seq_id", "time_seq_id"])
        .with_columns(pl.col("demand_group_id", "demand_subgroup_id").cast(pl.UInt32),
                      country=pl.col("country").cast(contexts.schema["country"]),
                      dest_seq_id=pl.col("step_count").cast(pl.UInt32) + 1,
                      mode_seq_id=pl.col("_mode_index").cast(pl.UInt32) * 256 + pl.col("step_count") + 1,
                      iteration=pl.lit(1, dtype=pl.UInt16),
                      first_seen_iteration=pl.lit(1, dtype=pl.UInt16),
                      last_seen_iteration=pl.lit(1, dtype=pl.UInt16),
                      last_active_iteration=pl.lit(None, dtype=pl.UInt16))
        .collect(engine="streaming"))
    memberships = choices.select(MEMBERSHIP_COLUMNS + ["utility_profile_id", "home_zone_id"]).unique()
    physical = (choices.lazy().select("activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id",
                                      "home_zone_id", "mode").unique()
        .join(survey_steps.lazy(), on=["activity_seq_id", "time_seq_id"])
        .with_columns(**{"from": pl.col("home_zone_id").cast(pl.UInt16),
                         "to": pl.col("home_zone_id").cast(pl.UInt16)})
        .select(STEP_COLUMNS).unique().collect(engine="streaming"))
    return physical, memberships


def rank_home_zone_schedules(*, updater, candidates, context, costs, population_segments,
                             destination_saturation, activity_dur, home_night_dur, stay_home,
                             transport_zones, resolved_activity_parameters, activity_rigidity, parameters,
                             scoring_lookup=None, utility_profiles=None):
    """Rank schedules using the normal scoring formulas before indexing modeled times.

    This first pass is an experiment. The retained schedules are evaluated again
    by the normal evaluator, which performs canonical timing and plan indexing.
    """
    scored = updater.compute_plan_steps_candidates_utility(
        candidates=candidates.lazy(), transport_costs=costs, demand_groups=context,
        population_segments=population_segments, utility_profiles=utility_profiles,
        destination_saturation=destination_saturation,
        activity_dur=activity_dur, transport_zones=transport_zones,
        resolved_activity_parameters=resolved_activity_parameters,
        min_activity_time_constant=parameters.plan_update.min_activity_time_constant,
        allow_missing_costs_for_current_plans=True,
        use_destination_shadow_prices=parameters.plan_update.use_destination_shadow_prices,
        scoring_lookup=scoring_lookup,
    )
    timed = updater.schedule_updater.update_plan_timings(
        scored, activity_time_rigidity_by_activity=activity_rigidity,
        enabled=parameters.plan_update.update_plan_timings_from_modeled_travel_times,
    )
    time_columns = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
    schema = timed.collect_schema()
    timed = timed.with_columns([
        ((pl.col(column).cast(pl.Float64) * 3600.0).round(0) / 3600.0).cast(schema[column]).alias(column)
        for column in time_columns
    ]).with_columns(
        is_feasible=(pl.col("is_feasible") & (
            (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS))
            | (pl.col("next_departure_time") >= pl.col("arrival_time"))
        )).fill_null(False),
        utility=(pl.col("activity_utility_scale")
                 * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0) - pl.col("cost")),
        plan_id=pl.lit(0, dtype=pl.UInt32),
    )
    home_index = stay_home.select(PLAN_KEY_COLS).with_columns(pl.lit(0, dtype=pl.UInt32).alias("plan_id"))
    utility, _ = updater.get_possible_plan_utility(
        timed, home_night_dur, resolved_activity_parameters["home"].value_of_time_stay_home,
        stay_home, parameters.plan_update.min_activity_time_constant,
        plan_id_index=home_index, plan_ids_already_indexed=True,
    )
    return (
        utility.filter(pl.col("is_feasible") & (pl.col("mode_seq_id") != 0))
        .group_by("demand_group_id", "demand_subgroup_id", "activity_seq_id", "time_seq_id")
        .agg(pl.col("utility").max().alias("best_utility"))
        .sort(["demand_group_id", "demand_subgroup_id", "best_utility", "activity_seq_id", "time_seq_id"],
              descending=[False, False, True, False, False])
        .collect(engine="streaming")
    )
