"""Time and index physical candidates before attaching scoring contexts."""
from dataclasses import dataclass

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import trace_simulation_step
from .compact_candidate_steps import PLAN_COLUMNS, STEP_KEY_COLUMNS
from .demand_subgroups import DEMAND_UNIT_COLS
from .plan_ids import PLAN_KEY_COLS
from .plan_schedule_updater import PlanScheduleUpdater
from .time_sequences import TimeSequenceIndexCache, assign_time_sequence_ids


@dataclass(frozen=True)
class FactoredPlanTimings:
    """Timed physical plans and the context-to-modeled-schedule mapping.

    A timing plan may serve multiple contexts and travel profiles. Its local
    `_timing_plan` ID only identifies it within this result. Costs, route
    feasibility and activity utility still need to be applied per profile.
    """

    steps: pl.DataFrame
    context_links: pl.DataFrame
    timing_plans: pl.DataFrame
    time_sequence_steps: pl.DataFrame

    @property
    def context_mapping(self) -> pl.DataFrame:
        """Reconstruct the full mapping for callers that need plan keys."""
        return (self.context_links.join(self.timing_plans, on="_timing_plan", maintain_order="left")
                .select(DEMAND_UNIT_COLS + ["activity_seq_id", "previous_time_seq_id", "dest_seq_id",
                                            "mode_seq_id", "time_seq_id", "_timing_plan"]))

    def contexts_with_profiles(self, contexts: pl.DataFrame, profile_columns: list[str]) -> pl.DataFrame:
        """Attach plan keys and profiles without retaining another full mapping."""
        return (self.context_links.lazy().join(self.timing_plans.lazy(), on="_timing_plan", maintain_order="left")
                .join(contexts.lazy().select(DEMAND_UNIT_COLS + profile_columns),
                      on=DEMAND_UNIT_COLS, how="left", maintain_order="left")
                .select(DEMAND_UNIT_COLS + ["activity_seq_id", "previous_time_seq_id", "dest_seq_id",
                                            "mode_seq_id", "time_seq_id", "_timing_plan"] + profile_columns)
                .collect(engine="streaming"))


@trace_simulation_step("time shared physical candidates")
def time_factored_candidates(
    physical_steps: pl.DataFrame,
    memberships: pl.DataFrame,
    context_profiles: pl.DataFrame,
    travel_times: pl.DataFrame,
    schedule_steps: pl.DataFrame,
    *,
    activity_time_rigidity_by_activity: dict[str, float],
    enabled: bool,
    index_cache: TimeSequenceIndexCache | None = None,
    schedule_updater: PlanScheduleUpdater | None = None,
) -> FactoredPlanTimings:
    """Share timing when a physical plan has the same ordered travel times.

    Transport profiles can have different costs or route availability even
    when their times agree. Those values must not be shared by this function.
    The existing schedule index remains responsible for persistent IDs.
    """
    travel_keys = ["utility_profile_id", "from", "to", "mode"]
    if physical_steps.select(STEP_KEY_COLUMNS).n_unique() != physical_steps.height:
        raise ValueError("A physical plan has duplicate or conflicting step keys.")
    if context_profiles.select(DEMAND_UNIT_COLS).n_unique() != context_profiles.height:
        raise ValueError("A scoring context has more than one transport profile.")
    if travel_times.select(travel_keys).n_unique() != travel_times.height:
        raise ValueError("A transport profile has conflicting travel times for a trip.")

    contexts = (memberships.select(PLAN_KEY_COLS).unique(maintain_order=True)
                .join(context_profiles.select(DEMAND_UNIT_COLS + ["utility_profile_id"]),
                      on=DEMAND_UNIT_COLS, how="left", maintain_order="left"))
    if contexts["utility_profile_id"].null_count():
        raise ValueError("A scoring context is missing its transport profile.")
    travel_plans = (contexts.select(PLAN_COLUMNS + ["utility_profile_id"]).unique(maintain_order=True)
                    .with_row_index("_travel_plan"))
    travel_steps = (travel_plans.join(physical_steps, on=PLAN_COLUMNS, how="inner")
                    .join(travel_times.select(travel_keys + ["time"]), on=travel_keys, how="left"))
    if travel_steps["time"].null_count():
        raise ValueError("A candidate trip is missing its modeled travel time.")
    counts = physical_steps.group_by(PLAN_COLUMNS).len(name="expected")
    actual = travel_steps.group_by("_travel_plan").len(name="actual")
    if (travel_plans.join(counts, on=PLAN_COLUMNS, how="left").join(actual, on="_travel_plan", how="left")
            .filter(pl.col("expected").is_null() | pl.col("actual").is_null()
                    | (pl.col("expected") != pl.col("actual"))).height):
        raise ValueError("A candidate plan is missing one or more physical steps.")

    signatures = travel_steps.group_by("_travel_plan", maintain_order=True).agg(
        *[pl.col(name).first() for name in PLAN_COLUMNS],
        pl.col("time").sort_by("seq_step_index").alias("_travel_times"),
    )
    timing_plans = (signatures.select(PLAN_COLUMNS + ["_travel_times"]).unique(maintain_order=True)
                    .with_row_index("_timing_plan"))
    travel_to_timing = (signatures.join(timing_plans, on=PLAN_COLUMNS + ["_travel_times"], maintain_order="left")
                        .select("_travel_plan", "_timing_plan"))
    timing_input = (travel_steps.join(travel_to_timing, on="_travel_plan")
                    .unique(subset=["_timing_plan", "seq_step_index"], keep="first")
                    .drop("_travel_plan", "utility_profile_id")
                    .rename({"_timing_plan": "demand_group_id"})
                    .with_columns(demand_subgroup_id=pl.lit(0, dtype=pl.UInt32)))
    updater = schedule_updater if schedule_updater is not None else PlanScheduleUpdater()
    timed = updater.update_plan_timings(
        timing_input.lazy(), activity_time_rigidity_by_activity=activity_time_rigidity_by_activity,
        enabled=enabled,
    ).collect(engine="streaming")
    indexed, catalogue, timing_mapping = assign_time_sequence_ids(timed, schedule_steps, index_cache=index_cache)
    indexed = indexed.rename({"demand_group_id": "_timing_plan"}).drop("demand_subgroup_id")
    time_ids = timing_mapping.select(
        pl.col("demand_group_id").alias("_timing_plan"), pl.col("time_seq_id").alias("_modeled_time_seq_id")
    )
    context_links = (contexts.lazy()
                     .join(travel_plans.lazy(), on=PLAN_COLUMNS + ["utility_profile_id"], maintain_order="left")
                     .join(travel_to_timing.lazy(), on="_travel_plan", maintain_order="left")
                     .select(DEMAND_UNIT_COLS + ["_timing_plan"])
                     .collect(engine="streaming"))
    modeled_plans = (timing_plans.select(PLAN_COLUMNS + ["_timing_plan"])
                     .join(time_ids, on="_timing_plan", maintain_order="left")
                     .rename({"time_seq_id": "previous_time_seq_id", "_modeled_time_seq_id": "time_seq_id"}))
    if (context_links.height != contexts.height or context_links["_timing_plan"].null_count()
            or modeled_plans["time_seq_id"].null_count()):
        raise ValueError("A scoring context is missing its modeled timing.")
    return FactoredPlanTimings(indexed, context_links, modeled_plans, catalogue)
