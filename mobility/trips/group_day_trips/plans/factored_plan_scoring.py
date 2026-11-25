"""Score timed physical plans once per activity and transport profile."""
from dataclasses import dataclass
import math
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import simulation_step, trace_simulation_step
from .demand_subgroups import DEMAND_UNIT_COLS
from .factored_plan_timing import FactoredPlanTimings
from .plan_ids import PLAN_KEY_COLS


@dataclass(frozen=True)
class FactoredSectionScores:
    """Plan utilities and their mapping to scoring contexts."""

    utilities: pl.DataFrame
    context_plans: pl.DataFrame
    class_utilities: pl.DataFrame | None = None


@dataclass
class SectionScoreCache:
    """Retain sections and scores while costs and opportunity inputs are fixed.

    Discard this cache when a scenario or outer-step scoring input changes.
    """

    sections: pl.DataFrame | None = None
    scores: pl.DataFrame | None = None
    budget: Any | None = None

    def clear(self) -> None:
        """Release reusable scores and their share of the memory budget."""
        if self.budget is not None:
            self.budget.used_bytes -= (
                (self.sections.estimated_size() if self.sections is not None else 0)
                + (self.scores.estimated_size() if self.scores is not None else 0)
            )
        self.sections = None
        self.scores = None

    def retain(self, sections: pl.DataFrame, scores: pl.DataFrame) -> None:
        """Keep both tables only when the shared scoring budget can hold them."""
        if self.budget is not None:
            previous = ((self.sections.estimated_size() if self.sections is not None else 0)
                        + (self.scores.estimated_size() if self.scores is not None else 0))
            self.budget.used_bytes -= previous
            size = sections.estimated_size() + scores.estimated_size()
            if self.budget.used_bytes + size > self.budget.max_bytes:
                self.sections = None
                self.scores = None
                return
            self.budget.used_bytes += size
        self.sections = sections
        self.scores = scores


@trace_simulation_step("score shared timed sections")
def score_factored_sections(
    timings: FactoredPlanTimings,
    contexts: pl.DataFrame,
    score_lookup: pl.DataFrame,
    home_night_durations: pl.DataFrame,
    *,
    rigid_activities: set[str],
    value_of_time_stay_home: float,
    min_activity_time_constant: float,
    cache: SectionScoreCache | None = None,
    map_contexts: bool = True,
) -> FactoredSectionScores:
    """Score equal timed sections once per activity and transport profile.

    Each section stores its step utilities so complete-plan aggregation can
    retain the evaluator's sorted sum order. Durations and travel times are
    shared by all profiles using a timed physical plan.
    A section's activity duration was already calculated using both adjacent
    trips; a rigid boundary does not imply fixed arrival or departure times.
    """
    profile_columns = ["country", "csp", "utility_profile_id"]
    profile_key = profile_columns + ["_timing_plan"]
    section_key = profile_columns + ["_section_id"]
    score_keys = profile_columns + ["activity", "from", "to", "mode"]
    score_values = ["cost", "time", "is_feasible", "_missing_cost_row", "_invalid_cost_row",
                    "min_activity_time", "activity_utility_scale"]
    if score_lookup.select(score_keys).n_unique() != score_lookup.height:
        raise ValueError("The scoring lookup has conflicting entries for the same profile and trip.")
    context_plans = timings.contexts_with_profiles(contexts, profile_columns)
    if context_plans["utility_profile_id"].null_count():
        raise ValueError("A candidate context is missing its scoring profile.")
    # The score of a step needs its activity, physical trip, modeled duration
    # and timing feasibility. Sequence IDs and absolute position do not alter
    # that score; preserve order inside each section for later reuse.
    rows = (timings.steps.sort("_timing_plan", "seq_step_index")
            .with_columns(_last=pl.col("seq_step_index") == pl.col("seq_step_index").max().over("_timing_plan"),
                          _rigid=pl.col("activity").cast(pl.String).is_in(sorted(rigid_activities)))
            .with_columns(_boundary=pl.col("_rigid").shift(1).over("_timing_plan").fill_null(False))
            .with_columns(_section_in_plan=pl.col("_boundary").cast(pl.UInt32).cum_sum().over("_timing_plan"))
            .with_columns(_timing_feasible=pl.col("_last") | (pl.col("next_departure_time") >= pl.col("arrival_time"))))
    atom_columns = ["activity", "from", "to", "mode", "time", "duration_per_pers", "_timing_feasible"]
    plan_sections = rows.group_by("_timing_plan", "_section_in_plan", maintain_order=True).agg(
        pl.struct(atom_columns).alias("_atoms"))
    section_values = plan_sections.select("_atoms").unique(maintain_order=True)
    if cache is not None and cache.sections is not None:
        new_sections = section_values.join(cache.sections, on="_atoms", how="anti").with_row_index(
            "_section_id", offset=cache.sections.height,
        )
        sections = pl.concat([cache.sections, new_sections])
    else:
        sections = section_values.with_row_index("_section_id")
    plan_sections = (plan_sections.join(sections, on="_atoms", maintain_order="left")
                     .select("_timing_plan", "_section_in_plan", "_section_id"))
    del rows, section_values
    classes = context_plans.select(profile_key).unique(maintain_order=True)
    needed = (classes.join(plan_sections, on="_timing_plan").select(section_key).unique(maintain_order=True))
    previous_scores = cache.scores if cache is not None else None
    to_score = needed if previous_scores is None else needed.join(
        previous_scores.select(section_key), on=section_key, how="anti",
    )
    score_rows = score_lookup.select(score_keys + score_values).with_columns(_score_present=pl.lit(True))
    scored_batches = []
    for batch in to_score.iter_slices(100_000):
        section_steps = (batch.join(sections, on="_section_id").explode("_atoms").unnest("_atoms")
                         .rename({"time": "_timing_travel_time"})
                         .join(score_rows, on=score_keys, how="left"))
        if section_steps["_score_present"].null_count():
            raise ValueError("A candidate section is missing from the scoring lookup.")
        if section_steps.filter(~pl.col("time").eq_missing(pl.col("_timing_travel_time"))).height:
            raise ValueError("A section was timed with a different transport profile's travel time.")
        scored_batches.append(section_steps.with_columns(
            _step_utility=pl.col("activity_utility_scale")
            * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0) - pl.col("cost"),
            _step_feasible=pl.col("is_feasible") & pl.col("_timing_feasible"),
        ).group_by(section_key).agg(
            pl.col("_step_utility").alias("_utilities"),
            pl.col("_step_feasible").all().alias("_feasible"),
            pl.col("_missing_cost_row").sum().alias("_missing"),
            pl.col("_invalid_cost_row").sum().alias("_invalid"),
        ))
        del section_steps
    new_scores = pl.concat(scored_batches) if scored_batches else previous_scores.head(0)
    all_scores = new_scores if previous_scores is None else pl.concat([previous_scores, new_scores])
    section_scores = needed.join(all_scores, on=section_key, how="inner")
    if cache is not None:
        cache.retain(sections, all_scores)
    del scored_batches, new_scores, all_scores, previous_scores, to_score, needed, score_rows, sections
    # A timing plan already fixes the actual travel times and durations. Sum
    # these once per plan rather than repeating them for each scoring profile.
    plan_home_night = timings.steps.group_by("_timing_plan").agg(
        home_night_per_pers=(24.0
            - pl.col("duration_per_pers").cast(pl.Float64).sort().implode().list.sum()
            - pl.col("time").fill_null(0.0).cast(pl.Float64).sort().implode().list.sum()).clip(0.0),
    )
    # Bound the plan-to-section join while preserving sorted step sums.
    class_parts = classes.partition_by(profile_columns, as_dict=True, maintain_order=True)
    score_parts = section_scores.partition_by(profile_columns, as_dict=True, maintain_order=True)
    plan_scores = []
    for profile, profile_classes in class_parts.items():
        profile_scores = score_parts[profile]
        for batch in profile_classes.iter_slices(100_000):
            plan_scores.append(batch.join(plan_sections, on="_timing_plan")
                .join(profile_scores, on=section_key)
                .group_by(profile_key).agg(
                    utility=pl.col("_utilities").explode().sort().implode().list.sum(),
                    is_feasible=pl.col("_feasible").all(),
                    missing_cost_row_count=pl.col("_missing").sum(),
                    invalid_cost_row_count=pl.col("_invalid").sum(),
                ))
    class_utility = (pl.concat(plan_scores).join(plan_home_night, on="_timing_plan")
        .join(home_night_durations, on=["country", "csp"], how="inner")
        .with_columns(min_activity_time=pl.col("mean_home_night_per_pers") * math.exp(-min_activity_time_constant))
        .with_columns(utility=pl.col("utility") + value_of_time_stay_home * pl.col("mean_home_night_per_pers")
                      * (pl.col("home_night_per_pers") / pl.col("min_activity_time")).log().clip(0.0))
        .select(profile_key + ["utility", "is_feasible", "missing_cost_row_count", "invalid_cost_row_count"]))
    if not map_contexts:
        return FactoredSectionScores(pl.DataFrame(), pl.DataFrame(), class_utility)
    del class_parts, score_parts, plan_scores, plan_home_night, classes, section_scores, plan_sections
    del profile_classes, profile_scores, batch
    utilities = map_class_utilities(class_utility, context_plans, profile_columns)
    return FactoredSectionScores(utilities, context_plans, class_utility)


def map_class_utilities(class_utility, context_plans, profile_columns):
    """Map scored timing plans to their current contexts."""
    with simulation_step("join scoring contexts"):
        # Every scoring context has one profile, so modeled-plan collisions can
        # be resolved within each profile without building one large wide join.
        utility_columns = ["utility", "is_feasible", "missing_cost_row_count", "invalid_cost_row_count"]
        # Group narrow row references once instead of scanning all context plans
        # for every profile. The references retain the original collision order.
        context_rows = context_plans.with_row_index("_row").select(profile_columns + ["_row"])
        context_parts = context_rows.partition_by(profile_columns, as_dict=True, maintain_order=True)
        utility_by_profile = class_utility.partition_by(profile_columns, as_dict=True, maintain_order=True)
        utility_parts = []
        for profile, profile_utility in utility_by_profile.items():
            context_part = context_plans[context_parts[profile]["_row"]].select(PLAN_KEY_COLS + ["_timing_plan"])
            # Keep all alternatives from one context together while limiting the
            # number of rows in a join. One exceptionally large context can still
            # exceed the target, since its collisions must be resolved together.
            batches = (context_part.group_by("demand_group_id").len().sort("demand_group_id")
                       .with_columns(_context_batch=((pl.col("len").cast(pl.UInt64).cum_sum()
                                                       - pl.col("len")) // 100_000))
                       .select("demand_group_id", "_context_batch"))
            context_part = context_part.join(batches, on="demand_group_id")
            score_part = profile_utility.select(["_timing_plan"] + utility_columns)
            for batch in context_part.partition_by("_context_batch", maintain_order=True):
                # All alternatives for one context remain together, including
                # modeled plans that originated from different reference times.
                utility_parts.append(batch.join(score_part, on="_timing_plan", how="inner")
                                     .select(PLAN_KEY_COLS + utility_columns)
                                     .unique(subset=PLAN_KEY_COLS, keep="first", maintain_order=True))
        utilities = pl.concat(utility_parts)
    return utilities
