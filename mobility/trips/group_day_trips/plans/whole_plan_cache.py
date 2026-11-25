"""Reuse complete plan scores while a scenario's scoring inputs are fixed."""
from dataclasses import dataclass

import polars as pl

from .compact_candidate_steps import PLAN_COLUMNS, STEP_COLUMNS
from .demand_subgroups import DEMAND_UNIT_COLS
from .factored_plan_scoring import FactoredSectionScores, map_class_utilities, score_factored_sections
from .factored_plan_timing import FactoredPlanTimings, time_factored_candidates
from .plan_ids import PLAN_KEY_COLS

PROFILE = ["country", "csp", "utility_profile_id"]
KEY = PLAN_COLUMNS + PROFILE
VALUES = ["utility", "is_feasible", "missing_cost_row_count", "invalid_cost_row_count"]
LOOKUP_KEY = PROFILE + ["activity", "from", "to", "mode"]
LOOKUP_VALUES = ["cost", "time", "is_feasible", "_missing_cost_row", "_invalid_cost_row", "min_activity_time", "activity_utility_scale"]


@dataclass
class WholePlanMemoryBudget:
    """Limit retained complete-plan data across scenarios in one outer step."""

    max_bytes: int = 3 * 1024 * 1024 * 1024
    used_bytes: int = 0


@dataclass
class WholePlanCache:
    """A scenario's scores, discarded at the end of its outer-step search."""

    budget: WholePlanMemoryBudget
    state: dict | None = None
    retained_bytes: int = 0

    def retain(self, state: dict | None) -> None:
        self.budget.used_bytes -= self.retained_bytes
        self.state = None
        self.retained_bytes = 0
        if state is None:
            return
        # Count references conservatively, including lookup and schedule tables.
        # Shared buffers can be counted more than once, but never omitted.
        size = sum(value.estimated_size() for value in state.values() if isinstance(value, pl.DataFrame))
        if self.budget.used_bytes + size <= self.budget.max_bytes:
            self.state = state
            self.retained_bytes = size
            self.budget.used_bytes += size


def make_state(physical, lookup, timing, scored, *, home_night_dur, home_value, min_activity_time_constant, rigidity, enabled):
    plans = timing.timing_plans.rename({"time_seq_id": "_modeled_time_seq_id", "previous_time_seq_id": "time_seq_id"})
    scores = (scored.class_utilities.join(plans, on="_timing_plan")
              .select(KEY + ["_timing_plan", "_modeled_time_seq_id"] + VALUES))
    if scores.select(KEY).n_unique() != scores.height:
        return None
    return {"scores": scores, "physical": physical, "lookup": lookup,
            "timing_steps": timing.steps, "timing_plans": timing.timing_plans,
            "schedule_steps": timing.time_sequence_steps, "home_night_dur": home_night_dur, "home_value": home_value,
            "min_activity_time_constant": min_activity_time_constant, "rigidity": dict(rigidity), "enabled": enabled}


def prepare_reuse(previous, classes, physical, lookup, schedule_steps, home_night_dur, home_value, min_activity_time_constant, rigidity, enabled):
    """Match current alternatives while verifying fixed inputs for this run."""
    if previous is None or lookup is None:
        return None
    if (previous["home_value"] != home_value or previous["min_activity_time_constant"] != min_activity_time_constant
            or previous["rigidity"] != rigidity or previous["enabled"] != enabled
            or not previous["home_night_dur"].equals(home_night_dur, null_equal=True)):
        return None
    # Modeled schedule IDs are meaningful only if prior indexed rows still
    # have the same contents in the current schedule catalogue.
    previous_schedule = previous["schedule_steps"]
    if (previous_schedule.schema != schedule_steps.schema or
        previous_schedule.join(schedule_steps, on=previous_schedule.columns, how="anti", nulls_equal=True).height):
        return None
    oldkeys = previous["scores"].select(KEY)
    matching = classes.join(oldkeys, on=KEY, how="semi", maintain_order="left")
    if matching.is_empty():
        return None
    newkeys = classes.join(oldkeys, on=KEY, how="anti", maintain_order="left")
    old_plans = matching.select(PLAN_COLUMNS).unique()
    current_old = physical.join(old_plans, on=PLAN_COLUMNS, how="semi")
    prior_old = previous["physical"].join(old_plans, on=PLAN_COLUMNS, how="semi")
    if (current_old.height != prior_old.height or
        current_old.join(prior_old, on=STEP_COLUMNS, how="anti", nulls_equal=True).height):
        return None
    before = previous["lookup"].select(LOOKUP_KEY + LOOKUP_VALUES)
    now = lookup.select(LOOKUP_KEY + LOOKUP_VALUES)
    if before.join(now, on=LOOKUP_KEY + LOOKUP_VALUES, how="anti", nulls_equal=True).height:
        return None
    return matching, newkeys


def run_reuse(previous, physical, memberships, profiles, lookup, schedule_steps, home_night_dur,
              home_value, min_activity_time_constant, rigidity, enabled, section_cache,
              time_index_cache, schedule_updater, prepared):
    contexts = (memberships.select(PLAN_KEY_COLS).unique(maintain_order=True)
                .join(profiles, on=DEMAND_UNIT_COLS, how="left", maintain_order="left"))
    matching, newkeys = prepared
    newtiming = None
    newscored = None
    if not newkeys.is_empty():
        newmembers = memberships.join(profiles, on=DEMAND_UNIT_COLS).join(newkeys, on=KEY, how="semi").select(memberships.columns)
        newphysical = physical.join(newkeys.select(PLAN_COLUMNS).unique(), on=PLAN_COLUMNS, how="semi")
        travel = lookup.select("utility_profile_id", "from", "to", "mode", "time").unique()
        newtiming = time_factored_candidates(newphysical, newmembers, profiles, travel, schedule_steps,
                     activity_time_rigidity_by_activity=rigidity, enabled=enabled,
                     index_cache=time_index_cache, schedule_updater=schedule_updater)
        newscored = score_factored_sections(newtiming, profiles, lookup, home_night_dur,
                     rigid_activities={n for n,v in rigidity.items() if v == 1.0},
                     value_of_time_stay_home=home_value, min_activity_time_constant=min_activity_time_constant,
                     cache=section_cache, map_contexts=False)
    oldscores = previous["scores"].join(matching, on=KEY, how="semi")
    oldids = oldscores.select("_timing_plan").unique()
    oldsteps = previous["timing_steps"].join(oldids, on="_timing_plan", how="semi")
    oldplans = previous["timing_plans"].join(oldids, on="_timing_plan", how="semi")
    if newtiming is not None:
        # Timing IDs are local to each evaluation. Move new IDs past the
        # retained range before joining the two sets of complete plans.
        offset = int(oldids["_timing_plan"].max()) + 1
        shifted_id = (pl.col("_timing_plan") + offset).cast(pl.UInt32)
        newsteps = newtiming.steps.with_columns(shifted_id)
        newplans = newtiming.timing_plans.with_columns(shifted_id)
        newclasses = newscored.class_utilities.with_columns(shifted_id)
        newstable = (newclasses.join(newplans.rename({"time_seq_id": "_modeled_time_seq_id", "previous_time_seq_id": "time_seq_id"}), on="_timing_plan")
                     .select(KEY + ["_timing_plan", "_modeled_time_seq_id"] + VALUES))
        allscores = pl.concat([oldscores, newstable])
        timing_steps = pl.concat([oldsteps, newsteps])
        timing_plans = pl.concat([oldplans, newplans])
        time_sequence_steps = newtiming.time_sequence_steps
    else:
        allscores = oldscores
        timing_steps = oldsteps
        timing_plans = oldplans
        time_sequence_steps = schedule_steps
    if allscores.select(KEY).n_unique() != allscores.height:
        return None
    fullcontexts = (contexts.join(allscores.select(KEY + ["_timing_plan", "_modeled_time_seq_id"]), on=KEY, how="left", maintain_order="left")
                    .rename({"time_seq_id": "previous_time_seq_id", "_modeled_time_seq_id": "time_seq_id"})
                    .select(DEMAND_UNIT_COLS + ["activity_seq_id", "previous_time_seq_id", "dest_seq_id", "mode_seq_id", "time_seq_id", "_timing_plan"] + PROFILE))
    if fullcontexts["_timing_plan"].null_count():
        return None
    class_scores = allscores.select(PROFILE + ["_timing_plan"] + VALUES).unique(maintain_order=True)
    utilities = map_class_utilities(class_scores, fullcontexts, PROFILE)
    timing = FactoredPlanTimings(timing_steps, fullcontexts.select(DEMAND_UNIT_COLS + ["_timing_plan"]),
                                 timing_plans, time_sequence_steps)
    scored = FactoredSectionScores(utilities, fullcontexts, class_scores)
    out = {"scores": allscores, "physical": physical, "lookup": lookup,
           "timing_steps": timing.steps, "timing_plans": timing.timing_plans,
           "schedule_steps": timing.time_sequence_steps, "home_night_dur": home_night_dur, "home_value": home_value,
           "min_activity_time_constant": min_activity_time_constant, "rigidity": dict(rigidity), "enabled": enabled}
    return timing, scored, out
