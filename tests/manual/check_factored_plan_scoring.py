"""Check shared timing and profile scoring against two real captured steps."""
import argparse
import json
from pathlib import Path
from time import perf_counter

import polars as pl

from mobility.trips.group_day_trips.plans.factored_plan_timing import time_factored_candidates
from mobility.trips.group_day_trips.plans.factored_plan_scoring import score_factored_candidates, score_factored_sections
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
args = parser.parse_args()
unit = ["demand_group_id", "demand_subgroup_id"]
score_keys = ["country", "csp", "activity", "from", "to", "mode", "utility_profile_id"]
values = ["cost", "time", "distance", "is_feasible", "_missing_cost_row", "_invalid_cost_row",
          "min_activity_time", "activity_utility_scale"]
for step in (1, 2):
    folder = args.folder / f"batch-{step}"
    physical = pl.read_parquet(folder / "physical.parquet")
    memberships = pl.read_parquet(folder / "members.parquet")
    contexts = pl.read_parquet(folder / "contexts.parquet")
    settings = json.loads((folder / "settings.json").read_text(encoding="utf-8"))
    catalogue = pl.read_parquet(folder / "schedule-catalogue.parquet")
    # Reconstruct the lookup from actual scoring inputs for both steps. It is
    # an input to this benchmark, not part of the measured scoring work.
    captured = pl.read_parquet(folder / "expanded-scored.parquet")
    if "utility_profile_id" not in captured.columns:
        captured = captured.join(contexts.select(unit + ["utility_profile_id"]), on=unit)
    lookup = captured.select(score_keys + values).unique()
    travel = lookup.select("utility_profile_id", "from", "to", "mode", "time").unique()
    home = pl.read_parquet(folder / "home-durations.parquet")
    expected = pl.read_parquet(folder / "utilities.parquet").filter(pl.col("mode_seq_id") != 0)
    stay = pl.read_parquet(folder / "utilities.parquet").filter(pl.col("mode_seq_id") == 0)
    # The captured stay-home utility is value * mean * constant. Infer the
    # common value solely for this benchmark's captured input.
    home_values = (stay.join(contexts.select(unit + ["country", "csp"]), on=unit)
                   .join(home, on=["country", "csp"])
                   .select((pl.col("utility") / pl.col("mean_home_night_per_pers") / 1.25).alias("value")))
    value = home_values["value"].drop_nulls().first()
    if home_values.filter((pl.col("value") - value).abs() > 1e-10).height:
        raise ValueError("Home utility does not have a common value in this capture.")
    start = perf_counter()
    timing = time_factored_candidates(physical, memberships, contexts, travel, catalogue, **settings)
    timing_seconds = perf_counter() - start
    start = perf_counter()
    result = score_factored_candidates(timing, contexts, lookup, home,
                                       value_of_time_stay_home=value, min_activity_time_constant=1.25)
    scoring_seconds = perf_counter() - start
    start = perf_counter()
    sections = score_factored_sections(
        timing, contexts, lookup, home,
        rigid_activities={name for name, r in settings["activity_time_rigidity_by_activity"].items() if r == 1.0},
        value_of_time_stay_home=value, min_activity_time_constant=1.25,
    )
    section_seconds = perf_counter() - start
    if not sections.utilities.sort(PLAN_KEY_COLS).equals(result.utilities.sort(PLAN_KEY_COLS)):
        raise ValueError("Section scoring differs from physical-plan scoring.")
    comparison = result.utilities.join(expected.select(PLAN_KEY_COLS + ["utility", "is_feasible", "missing_cost_row_count",
                                            "invalid_cost_row_count"]), on=PLAN_KEY_COLS, suffix="_expected")
    if (comparison.height != expected.height or comparison.height != result.utilities.height
        or comparison.filter((pl.col("is_feasible") != pl.col("is_feasible_expected"))
                             | (pl.col("missing_cost_row_count") != pl.col("missing_cost_row_count_expected"))
                             | (pl.col("invalid_cost_row_count") != pl.col("invalid_cost_row_count_expected"))).height):
        raise ValueError("Shared scores differ in plan coverage or feasibility.")
    error = comparison.filter(pl.col("is_feasible")).select(
        (pl.col("utility") - pl.col("utility_expected")).abs().max()).item()
    if error > 1e-8:
        raise ValueError(f"Shared utility differs by {error}.")
    captured_mapping = pl.read_parquet(folder / "modeled-mapping.parquet")
    mapping = timing.context_mapping.drop("_timing_plan")
    keys = unit + ["activity_seq_id", "previous_time_seq_id", "dest_seq_id", "mode_seq_id"]
    compare_mapping = mapping.join(captured_mapping, on=keys, suffix="_expected")
    if (compare_mapping.height != captured_mapping.height or compare_mapping.height != mapping.height
        or compare_mapping.filter(pl.col("time_seq_id") != pl.col("time_seq_id_expected")).height):
        raise ValueError("Shared timing assigns different modeled schedule IDs.")
    print("FACTORED_SCORING_CHECK", json.dumps({"step": step, "physical_steps": physical.height,
          "context_plans": memberships.height, "timed_physical_steps": timing.steps.height,
          "profile_steps": result.profile_steps.height, "modeled_plans": result.utilities.height,
          "section_profiles": sections.section_scores.height,
          "physical_section_references": sections.plan_sections.height,
          "timing_seconds": timing_seconds, "scoring_seconds": scoring_seconds,
          "section_scoring_seconds": section_seconds,
          "max_feasible_utility_error": error}), flush=True)
