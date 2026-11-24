"""Compare cached and rebuilt section scores for consecutive real inner batches."""
import argparse
import json
from pathlib import Path
from time import perf_counter

import polars as pl
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.plans.factored_plan_scoring import SectionScoreCache, score_factored_sections
from mobility.trips.group_day_trips.plans.factored_plan_timing import time_factored_candidates
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
args = parser.parse_args()
score_keys = ["country", "csp", "activity", "from", "to", "mode", "utility_profile_id"]
score_values = ["cost", "time", "distance", "is_feasible", "_missing_cost_row", "_invalid_cost_row",
                "min_activity_time", "activity_utility_scale"]
unit = ["demand_group_id", "demand_subgroup_id"]
cache = SectionScoreCache()
previous_lookup = None
for batch in (1, 2):
    folder = args.folder / f"batch-{batch}"
    physical = pl.read_parquet(folder / "physical.parquet")
    memberships = pl.read_parquet(folder / "members.parquet")
    contexts = pl.read_parquet(folder / "contexts.parquet")
    settings = json.loads((folder / "settings.json").read_text(encoding="utf-8"))
    catalogue = pl.read_parquet(folder / "schedule-catalogue.parquet")
    lookup = pl.read_parquet(folder / "step-scores.parquet")
    if previous_lookup is not None:
        common = previous_lookup.join(lookup, on=score_keys, how="left", suffix="_next")
        if common.height != previous_lookup.height or common.filter(pl.any_horizontal([
            ~pl.col(name).eq_missing(pl.col(name + "_next")) for name in score_values
        ])).height:
            raise ValueError("Scoring inputs changed across batches; the cache is not valid.")
    previous_lookup = lookup
    travel = lookup.select("utility_profile_id", "from", "to", "mode", "time").unique()
    home = pl.read_parquet(folder / "home-durations.parquet")
    expected = pl.read_parquet(folder / "utilities.parquet")
    stay = expected.filter(pl.col("mode_seq_id") == 0)
    home_values = (stay.join(contexts.select(unit + ["country", "csp"]), on=unit).join(home, on=["country", "csp"])
                   .select((pl.col("utility") / pl.col("mean_home_night_per_pers") / 1.25).alias("value")))
    value = home_values["value"].drop_nulls().first()
    if home_values.filter((pl.col("value") - value).abs() > 1e-10).height:
        raise ValueError("Home utility does not have a common value in this capture.")
    start = perf_counter()
    timing = time_factored_candidates(physical, memberships, contexts, travel, catalogue, **settings)
    timing_seconds = perf_counter() - start
    old_sections = 0 if cache.sections is None else cache.sections.height
    old_scores = 0 if cache.scores is None else cache.scores.height
    kwargs = dict(rigid_activities={name for name, r in settings["activity_time_rigidity_by_activity"].items() if r == 1.0},
                  value_of_time_stay_home=value, min_activity_time_constant=1.25)
    start = perf_counter()
    retained = score_factored_sections(timing, contexts, lookup, home, cache=cache, **kwargs)
    retained_seconds = perf_counter() - start
    start = perf_counter()
    rebuilt = score_factored_sections(timing, contexts, lookup, home, **kwargs)
    rebuild_seconds = perf_counter() - start
    assert_frame_equal(retained.utilities.sort(PLAN_KEY_COLS), rebuilt.utilities.sort(PLAN_KEY_COLS), check_exact=True)
    mobile = expected.filter(pl.col("mode_seq_id") != 0)
    checked = retained.utilities.join(mobile, on=PLAN_KEY_COLS, suffix="_expected")
    if (checked.height != mobile.height or checked.height != retained.utilities.height
        or checked.filter((pl.col("is_feasible") != pl.col("is_feasible_expected"))
                          | (pl.col("missing_cost_row_count") != pl.col("missing_cost_row_count_expected"))
                          | (pl.col("invalid_cost_row_count") != pl.col("invalid_cost_row_count_expected"))).height):
        raise ValueError("Section scores differ in coverage or feasibility.")
    error = checked.filter(pl.col("is_feasible")).select(
        (pl.col("utility") - pl.col("utility_expected")).abs().max()).item()
    if error > 1e-8:
        raise ValueError(f"Section utility differs from captured scoring by {error}.")
    mapping = pl.read_parquet(folder / "modeled-mapping.parquet")
    keys = unit + ["activity_seq_id", "previous_time_seq_id", "dest_seq_id", "mode_seq_id"]
    actual_mapping = timing.context_mapping.drop("_timing_plan")
    compared = actual_mapping.join(mapping, on=keys, suffix="_expected")
    if (compared.height != mapping.height or compared.height != actual_mapping.height
        or compared.filter(pl.col("time_seq_id") != pl.col("time_seq_id_expected")).height):
        raise ValueError("Section timing changed modeled schedule IDs.")
    print("SECTION_INNER_BATCH", json.dumps({"batch": batch, "context_plans": memberships.height,
          "modeled_plans": retained.utilities.height, "timed_physical_steps": timing.steps.height,
          "retained_section_ids": old_sections, "new_sections": cache.sections.height - old_sections,
          "retained_profile_section_scores": old_scores, "new_profile_section_scores": cache.scores.height - old_scores,
          "section_cache_bytes": cache.sections.estimated_size() + cache.scores.estimated_size(),
          "timing_seconds": timing_seconds, "retained_scoring_seconds": retained_seconds,
          "rebuild_scoring_seconds": rebuild_seconds, "max_feasible_utility_error": error}), flush=True)
