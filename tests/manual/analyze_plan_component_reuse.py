"""Measure reuse and duration-binning error in a captured Geneva scoring batch.

Usage: pixi run python tests/manual/analyze_plan_component_reuse.py CAPTURE_FOLDER

The capture contains reference and modeled steps, scoring inputs and rigidity
settings. This is an offline census, not a simulation or performance benchmark.
"""
import argparse
import json
from pathlib import Path
from time import perf_counter

import polars as pl

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--utility-capture", type=Path, help="Matching candidate-utility.parquet capture")
args = parser.parse_args()
start = perf_counter()
keys = PLAN_KEY_COLS + ["seq_step_index"]
source = pl.read_parquet(args.folder / "indexed-2.parquet")
steps = source.unique(subset=keys, keep="first").sort(keys)
reference = (pl.read_parquet(args.folder / "input-2.parquet")
             .unique(subset=keys, keep="first").sort(keys))
settings = json.loads((args.folder / "settings.json").read_text(encoding="utf-8"))
print("SETTINGS", json.dumps(settings), flush=True)
print("CAPTURE", json.dumps({"input_rows": source.height, "unique_steps": steps.height,
      "context_plans": steps.select(PLAN_KEY_COLS).n_unique(),
      "context_count": steps.select("demand_group_id", "demand_subgroup_id").n_unique()}), flush=True)

# The updater adjusts each trip from its own reference times, travel time and
# destination activity's rigidity. Activity duration also uses the following
# trip's adjusted departure (or the last step's reference end time).
trip_keys = ["activity", "departure_time", "arrival_time", "time"]
activity_keys = ["duration_per_pers", "min_activity_time", "activity_utility_scale"]
cost_keys = ["from", "to", "mode", "cost", "time", "is_feasible"]
print("EXACT_COMPONENTS", json.dumps({
    "trip_timing_inputs": reference.select(trip_keys).n_unique(),
    "cost_results_by_od_mode": steps.select(cost_keys).n_unique(),
    "activity_utility_inputs": steps.select(activity_keys).n_unique(),
}), flush=True)

# Keep the next trip only within the same complete input plan. The section key
# includes the current and next timing inputs and the current fallback end time;
# it is a conservative key for computing one modeled activity duration.
next_columns = [pl.col(name).shift(-1).over(PLAN_KEY_COLS).alias("following_" + name) for name in trip_keys]
paired = reference.with_columns(next_columns)
section_keys = trip_keys + ["next_departure_time"] + ["following_" + name for name in trip_keys]
print("EXACT_SECTIONS", json.dumps({"observed": paired.height,
      "unique_adjacent_timing_inputs": paired.select(section_keys).n_unique()}), flush=True)

# The existing scorer uses max(duration, 0.001 h) and a clipped logarithm.
# This experiment bins only the duration passed to the activity-utility term;
# it does not modify timings, routing cost, feasibility or the home-night term.
base = steps.with_columns(
    original_term=(pl.col("activity_utility_scale")
                   * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0))
)
choices = None
if args.utility_capture:
    selected_contexts = steps.select("demand_group_id", "demand_subgroup_id").unique()
    choices = (pl.read_parquet(args.utility_capture / "candidate-utility.parquet")
               .filter(pl.col("is_feasible"))
               .join(selected_contexts, on=["demand_group_id", "demand_subgroup_id"], how="semi"))
    mobile = choices.filter(pl.col("mode_seq_id") != 0)
    modeled = (steps.group_by(PLAN_KEY_COLS).agg(
        pl.col("is_feasible").all().alias("travel_cost_feasible"),
        ((pl.col("next_departure_time") >= pl.col("arrival_time"))
         | (pl.col("seq_step_index") == pl.col("seq_step_index").max())).all().alias("timing_feasible"),
    ).filter(pl.col("travel_cost_feasible") & pl.col("timing_feasible")))
    if (mobile.join(modeled, on=PLAN_KEY_COLS, how="anti").height
            or modeled.join(mobile, on=PLAN_KEY_COLS, how="anti").height):
        raise ValueError("Utility capture does not match the feasible plans in the timing capture.")
    timing_columns = ["activity", "from", "to", "mode", "departure_time", "arrival_time", "next_departure_time"]
    expected = (steps.join(mobile.select(PLAN_KEY_COLS + ["plan_id"]), on=PLAN_KEY_COLS)
                .select(["plan_id", "seq_step_index"] + timing_columns))
    observed = pl.read_parquet(args.utility_capture / "candidate-steps.parquet")
    paired_steps = expected.join(observed, on=["plan_id", "seq_step_index"], suffix="_capture")
    differences = [~pl.col(name).eq_missing(pl.col(name + "_capture")) for name in timing_columns]
    if paired_steps.height != expected.height or paired_steps.filter(pl.any_horizontal(differences)).height:
        raise ValueError("Utility capture has different physical or modeled timing steps.")
    print("MATCHED_CHOICE_CAPTURE", json.dumps({"feasible_mobile": mobile.height,
          "feasible_stay_home": choices.height - mobile.height, "matched_steps": expected.height}), flush=True)
for minutes in (1, 5, 15):
    interval = minutes / 60.0
    binned = base.with_columns(
        binned_duration=((pl.col("duration_per_pers").cast(pl.Float64) / interval)
                         .round(0) * interval).clip(lower_bound=0.001),
    ).with_columns(
        binned_term=(pl.col("activity_utility_scale")
                     * (pl.col("binned_duration") / pl.col("min_activity_time")).log().clip(0.0)),
    ).with_columns(term_error=pl.col("binned_term") - pl.col("original_term"))
    input_count = binned.select("binned_duration", "min_activity_time", "activity_utility_scale").n_unique()
    errors = (binned.group_by(PLAN_KEY_COLS).agg(
        pl.col("term_error").sum().alias("error"),
        pl.col("is_feasible").all().alias("travel_cost_feasible"),
        ((pl.col("next_departure_time") >= pl.col("arrival_time"))
         | (pl.col("seq_step_index") == pl.col("seq_step_index").max())).all().alias("timing_feasible"),
    ).filter(pl.col("travel_cost_feasible") & pl.col("timing_feasible"))
      .with_columns(pl.col("error").abs().alias("absolute_error")))
    summary = errors.select(
        pl.len().alias("feasible_plans"),
        pl.col("absolute_error").mean().alias("mean_absolute_error"),
        pl.col("absolute_error").median().alias("median_absolute_error"),
        pl.col("absolute_error").quantile(0.9).alias("p90_absolute_error"),
        pl.col("absolute_error").quantile(0.99).alias("p99_absolute_error"),
        pl.col("absolute_error").max().alias("max_absolute_error"),
        (pl.col("absolute_error") > 0.1).sum().alias("plans_above_0_1"),
        (pl.col("absolute_error") > 1.0).sum().alias("plans_above_1"),
    ).to_dicts()[0]
    summary.update(minutes=minutes, unique_activity_utility_inputs=input_count)
    print("DURATION_BIN", json.dumps(summary), flush=True)
    if choices is not None:
        # Stay-home has no binned activity term. Include it in the fresh MNL;
        # its full utility and all home-night terms remain at captured values.
        options = (choices.join(errors.select(PLAN_KEY_COLS + ["error"]), on=PLAN_KEY_COLS, how="left")
                   .with_columns(pl.col("error").fill_null(0.0))
                   .with_columns(
                       original_weight=((pl.col("utility") - pl.col("utility").max().over(
                           ["demand_group_id", "demand_subgroup_id"])) * 0.25).exp(),
                       binned_utility=pl.col("utility") + pl.col("error"),
                   ).with_columns(
                       binned_weight=((pl.col("binned_utility") - pl.col("binned_utility").max().over(
                           ["demand_group_id", "demand_subgroup_id"])) * 0.25).exp(),
                   ).with_columns(
                       original_prob=pl.col("original_weight") / pl.col("original_weight").sum().over(
                           ["demand_group_id", "demand_subgroup_id"]),
                       binned_prob=pl.col("binned_weight") / pl.col("binned_weight").sum().over(
                           ["demand_group_id", "demand_subgroup_id"]),
                   ))
        context_error = options.group_by("demand_group_id", "demand_subgroup_id").agg(
            ((pl.col("original_prob") - pl.col("binned_prob")).abs().sum() / 2).alias("tv")
        )
        populations = pl.read_parquet(args.utility_capture / "contexts.parquet").select(
            "demand_group_id", "demand_subgroup_id", "n_persons"
        )
        context_error = context_error.join(populations, on=["demand_group_id", "demand_subgroup_id"])
        print("FRESH_MNL_BIN", json.dumps({"minutes": minutes, "contexts": context_error.height,
              "population_weighted_tv": float((context_error["tv"] * context_error["n_persons"]).sum()
                                               / context_error["n_persons"].sum()),
              "median_context_tv": float(context_error["tv"].median()),
              "p90_context_tv": float(context_error["tv"].quantile(0.9)),
              "max_context_tv": float(context_error["tv"].max())}), flush=True)

# Linear interpolation needs two neighboring values instead of one; it is an
# accuracy experiment only and does not establish that lookup is faster.
for minutes in (5, 15):
    interval = minutes / 60.0
    interpolated = (base.with_columns(
        lower=(pl.col("duration_per_pers").cast(pl.Float64) / interval).floor() * interval,
    ).with_columns(
        upper=pl.col("lower") + interval,
        fraction=(pl.col("duration_per_pers").cast(pl.Float64) - pl.col("lower")) / interval,
    ).with_columns(
        lower_term=(pl.col("lower").clip(lower_bound=0.001) / pl.col("min_activity_time")).log().clip(0.0),
        upper_term=(pl.col("upper").clip(lower_bound=0.001) / pl.col("min_activity_time")).log().clip(0.0),
    ).with_columns(
        term_error=(pl.col("activity_utility_scale")
                    * (pl.col("lower_term") * (1 - pl.col("fraction"))
                       + pl.col("upper_term") * pl.col("fraction")) - pl.col("original_term"))
    ))
    errors = (interpolated.group_by(PLAN_KEY_COLS).agg(
        pl.col("term_error").sum().abs().alias("absolute_error"),
        pl.col("is_feasible").all().alias("travel_cost_feasible"),
        ((pl.col("next_departure_time") >= pl.col("arrival_time"))
         | (pl.col("seq_step_index") == pl.col("seq_step_index").max())).all().alias("timing_feasible"),
    ).filter(pl.col("travel_cost_feasible") & pl.col("timing_feasible")))
    print("INTERPOLATED_BIN", json.dumps({"minutes": minutes,
          "mean_absolute_error": float(errors["absolute_error"].mean()),
          "p90_absolute_error": float(errors["absolute_error"].quantile(0.9)),
          "p99_absolute_error": float(errors["absolute_error"].quantile(0.99)),
          "max_absolute_error": float(errors["absolute_error"].max())}), flush=True)

# Rounding both sides of the timing-feasibility check is a separate thought
# experiment. It does not claim that a full binned timing model behaves this way.
non_last = steps.filter(pl.col("seq_step_index") != pl.col("seq_step_index").max().over(PLAN_KEY_COLS))
for minutes in (1, 5, 15):
    interval = minutes / 60.0
    a = (pl.col("arrival_time").cast(pl.Float64) / interval).round(0)
    d = (pl.col("next_departure_time").cast(pl.Float64) / interval).round(0)
    changes = non_last.with_columns(
        before=pl.col("next_departure_time") >= pl.col("arrival_time"),
        after=d >= a,
    ).filter(pl.col("before") != pl.col("after"))
    print("TIMING_CHECK_BIN", json.dumps({"minutes": minutes, "checked_steps": non_last.height,
          "changed_steps": changes.height, "changed_plans": changes.select(PLAN_KEY_COLS).n_unique(),
          "infeasible_made_feasible": changes.filter(~pl.col("before") & pl.col("after")).height}), flush=True)

print("ELAPSED_SECONDS", round(perf_counter() - start, 2), flush=True)
