"""Verify the proposed section boundaries against actual scored plan inputs.

This expands captured scoring contexts and constructs section rows for a
correctness check. It is not a fast evaluator or a cross-step timing benchmark.
"""
import argparse
import json
from pathlib import Path
from time import perf_counter

import numpy as np
import polars as pl

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.compact_candidate_steps import PLAN_COLUMNS

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--constant", type=float, default=1.25)
parser.add_argument("--index", type=Path, required=True)
args = parser.parse_args()
unit = ["demand_group_id", "demand_subgroup_id"]
for step in (1, 2):
    folder = args.folder / f"batch-{step}"
    settings = json.loads((folder / "settings.json").read_text(encoding="utf-8"))
    rigidity = settings["activity_time_rigidity_by_activity"]
    data = pl.read_parquet(folder / "expanded-scored.parquet").sort(PLAN_KEY_COLS + ["seq_step_index"])
    start = perf_counter()
    # Each trip uses the rigidities of both adjoining activities. Carry its
    # modeled arrival into the following activity, even at a section boundary.
    data = (data.with_columns(
        rigidity=pl.col("activity").cast(pl.String).replace_strict(rigidity, default=None).cast(pl.Float64))
        .with_columns(previous_rigidity=pl.col("rigidity").shift(1).over(PLAN_KEY_COLS).fill_null(rigidity.get("home", 0.0)),
                      delta=pl.col("time") - (pl.col("arrival_time") - pl.col("departure_time")).clip(0.0, 24.0))
        .with_columns(share=(1.0 + pl.col("rigidity") - pl.col("previous_rigidity")) / 2.0)
        .with_columns(departure_time=(pl.col("departure_time") - pl.col("share") * pl.col("delta")).cast(pl.Float32),
                      arrival_time=(pl.col("arrival_time") + (1.0 - pl.col("share")) * pl.col("delta")).cast(pl.Float32))
        .with_columns(next_departure_time=pl.col("departure_time").shift(-1).over(PLAN_KEY_COLS)
                      .fill_null(pl.col("next_departure_time")).cast(pl.Float32))
        .with_columns(duration_per_pers=(pl.col("next_departure_time") - pl.col("arrival_time")).clip(0.001).cast(pl.Float32))
        .with_columns(*[((pl.col(name).cast(pl.Float64) * 3600).round(0) / 3600).cast(pl.Float32).alias(name)
                         for name in ("departure_time", "arrival_time", "next_departure_time", "duration_per_pers")])
        .with_columns(is_last=pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS),
                      rigid=(pl.col("rigidity") == 1.0))
        .with_columns(section=pl.col("rigid").cast(pl.UInt32).cum_sum().over(PLAN_KEY_COLS)))
    data = data.join(pl.read_parquet(args.index / f"plans-{step}.parquet"), on=PLAN_COLUMNS, maintain_order="left")
    references = pl.read_parquet(args.index / f"references-{step}.parquet").rename({"section": "section_id"})
    key = PLAN_KEY_COLS + ["structural_plan", "section"]
    trips = data.select(key[:-1] + [(pl.col("section") - pl.col("rigid").cast(pl.UInt32)).alias("section"),
                         (-pl.col("cost")).alias("utility"), pl.col("time").cast(pl.Float64).alias("travel"),
                         pl.lit(0.0).alias("duration"), pl.col("is_feasible").alias("feasible")])
    activities = data.select(key + [
        (pl.col("activity_utility_scale") * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0)).alias("utility"),
        pl.lit(0.0).alias("travel"), pl.col("duration_per_pers").cast(pl.Float64).alias("duration"),
        (pl.col("is_last") | (pl.col("next_departure_time") >= pl.col("arrival_time"))).alias("feasible")])
    section_rows = (pl.concat([trips, activities]).join(
        references, left_on=["structural_plan", "section"], right_on=["structural_plan", "section_in_plan"], how="left"))
    if section_rows.height != data.height * 2 or section_rows["section_id"].null_count():
        raise ValueError("Stored section references did not cover all trip and activity rows.")
    sections = section_rows.group_by(PLAN_KEY_COLS + ["section_id"]).agg(
        pl.col("utility").sum(), pl.col("travel").sum(), pl.col("duration").sum(), pl.col("feasible").all())
    plans = sections.group_by(PLAN_KEY_COLS).agg(pl.col("utility").sum().alias("section_utility"),
        pl.col("travel").sum(), pl.col("duration").sum(), pl.col("feasible").all().alias("result_feasible"))
    contexts = pl.read_parquet(folder / "contexts.parquet")
    home = pl.read_parquet(folder / "home-durations.parquet")
    expected = pl.read_parquet(folder / "utilities.parquet")
    stay = expected.filter(pl.col("mode_seq_id") == 0).select(unit + [pl.col("utility").alias("stay_utility")])
    plans = (plans.join(contexts.select(unit + ["country", "csp"]), on=unit).join(home, on=["country", "csp"])
             .join(stay, on=unit).with_columns(home_hours=(24.0 - pl.col("duration") - pl.col("travel")).clip(0.0))
             .with_columns(result_utility=pl.col("section_utility") + pl.col("stay_utility") / args.constant
                 * (pl.col("home_hours") / (pl.col("mean_home_night_per_pers") * np.exp(-args.constant))).log().clip(0.0)))
    work_seconds = perf_counter() - start
    mapping = pl.read_parquet(folder / "modeled-mapping.parquet").rename(
        {"time_seq_id": "modeled_time_seq_id", "previous_time_seq_id": "time_seq_id"})
    matched = (plans.join(mapping, on=PLAN_KEY_COLS).join(
        expected.filter(pl.col("mode_seq_id") != 0).rename({"time_seq_id": "modeled_time_seq_id",
                    "utility": "expected_utility", "is_feasible": "expected_feasible"}),
        on=[name for name in PLAN_KEY_COLS if name != "time_seq_id"] + ["modeled_time_seq_id"]))
    if matched.height != plans.height or matched.filter(pl.col("result_feasible") != pl.col("expected_feasible")).height:
        raise ValueError("Section feasibility or plan coverage differs.")
    error = matched.filter(pl.col("expected_feasible")).select(
        (pl.col("result_utility") - pl.col("expected_utility")).abs().max()).item()
    if error > 1e-8:
        raise ValueError(f"Section utility differs by {error}.")
    ref = (pl.read_parquet(folder / "modeled-steps.parquet")
           .unique(subset=PLAN_KEY_COLS + ["seq_step_index"], keep="first")
           .rename({"time_seq_id": "modeled_time_seq_id"}))
    cols = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
    actual = (data.join(mapping, on=PLAN_KEY_COLS).join(
        ref.select([name for name in PLAN_KEY_COLS if name != "time_seq_id"] + ["modeled_time_seq_id", "seq_step_index"] + cols),
        on=[name for name in PLAN_KEY_COLS if name != "time_seq_id"] + ["modeled_time_seq_id", "seq_step_index"], suffix="_expected"))
    if actual.height != data.height or actual.filter(pl.any_horizontal(
        [~pl.col(name).eq_missing(pl.col(name + "_expected")) for name in cols])).height:
        raise ValueError("Section clock times or durations differ from captured timings.")
    print("SECTION_BOUNDARY_CHECK", json.dumps({"step": step, "plans": matched.height, "steps": actual.height,
          "section_occurrences": sections.height, "max_feasible_utility_error": error,
          "diagnostic_seconds": work_seconds}), flush=True)
