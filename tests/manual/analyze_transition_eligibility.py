"""Estimate which captured choices match occupied plans in restricted phases.

Run after capture_transition_inputs.py. The capture's actual phase may differ:
this reports what mode, destination and no-transition restrictions would admit
for the captured state, not the outcome of running those phases. The first
sorted demand units are used to bound memory and are not a random sample.
"""

import argparse
from pathlib import Path

import polars as pl

from mobility.trips.group_day_trips.plans.demand_subgroups import DEMAND_UNIT_COLS
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.compact_candidate_steps import PLAN_COLUMNS


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--units", type=int, default=1000)
parser.add_argument("--unit-map", type=Path, help="Optional captured scoring-context map to compare.")
args = parser.parse_args()

current = pl.read_parquet(args.folder / "current-plans.parquet")
units = current.select(DEMAND_UNIT_COLS).unique().sort(DEMAND_UNIT_COLS).head(args.units)
current = current.join(units, on=DEMAND_UNIT_COLS, how="semi")
choices = (
    pl.scan_parquet(args.folder / "possible-plan-utility.parquet")
    .join(units.lazy(), on=DEMAND_UNIT_COLS, how="semi")
    .select(PLAN_KEY_COLS + ["reference_time_seq_id", "is_feasible", "utility"])
    .collect(engine="streaming")
)
origins = current.join(choices, on=PLAN_KEY_COLS, how="left")
if origins["is_feasible"].null_count():
    raise ValueError("Some occupied plans are absent from the captured choices.")

print("UNITS", units.height, "OCCUPIED_PLANS", current.height,
      "REPRESENTED_PEOPLE", round(current["n_persons"].sum(), 3),
      "CHOICES", choices.height, flush=True)
context_map = None
if args.unit_map:
    context_map = pl.read_parquet(args.unit_map).join(units, on=DEMAND_UNIT_COLS, how="semi")
    if context_map.height != units.height or context_map.select(DEMAND_UNIT_COLS).n_unique() != units.height:
        raise ValueError("The supplied map does not cover the selected demand units exactly once.")
    mapped = choices.join(context_map, on=DEMAND_UNIT_COLS)
    context_sizes = context_map.group_by("_score_context_id").len(name="unit_count")
    consistency = (
        mapped.group_by(["_score_context_id"] + PLAN_COLUMNS)
        .agg(pl.len().alias("rows"), pl.col("utility").n_unique().alias("utilities"),
             pl.col("is_feasible").n_unique().alias("feasibility"))
        .join(context_sizes, on="_score_context_id")
        .filter((pl.col("rows") != pl.col("unit_count")) | (pl.col("utilities") != 1) | (pl.col("feasibility") != 1))
    )
    if consistency.height:
        raise ValueError("The supplied contexts do not share identical choices and utilities in this capture.")
    total_context_choices = mapped.select(["_score_context_id"] + PLAN_COLUMNS).unique().height
    print("EXTERNAL_CONTEXTS", context_map["_score_context_id"].n_unique(),
          "CONTEXT_CHOICES", total_context_choices, flush=True)
for scope in ("mode", "destination", "no_transitions"):
    active = origins if scope == "no_transitions" else origins.filter(pl.col("mode_seq_id") != 0)
    forced_units = active.filter(~pl.col("is_feasible")).select(DEMAND_UNIT_COLS).unique()
    parts = [
        choices.join(current.select(PLAN_KEY_COLS), on=PLAN_KEY_COLS, how="semi").select(PLAN_KEY_COLS),
        choices.join(forced_units, on=DEMAND_UNIT_COLS, how="semi").select(PLAN_KEY_COLS),
    ]
    if scope != "no_transitions":
        keys = DEMAND_UNIT_COLS + ["activity_seq_id", "reference_time_seq_id"]
        if scope == "mode":
            keys += ["dest_seq_id"]
        parts.append(
            choices.filter(pl.col("mode_seq_id") != 0)
            .join(active.select(keys).unique(), on=keys, how="semi")
            .select(PLAN_KEY_COLS)
        )
    admitted = pl.concat(parts).unique()
    details = ["SCOPE", scope, "ADMITTED_CHOICES", admitted.height,
               "SHARE", round(admitted.height / choices.height, 4) if choices.height else None,
               "FORCED_UNITS", forced_units.height]
    if context_map is not None:
        context_choices = admitted.join(context_map, on=DEMAND_UNIT_COLS).select(
            ["_score_context_id"] + PLAN_COLUMNS
        ).unique().height
        details += ["ADMITTED_CONTEXT_CHOICES", context_choices,
                    "CONTEXT_SHARE", round(context_choices / total_context_choices, 4)]
    print(*details, flush=True)
