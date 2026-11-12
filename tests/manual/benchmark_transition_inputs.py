"""Compare transition calculation time and RAM on captured real inputs.

Run this in separate processes with METHOD ``previous`` and ``current`` after
``capture_transition_inputs.py``. The two order-independent fingerprints cover
all output columns and can be compared without writing a large transition table.
"""

import argparse
import json
import math
import threading
from pathlib import Path
from time import perf_counter

import polars as pl
import psutil

from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("method", choices=["previous", "current"])
parser.add_argument("--unit-limit", type=int)
args = parser.parse_args()
parameters = json.loads((args.folder / "parameters.json").read_text(encoding="utf-8"))
scope = BehaviorChangeScope(parameters.pop("behavior_change_scope"))
if scope != BehaviorChangeScope.FULL_REPLANNING or parameters["enable_transition_distance_model"]:
    raise ValueError("This comparison currently supports full replanning without transition distances.")
current = pl.read_parquet(args.folder / "current-plans.parquet")
choices = pl.scan_parquet(args.folder / "possible-plan-utility.parquet")
if args.unit_limit is not None:
    units = current.select("demand_group_id", "demand_subgroup_id").unique().sort(
        ["demand_group_id", "demand_subgroup_id"]
    ).head(args.unit_limit)
    current = current.join(units, on=["demand_group_id", "demand_subgroup_id"], how="semi")
    choices = choices.join(units.lazy(), on=["demand_group_id", "demand_subgroup_id"], how="semi")
updater = PlanUpdater()


def previous_pairs(current_plans, possible_plan_utility, behavior_change_scope, *,
                   transition_utility_pruning_delta, transition_logit_scale, min_transition_utility_gain):
    """Reproduce the former full-replanning join and post-join filters."""
    has_feasibility = "is_feasible" in possible_plan_utility.collect_schema().names()
    targets = possible_plan_utility.filter(pl.col("is_feasible")) if has_feasibility else possible_plan_utility
    forced = ~pl.col("is_feasible").fill_null(True) if has_feasibility else pl.lit(False)
    same = (
        (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
        & (pl.col("time_seq_id") == pl.col("time_seq_id_trans"))
        & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
        & (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
    )
    threshold = updater._scale_utility_pruning_delta(transition_utility_pruning_delta, transition_logit_scale)
    utility_filter = (
        forced | same | (pl.col("utility_trans") >= pl.col("max_utility_trans") - threshold)
        if math.isfinite(threshold) else pl.lit(True)
    )
    gain_filter = forced | same | (pl.col("utility_trans") >= pl.col("utility") + min_transition_utility_gain)
    return (
        current_plans.lazy().select(PLAN_KEY_COLS + ["utility"])
        .rename({"utility": "utility_prev_from"})
        .join(possible_plan_utility, on=PLAN_KEY_COLS)
        .rename({"plan_id": "plan_id_from"})
        .join_where(
            targets,
            (pl.col("demand_group_id") == pl.col("demand_group_id_trans"))
            & (pl.col("demand_subgroup_id") == pl.col("demand_subgroup_id_trans")),
            suffix="_trans",
        )
        .drop("demand_group_id_trans", "demand_subgroup_id_trans")
        .rename({"plan_id": "plan_id_trans"})
        .with_columns(max_utility_trans=pl.col("utility_trans").max().over(PLAN_KEY_COLS))
        .filter(utility_filter & gain_filter)
        .drop("max_utility_trans")
    )


if args.method == "previous":
    updater.build_allowed_plan_transitions = previous_pairs
stop = threading.Event()
process = psutil.Process()
peak = [process.memory_info().rss]


def sample_memory():
    while not stop.wait(0.05):
        peak[0] = max(peak[0], process.memory_info().rss)


thread = threading.Thread(target=sample_memory, daemon=True)
thread.start()
started = perf_counter()
try:
    result = updater.get_transition_probabilities(
        current, choices, pl.DataFrame(), scope, transport_zones=None, **parameters,
    )
finally:
    elapsed = perf_counter() - started
    stop.set()
    thread.join()
print("METHOD", args.method, "SECONDS", round(elapsed, 3), "PEAK_GIB", round(peak[0] / 2**30, 3), flush=True)
print("ROWS", result.height, "HASHES", result.hash_rows(seed=0).sum(), result.hash_rows(seed=1).sum(), flush=True)
