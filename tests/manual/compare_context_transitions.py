"""Compare unit and context transition allocations on a captured choice set.

The context map can come from a separate capture; identical choices, utilities
and feasibility are checked before using it. The first sorted demand units are
selected to bound memory and are not a random sample.
"""

import argparse
import json
from pathlib import Path
from time import perf_counter

import polars as pl
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from mobility.trips.group_day_trips.plans.compact_candidate_steps import PLAN_COLUMNS
from mobility.trips.group_day_trips.plans.demand_subgroups import DEMAND_UNIT_COLS
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS, add_plan_id
from mobility.trips.group_day_trips.plans.plan_updater import CandidatePlanEvaluation, PlanUpdater
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("unit_map", type=Path)
parser.add_argument("--units", type=int, default=1000)
args = parser.parse_args()
parameters = json.loads((args.folder / "parameters.json").read_text(encoding="utf-8"))
scope = BehaviorChangeScope(parameters.pop("behavior_change_scope"))
if parameters["enable_transition_distance_model"]:
    raise ValueError("This capture does not include the step details needed for transition distances.")

current = pl.read_parquet(args.folder / "current-plans.parquet")
units = current.select(DEMAND_UNIT_COLS).unique().sort(DEMAND_UNIT_COLS).head(args.units)
current = current.join(units, on=DEMAND_UNIT_COLS, how="semi")
unit_map = pl.read_parquet(args.unit_map).join(units, on=DEMAND_UNIT_COLS, how="semi")
if unit_map.height != units.height or unit_map.select(DEMAND_UNIT_COLS).n_unique() != units.height:
    raise ValueError("The supplied map must cover the selected units exactly once.")
choices = (
    pl.scan_parquet(args.folder / "possible-plan-utility.parquet")
    .join(units.lazy(), on=DEMAND_UNIT_COLS, how="semi").collect(engine="streaming")
)
mapped = choices.join(unit_map, on=DEMAND_UNIT_COLS)
context_sizes = unit_map.group_by("_score_context_id").len(name="units")
check = (
    mapped.group_by(["_score_context_id"] + PLAN_COLUMNS)
    .agg(pl.len().alias("rows"), pl.col("utility").n_unique().alias("utilities"),
         pl.col("is_feasible").n_unique().alias("feasibility"),
         pl.col("reference_time_seq_id").n_unique().alias("references"))
    .join(context_sizes, on="_score_context_id")
    .filter((pl.col("rows") != pl.col("units")) | (pl.col("utilities") != 1)
            | (pl.col("feasibility") != 1) | (pl.col("references") != 1))
)
if check.height:
    raise ValueError("The supplied map groups units with different captured choices.")

context_choices = (
    mapped.drop(DEMAND_UNIT_COLS + ["plan_id"])
    .rename({"_score_context_id": "demand_group_id"})
    .with_columns(demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
    .unique()
)
context_choices, index = add_plan_id(context_choices, previous_index=None)
context_origins = (
    current.join(unit_map, on=DEMAND_UNIT_COLS).drop(DEMAND_UNIT_COLS + ["plan_id"])
    .rename({"_score_context_id": "demand_group_id"})
    .with_columns(demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
    .group_by(PLAN_KEY_COLS)
    .agg(pl.col("n_persons").sort().implode().list.sum(), pl.col("utility").first())
    .join(index, on=PLAN_KEY_COLS)
)
updater = PlanUpdater()
times = choices.select("time_seq_id", "reference_time_seq_id").unique()
if times.group_by("time_seq_id").len().filter(pl.col("len") > 1).height:
    raise ValueError("The captured time IDs do not have unique reference times.")
compact = CandidatePlanEvaluation(
    candidate_plan_steps=pl.DataFrame(), possible_plan_steps=pl.DataFrame(),
    possible_plan_utility=context_choices.drop("reference_time_seq_id"), plan_id_index=index,
    time_sequence_steps=times, scoring_contexts=ScoringContexts(pl.DataFrame(), unit_map),
    time_mapping=context_choices.select(PLAN_KEY_COLS).with_columns(previous_time_seq_id=pl.col("time_seq_id")),
    current_plans=context_origins,
)
unit_index = choices.select(PLAN_KEY_COLS + ["plan_id"])
expanded = CandidatePlanEvaluation(
    candidate_plan_steps=pl.DataFrame(), possible_plan_steps=pl.DataFrame(),
    possible_plan_utility=choices, plan_id_index=unit_index, current_plans=current,
    compact_transition_evaluation=compact,
)
started = perf_counter()
shared_probabilities = updater.get_context_transition_probabilities(
    expanded, scope, transport_zones=None, **parameters,
)
shared_seconds = perf_counter() - started
direct_probabilities = updater.get_transition_probabilities(
    current, choices.lazy(), pl.DataFrame(), scope, transport_zones=None, **parameters,
)
keys = ["plan_id_from", "plan_id_trans"]
assert_frame_equal(shared_probabilities.select(direct_probabilities.columns).sort(keys),
                   direct_probabilities.sort(keys), check_exact=False, rel_tol=1e-10, abs_tol=1e-8)
print("SHARED_PROBABILITIES_MATCH", shared_probabilities.height,
      "SHARED_SECONDS", round(shared_seconds, 3), flush=True)
needed_ids = pl.concat([
    current.select("plan_id").rename({"plan_id": "plan_id_trans"}),
    shared_probabilities.select("plan_id_trans"),
]).unique()
print("UNIT_INDEX_CHOICES", choices.select("plan_id").n_unique(),
      "OCCUPIED_OR_REACHABLE", needed_ids.height, flush=True)
started = perf_counter()
context_probabilities = updater.get_transition_probabilities(
    context_origins, context_choices.lazy(), pl.DataFrame(), scope, transport_zones=None, **parameters,
)
probability_ids = pl.concat([
    context_origins.select("plan_id"),
    context_probabilities.select(pl.col("plan_id_trans").alias("plan_id")),
]).unique()
probability_seconds = perf_counter() - started
started = perf_counter()
active = context_origins if scope == BehaviorChangeScope.NO_TRANSITIONS else context_origins.filter(pl.col("mode_seq_id") != 0)
forced = (
    active.select(PLAN_KEY_COLS)
    .join(context_choices.select(PLAN_KEY_COLS + ["is_feasible"]), on=PLAN_KEY_COLS, how="left")
    .filter(~pl.col("is_feasible").fill_null(False))
    .select(DEMAND_UNIT_COLS).unique()
)
structural = updater._reachable_scoring_candidates(
    context_choices, context_origins, times, scope, forced,
)
structural_seconds = perf_counter() - started
assert probability_ids.join(structural.select("plan_id"), on="plan_id", how="anti").is_empty()
print("CONTEXT_STEP_SELECTION", "EXACT", probability_ids.height, "STRUCTURAL", structural.height,
      "EXACT_SECONDS", round(probability_seconds, 3), "STRUCTURAL_SECONDS", round(structural_seconds, 3), flush=True)


def estimate(origins, options, min_persons):
    started = perf_counter()
    probabilities = updater.get_transition_probabilities(
        origins, options.lazy(), pl.DataFrame(), scope, transport_zones=None, **parameters,
    )
    allocation, _ = updater.apply_transitions(
        origins, probabilities, iteration=6,
        min_plan_persons=min_persons,
        stay_home_plans=options.filter(pl.col("mode_seq_id") == 0).select(DEMAND_UNIT_COLS + ["plan_id", "utility"]),
        plan_pruning_start_step=2,
    )
    return allocation, probabilities.height, perf_counter() - started


def aggregate_units(allocation):
    return (
        allocation.join(unit_map, on=DEMAND_UNIT_COLS)
        .group_by(["_score_context_id"] + PLAN_COLUMNS)
        .agg(pl.col("n_persons").sort().implode().list.sum())
        .sort(["_score_context_id"] + PLAN_COLUMNS)
    )


def aggregate_contexts(allocation):
    return (
        allocation.rename({"demand_group_id": "_score_context_id"})
        .select(["_score_context_id"] + PLAN_COLUMNS + ["n_persons"])
        .sort(["_score_context_id"] + PLAN_COLUMNS)
    )


print("UNITS", units.height, "CONTEXTS", context_sizes.height,
      "UNIT_CHOICES", choices.height, "CONTEXT_CHOICES", context_choices.height, flush=True)
for min_persons in (0.0, 1.0):
    unit_allocation, unit_pairs, unit_seconds = estimate(current, choices, min_persons)
    started = perf_counter()
    reachable = updater.estimate_reachable_allocation(
        compact, current, 6, scope, transport_zones=None,
        min_plan_persons=min_persons,
        plan_pruning_start_step=2, **parameters,
    )
    reachable_seconds = perf_counter() - started
    expected_reachable = aggregate_units(unit_allocation)
    actual_reachable = aggregate_contexts(reachable)
    assert_frame_equal(actual_reachable, expected_reachable, check_exact=False, rel_tol=1e-10, abs_tol=1e-8)
    print("REACHABLE_ALLOCATION_MATCH", min_persons, "SECONDS", round(reachable_seconds, 3), flush=True)
    context_allocation, context_pairs, context_seconds = estimate(context_origins, context_choices, min_persons)
    actual = aggregate_units(unit_allocation)
    compact_allocation = aggregate_contexts(context_allocation)
    joined = actual.join(compact_allocation, on=["_score_context_id"] + PLAN_COLUMNS, how="full", coalesce=True).with_columns(
        pl.col("n_persons").fill_null(0.0), pl.col("n_persons_right").fill_null(0.0),
    )
    moved = (joined["n_persons"] - joined["n_persons_right"]).abs().sum() / 2.0
    if min_persons == 0.0:
        assert_frame_equal(actual, compact_allocation, check_exact=False, rel_tol=1e-10, abs_tol=1e-8)
    print("MIN_PLAN_PERSONS", min_persons, "UNIT_PAIRS", unit_pairs, "CONTEXT_PAIRS", context_pairs,
          "UNIT_SECONDS", round(unit_seconds, 3), "CONTEXT_SECONDS", round(context_seconds, 3),
          "REALLOCATION_DIFFERENCE", round(moved, 6), flush=True)
