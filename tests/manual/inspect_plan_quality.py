"""Inspect sampled plan utilities and characteristics in a cached Geneva run.

Run with `pixi run python tests/manual/inspect_plan_quality.py`. The probe
stops after the chosen scenario and batch; it does not change model behavior.
The regular Geneva probe requires the input caches to already exist.
"""

import json
import os
import runpy
from pathlib import Path

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.demand_subgroups import DEMAND_UNIT_COLS
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


class CaptureComplete(Exception):
    """Stop once the requested choice-set summaries have been recorded."""


target_step = int(os.environ.get("MOBILITY_QUALITY_STEP", "2"))
target_batch = int(os.environ.get("MOBILITY_QUALITY_BATCH", "3"))
target_scenario = os.environ.get("MOBILITY_QUALITY_SCENARIO", "carbon_tax")
output = Path(os.environ.get("MOBILITY_QUALITY_OUTPUT", Path(os.environ["TEMP"]) / "mobility-plan-quality.jsonl"))
original_allocate = PlanUpdater.estimate_mnl_allocation
first_batch_keys = None


def capture(updater, evaluation, demand_groups, *, logit_scale, include_plan_steps=True):
    global first_batch_keys
    active = _ACTIVE_TRACE.get()
    labels = active[2] if active else {}
    capture_steps = (labels.get("step") == target_step and labels.get("scenario") == target_scenario
                     and labels.get("search_batch") == target_batch)
    allocation, steps = original_allocate(
        updater, evaluation, demand_groups, logit_scale=logit_scale,
        include_plan_steps=include_plan_steps or capture_steps,
    )
    if labels.get("step") != target_step or labels.get("scenario") != target_scenario:
        return allocation, steps
    batch = labels.get("search_batch")
    if batch == 1:
        first_batch_keys = allocation.select(PLAN_KEY_COLS).unique()
    if batch != target_batch:
        return allocation, steps

    # Each row is one feasible alternative for a scoring context. The utility
    # gap is relative to all feasible choices in that same context, including
    # staying home. Its allocation is the expected number of people under MNL.
    population = evaluation.scoring_contexts.demand_groups.select(
        DEMAND_UNIT_COLS + [pl.col("n_persons").alias("context_population")]
    )
    choices = (
        allocation.lazy()
        .with_columns(gap=pl.col("utility").max().over(DEMAND_UNIT_COLS) - pl.col("utility"))
        .join(population.lazy(), on=DEMAND_UNIT_COLS)
        .with_columns(probability=pl.col("n_persons") / pl.col("context_population"))
        .filter(pl.col("mode_seq_id") != 0)
    )
    # The steps were generated from the same scored plan and context. Count
    # modes and activities as overlapping indicators, not exclusive categories.
    features = (
        steps.lazy().filter(pl.col("mode_seq_id") != 0).group_by(PLAN_KEY_COLS).agg(
            pl.len().alias("legs"),
            (pl.col("arrival_time") - pl.col("departure_time")).sum().alias("travel_hours"),
            *[(pl.col("mode").cast(pl.String) == mode).any().alias(name) for mode, name in [
                ("car", "car"), ("walk", "walk"), ("bicycle", "bicycle"),
                ("walk/public_transport/walk", "public_transport"),
            ]],
            *[(pl.col("activity").cast(pl.String) == name).any().alias(name)
              for name in ["work", "studies", "shopping", "leisure", "other"]],
        )
    )
    joined = choices.join(features, on=PLAN_KEY_COLS, how="left")
    if first_batch_keys is None:
        raise ValueError("The first batch must be captured before the requested batch")
    # These are overlaps with batch one, not a claim about the exact draw that
    # produced each later alternative. Modeled timing may change across batches.
    comparisons = [
        ("seen_activity", DEMAND_UNIT_COLS + ["activity_seq_id"]),
        ("seen_activity_destination", DEMAND_UNIT_COLS + ["activity_seq_id", "dest_seq_id"]),
        ("seen_activity_destination_mode", DEMAND_UNIT_COLS + ["activity_seq_id", "dest_seq_id", "mode_seq_id"]),
        ("seen_batch_one", PLAN_KEY_COLS),
    ]
    for name, keys in comparisons:
        earlier = first_batch_keys.lazy().select(keys).unique().with_columns(pl.lit(True).alias(name))
        joined = joined.join(earlier, on=keys, how="left").with_columns(pl.col(name).fill_null(False))
    columns = ["legs", "travel_hours", "car", "walk", "bicycle", "public_transport",
               "work", "studies", "shopping", "leisure", "other"] + [name for name, _ in comparisons]
    joined = joined.with_columns(
        band=pl.when(pl.col("gap") <= 1).then(pl.lit("0–1"))
        .when(pl.col("gap") <= 4).then(pl.lit("1–4"))
        .when(pl.col("gap") <= 8).then(pl.lit("4–8"))
        .when(pl.col("gap") <= 12).then(pl.lit("8–12"))
        .otherwise(pl.lit("over 12"))
    )
    summary = joined.group_by("band").agg(
        pl.len().alias("options"), pl.col("n_persons").sum().alias("people"),
        pl.col("legs").is_null().sum().alias("missing_details"),
        *[pl.col(name).cast(pl.Float64).mean().alias(name) for name in columns],
        *[(pl.col(name).cast(pl.Float64) * pl.col("n_persons")).sum()
          .truediv(pl.col("n_persons").sum()).alias(name + "_weighted") for name in columns],
    ).sort("band").collect(engine="streaming")
    record = {"step": target_step, "batch": batch, "scenario": target_scenario,
              "logit_scale": logit_scale, "bands": summary.to_dicts()}
    output.write_text(json.dumps(record, indent=2), encoding="utf-8")
    print("PLAN_QUALITY", json.dumps(record), flush=True)
    raise CaptureComplete()


PlanUpdater.estimate_mnl_allocation = capture
try:
    runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
except CaptureComplete:
    print("PLAN_QUALITY_COMPLETE", output, flush=True)
