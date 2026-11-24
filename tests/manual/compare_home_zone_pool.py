"""Compare one direct home-zone pool with generated plans for the same context.

Pass a home-zone capture folder, a Geneva mnl-batches folder, and a context ID.
Both runs must use the same fixed costs and activity parameters. Plans are
matched by their scored physical steps rather than run-local sequence IDs.
"""
import argparse
import json
from pathlib import Path

import polars as pl

from mobility.trips.group_day_trips.core.multi_scenario import flow_change_index
from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


STEP_COLUMNS = ["seq_step_index", "activity", "from", "to", "mode", "departure_time",
                "arrival_time", "next_departure_time", "duration_per_pers"]


def plan_content(steps, allocation):
    return (
        steps.with_columns(pl.col("activity", "mode").cast(pl.String))
        .sort("plan_id", "seq_step_index").group_by("plan_id")
        .agg(pl.struct(STEP_COLUMNS).alias("steps"))
        .join(allocation.select("plan_id", "utility", "n_persons"), on="plan_id")
    )


def weighted_flows(plans, people):
    return (
        plans.explode("steps").unnest("steps").filter(pl.col("mode") != "stay_home")
        .group_by("from", "to", "mode", "activity")
        .agg((pl.col("probability") * people).sum().alias("volume"))
    )


def transition_from_home(plans, context, people, iteration, steps=1, scale=0.25):
    """Apply Geneva's transition rules to a fixed stay-home origin."""
    indexed = plans.with_row_index("_id", offset=1).with_columns(
        _is_home=pl.col("steps").list.first().struct.field("mode") == "stay_home"
    ).with_columns(
        demand_group_id=pl.lit(context, dtype=pl.UInt32),
        demand_subgroup_id=pl.lit(0, dtype=pl.UInt32),
        plan_id=pl.when(pl.col("_is_home")).then(0).otherwise(pl.col("_id")).cast(pl.UInt32),
        **{column: pl.when(pl.col("_is_home")).then(0).otherwise(pl.col("_id")).cast(pl.UInt32)
           for column in ("activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id")},
        is_feasible=pl.lit(True),
    )
    choices = indexed.select(PLAN_KEY_COLS + ["plan_id", "utility", "is_feasible"])
    current = choices.filter(pl.col("mode_seq_id") == 0).drop("is_feasible").with_columns(
        n_persons=pl.lit(people)
    )
    if current.height != 1:
        raise ValueError("The combined choice set must contain exactly one stay-home plan.")
    updater = PlanUpdater()
    for step in range(iteration, iteration + steps):
        transitions = updater.get_transition_probabilities(
            current, choices.lazy(), pl.DataFrame(), BehaviorChangeScope.FULL_REPLANNING,
            transport_zones=None, transition_revision_probability=0.4, transition_logit_scale=scale,
            transition_utility_pruning_delta=1.0, min_transition_utility_gain=0.1,
        )
        result, _ = updater.apply_transitions(
            current, transitions, step, min_plan_persons=1.0, plan_pruning_start_step=2,
            stay_home_plans=choices.filter(pl.col("mode_seq_id") == 0).lazy(),
            behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING, include_events=False,
        )
        current = result
    mobile = result.filter(pl.col("mode_seq_id") != 0).join(indexed.select("plan_id", "steps"), on="plan_id")
    return {"iteration": iteration + steps - 1, "people": people,
            "stay_home_people": result.filter(pl.col("mode_seq_id") == 0)["n_persons"].sum(),
            "mobile_people": mobile["n_persons"].sum(), "mobile_plans": mobile.height,
            "mobile_trips_per_person": mobile.select(
                (pl.col("steps").list.len() * pl.col("n_persons")).sum() / pl.col("n_persons").sum()
            ).item(),
            "mobile_mean_utility": mobile.select(
                (pl.col("utility") * pl.col("n_persons")).sum() / pl.col("n_persons").sum()
            ).item()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("home_pool", type=Path)
    parser.add_argument("generated", type=Path)
    parser.add_argument("context", type=int)
    parser.add_argument("--batch", type=int, default=10)
    parser.add_argument("--logit-scale", type=float, default=0.25)
    args = parser.parse_args()
    selected = json.loads((args.generated / "selected-subgroup.json").read_text(encoding="utf-8"))
    if selected["scoring_context"] != args.context:
        raise ValueError("The generated probe captured a different scoring context.")
    home = plan_content(
        pl.read_parquet(args.home_pool / f"steps-{args.context}.parquet"),
        pl.read_parquet(args.home_pool / f"allocation-{args.context}.parquet"),
    )
    generated = plan_content(
        pl.read_parquet(args.generated / f"batch-{args.batch}-subgroup-steps.parquet"),
        pl.read_parquet(args.generated / f"batch-{args.batch}-subgroup-plans.parquet"),
    )
    common = generated.join(home.select("steps", pl.col("utility").alias("home_utility")), on="steps")
    if common.height and common.select((pl.col("utility") - pl.col("home_utility")).abs().max()).item() > 1e-8:
        raise ValueError("Identical plans have different utilities; the runs are not comparable.")
    people = generated["n_persons"].sum()
    if abs(people - home["n_persons"].sum()) > 1e-6:
        raise ValueError("The captured contexts have different populations.")
    generated_only = generated.join(home.select("steps"), on="steps", how="anti")
    pooled = pl.concat([
        home.select("steps", "utility").with_columns(pl.lit("home").alias("source")),
        generated_only.select("steps", "utility").with_columns(pl.lit("generated").alias("source")),
    ])
    pooled = pooled.with_columns(
        _weight=((pl.col("utility") - pl.col("utility").max()) * args.logit_scale).exp()
    ).with_columns(probability=pl.col("_weight") / pl.col("_weight").sum())
    baseline = generated.with_columns(probability=pl.col("n_persons") / people)
    base_flows, pooled_flows = weighted_flows(baseline, people), weighted_flows(pooled, people)
    print(json.dumps({
        "context": args.context, "people": people, "generated_plans": generated.height,
        "home_plans": home.height, "common_plans": common.height, "pooled_plans": pooled.height,
        "generated_only_probability": pooled.filter(pl.col("source") == "generated")["probability"].sum(),
        "baseline_mean_utility": baseline.select((pl.col("utility") * pl.col("probability")).sum()).item(),
        "pooled_mean_utility": pooled.select((pl.col("utility") * pl.col("probability")).sum()).item(),
        "baseline_trips_per_person": base_flows["volume"].sum() / people,
        "pooled_trips_per_person": pooled_flows["volume"].sum() / people,
        "flow_change_index": flow_change_index(base_flows, pooled_flows),
        "baseline_by_mode": base_flows.group_by("mode").agg(pl.col("volume").sum()).sort("mode").to_dicts(),
        "pooled_by_mode": pooled_flows.group_by("mode").agg(pl.col("volume").sum()).sort("mode").to_dicts(),
        "transition_from_home_baseline_step_1": transition_from_home(baseline, args.context, 100.0, 1, scale=args.logit_scale),
        "transition_from_home_pooled_step_1": transition_from_home(pooled, args.context, 100.0, 1, scale=args.logit_scale),
        "transition_from_home_pooled_floor_sensitivity": transition_from_home(pooled, args.context, 100.0, 2, scale=args.logit_scale),
        "transition_from_home_baseline_step_10": transition_from_home(baseline, args.context, 100.0, 1, steps=10, scale=args.logit_scale),
        "transition_from_home_pooled_step_10": transition_from_home(pooled, args.context, 100.0, 1, steps=10, scale=args.logit_scale),
    }, indent=2))


if __name__ == "__main__":
    main()
