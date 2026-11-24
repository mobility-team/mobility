"""Compare capped survey schedules with a fully scored home-zone pool.

The input folder must contain an uncapped capture and ``top-K`` captures from
the same contexts. Utility ranking uses the fully scored pool and therefore
does not measure the cost of discovering the best schedules in advance.
"""
import argparse
import json
import math
from pathlib import Path

import polars as pl

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("--limits", default="10,100,1000")
    parser.add_argument("--scale", type=float, default=0.25)
    parser.add_argument("--transition-window", type=float, default=1.0)
    parser.add_argument("--min-gain", type=float, default=0.1)
    args = parser.parse_args()
    limits = [int(value) for value in args.limits.split(",")]
    for utility_file in sorted(args.folder.glob("utility-*.parquet")):
        context = int(utility_file.stem.removeprefix("utility-"))
        utilities = pl.read_parquet(utility_file).filter(pl.col("is_feasible"))
        mapping = pl.read_parquet(args.folder / f"mapping-{context}.parquet")
        alternatives = mapping.join(utilities, on=PLAN_KEY_COLS)
        home = utilities.filter(pl.col("mode_seq_id") == 0).select("plan_id")
        best = utilities["utility"].max()
        home_utility = utilities.filter(pl.col("mode_seq_id") == 0)["utility"].item()
        window = args.transition_window / args.scale if args.scale > 0 else math.inf
        transition_targets = utilities.filter(
            (pl.col("mode_seq_id") != 0) & (pl.col("utility") >= best - window)
            & (pl.col("utility") >= home_utility + args.min_gain)
        ).select("plan_id")
        weights = utilities.select(
            "plan_id", "utility", ((pl.col("utility") - best) * args.scale).exp().alias("weight")
        )
        total = weights["weight"].sum()
        ranked = (
            alternatives.group_by("activity_seq_id", "previous_time_seq_id")
            .agg(pl.col("utility").max().alias("best_utility"))
            .sort(["best_utility", "activity_seq_id", "previous_time_seq_id"], descending=[True, False, False])
        )
        for limit in limits:
            popularity = pl.read_parquet(args.folder / f"top-{limit}" / f"mapping-{context}.parquet")
            methods = [
                ("survey_weight", popularity.select("activity_seq_id", "previous_time_seq_id").unique()),
                ("utility_oracle", ranked.head(limit).select("activity_seq_id", "previous_time_seq_id")),
            ]
            first_pass = args.folder / f"utility-{limit}" / f"mapping-{context}.parquet"
            if first_pass.exists():
                methods.append(("utility_first_pass", pl.read_parquet(first_pass)
                                .select("activity_seq_id", "previous_time_seq_id").unique()))
            for method, schedules in methods:
                plan_ids = pl.concat([
                    alternatives.join(schedules, on=["activity_seq_id", "previous_time_seq_id"], how="semi")
                    .select("plan_id"), home,
                ]).unique()
                retained = weights.join(plan_ids, on="plan_id", how="semi")
                retained_weight = retained["weight"].sum()
                print(json.dumps({"context": context, "method": method, "k_schedules": limit,
                                  "retained_plans": retained.height,
                                  "best_utility_gap": best - retained["utility"].max(),
                                  "fraction_of_full_mnl_weight": retained_weight / total,
                                  "transition_targets": transition_targets.height,
                                  "transition_targets_missing": transition_targets.join(
                                      plan_ids, on="plan_id", how="anti"
                                  ).height,
                                  "mean_utility": retained.select(
                                      (pl.col("utility") * pl.col("weight")).sum() / pl.col("weight").sum()
                                  ).item()}))


if __name__ == "__main__":
    main()
