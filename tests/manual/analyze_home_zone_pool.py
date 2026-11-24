"""Compare saved home-zone choice sets with generated candidate utilities.

The generated-only probability is an upper bound: it assumes every generated
plan is absent from the home-zone pool, including the shared stay-home plan.
The saved contexts are a diagnostic sample, not a population-representative
simulation result.
"""
import argparse
import json
import math
from pathlib import Path

import polars as pl


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("home_pool", type=Path)
    parser.add_argument("generated_utility_pool", type=Path)
    parser.add_argument("--scale", type=float, default=0.25)
    parser.add_argument("--subgroup-people", type=float, default=100.0)
    args = parser.parse_args()
    baseline = pl.read_parquet(args.generated_utility_pool).filter(pl.col("is_feasible"))
    baseline_contexts = pl.read_parquet(args.generated_utility_pool.with_name(
        args.generated_utility_pool.name.replace("utility-pool", "context-utility")
    ))
    for file in sorted(args.home_pool.glob("allocation-*.parquet")):
        context = int(file.stem.removeprefix("allocation-"))
        allocation = pl.read_parquet(file)
        direct = pl.read_parquet(args.home_pool / f"utility-{context}.parquet").filter(pl.col("is_feasible"))
        generated = baseline.filter(pl.col("demand_group_id") == context)
        if generated.is_empty():
            raise ValueError(f"The generated pool has no feasible plans for context {context}.")
        best = max(direct["utility"].max(), generated["utility"].max())
        direct_weight = direct.select(((pl.col("utility") - best) * args.scale).exp().sum()).item()
        generated_weight = generated.select(((pl.col("utility") - best) * args.scale).exp().sum()).item()
        population = allocation["n_persons"].sum()
        reference_context = baseline_contexts.filter(pl.col("demand_group_id") == context)
        if reference_context.height != 1 or abs(reference_context["n_persons"].item() - population) > 1e-6:
            raise ValueError(f"The generated run has a different population for context {context}.")
        shares = allocation.with_columns(probability=pl.col("n_persons") / population).sort(
            "probability", descending=True
        ).with_columns(cumulative=pl.col("probability").cum_sum())
        home_utility = direct.filter(pl.col("mode_seq_id") == 0)["utility"].item()
        safe_below_floor = (
            direct.filter((pl.col("mode_seq_id") != 0)
                          & (pl.col("utility") < home_utility - math.log(args.subgroup_people) / args.scale)).height
            if args.scale > 0 and args.subgroup_people > 0 else 0
        )
        print(json.dumps({
            "context": context, "context_people": population,
            "direct_feasible": direct.height, "generated_feasible": generated.height,
            "direct_best": direct["utility"].max(), "generated_best": generated["utility"].max(),
            "generated_only_probability_upper_bound": generated_weight / (direct_weight + generated_weight),
            "plans_for_99_percent": shares.filter(pl.col("cumulative") < 0.99).height + 1,
            "plans_for_95_percent": shares.filter(pl.col("cumulative") < 0.95).height + 1,
            "fresh_probability_below_one_person": shares.filter(
                pl.col("probability") * args.subgroup_people < 1.0
            )["probability"].sum(),
            "plans_below_one_person_using_stay_home_bound": safe_below_floor,
        }))


if __name__ == "__main__":
    main()
