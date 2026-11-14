"""Compare two fixed-state sampling probes against their combined best utility.

Run the Geneva probe with MOBILITY_PROBE_MNL_BATCHES=1 and
MOBILITY_PROBE_UTILITY_POOLS=1 for each method, then pass their work folders.
The combined best is a finite-sample benchmark, not the true optimum.
"""
import argparse
import json
from pathlib import Path

import polars as pl


KEYS = ["demand_group_id", "demand_subgroup_id"]


def read_run(folder):
    summaries = [json.loads(line) for line in (folder / "mnl-batches" / "batches.jsonl").read_text().splitlines()]
    trace = [json.loads(line) for line in (folder / "trace.jsonl").read_text().splitlines()]
    durations = {row["search_batch"]: row["wall_seconds"] for row in trace if row["operation"] == "search batch"}
    elapsed = 0.0
    for row in summaries:
        elapsed += durations[row["batch"]] - row["diagnostic_seconds"]
        row["search_seconds"] = elapsed
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()
    folders = {"baseline": args.baseline, "candidate": args.candidate}
    runs = {name: read_run(folder) for name, folder in folders.items()}
    scales = {row["logit_scale"] for run in runs.values() for row in run}
    if len(scales) != 1:
        raise ValueError("The runs must use the same choice scale")
    scale = scales.pop()
    if scale <= 0:
        raise ValueError("The choice scale must be positive")
    best = {}
    for name, folder in folders.items():
        batch = runs[name][-1]["batch"]
        best[name] = pl.read_parquet(folder / "mnl-batches" / f"batch-{batch}-context-utility.parquet")
    reference = best["baseline"].join(best["candidate"], on=KEYS, suffix="_other", how="full", coalesce=True)
    if (reference.height != best["baseline"].height or reference.height != best["candidate"].height
            or reference.select(pl.any_horizontal(pl.all().is_null()).any()).item()
            or reference.select((pl.col("n_persons") - pl.col("n_persons_other")).abs().max()).item() > 1e-6):
        raise ValueError("The runs do not have the same scoring contexts and population")
    reference = reference.select(*KEYS, "n_persons",
                                 pl.max_horizontal("best_utility", "best_utility_other").alias("reference"))
    times = [min(run[-1]["search_seconds"] for run in runs.values()) * fraction for fraction in (0.5, 1.0)]
    for name, folder in folders.items():
        for target in sorted(set(times + [runs[name][-1]["search_seconds"]])):
            completed = [row for row in runs[name] if row["search_seconds"] <= target]
            if not completed:
                continue
            row = completed[-1]
            batch = row["batch"]
            context = pl.read_parquet(folder / "mnl-batches" / f"batch-{batch}-context-utility.parquet")
            context = context.join(reference.select(KEYS + ["reference"]), on=KEYS)
            gap = context.select(
                ((pl.col("reference") - pl.col("best_utility")) * pl.col("n_persons")).sum()
                / pl.col("n_persons").sum()
            ).item()
            pool = pl.read_parquet(folder / "mnl-batches" / f"batch-{batch}-utility-pool.parquet")
            pool = pool.join(reference.select(KEYS + ["reference"]), on=KEYS)
            qualifying = pool.filter(
                (pl.col("mode_seq_id") != 0) & pl.col("is_feasible")
                & ((pl.col("reference") - pl.col("utility")) * scale <= 4.605170186)
            ).height
            print(json.dumps({"method": name, "comparison_seconds": target, "completed_batch": batch,
                              "search_seconds": row["search_seconds"], "mean_best_utility_gap": gap,
                              "qualifying_plan_contexts": qualifying,
                              "qualifying_per_second": qualifying / row["search_seconds"],
                              "mobile_candidates": row["mobile_candidates"],
                              "infeasible_candidates": row["infeasible_candidates"], "logit_scale": scale}))


if __name__ == "__main__":
    main()
