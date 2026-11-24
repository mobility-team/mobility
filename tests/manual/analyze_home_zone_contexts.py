"""Compare fixed-state survey and home-pool choices in captured contexts."""
import argparse
import json
from pathlib import Path

import polars as pl

from mobility.trips.group_day_trips.plans.activity_profiles import get_survey_schedule_activities


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
args = parser.parse_args()
folder = args.folder
survey = pl.read_parquet(folder / "survey_plans.parquet").filter(pl.col("is_weekday"))
steps = pl.read_parquet(folder / "survey_plan_steps.parquet")
contexts = pl.read_parquet(folder / "scoring-contexts.parquet")
lengths = steps.group_by("activity_seq_id", "time_seq_id").agg(pl.len().alias("trips"))
sequence_lengths = lengths.group_by("activity_seq_id").agg(
    pl.col("trips").first(), pl.col("trips").n_unique().alias("length_count"),
)
if sequence_lengths.filter(pl.col("length_count") != 1).height:
    raise ValueError("A survey activity sequence has more than one number of steps.")
sequence_lengths = sequence_lengths.drop("length_count")
schedules = (survey.group_by("country", "activity_seq_id", "time_seq_id")
             .agg(pl.col("plan_weight_mass").sum().alias("weight"))
             .join(get_survey_schedule_activities(steps), on=["activity_seq_id", "time_seq_id"])
             .join(lengths, on=["activity_seq_id", "time_seq_id"]))
records = []
for path in sorted(folder.glob("allocation-*.parquet")):
    context_id = int(path.stem.split("-")[-1])
    group = contexts.filter(pl.col("demand_group_id") == context_id).row(0, named=True)
    eligible = schedules.filter(
        (pl.col("country") == group["country"])
        & (pl.col("activities").list.set_difference(
            pl.lit(group["allowed_activities"], dtype=pl.List(pl.String))).list.len() == 0)
    )
    allocations = (pl.read_parquet(path).filter(pl.col("mode_seq_id") != 0)
                   .join(sequence_lengths, on="activity_seq_id", how="left"))
    if allocations["trips"].null_count():
        raise ValueError(f"Context {context_id} has a plan without survey steps.")
    utility = pl.read_parquet(folder / f"utility-{context_id}.parquet")
    best = (utility.filter(pl.col("is_feasible") & (pl.col("mode_seq_id") != 0))
            .sort("utility", descending=True).head(1).join(sequence_lengths, on="activity_seq_id"))
    survey_mass = eligible["weight"].sum()
    persons = allocations["n_persons"].sum()
    records.append({
        "context": context_id, "country": group["country"], "csp": group["csp"],
        "mobile_people": persons, "best_trips": best["trips"].item(),
        "survey_mean_trips": eligible.select((pl.col("trips") * pl.col("weight")).sum()).item() / survey_mass,
        "survey_8_plus_share": eligible.filter(pl.col("trips") >= 8)["weight"].sum() / survey_mass,
        "choice_mean_trips": allocations.select((pl.col("trips") * pl.col("n_persons")).sum()).item() / persons,
        "choice_8_plus_share": allocations.filter(pl.col("trips") >= 8)["n_persons"].sum() / persons,
    })
result = pl.DataFrame(records)
print("CONTEXT_RESULTS", json.dumps(result.to_dicts()))
print("SUMMARY", json.dumps(result.select(
    pl.len().alias("contexts"), pl.col("best_trips").min().alias("best_trips_min"),
    pl.col("best_trips").median().alias("best_trips_median"),
    pl.col("survey_mean_trips").median().alias("survey_mean_median"),
    pl.col("choice_mean_trips").median().alias("choice_mean_median"),
    pl.col("survey_8_plus_share").median().alias("survey_8_plus_median"),
    pl.col("choice_8_plus_share").median().alias("choice_8_plus_median"),
    (pl.col("choice_mean_trips") > pl.col("survey_mean_trips")).sum().alias("longer_choice_contexts"),
).to_dicts()))
