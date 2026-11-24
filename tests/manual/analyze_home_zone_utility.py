"""Explain scored home-zone plans from a saved single-context Geneva capture.

Usage: pixi run python tests/manual/analyze_home_zone_utility.py FOLDER [CONTEXT]
The capture must contain scored steps, plan utilities, allocations, time mappings
and survey plans. This examines a fixed first-step choice set, not a simulation.
"""
import argparse
import json
import math
from pathlib import Path

import polars as pl
from mobility.trips.group_day_trips.plans.activity_profiles import get_survey_schedule_activities


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("context", type=int, nargs="?", default=2946)
args = parser.parse_args()
folder, context_id = args.folder, args.context
steps = pl.read_parquet(folder / f"steps-{context_id}.parquet")
utilities = pl.read_parquet(folder / f"utility-{context_id}.parquet")
allocation = pl.read_parquet(folder / f"allocation-{context_id}.parquet")
mapping = pl.read_parquet(folder / f"mapping-{context_id}.parquet")
context = pl.read_parquet(folder / "scoring-contexts.parquet").filter(pl.col("demand_group_id") == context_id)
country, csp = context.select("country", "csp").row(0)
survey_rows = pl.read_parquet(folder / "survey_plans.parquet").filter(
    (pl.col("country").cast(pl.String) == country) & pl.col("is_weekday")
)
survey = survey_rows.group_by("activity_seq_id", "time_seq_id").agg(
    pl.col("plan_weight_mass").sum().alias("country_weight"),
    pl.col("plan_weight_mass").filter(pl.col("csp").cast(pl.String) == csp).sum().alias("csp_weight"),
)
allowed = context["allowed_activities"].item().to_list()
schedule_activities = get_survey_schedule_activities(pl.read_parquet(folder / "survey_plan_steps.parquet"))
eligible_survey = survey.join(schedule_activities, on=["activity_seq_id", "time_seq_id"]).filter(
    pl.col("activities").list.set_difference(pl.lit(allowed, dtype=pl.List(pl.String))).list.len() == 0
)
sampling_mass = eligible_survey["country_weight"].sum()

# Step utility is activity utility minus generalized travel cost. The rest of
# total plan utility is the separately scored time at home outside the steps.
details = steps.group_by("plan_id").agg(
    pl.len().alias("trips"), pl.col("mode").cast(pl.String).first().alias("mode"),
    pl.col("activity").sort_by("seq_step_index").alias("activities"),
    pl.col("duration_per_pers").sum().alias("activity_hours"),
    pl.col("time").sum().alias("travel_hours"), pl.col("distance").sum().alias("distance_km"),
    pl.col("cost").sum().alias("travel_cost"),
    (pl.col("utility") + pl.col("cost")).sum().alias("activity_utility"),
    pl.col("utility").sum().alias("step_utility"),
)
keys = ["demand_group_id", "demand_subgroup_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]
references = mapping.select(keys + ["previous_time_seq_id"]).unique().join(
    survey.rename({"time_seq_id": "previous_time_seq_id"}),
    on=["activity_seq_id", "previous_time_seq_id"], how="left",
).group_by(keys).agg(pl.col("country_weight").sum().alias("country_weight"),
                     pl.col("csp_weight").sum().alias("csp_weight"), pl.len().alias("survey_references"))
plans = (utilities.filter(pl.col("is_feasible") & (pl.col("mode_seq_id") != 0))
         .join(details, on="plan_id").join(references, on=keys, how="left")
         .join(allocation.select("plan_id", "n_persons"), on="plan_id", how="left")
         .with_columns(home_time_utility=pl.col("utility") - pl.col("step_utility"),
                       n_persons=pl.col("n_persons").fill_null(0.0))
         .sort("utility", descending=True))
stay_home = utilities.filter(pl.col("mode_seq_id") == 0)
print("CONTEXT", json.dumps(context.to_dicts(), default=str))
print("STAY_HOME", json.dumps(stay_home.select("utility", "is_feasible").to_dicts()))
print("PLAN_COUNTS", json.dumps({"feasible": plans.height, "zero_csp_weight": plans.filter(
    pl.col("csp_weight").fill_null(0) == 0).height, "zero_country_weight": plans.filter(
    pl.col("country_weight").fill_null(0) == 0).height, "sampling_mass": sampling_mass,
    "allocated_people": plans["n_persons"].sum()}))
print("TOP_PLANS", json.dumps(plans.head(10).select(
    "plan_id", "activity_seq_id", "time_seq_id", "mode", "trips", "activities", "utility",
    "activity_utility", "travel_cost", "home_time_utility", "travel_hours", "distance_km", "country_weight", "csp_weight",
    "survey_references", "n_persons",
).to_dicts()))
by_length = plans.group_by("trips").agg(
    pl.len().alias("plans"), pl.col("utility").max().alias("best_utility"),
    pl.col("utility").median().alias("median_utility"), pl.col("n_persons").sum().alias("people"),
    (pl.col("activity_utility") * pl.col("n_persons")).sum().alias("weighted_activity"),
    (pl.col("travel_cost") * pl.col("n_persons")).sum().alias("weighted_cost"),
    (pl.col("home_time_utility") * pl.col("n_persons")).sum().alias("weighted_home"),
).sort("trips")
print("BY_LENGTH", json.dumps(by_length.to_dicts()))
print("BEST_BY_LENGTH", json.dumps(plans.sort("utility", descending=True).group_by("trips", maintain_order=True)
    .first().sort("trips").select("trips", "utility", "activity_utility", "travel_cost", "home_time_utility", "mode")
    .to_dicts()))
top_weight = plans["country_weight"][0]
print("TOP_SCHEDULE_DRAW", json.dumps({"probability_per_draw": top_weight / sampling_mass,
    "at_least_once_in_15_independent_draws": 1.0 - (1.0 - top_weight / sampling_mass) ** 15}))
# A sensitivity only: multiplying MNL weights by survey frequency is a new
# choice model, and would double-count frequency if naively combined with the
# current weighted sampling. Condition on the mobile alternatives in both cases.
mass = plans.with_columns(weighted_people=pl.col("n_persons") * pl.col("country_weight"))
print("SURVEY_PRIOR_SENSITIVITY", json.dumps(mass.select(
    (pl.col("trips") * pl.col("n_persons")).sum().truediv(pl.col("n_persons").sum()).alias("ordinary_mean_trips"),
    (pl.col("trips") * pl.col("weighted_people")).sum().truediv(pl.col("weighted_people").sum()).alias("prior_mean_trips"),
    pl.col("n_persons").filter(pl.col("trips") >= 8).sum().truediv(pl.col("n_persons").sum()).alias("ordinary_8_plus_share"),
    pl.col("weighted_people").filter(pl.col("trips") >= 8).sum().truediv(pl.col("weighted_people").sum()).alias("prior_8_plus_share"),
).to_dicts()))
# Isolate the effect of rewarding repeated episodes: keep every original plan,
# travel cost, clock time and night-at-home contribution, but calculate one
# activity payoff from the sum of its episodes. This is not a proposed model
# and does not reschedule plans or apply behavioral transitions.
mobile_steps = steps.join(plans.select("plan_id"), on="plan_id", how="semi").with_columns(
    log_duration=(pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0),
)
scales = mobile_steps.filter(pl.col("log_duration") > 0).group_by("activity").agg(
    ((pl.col("utility") + pl.col("cost")) / pl.col("log_duration")).median().alias("scale"),
    pl.col("min_activity_time").first(),
)
print("SCORING_RECONSTRUCTION", json.dumps(mobile_steps.join(scales.select("activity", "scale"), on="activity", how="left")
    .select(((pl.col("utility") + pl.col("cost")) - (pl.col("scale") * pl.col("log_duration")).fill_null(0.0))
            .abs().max().alias("maximum_step_utility_difference")).to_dicts()))
# The captured Geneva setup uses k=2 and a home-night value of time of 2.
# Rescore the same feasible plans, including night and stay-home utilities.
home_night_mean = (pl.read_parquet(folder / "home_night_dur.parquet")
                   .filter((pl.col("country").cast(pl.String) == country) & (pl.col("csp").cast(pl.String) == csp))
                   ["mean_home_night_per_pers"].item())
scaled_steps = mobile_steps.join(scales.select("activity", "scale"), on="activity", how="left")
for coefficient in (0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0):
    change = 2.0 - coefficient
    delta = (scaled_steps.with_columns(
        delta=(pl.col("scale") * ((pl.col("log_duration") - change).clip(0.0)
              - pl.col("log_duration"))).fill_null(0.0))
        .group_by("plan_id").agg(pl.col("delta").sum()))
    rescored = plans.join(delta, on="plan_id").with_columns(
        revised_utility=pl.col("utility") + pl.col("delta")
        + (pl.col("home_time_utility") - change * 2.0 * home_night_mean).clip(0.0)
        - pl.col("home_time_utility"),
    )
    stay_utility = coefficient * 2.0 * home_night_mean
    anchor = max(stay_utility, rescored["revised_utility"].max())
    rescored = rescored.with_columns(weight=((pl.col("revised_utility") - anchor) * 0.25).exp())
    mobile_weight = rescored["weight"].sum()
    home_weight = math.exp((stay_utility - anchor) * 0.25)
    long_weight = rescored.filter(pl.col("trips") >= 8)["weight"].sum()
    total_trips = rescored.select((pl.col("trips") * pl.col("weight")).sum()).item()
    print("COEFFICIENT_SENSITIVITY", json.dumps({"coefficient": coefficient,
          "mobile_share": mobile_weight / (mobile_weight + home_weight),
          "stay_home_share": home_weight / (mobile_weight + home_weight),
          "mean_trips_per_person": total_trips / (mobile_weight + home_weight),
          "mean_trips_per_mobile_person": total_trips / mobile_weight,
          "mobile_8_plus_share": long_weight / mobile_weight,
          "best_mobile_gain_over_stay_home": rescored["revised_utility"].max() - stay_utility,
          "best_mobile_trips": rescored.sort("revised_utility", descending=True)["trips"][0]}))
combined_activity = (mobile_steps.group_by("plan_id", "activity")
    .agg(pl.col("duration_per_pers").sum().alias("duration"),
         (pl.col("utility") + pl.col("cost")).sum().alias("original"))
    .join(scales, on="activity", how="left")
    .with_columns(combined=(pl.col("scale") * (pl.col("duration") / pl.col("min_activity_time"))
                  .log().clip(0.0)).fill_null(0.0))
    .with_columns(
        combined_non_home=pl.when(pl.col("activity") == "home").then(pl.col("original"))
        .otherwise(pl.col("combined")),
        combined_shopping=pl.when(pl.col("activity") == "shopping").then(pl.col("combined"))
        .otherwise(pl.col("original")),
    )
    .group_by("plan_id").agg(pl.col("combined").sum().alias("combined_activity"),
                              pl.col("combined_non_home", "combined_shopping").sum()))
counterfactual = plans.join(combined_activity, on="plan_id").with_columns(
    revised_utility=pl.col("utility") - pl.col("activity_utility") + pl.col("combined_activity"),
    non_home_utility=pl.col("utility") - pl.col("activity_utility") + pl.col("combined_non_home"),
    shopping_utility=pl.col("utility") - pl.col("activity_utility") + pl.col("combined_shopping"),
)
for label, column in [("all", "revised_utility"), ("non_home", "non_home_utility"), ("shopping", "shopping_utility")]:
    weighted = counterfactual.with_columns(weight=((pl.col(column) - pl.col(column).max()) * 0.25).exp())
    print("EPISODE_COMBINING_SENSITIVITY", label, json.dumps(weighted.select(
        (pl.col("trips") * pl.col("weight")).sum().truediv(pl.col("weight").sum()).alias("mean_trips"),
        pl.col("weight").filter(pl.col("trips") >= 8).sum().truediv(pl.col("weight").sum()).alias("8_plus_share"),
        pl.col(column).max().alias("best_utility"),
    ).to_dicts()))
print("EPISODE_COMBINING_TOP", json.dumps(counterfactual.sort("revised_utility", descending=True).head(3)
    .select("trips", "activities", "mode", "utility", "revised_utility").to_dicts()))
print("SURVEY_LENGTHS", json.dumps(eligible_survey.join(
    pl.read_parquet(folder / "survey_plan_steps.parquet").group_by("activity_seq_id", "time_seq_id").len(name="trips"),
    on=["activity_seq_id", "time_seq_id"],
).group_by("trips").agg(pl.col("country_weight").sum().alias("weight")).sort("trips")
 .with_columns(share=pl.col("weight") / sampling_mass).to_dicts()))
print("COSTS_BY_MODE", json.dumps(steps.group_by("mode").agg(
    pl.col("distance").first(), pl.col("time").first(), pl.col("cost").first(),
    pl.col("distance").n_unique().alias("distance_values"), pl.col("time").n_unique().alias("time_values"),
    pl.col("cost").n_unique().alias("cost_values"),
).sort("mode").to_dicts()))
print("TOP_STEPS", json.dumps(steps.filter(pl.col("plan_id") == plans["plan_id"][0])
    .sort("seq_step_index").select("seq_step_index", "activity", "duration_per_pers", "time", "distance", "cost", "utility")
    .with_columns(activity_utility=pl.col("utility") + pl.col("cost")).to_dicts()))
