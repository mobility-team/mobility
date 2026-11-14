"""Compare preceding-arrival estimates with scored plans.

Run with the mnl-batches folder from MOBILITY_PROBE_RIGID_UTILITY_BATCH=10.
This isolates timing error using the final context's utility coefficients;
the guide itself uses profile averages, so it is not its complete error.
"""

import argparse
import json
import math
from pathlib import Path

import polars as pl
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


parser = argparse.ArgumentParser()
parser.add_argument("folder", type=Path)
parser.add_argument("--batch", type=int, default=10)
args = parser.parse_args()
folder = args.folder
batch = args.batch
steps = pl.read_parquet(folder / f"batch-{batch}-rigid-steps.parquet")
plans = pl.read_parquet(folder / f"batch-{batch}-rigid-plans.parquet")
catalog = pl.read_parquet(folder / f"batch-{batch}-schedule-catalog.parquet")
mapping = pl.read_parquet(folder / f"batch-{batch}-rigid-time-mapping.parquet")

unambiguous = (
    mapping.group_by(PLAN_KEY_COLS).agg(
        pl.col("previous_time_seq_id").n_unique().alias("previous_schedule_count"),
        pl.col("previous_time_seq_id").first(),
    )
    .filter(pl.col("previous_schedule_count") == 1)
    .drop("previous_schedule_count")
)
reference_steps = (
    catalog.select(
        pl.col("time_seq_id").alias("previous_time_seq_id"), "seq_step_index",
        pl.col("departure_time").alias("reference_departure"),
        pl.col("arrival_time").alias("reference_arrival"),
        pl.col("next_departure_time").alias("reference_next_departure"),
    )
)
joined = (
    steps.join(plans.select("plan_id", "n_persons"), on="plan_id")
    .join(unambiguous, on=PLAN_KEY_COLS, how="inner")
    .join(reference_steps, on=["previous_time_seq_id", "seq_step_index"], how="inner")
    .sort(["plan_id", "seq_step_index"])
    .with_columns(
        previous_duration=pl.col("duration_per_pers").shift(1).over("plan_id"),
        previous_minimum=pl.col("min_activity_time").shift(1).over("plan_id"),
        previous_utility=pl.col("utility").shift(1).over("plan_id"),
        previous_cost=pl.col("cost").shift(1).over("plan_id"),
        previous_reference_arrival=pl.col("reference_arrival").shift(1).over("plan_id"),
        previous_reference_trip_departure=pl.col("reference_departure").shift(1).over("plan_id"),
        previous_reference_departure=pl.col("reference_next_departure").shift(1).over("plan_id"),
        previous_activity=pl.col("activity").shift(1).over("plan_id"),
    )
    # Work and studies have rigidity 1 in the Geneva setup used by this probe.
    .filter((pl.col("seq_step_index") > 1) & pl.col("activity").is_in(["work", "studies"]))
    .with_columns(
        reference_duration=(pl.col("previous_reference_departure") - pl.col("previous_reference_arrival")).clip(0.001),
        actual_log=(pl.col("previous_duration") / pl.col("previous_minimum")).log().clip(0.0),
        predicted_duration=(
            pl.col("reference_departure") - pl.col("previous_reference_arrival")
            - (pl.col("time") - (pl.col("reference_arrival") - pl.col("reference_departure")).clip(0.0, 24.0))
        ).clip(0.001),
    )
)
# A zero log utility cannot reveal the exact coefficient from the final score.
usable = (
    joined.filter(pl.col("actual_log") > 1e-9)
    .with_columns(scale=(pl.col("previous_utility") + pl.col("previous_cost")) / pl.col("actual_log"))
    .with_columns(
        predicted_delta=pl.col("scale") * (
            (pl.col("predicted_duration") / pl.col("previous_minimum")).log().clip(0.0)
            - (pl.col("reference_duration") / pl.col("previous_minimum")).log().clip(0.0)
        ),
        actual_delta=pl.col("scale") * (
            (pl.col("previous_duration") / pl.col("previous_minimum")).log().clip(0.0)
            - (pl.col("reference_duration") / pl.col("previous_minimum")).log().clip(0.0)
        ),
    )
    .with_columns(error=pl.col("predicted_delta") - pl.col("actual_delta"))
)
summary = {
    "context": json.loads((folder / "rigid-subgroup.json").read_text(encoding="utf-8")),
    "chosen_plan_rows": plans.height,
    "chosen_plans_with_unambiguous_rigid_previous_pair": joined["plan_id"].n_unique(),
    "rigid_previous_pairs": joined.height,
    "usable_pairs": usable.height,
    "usable_chosen_people": usable.select("plan_id", "n_persons").unique()["n_persons"].sum(),
    "total_chosen_people": plans["n_persons"].sum(),
}
if usable.height:
    summary.update(usable.select(
        ((pl.col("error").abs() * pl.col("n_persons")).sum() / pl.col("n_persons").sum())
        .alias("mean_absolute_utility_error_per_pair"),
        ((pl.col("error") * pl.col("n_persons")).sum() / pl.col("n_persons").sum())
        .alias("mean_signed_utility_error_per_pair"),
        ((pl.col("actual_delta").abs() * pl.col("n_persons")).sum() / pl.col("n_persons").sum())
        .alias("mean_absolute_actual_utility_change_per_pair"),
        pl.col("error").abs().max().alias("maximum_absolute_utility_error"),
        ((pl.col("predicted_duration") - pl.col("previous_duration")).abs() * 60 * pl.col("n_persons")).sum()
        .truediv(pl.col("n_persons").sum()).alias("mean_absolute_duration_error_minutes"),
    ).row(0, named=True))

# Reconstruct the anchor-pass location approximation for this context. The
# actual mode chain and the best option for the context can differ from the
# profile which originally generated a shared candidate, so this isolates the
# effect of estimated arrival rather than reproducing every sampling weight.
context = summary["context"]
option_path = folder.parent / f"travel-options-profile-{context.get('utility_profile_id')}.parquet"
if usable.height and option_path.exists() and "from" in steps.columns:
    anchors = {"home", "work", "studies"}
    by_plan = {key[0]: group.sort("seq_step_index").to_dicts()
               for key, group in steps.partition_by("plan_id", as_dict=True).items()}
    cases = []
    for row in usable.iter_rows(named=True):
        plan = by_plan[row["plan_id"]]
        previous = next((item for item in plan if item["seq_step_index"] == row["seq_step_index"] - 1), None)
        if previous is None:
            continue
        older_anchors = [item for item in plan if item["seq_step_index"] < previous["seq_step_index"]
                         and item["activity"] in anchors]
        last_known = older_anchors[-1]["to"] if older_anchors else plan[0]["from"]
        target = previous["to"] if previous["activity"] in anchors else last_known
        cases.append({**row, "proxy_from": last_known, "proxy_to": target,
                      "proxy_is_actual": last_known == previous["from"] and target == previous["to"]})
    if cases:
        pairs = pl.DataFrame(cases).select(
            pl.col("proxy_from").cast(pl.Int32).alias("from"), pl.col("proxy_to").cast(pl.Int32).alias("to")
        ).unique()
        options = pl.read_parquet(option_path).join(pairs, on=["from", "to"], how="semi")
        lookup = {(row["from"], row["to"]): row for row in options.iter_rows(named=True)}
        modeled_errors = []
        for row in cases:
            option = lookup.get((row["proxy_from"], row["proxy_to"]))
            if option is None or option["has_unknown_time"]:
                continue
            rigidity = 1.0 if row["previous_activity"] in anchors else 0.0
            previous_reference_travel = max(0.0, min(24.0,
                row["previous_reference_arrival"] - row["previous_reference_trip_departure"]))
            current_reference_travel = max(0.0, min(24.0, row["reference_arrival"] - row["reference_departure"]))
            current_departure = row["reference_departure"] - (row["time"] - current_reference_travel)
            choices = []
            for kind in ("fastest", "cheapest"):
                arrival = row["previous_reference_arrival"] + (1.0 - rigidity) * (
                    option[f"{kind}_time"] - previous_reference_travel
                )
                duration = max(0.001, current_departure - arrival)
                delta = row["scale"] * (
                    max(0.0, math.log(duration / row["previous_minimum"]))
                    - max(0.0, math.log(row["reference_duration"] / row["previous_minimum"]))
                )
                choices.append((delta - option[f"{kind}_cost"], delta, duration))
            _, predicted_delta, predicted_duration = max(choices)
            modeled_errors.append({**row, "modeled_delta": predicted_delta, "modeled_duration": predicted_duration})
        if modeled_errors:
            m = pl.DataFrame(modeled_errors)
            summary["modeled_previous_arrival"] = {
                "pairs": m.height,
                "proxy_is_actual_pairs": m.filter(pl.col("proxy_is_actual")).height,
                **m.select(
                    (pl.col("n_persons").filter(pl.col("proxy_is_actual")).sum()
                     / pl.col("n_persons").sum()).alias("proxy_is_actual_weight_share"),
                    ((pl.col("modeled_delta") - pl.col("actual_delta")).abs() * pl.col("n_persons")).sum()
                    .truediv(pl.col("n_persons").sum()).alias("mean_absolute_utility_error_per_pair"),
                    (pl.col("modeled_delta") - pl.col("actual_delta")).abs().max()
                    .alias("maximum_absolute_utility_error"),
                    ((pl.col("modeled_duration") - pl.col("previous_duration")).abs() * 60 * pl.col("n_persons")).sum()
                    .truediv(pl.col("n_persons").sum()).alias("mean_absolute_duration_error_minutes"),
                    ((pl.col("predicted_delta") - pl.col("actual_delta")).abs() * pl.col("n_persons")).sum()
                    .truediv(pl.col("n_persons").sum()).alias("reference_arrival_utility_error_same_pairs"),
                ).row(0, named=True),
            }
print(json.dumps(summary, indent=2))
