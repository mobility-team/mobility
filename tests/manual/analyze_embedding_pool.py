"""Study near-duplicate candidate and allocated plans in a Geneva capture.

Usage: pixi run python tests/manual/analyze_embedding_pool.py CAPTURE ZONES
For a bounded, reproducible sample of scoring contexts, use PlanDistance's
hourly activity/mode and centroid features. Correct the missing-hour cumulative
state values for this analysis only. Compare every pair inside each context;
allocated-plan pairs additionally have to belong to the same demand unit.
"""
import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
import numpy as np
import polars as pl
from scipy.spatial.distance import cdist

from mobility.trips.group_day_trips.plans.plan_distance import PlanDistance


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("zones", type=Path)
parser.add_argument("--per-country", type=int, default=50)
parser.add_argument("--minutes", type=float, default=30.0)
parser.add_argument("--kilometres", type=float, default=1.0)
parser.add_argument("--same-mode", action="store_true", help="Require identical mode sequences")
parser.add_argument("--same-destination", action="store_true", help="Require identical destination sequences")
parser.add_argument("--same-activity", action="store_true", help="Require identical activity sequences")
args = parser.parse_args()
folder = args.folder
unit = ["demand_group_id", "demand_subgroup_id"]
plan = ["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]
context_plan = unit + plan
utility = pl.read_parquet(folder / "candidate-utility.parquet").filter(
    pl.col("is_feasible") & (pl.col("mode_seq_id") != 0)
)
contexts = pl.read_parquet(folder / "contexts.parquet")
unit_map = pl.read_parquet(folder / "unit-map.parquet")
allocations = (pl.read_parquet(folder / "allocated-plans.parquet").filter(pl.col("mode_seq_id") != 0)
               .join(unit_map, on=unit)
               .rename({"demand_group_id": "unit_group", "demand_subgroup_id": "unit_subgroup",
                        "_score_context_id": "demand_group_id"})
               .with_columns(demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
               .join(utility.select(context_plan + ["plan_id"]).rename({"plan_id": "candidate_plan_id"}),
                     on=context_plan, how="inner"))
plan_counts = utility.group_by(unit).len(name="candidates").filter(pl.col("candidates") >= 2)
context_choices = (contexts.join(plan_counts, on=unit)
                   .sort("country", "n_persons", "demand_group_id"))
selected = []
for country in context_choices.partition_by("country", maintain_order=True):
    n = min(args.per_country, country.height)
    selected += [country["demand_group_id"][(2 * i + 1) * country.height // (2 * n)] for i in range(n)]
chosen = utility.filter(pl.col("demand_group_id").is_in(selected)).sort("demand_group_id", "plan_id")
selected_steps = (pl.scan_parquet(folder / "candidate-steps.parquet")
                  .join(chosen.select("plan_id").lazy(), on="plan_id", how="semi")
                  .collect(engine="streaming"))
zones = gpd.read_file(args.zones, ignore_geometry=True)
zone_ids = set(zones["transport_zone_id"])
missing_zones = (set(selected_steps["from"].to_list()) | set(selected_steps["to"].to_list())) - zone_ids
if missing_zones:
    raise ValueError(f"No centroid is available for {len(missing_zones)} zones in the sample.")
zones["geometry"] = None
features = PlanDistance().build_plan_features(selected_steps, transport_zones=SimpleNamespace(get=lambda: zones))

# Existing state features omit hours with zero activity and fill them with zero.
# Carry the cumulative total forward so it remains cumulative after an episode.
ids = features["plan_id"].to_numpy()
index_by_id = {int(plan_id): index for index, plan_id in enumerate(ids)}
columns = [name for name in features.columns if name != "plan_id"]
raw = features.select(columns).to_numpy().astype(np.float64, copy=True)
corrected = raw.copy()
states = sorted({name.split("_", 3)[3] for name in columns if name.startswith("state_progress_h")})
position = {name: index for index, name in enumerate(columns)}
state_indices = [[position[f"state_progress_h{hour:02d}_{state}"] for hour in range(24)] for state in states]
for indices in state_indices:
    corrected[:, indices] = np.maximum.accumulate(corrected[:, indices], axis=1)
state_cumulative = np.stack([corrected[:, indices] for indices in state_indices], axis=2) * 24.0
state_hourly = np.diff(state_cumulative, axis=1, prepend=np.zeros((len(ids), 1, len(states))))
x = corrected[:, [position[f"x_progress_h{hour:02d}"] for hour in range(24)]] * 24.0
y = corrected[:, [position[f"y_progress_h{hour:02d}"] for hour in range(24)]] * 24.0
xy = np.stack([np.diff(x, axis=1, prepend=np.zeros((len(ids), 1))),
               np.diff(y, axis=1, prepend=np.zeros((len(ids), 1)))], axis=2)
weights = np.array([1.0 / 100_000.0 if name.startswith(("x_progress_", "y_progress_")) else 1.0
                    for name in columns])
weighted = corrected * weights
raw_weighted = raw * weights
records = []
pair_examples = []
all_utility_gaps = []
same_activity_sequence_pairs = same_destination_sequence_pairs = same_mode_sequence_pairs = 0
total_candidate_count = total_near_candidate_count = total_equal_candidate_count = 0
total_mnl_weight_near = total_context_population = 0.0
allocated_people = allocated_people_near = 0.0
allocated_unit_plans = allocated_unit_plans_near = 0
greedy_candidate_removals = 0
greedy_mnl_weight_removed = 0.0
greedy_allocated_people_moved = 0.0
changed_nearest = 0
valid_states = np.abs(state_hourly.sum(axis=2) - 1.0).max(axis=1) <= 1e-5
for frame in chosen.partition_by("demand_group_id", maintain_order=True):
    context_id = int(frame["demand_group_id"][0])
    frame_ids = frame["plan_id"].to_numpy()
    indices = np.array([index_by_id[int(value)] for value in frame_ids])
    if not valid_states[indices].all():
        records.append({"context": context_id, "invalid_hourly_coverage": True})
        continue
    hourly = state_hourly[indices].reshape(len(indices), -1)
    # Half the L1 difference is the number of hours assigned to different
    # activity/mode states, as represented by the hourly interval fractions.
    state_hours = cdist(hourly, hourly, metric="cityblock") / 2.0
    coordinates = xy[indices]
    spatial_km = np.linalg.norm(coordinates[:, None, :, :] - coordinates[None, :, :, :], axis=3).mean(axis=2) / 1000.0
    near = (state_hours <= args.minutes / 60.0) & (spatial_km <= args.kilometres)
    for flag, name in [(args.same_mode, "mode_seq_id"), (args.same_destination, "dest_seq_id"),
                       (args.same_activity, "activity_seq_id")]:
        if flag:
            sequence = frame[name].to_numpy()
            near &= sequence[:, None] == sequence[None, :]
    np.fill_diagonal(near, False)
    same_summary = (state_hours <= 1e-9) & (spatial_km <= 1e-9)
    np.fill_diagonal(same_summary, False)
    near_any = near.any(axis=1)
    exact_any = same_summary.any(axis=1)
    distances = cdist(weighted[indices], weighted[indices])
    np.fill_diagonal(distances, np.inf)
    neighbor = distances.argmin(axis=1)
    raw_distances = cdist(raw_weighted[indices], raw_weighted[indices])
    np.fill_diagonal(raw_distances, np.inf)
    changed_nearest += int((neighbor != raw_distances.argmin(axis=1)).sum())
    utility_values = frame["utility"].to_numpy()
    mnl = np.exp((utility_values - utility_values.max()) * 0.25)
    mnl /= mnl.sum()
    population = float(contexts.filter(pl.col("demand_group_id") == context_id)["n_persons"].item())
    total_context_population += population
    total_mnl_weight_near += population * float(mnl[near_any].sum())
    covered = np.zeros(len(indices), dtype=bool)
    representatives = np.zeros(len(indices), dtype=bool)
    for i in np.argsort(-utility_values, kind="stable"):
        if not covered[i]:
            representatives[i] = True
            covered[i] = True
            covered |= near[i]
    greedy_candidate_removals += int((~representatives).sum())
    greedy_mnl_weight_removed += population * float(mnl[~representatives].sum())
    total_candidate_count += len(indices)
    total_near_candidate_count += int(near_any.sum())
    total_equal_candidate_count += int(exact_any.sum())
    pairs = np.transpose(np.triu(near, k=1).nonzero())
    utility_gaps = np.abs(utility_values[pairs[:, 0]] - utility_values[pairs[:, 1]]) if len(pairs) else np.array([])
    all_utility_gaps.extend(utility_gaps.tolist())
    if len(pairs):
        for column, counter in [("activity_seq_id", "activity"), ("dest_seq_id", "destination"), ("mode_seq_id", "mode")]:
            values = frame[column].to_numpy()
            count = int((values[pairs[:, 0]] == values[pairs[:, 1]]).sum())
            if counter == "activity":
                same_activity_sequence_pairs += count
            elif counter == "destination":
                same_destination_sequence_pairs += count
            else:
                same_mode_sequence_pairs += count
    for i, j in pairs:
        pair_examples.append({"context": context_id, "plan_a": int(frame_ids[i]), "plan_b": int(frame_ids[j]),
                              "state_minutes": float(state_hours[i, j] * 60), "mean_distance_km": float(spatial_km[i, j]),
                              "utility_gap": float(abs(utility_values[i] - utility_values[j]))})
    context_allocations = allocations.filter(pl.col("demand_group_id") == context_id)
    local_by_plan_id = {int(value): index for index, value in enumerate(frame_ids)}
    for group in context_allocations.partition_by(["unit_group", "unit_subgroup"], maintain_order=False):
        local = np.array([local_by_plan_id[int(value)] for value in group["candidate_plan_id"]])
        volumes = group["n_persons"].to_numpy()
        has_peer = near[np.ix_(local, local)].any(axis=1) if len(local) > 1 else np.zeros(len(local), dtype=bool)
        allocated_unit_plans += len(local)
        allocated_unit_plans_near += int(has_peer.sum())
        allocated_people += float(volumes.sum())
        allocated_people_near += float(volumes[has_peer].sum())
        # Separately for each original unit, greedily keep the most allocated
        # remaining option and assign only neighbors directly close to it.
        covered = np.zeros(len(local), dtype=bool)
        representatives = np.zeros(len(local), dtype=bool)
        for i in np.argsort(-volumes, kind="stable"):
            if not covered[i]:
                representatives[i] = True
                covered[i] = True
                covered |= near[local[i], local]
        greedy_allocated_people_moved += float(volumes[~representatives].sum())
    records.append({"context": context_id, "country": str(contexts.filter(pl.col("demand_group_id") == context_id)["country"].item()),
        "candidates": len(indices), "near_candidates": int(near_any.sum()), "equal_hourly_candidates": int(exact_any.sum()),
        "near_pairs": len(pairs), "mnl_near_share": float(mnl[near_any].sum()),
        "median_nearest_embedding_distance": float(np.median(distances[np.arange(len(indices)), neighbor])),
        "median_near_utility_gap": float(np.median(utility_gaps)) if len(utility_gaps) else None})

summary = {"sample_contexts": len(selected), "valid_contexts": sum("invalid_hourly_coverage" not in row for row in records),
    "candidate_plans": total_candidate_count, "near_candidate_plans": total_near_candidate_count,
    "same_hourly_summary_candidates": total_equal_candidate_count,
    "weighted_mnl_near_share": total_mnl_weight_near / total_context_population if total_context_population else None,
    "allocated_unit_plans": allocated_unit_plans, "near_allocated_unit_plans": allocated_unit_plans_near,
    "allocated_mobile_people": allocated_people, "near_allocated_mobile_people": allocated_people_near,
    "greedy_candidate_removals": greedy_candidate_removals,
    "greedy_mnl_removed_share": greedy_mnl_weight_removed / total_context_population if total_context_population else None,
    "greedy_allocated_mobile_people_moved": greedy_allocated_people_moved,
    "nearest_changed_after_cumulative_fix": changed_nearest,
    "near_pairs": len(all_utility_gaps),
    "near_pair_utility_gap_median": float(np.median(all_utility_gaps)) if all_utility_gaps else None,
    "near_pair_utility_gap_p90": float(np.quantile(all_utility_gaps, .9)) if all_utility_gaps else None,
    "near_pair_utility_gap_max": float(max(all_utility_gaps)) if all_utility_gaps else None,
    "near_pairs_same_activity_sequence": same_activity_sequence_pairs,
    "near_pairs_same_destination_sequence": same_destination_sequence_pairs,
    "near_pairs_same_mode_sequence": same_mode_sequence_pairs,
    "minutes": args.minutes, "kilometres": args.kilometres,
    "same_mode_required": args.same_mode, "same_destination_required": args.same_destination,
    "same_activity_required": args.same_activity,
    "sampled_people": total_context_population,
    "feature_columns": len(columns), "states": states}
print("EMBEDDING_SUMMARY", json.dumps(summary), flush=True)
print("EMBEDDING_CONTEXTS", json.dumps(records), flush=True)
print("EMBEDDING_EXAMPLES", json.dumps(sorted(pair_examples, key=lambda value: value["utility_gap"], reverse=True)[:15]), flush=True)
