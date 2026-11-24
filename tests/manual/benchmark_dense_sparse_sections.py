"""Compare sparse and dense profile scoring of shared timed sections.

Usage: pixi run python tests/manual/benchmark_dense_sparse_sections.py CAPTURE
       --strategy sparse|dense

Both paths start from the same captured pre-expansion physical candidates,
memberships and retained scoring lookup. They factor physical timing by the
actual sequence of travel times, rather than by utility profile ID. The score
lookup is supplied by the capture; candidate generation, lookup construction,
modeled-ID assignment and behavioral transitions are outside this benchmark.
"""
import argparse
import json
import threading
from pathlib import Path
from time import perf_counter

import numpy as np
import polars as pl
import psutil

from mobility.trips.group_day_trips.plans.compact_candidate_steps import PLAN_COLUMNS
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--strategy", choices=["sparse", "dense"], required=True)
parser.add_argument("--constant", type=float, default=1.25)
args = parser.parse_args()
unit = ["demand_group_id", "demand_subgroup_id"]
profile_cols = ["country", "csp", "utility_profile_id"]
travel_keys = ["utility_profile_id", "from", "to", "mode"]
activity_keys = ["country", "csp", "activity", "to"]
process = psutil.Process()
stop = threading.Event()
peak = [process.memory_info().rss]


def monitor():
    while not stop.wait(0.01):
        peak[0] = max(peak[0], process.memory_info().rss)


thread = threading.Thread(target=monitor, daemon=True)
thread.start()
start = perf_counter()
physical = pl.read_parquet(args.folder / "physical.parquet")
members = pl.read_parquet(args.folder / "members.parquet")
contexts = pl.read_parquet(args.folder / "contexts.parquet")
scores = pl.read_parquet(args.folder / "step-scores.parquet")
settings = json.loads((args.folder / "settings.json").read_text(encoding="utf-8"))
members = members.join(contexts.select(unit + ["utility_profile_id"]), on=unit, how="left")
profiles = members.select(profile_cols).unique(maintain_order=True).with_row_index("profile")
members = members.join(profiles, on=profile_cols, maintain_order="left")
travel_plans = (members.select(PLAN_COLUMNS + ["utility_profile_id"]).unique(maintain_order=True)
                .with_row_index("travel_plan"))
travel = scores.select(travel_keys + ["cost", "time", "is_feasible"]).unique()
if travel.select(travel_keys).n_unique() != travel.height:
    raise ValueError("Transport lookup has conflicting values for a profile and OD/mode.")
activity_lookup = scores.select(activity_keys + ["min_activity_time", "activity_utility_scale"]).unique()
if activity_lookup.select(activity_keys).n_unique() != activity_lookup.height:
    raise ValueError("Activity lookup has conflicting values for its scoring inputs.")
load_seconds = perf_counter() - start
work_start = perf_counter()

# Timing identity is the physical plan and its ordered travel-time values.
# Cost and route availability remain profile-specific scoring inputs.
start = perf_counter()
travel_steps = (travel_plans.join(physical, on=PLAN_COLUMNS)
                .join(travel.select(travel_keys + ["time"]), on=travel_keys, how="left"))
if travel_steps["time"].null_count():
    raise ValueError("Missing travel time for an existing candidate.")
signatures = (travel_steps.group_by("travel_plan", maintain_order=True)
              .agg(*[pl.col(name).first() for name in PLAN_COLUMNS],
                   pl.col("time").sort_by("seq_step_index").alias("travel_times")))
timing_plans = signatures.select(PLAN_COLUMNS + ["travel_times"]).unique(maintain_order=True).with_row_index("timing_plan")
travel_to_timing = signatures.join(timing_plans, on=PLAN_COLUMNS + ["travel_times"], maintain_order="left") \
    .select("travel_plan", "timing_plan")
timing_input = (travel_steps.join(travel_to_timing, on="travel_plan")
                .unique(subset=["timing_plan", "seq_step_index"], keep="first")
                .drop("travel_plan", "utility_profile_id")
                .rename({"timing_plan": "demand_group_id"})
                .with_columns(demand_subgroup_id=pl.lit(0, dtype=pl.UInt32), is_feasible=pl.lit(True)))
timed = (PlanScheduleUpdater().update_plan_timings(timing_input.lazy(), **settings)
         .with_columns(*[((pl.col(name).cast(pl.Float64) * 3600).round(0) / 3600)
                         .cast(pl.Float32).alias(name) for name in
                         ("departure_time", "arrival_time", "next_departure_time", "duration_per_pers")])
         .with_columns(timing_feasible=((pl.col("next_departure_time") >= pl.col("arrival_time"))
                       | (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS))))
         .collect(engine="streaming").rename({"demand_group_id": "timing_plan"}).drop("demand_subgroup_id"))
timing_seconds = perf_counter() - start

start = perf_counter()
trip_cols = ["from", "to", "mode", "departure_time", "arrival_time", "time"]
activity_cols = ["to", "activity", "arrival_time", "next_departure_time", "duration_per_pers", "timing_feasible"]
trip = timed.select(trip_cols).unique(maintain_order=True).with_row_index("trip")
activity = timed.select(activity_cols).unique(maintain_order=True).with_row_index("activity_atom")
rows = timed.join(trip, on=trip_cols, maintain_order="left").join(activity, on=activity_cols, maintain_order="left")
rigid = [name for name, value in settings["activity_time_rigidity_by_activity"].items() if value == 1.0]
trip_rows = rows.select("timing_plan", (pl.col("seq_step_index").cast(pl.UInt32) * 2).alias("position"),
                        (pl.col("trip").cast(pl.UInt64) * 2).alias("token"), pl.lit(False).alias("anchor"))
activity_rows = rows.select("timing_plan", (pl.col("seq_step_index").cast(pl.UInt32) * 2 + 1).alias("position"),
                            (pl.col("activity_atom").cast(pl.UInt64) * 2 + 1).alias("token"),
                            pl.col("activity").cast(pl.String).is_in(rigid).alias("anchor"))
atoms = pl.concat([trip_rows, activity_rows]).sort("timing_plan", "position")
atoms = atoms.with_columns(previous_anchor=pl.col("anchor").shift(1).over("timing_plan").fill_null(True))
atoms = atoms.with_columns(boundary=pl.col("anchor") | pl.col("previous_anchor"))
atoms = atoms.with_columns(section_in_plan=pl.col("boundary").cast(pl.UInt32).cum_sum().over("timing_plan"))
groups = atoms.group_by("timing_plan", "section_in_plan", maintain_order=True).agg(pl.col("token").alias("tokens"))
sections = groups.select("tokens").unique(maintain_order=True).with_row_index("section")
references = (groups.join(sections, on="tokens", maintain_order="left")
              .select("timing_plan", "section").sort("timing_plan"))
section_atoms = sections.explode("tokens").rename({"tokens": "token"})
section_lengths = sections["tokens"].list.len().to_numpy().astype(np.int64)
section_offsets = np.r_[0, np.cumsum(section_lengths)]
section_tokens = section_atoms["token"].to_numpy().astype(np.int64)
section_by_atom = section_atoms["section"].to_numpy()
plan_counts = np.bincount(references["timing_plan"].to_numpy(), minlength=timing_plans.height)
plan_offsets = np.r_[0, np.cumsum(plan_counts)]
plan_sections = references["section"].to_numpy()
if (plan_counts == 0).any():
    raise ValueError("A timing plan has no section.")
plan_classes = (members.select(PLAN_COLUMNS + ["utility_profile_id", "profile"]).unique(maintain_order=True)
                .join(travel_plans, on=PLAN_COLUMNS + ["utility_profile_id"])
                .join(travel_to_timing, on="travel_plan"))
context_plans = (members.select(PLAN_KEY_COLS + ["country", "csp", "profile", "utility_profile_id"])
                 .join(plan_classes, on=PLAN_COLUMNS + ["utility_profile_id", "profile"], maintain_order="left"))
if context_plans.height != members.height or context_plans["timing_plan"].null_count():
    raise ValueError("Lost a candidate context/plan membership.")
section_build_seconds = perf_counter() - start

# Build the sparse section/profile membership once. Dense scoring deliberately
# fills all section/profile slots, with missing or incompatible inputs masked.
start = perf_counter()
needed_pairs = None
if args.strategy == "sparse":
    needed_pairs = (plan_classes.select("timing_plan", "profile").join(references, on="timing_plan")
                    .select("profile", "section").unique().sort("profile", "section"))
index_seconds = perf_counter() - start
needed_by_profile = ({frame["profile"][0]: frame["section"].to_numpy()
                      for frame in needed_pairs.partition_by("profile", maintain_order=True)}
                     if needed_pairs is not None else {})
classes_by_profile = {frame["profile"][0]: frame["timing_plan"].to_numpy()
                      for frame in plan_classes.select("profile", "timing_plan").unique().partition_by("profile")}
section_to_plan_time = (section_atoms.join(trip.select((pl.col("trip").cast(pl.UInt64) * 2).alias("token"),
                                                       pl.col("time").alias("trip_time")), on="token", how="left")
                        .join(activity.select((pl.col("activity_atom").cast(pl.UInt64) * 2 + 1).alias("token"),
                                              pl.col("duration_per_pers").cast(pl.Float64).alias("act_duration")),
                              on="token", how="left")
                        .group_by("section").agg(pl.col("trip_time").fill_null(0).sum().alias("travel"),
                                                  pl.col("act_duration").fill_null(0).sum().alias("duration"))
                        .sort("section"))
section_duration = section_to_plan_time["duration"].to_numpy()
section_travel = section_to_plan_time["travel"].to_numpy()
plan_duration = np.add.reduceat(section_duration[plan_sections], plan_offsets[:-1])
plan_travel = np.add.reduceat(section_travel[plan_sections], plan_offsets[:-1])
trip = trip.with_columns((pl.col("trip").cast(pl.UInt64) * 2).alias("token"))
activity = activity.with_columns((pl.col("activity_atom").cast(pl.UInt64) * 2 + 1).alias("token"))
token_count = max(trip.height * 2, activity.height * 2 + 1)
plan_utilities = np.zeros((profiles.height, timing_plans.height), dtype=np.float64)
plan_feasible = np.zeros((profiles.height, timing_plans.height), dtype=bool)
dense_utilities = np.zeros((profiles.height, sections.height), dtype=np.float64) if args.strategy == "dense" else None
dense_feasible = np.zeros((profiles.height, sections.height), dtype=bool) if args.strategy == "dense" else None
sparse_scores = {}
atom_evaluations = section_evaluations = peak_profile_atoms = 0
dense_tokens = np.unique(section_tokens) if args.strategy == "dense" else None
start = perf_counter()
for profile_row in profiles.iter_rows(named=True):
    profile = profile_row["profile"]
    if profile not in classes_by_profile:
        continue
    if args.strategy == "dense":
        selected_sections = np.arange(sections.height, dtype=np.uint32)
        selected_tokens = section_tokens
        selected_offsets = section_offsets[:-1]
        unique_tokens = dense_tokens
    else:
        selected_sections = needed_by_profile[profile]
        mask = np.zeros(sections.height, dtype=bool)
        mask[selected_sections] = True
        selected_tokens = section_tokens[mask[section_by_atom]]
        selected_offsets = np.r_[0, np.cumsum(section_lengths[selected_sections])[:-1]]
        unique_tokens = np.unique(selected_tokens)
    peak_profile_atoms = max(peak_profile_atoms, len(unique_tokens))
    trip_ids = (unique_tokens[unique_tokens % 2 == 0] // 2).astype(np.uint32)
    activity_ids = (unique_tokens[unique_tokens % 2 == 1] // 2).astype(np.uint32)
    profile_travel = (travel.filter(pl.col("utility_profile_id") == profile_row["utility_profile_id"])
                      .select("from", "to", "mode", "cost", pl.col("time").alias("profile_time"),
                              pl.col("is_feasible").alias("route_feasible")))
    profile_activity = (activity_lookup.filter((pl.col("country") == profile_row["country"])
                                              & (pl.col("csp") == profile_row["csp"]))
                        .select("activity", "to", "min_activity_time", "activity_utility_scale"))
    trip_input = trip if args.strategy == "dense" else trip.filter(pl.col("trip").is_in(trip_ids))
    activity_input = activity if args.strategy == "dense" else activity.filter(pl.col("activity_atom").is_in(activity_ids))
    trip_scores = (trip_input.join(
        profile_travel, on=["from", "to", "mode"], how="left").with_columns(
            valid=(pl.col("route_feasible").fill_null(False) & (pl.col("time") == pl.col("profile_time")).fill_null(False)),
            utility=(-pl.col("cost")).fill_null(0.0)))
    activity_scores = (activity_input.join(
        profile_activity, on=["activity", "to"], how="left").with_columns(
            valid=pl.col("timing_feasible").fill_null(False) & pl.col("min_activity_time").is_not_null(),
            utility=(pl.col("activity_utility_scale")
                     * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0)).fill_null(0.0)))
    token_utility = np.zeros(token_count, dtype=np.float64)
    token_feasible = np.zeros(token_count, dtype=bool)
    for frame in (trip_scores, activity_scores):
        ids = frame["token"].to_numpy().astype(np.int64)
        token_utility[ids] = frame["utility"].to_numpy()
        token_feasible[ids] = frame["valid"].to_numpy()
    section_utility_values = np.add.reduceat(token_utility[selected_tokens], selected_offsets)
    section_feasible_values = np.logical_and.reduceat(token_feasible[selected_tokens], selected_offsets)
    if args.strategy == "dense":
        dense_utilities[profile, selected_sections] = section_utility_values
        dense_feasible[profile, selected_sections] = section_feasible_values
        scores_by_section = dense_utilities[profile]
        feasible_by_section = dense_feasible[profile]
    else:
        # Retain the scores in the same profile/section order as the sparse
        # index so later inner batches can reuse them without rescoring.
        sparse_scores[profile] = (section_utility_values.copy(), section_feasible_values.copy())
        scores_by_section = np.zeros(sections.height, dtype=np.float64)
        feasible_by_section = np.zeros(sections.height, dtype=bool)
        scores_by_section[selected_sections] = section_utility_values
        feasible_by_section[selected_sections] = section_feasible_values
    selected_plans = classes_by_profile[profile]
    counts = plan_counts[selected_plans]
    starts = np.cumsum(np.r_[0, counts[:-1]])
    positions = (np.repeat(plan_offsets[selected_plans], counts)
                 + np.arange(int(counts.sum())) - np.repeat(starts, counts))
    plan_utilities[profile, selected_plans] = np.add.reduceat(scores_by_section[plan_sections[positions]], starts)
    plan_feasible[profile, selected_plans] = np.logical_and.reduceat(feasible_by_section[plan_sections[positions]], starts)
    atom_evaluations += len(unique_tokens)
    section_evaluations += len(selected_sections)
score_seconds = perf_counter() - start

# Measure reusing already-computed scores for the same candidate set. The
# sparse path makes one temporary direct section-indexed array per profile;
# the dense path reads its existing row. Neither recalculates utilities.
cached_plan_sum_seconds = []
cache_test_start = perf_counter()
for repeat in range(3):
    start = perf_counter()
    cached_utilities = np.zeros_like(plan_utilities)
    cached_feasible = np.zeros_like(plan_feasible)
    for profile, selected_plans in classes_by_profile.items():
        if args.strategy == "dense":
            scores_by_section, feasible_by_section = dense_utilities[profile], dense_feasible[profile]
        else:
            scores_by_section = np.zeros(sections.height, dtype=np.float64)
            feasible_by_section = np.zeros(sections.height, dtype=bool)
            section_ids = needed_by_profile[profile]
            scores_by_section[section_ids], feasible_by_section[section_ids] = sparse_scores[profile]
        counts = plan_counts[selected_plans]
        starts = np.cumsum(np.r_[0, counts[:-1]])
        positions = (np.repeat(plan_offsets[selected_plans], counts)
                     + np.arange(int(counts.sum())) - np.repeat(starts, counts))
        cached_utilities[profile, selected_plans] = np.add.reduceat(scores_by_section[plan_sections[positions]], starts)
        cached_feasible[profile, selected_plans] = np.logical_and.reduceat(feasible_by_section[plan_sections[positions]], starts)
    cached_plan_sum_seconds.append(perf_counter() - start)
    if (not np.array_equal(cached_feasible, plan_feasible)
            or not np.allclose(cached_utilities, plan_utilities, rtol=1e-12, atol=1e-12)):
        raise ValueError("Reusing the score index did not reproduce plan sums.")
cache_test_seconds = perf_counter() - cache_test_start

start = perf_counter()
home = pl.read_parquet(args.folder / "home-durations.parquet")
stay = (pl.read_parquet(args.folder / "utilities.parquet").filter(pl.col("mode_seq_id") == 0)
        .select(unit + [pl.col("utility").alias("stay_utility")]))
output = context_plans.join(home, on=["country", "csp"]).join(stay, on=unit)
plan_ids = output["timing_plan"].to_numpy()
profile_ids = output["profile"].to_numpy()
night = np.maximum(24 - plan_duration[plan_ids] - plan_travel[plan_ids], 0)
home_min = output["mean_home_night_per_pers"].to_numpy() * np.exp(-args.constant)
home_scale = output["stay_utility"].to_numpy() / args.constant
utility = plan_utilities[profile_ids, plan_ids] + home_scale * np.maximum(
    np.log(np.maximum(night, np.finfo(np.float64).tiny) / home_min), 0)
output = output.with_columns(pl.Series("result_utility", utility),
                             pl.Series("result_feasible", plan_feasible[profile_ids, plan_ids]))
plan_seconds = perf_counter() - start
work_seconds = perf_counter() - work_start
rss_before_validation = max(peak[0], process.memory_info().rss)

mapping = pl.read_parquet(args.folder / "modeled-mapping.parquet").rename(
    {"time_seq_id": "modeled_time_seq_id", "previous_time_seq_id": "time_seq_id"})
expected = (pl.read_parquet(args.folder / "utilities.parquet").filter(pl.col("mode_seq_id") != 0)
            .rename({"time_seq_id": "modeled_time_seq_id", "utility": "expected_utility",
                     "is_feasible": "expected_feasible"}))
comparison = (output.join(mapping, on=PLAN_KEY_COLS)
              .join(expected, on=[name for name in PLAN_KEY_COLS if name != "time_seq_id"] + ["modeled_time_seq_id"]))
if comparison.height != members.height or comparison.filter(
    pl.col("result_feasible") != pl.col("expected_feasible")
).height:
    raise ValueError("Candidate coverage or feasibility differs from the captured evaluator.")
max_error = comparison.filter(pl.col("expected_feasible")).select(
    (pl.col("result_utility") - pl.col("expected_utility")).abs().max()).item()
if max_error > 1e-8:
    raise ValueError(f"Plan utility differs by {max_error}.")
reference = (pl.read_parquet(args.folder / "modeled-steps.parquet")
             .unique(subset=PLAN_KEY_COLS + ["seq_step_index"], keep="first")
             .rename({"time_seq_id": "modeled_time_seq_id"}))
expected_steps = (context_plans.select(PLAN_KEY_COLS + ["timing_plan"]).join(mapping, on=PLAN_KEY_COLS)
                  .join(reference.select([name for name in PLAN_KEY_COLS if name != "time_seq_id"]
                                         + ["modeled_time_seq_id", "seq_step_index", "departure_time", "arrival_time",
                                            "next_departure_time", "duration_per_pers"]),
                        on=[name for name in PLAN_KEY_COLS if name != "time_seq_id"] + ["modeled_time_seq_id"]))
timed_columns = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
checked = expected_steps.join(timed.select(["timing_plan", "seq_step_index"] + timed_columns),
                              on=["timing_plan", "seq_step_index"], suffix="_new")
if (checked.height != pl.scan_parquet(args.folder / "expanded-input.parquet").select(pl.len()).collect().item()
        or checked.filter(pl.any_horizontal([~pl.col(name).eq_missing(pl.col(name + "_new"))
                                            for name in timed_columns])).height):
    raise ValueError("Modeled timings do not match the captured evaluator.")
stop.set()
thread.join()
print("DENSE_SPARSE_RESULT", json.dumps({"strategy": args.strategy, "physical_steps": physical.height,
      "context_plans": members.height, "transport_profile_plans": travel_plans.height,
      "distinct_timing_plans": timing_plans.height, "timed_steps": timed.height,
      "sections": sections.height, "section_atom_references": section_atoms.height,
      "physical_plan_section_references": references.height, "profiles": profiles.height,
      "needed_section_profiles": None if needed_pairs is None else needed_pairs.height,
      "section_evaluations": section_evaluations,
      "atom_evaluations": atom_evaluations, "max_atoms_per_profile": peak_profile_atoms,
      "score_table_raw_bytes": (int(dense_utilities.nbytes + dense_feasible.nbytes)
                                if dense_utilities is not None else needed_pairs.estimated_size()
                                + sum(u.nbytes + f.nbytes for u, f in sparse_scores.values())),
      "matched_context_plans": comparison.height, "matched_context_steps": checked.height,
      "max_feasible_utility_error": max_error, "load_seconds": load_seconds,
      "timing_factor_and_update_seconds": timing_seconds, "section_build_seconds": section_build_seconds,
      "needed_index_seconds": index_seconds, "profile_scoring_seconds": score_seconds,
      "plan_attachment_seconds": plan_seconds, "total_work_seconds": work_seconds,
      "cached_plan_sum_seconds": cached_plan_sum_seconds,
      "initial_work_excluding_cached_checks_seconds": work_seconds - cache_test_seconds,
      "peak_process_rss_before_validation": rss_before_validation,
      "peak_process_rss_bytes_including_validation": peak[0]}), flush=True)
