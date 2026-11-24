"""Test integer-minute timing and section reuse on two captured inner batches.

The capture supplies candidate inputs and scoring lookups. Candidate generation,
lookup construction and modeled-ID assignment are outside the measured work.
Results are compared with the captured second-resolution evaluator after each batch.
Usage: pixi run python tests/manual/benchmark_incremental_sections.py CAPTURE --strategy incremental|rebuild
"""
import argparse
import ctypes
import json
import math
import threading
from pathlib import Path
from time import perf_counter

import numpy as np
import polars as pl
import psutil

from mobility.trips.group_day_trips.plans.compact_candidate_steps import PLAN_COLUMNS
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--strategy", choices=["rebuild", "incremental"], required=True)
parser.add_argument("--constant", type=float, default=1.25)
parser.add_argument("--kernel", type=Path, help="Optional compiled section and plan reduction kernel.")
parser.add_argument("--atom-input", choices=["needed", "all"], default="needed",
                    help="Join only atoms used by unresolved sections, or all atoms for each profile.")
args = parser.parse_args()
if args.kernel and args.atom_input != "all":
    parser.error("The compiled kernel requires --atom-input all.")
kernel = ctypes.CDLL(str(args.kernel.resolve())).score_sections_and_plans if args.kernel else None
if kernel:
    kernel.restype = None
    kernel.argtypes = ([ctypes.c_void_p, ctypes.c_size_t] + [ctypes.c_void_p] * 8
                       + [ctypes.c_void_p, ctypes.c_size_t] + [ctypes.c_void_p] * 4)
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

def run_batch(folder, cache, profile_registry):
    start = perf_counter()
    physical = pl.read_parquet(folder / "physical.parquet")
    members = pl.read_parquet(folder / "members.parquet")
    contexts = pl.read_parquet(folder / "contexts.parquet")
    scores = pl.read_parquet(folder / "step-scores.parquet")
    settings = json.loads((folder / "settings.json").read_text(encoding="utf-8"))
    # Survey clocks and modeled travel times enter the model as integer minutes.
    physical = physical.with_columns(*[(pl.col(name).cast(pl.Float64) * 60).round(0).cast(pl.Int32).alias(name)
                                       for name in ("departure_time", "arrival_time", "next_departure_time", "duration_per_pers")])
    scores = scores.with_columns((pl.col("time").cast(pl.Float64) * 60).round(0).cast(pl.Int32))
    members = members.join(contexts.select(unit + ["utility_profile_id"]), on=unit, how="left")
    profiles = profile_registry
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
    previous_sections = cache["sections"].height if cache.get("sections") is not None else 0

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
    batch_signatures = signatures.select(PLAN_COLUMNS + ["travel_times"]).unique(maintain_order=True)
    if cache.get("timing_plans") is None:
        timing_plans = batch_signatures.with_row_index("timing_plan")
        new_timing = timing_plans
    else:
        missing = batch_signatures.join(cache["timing_plans"], on=PLAN_COLUMNS + ["travel_times"], how="anti")
        new_timing = missing.with_row_index("timing_plan", offset=cache["timing_plans"].height)
        timing_plans = pl.concat([cache["timing_plans"], new_timing])
    travel_to_timing = signatures.join(timing_plans, on=PLAN_COLUMNS + ["travel_times"], maintain_order="left") \
        .select("travel_plan", "timing_plan")
    timing_input = (travel_steps.join(travel_to_timing, on="travel_plan").join(new_timing.select("timing_plan"), on="timing_plan")
                    .unique(subset=["timing_plan", "seq_step_index"], keep="first")
                    .drop("travel_plan", "utility_profile_id")
                    .rename({"timing_plan": "demand_group_id"})
                    .with_columns(demand_subgroup_id=pl.lit(0, dtype=pl.UInt32), is_feasible=pl.lit(True)))
    rigidity = pl.DataFrame({"activity": list(settings["arrival_time_rigidity_by_activity"]),
                             "rigidity": list(settings["arrival_time_rigidity_by_activity"].values())}).with_columns(
                                 pl.col("activity").cast(timing_input.schema["activity"]))
    if settings["enabled"]:
        timed = (timing_input.join(rigidity, on="activity", how="left")
                 .with_columns(delta=(pl.col("time") - (pl.col("arrival_time") - pl.col("departure_time")).clip(0, 1440)))
                 .with_columns(departure_time=(pl.col("departure_time") - (pl.col("rigidity") * pl.col("delta")).round(0).cast(pl.Int32)))
                 .with_columns(arrival_time=pl.col("departure_time") + pl.col("time"))
                 .sort(PLAN_KEY_COLS + ["seq_step_index"])
                 .with_columns(next_departure_time=pl.col("departure_time").shift(-1).over(PLAN_KEY_COLS)
                               .fill_null(pl.col("next_departure_time")))
                 .with_columns(duration_per_pers=(pl.col("next_departure_time") - pl.col("arrival_time")).clip(lower_bound=0))
                 .drop("rigidity", "delta"))
    else:
        timed = timing_input
    timed = (timed.with_columns(timing_feasible=((pl.col("next_departure_time") >= pl.col("arrival_time"))
                       | (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS))))
             .rename({"demand_group_id": "timing_plan"}).drop("demand_subgroup_id"))
    added_timed = timed
    if cache.get("timed") is not None:
        timed = pl.concat([cache["timed"], added_timed])
    timing_seconds = perf_counter() - start

    start = perf_counter()
    trip_cols = ["from", "to", "mode", "departure_time", "arrival_time", "time"]
    activity_cols = ["to", "activity", "arrival_time", "next_departure_time", "duration_per_pers", "timing_feasible"]
    trip_values = added_timed.select(trip_cols).unique(maintain_order=True)
    activity_values = added_timed.select(activity_cols).unique(maintain_order=True)
    if cache.get("trip") is None:
        trip = trip_values.with_row_index("trip")
        activity = activity_values.with_row_index("activity_atom")
    else:
        new_trip = trip_values.join(cache["trip"], on=trip_cols, how="anti").with_row_index("trip", offset=cache["trip"].height)
        new_activity = activity_values.join(cache["activity"], on=activity_cols, how="anti").with_row_index("activity_atom", offset=cache["activity"].height)
        trip = pl.concat([cache["trip"], new_trip])
        activity = pl.concat([cache["activity"], new_activity])
    rows = added_timed.join(trip, on=trip_cols, maintain_order="left").join(activity, on=activity_cols, maintain_order="left")
    rigid = [name for name, value in settings["arrival_time_rigidity_by_activity"].items() if value == 1.0]
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
    section_values = groups.select("tokens").unique(maintain_order=True)
    if cache.get("sections") is None:
        sections = section_values.with_row_index("section")
    else:
        new_sections = section_values.join(cache["sections"], on="tokens", how="anti").with_row_index("section", offset=cache["sections"].height)
        sections = pl.concat([cache["sections"], new_sections])
    references = (groups.join(sections, on="tokens", maintain_order="left")
                  .select("timing_plan", "section"))
    if cache.get("references") is not None:
        references = pl.concat([cache["references"], references])
    references = references.sort("timing_plan")
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

    # A dense table holds one score per section and profile. A slot whose
    # inputs are not present in the lookup remains unknown until a later batch.
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
    plan_duration = np.add.reduceat(section_duration[plan_sections], plan_offsets[:-1]) / 60.0
    plan_travel = np.add.reduceat(section_travel[plan_sections], plan_offsets[:-1]) / 60.0
    trip = trip.with_columns((pl.col("trip").cast(pl.UInt64) * 2).alias("token"))
    activity = activity.with_columns((pl.col("activity_atom").cast(pl.UInt64) * 2 + 1).alias("token"))
    token_count = max(trip.height * 2, activity.height * 2 + 1)
    plan_utilities = np.zeros((profiles.height, timing_plans.height), dtype=np.float64)
    plan_feasible = np.zeros((profiles.height, timing_plans.height), dtype=bool)
    dense_utilities = np.zeros((profiles.height, sections.height), dtype=np.float64)
    dense_feasible = np.zeros((profiles.height, sections.height), dtype=bool)
    dense_known = np.zeros((profiles.height, sections.height), dtype=bool)
    if cache.get("scores") is not None:
        old_count = cache["scores"].shape[1]
        dense_utilities[:, :old_count] = cache["scores"]
        dense_feasible[:, :old_count] = cache["feasible"]
        dense_known[:, :old_count] = cache["known"]
    atom_evaluations = section_evaluations = peak_profile_atoms = 0
    profile_input_seconds = atom_join_seconds = section_reduction_seconds = plan_sum_seconds = 0.0
    start = perf_counter()
    for profile_row in profiles.iter_rows(named=True):
        profile = profile_row["profile"]
        if profile not in classes_by_profile:
            continue
        selected_sections = np.flatnonzero(~dense_known[profile]).astype(np.uint32)
        if kernel is None:
            mask = np.zeros(sections.height, dtype=bool)
            mask[selected_sections] = True
            selected_tokens = section_tokens[mask[section_by_atom]]
            selected_offsets = np.r_[0, np.cumsum(section_lengths[selected_sections])[:-1]]
        if args.atom_input == "needed":
            unique_tokens = np.unique(selected_tokens)
            trip_ids = (unique_tokens[unique_tokens % 2 == 0] // 2).astype(np.uint32)
            activity_ids = (unique_tokens[unique_tokens % 2 == 1] // 2).astype(np.uint32)
            profile_atom_count = len(unique_tokens)
        else:
            profile_atom_count = trip.height + activity.height
        peak_profile_atoms = max(peak_profile_atoms, profile_atom_count)
        profile_start = perf_counter()
        profile_travel = (travel.filter(pl.col("utility_profile_id") == profile_row["utility_profile_id"])
                          .select("from", "to", "mode", "cost", pl.col("time").alias("profile_time"),
                                  pl.col("is_feasible").alias("route_feasible")))
        profile_activity = (activity_lookup.filter((pl.col("country") == profile_row["country"])
                                                  & (pl.col("csp") == profile_row["csp"]))
                            .select("activity", "to", "min_activity_time", "activity_utility_scale"))
        profile_input_seconds += perf_counter() - profile_start
        profile_start = perf_counter()
        trip_input = trip if args.atom_input == "all" else trip.filter(pl.col("trip").is_in(trip_ids))
        activity_input = activity if args.atom_input == "all" else activity.filter(pl.col("activity_atom").is_in(activity_ids))
        trip_scores = (trip_input.join(
            profile_travel, on=["from", "to", "mode"], how="left").with_columns(
                known=pl.col("route_feasible").is_not_null(),
                valid=(pl.col("route_feasible").fill_null(False) & (pl.col("time") == pl.col("profile_time")).fill_null(False)),
                utility=(-pl.col("cost")).fill_null(0.0)))
        activity_scores = (activity_input.join(
            profile_activity, on=["activity", "to"], how="left").with_columns(
                known=pl.col("min_activity_time").is_not_null(),
                valid=pl.col("timing_feasible").fill_null(False) & pl.col("min_activity_time").is_not_null(),
                utility=(pl.col("activity_utility_scale")
                         * (pl.col("duration_per_pers") / 60.0 / pl.col("min_activity_time")).log().clip(0.0)).fill_null(0.0)))
        atom_join_seconds += perf_counter() - profile_start
        profile_start = perf_counter()
        token_utility = np.zeros(token_count, dtype=np.float64)
        token_feasible = np.zeros(token_count, dtype=bool)
        token_known = np.zeros(token_count, dtype=bool)
        for frame in (trip_scores, activity_scores):
            ids = frame["token"].to_numpy().astype(np.int64)
            token_utility[ids] = frame["utility"].to_numpy()
            token_feasible[ids] = frame["valid"].to_numpy()
            token_known[ids] = frame["known"].to_numpy()
        selected_plans = classes_by_profile[profile]
        if kernel:
            pointer = lambda array: ctypes.c_void_p(array.ctypes.data)
            kernel(pointer(selected_sections), len(selected_sections), pointer(section_offsets), pointer(section_tokens),
                   pointer(token_utility), pointer(token_feasible), pointer(token_known),
                   pointer(dense_utilities[profile]), pointer(dense_feasible[profile]), pointer(dense_known[profile]),
                   pointer(selected_plans), len(selected_plans), pointer(plan_offsets), pointer(plan_sections),
                   pointer(plan_utilities[profile]), pointer(plan_feasible[profile]))
            section_reduction_seconds += perf_counter() - profile_start
        else:
            section_utility_values = np.add.reduceat(token_utility[selected_tokens], selected_offsets)
            section_feasible_values = np.logical_and.reduceat(token_feasible[selected_tokens], selected_offsets)
            section_known_values = np.logical_and.reduceat(token_known[selected_tokens], selected_offsets)
            dense_utilities[profile, selected_sections] = section_utility_values
            dense_feasible[profile, selected_sections] = section_feasible_values
            dense_known[profile, selected_sections] = section_known_values
            scores_by_section = dense_utilities[profile]
            feasible_by_section = dense_feasible[profile]
            section_reduction_seconds += perf_counter() - profile_start
            profile_start = perf_counter()
            selected_plans = classes_by_profile[profile]
            counts = plan_counts[selected_plans]
            starts = np.cumsum(np.r_[0, counts[:-1]])
            positions = (np.repeat(plan_offsets[selected_plans], counts)
                         + np.arange(int(counts.sum())) - np.repeat(starts, counts))
            plan_utilities[profile, selected_plans] = np.add.reduceat(scores_by_section[plan_sections[positions]], starts)
            plan_feasible[profile, selected_plans] = np.logical_and.reduceat(feasible_by_section[plan_sections[positions]], starts)
            plan_sum_seconds += perf_counter() - profile_start
        atom_evaluations += profile_atom_count
        section_evaluations += len(selected_sections)
    score_seconds = perf_counter() - start

    start = perf_counter()
    home = pl.read_parquet(folder / "home-durations.parquet")
    stay = (pl.read_parquet(folder / "utilities.parquet").filter(pl.col("mode_seq_id") == 0)
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

    mapping = pl.read_parquet(folder / "modeled-mapping.parquet").rename(
        {"time_seq_id": "modeled_time_seq_id", "previous_time_seq_id": "time_seq_id"})
    expected = (pl.read_parquet(folder / "utilities.parquet").filter(pl.col("mode_seq_id") != 0)
                .rename({"time_seq_id": "modeled_time_seq_id", "utility": "expected_utility",
                         "is_feasible": "expected_feasible"}))
    comparison = (output.join(mapping, on=PLAN_KEY_COLS)
                  .join(expected, on=[name for name in PLAN_KEY_COLS if name != "time_seq_id"] + ["modeled_time_seq_id"]))
    if comparison.height != members.height:
        raise ValueError("Candidate coverage differs from the captured evaluator.")
    # Independently check a deterministic sample with plain Python integer
    # arithmetic against the compact timed plans, including plan boundaries.
    sample = context_plans.select(PLAN_KEY_COLS + ["timing_plan"]).head(100)
    sample_steps = (pl.read_parquet(folder / "expanded-scored.parquet")
                    .join(sample, on=PLAN_KEY_COLS, how="inner").sort(PLAN_KEY_COLS + ["seq_step_index"]))
    sample_timed = timed.join(sample.select("timing_plan").unique(), on="timing_plan", how="inner")
    by_timing = {key[0]: group.sort("seq_step_index") for key, group in sample_timed.group_by("timing_plan")}
    sample_output = output.join(sample.select(PLAN_KEY_COLS), on=PLAN_KEY_COLS, how="inner")
    by_plan = {tuple(row[key] for key in PLAN_KEY_COLS): row for row in sample_output.to_dicts()}
    for key, group in sample_steps.group_by(PLAN_KEY_COLS, maintain_order=True):
        rows = group.to_dicts()
        departures = []
        arrivals = []
        for row in rows:
            start = round(row["departure_time"] * 60)
            end = round(row["arrival_time"] * 60)
            travel_minutes = round(row["time"] * 60)
            delta = travel_minutes - max(0, min(end - start, 1440))
            departure = start - round(settings["arrival_time_rigidity_by_activity"][row["activity"]] * delta)
            departures.append(departure)
            arrivals.append(departure + travel_minutes)
        expected_minutes = [(departures[i], arrivals[i],
                             departures[i + 1] if i + 1 < len(rows) else round(row["next_departure_time"] * 60))
                            for i, row in enumerate(rows)]
        expected_minutes = [(dep, arr, nxt, max(nxt - arr, 0)) for dep, arr, nxt in expected_minutes]
        actual_minutes = list(by_timing[rows[0]["timing_plan"]].select(
            "departure_time", "arrival_time", "next_departure_time", "duration_per_pers").iter_rows())
        if actual_minutes != expected_minutes:
            raise ValueError("Compact minute timing differs from the independent integer calculation.")
        result = by_plan[key]
        feasible = all(row["is_feasible"] and (i == len(rows) - 1 or nxt >= arr)
                       for i, (row, (_, arr, nxt, _)) in enumerate(zip(rows, expected_minutes)))
        utility = sum((row["activity_utility_scale"] * max(math.log(duration / 60 / row["min_activity_time"]), 0)
                       if duration > 0 else 0) - row["cost"]
                      for row, (_, _, _, duration) in zip(rows, expected_minutes))
        hours = max(24 - sum(round(row["time"] * 60) for row in rows) / 60
                    - sum(item[3] for item in expected_minutes) / 60, 0)
        home_min = result["mean_home_night_per_pers"] * math.exp(-args.constant)
        utility += result["stay_utility"] / args.constant * max(math.log(max(hours, 1e-300) / home_min), 0)
        if feasible != result["result_feasible"] or abs(utility - result["result_utility"]) > 1e-8:
            raise ValueError("Compact minute scoring differs from the independent integer calculation.")
    both = comparison.filter(pl.col("result_feasible") & pl.col("expected_feasible"))
    errors = both.select((pl.col("result_utility") - pl.col("expected_utility")).abs().alias("error"))["error"]
    # Distinct minute schedules form choices; aggregate probabilities to the
    # activity/destination/mode sequence so schedules that merge can be compared.
    behavior = unit + ["activity_seq_id", "dest_seq_id", "mode_seq_id"]
    signatures = (timed.sort("timing_plan", "seq_step_index").group_by("timing_plan", maintain_order=True)
                  .agg(pl.struct("departure_time", "arrival_time", "next_departure_time", "duration_per_pers").alias("minute_schedule")))
    minute_mobile = (output.filter(pl.col("result_feasible")).join(signatures, on="timing_plan")
                     .unique(subset=behavior + ["minute_schedule"]).select(behavior + [pl.col("result_utility").alias("utility")]))
    baseline_choices = pl.read_parquet(folder / "utilities.parquet").filter(pl.col("is_feasible"))
    stay_choices = baseline_choices.filter(pl.col("mode_seq_id") == 0).select(behavior + ["utility"])
    minute_choices = pl.concat([minute_mobile, stay_choices])
    def probabilities(choices):
        return (choices.with_columns(weight=((pl.col("utility") - pl.col("utility").max().over(unit)) * 0.25).exp())
                .with_columns(probability=pl.col("weight") / pl.col("weight").sum().over(unit))
                .group_by(behavior).agg(pl.col("probability").sum()))
    baseline_p = probabilities(baseline_choices)
    minute_p = probabilities(minute_choices)
    tv = (baseline_p.join(minute_p, on=behavior, how="full", coalesce=True, suffix="_minute")
          .with_columns(difference=(pl.col("probability").fill_null(0.0) - pl.col("probability_minute").fill_null(0.0)).abs())
          .group_by(unit).agg((pl.col("difference").sum() / 2).alias("tv"))
          .join(contexts.select(unit + ["n_persons"]), on=unit))
    weighted_tv = tv.select((pl.col("tv") * pl.col("n_persons")).sum() / pl.col("n_persons").sum()).item()
    # Counts are input alternatives; the probability metric deduplicates modeled schedules.
    print("MINUTE_RESULT", json.dumps({"batch": folder.name, "context_plans": members.height,
          "new_timing_plans": new_timing.height, "total_timing_plans": timing_plans.height,
          "new_sections": sections.height - previous_sections, "total_sections": sections.height,
          "changed_to_feasible": comparison.filter(~pl.col("expected_feasible") & pl.col("result_feasible")).height,
          "changed_to_infeasible": comparison.filter(pl.col("expected_feasible") & ~pl.col("result_feasible")).height,
          "feasible_in_both": both.height, "utility_error_median": errors.median(),
          "utility_error_p90": errors.quantile(0.9), "utility_error_p99": errors.quantile(0.99),
          "fresh_choice_weighted_tv": weighted_tv,
          "minimum_clock_minute": min(timed[name].min() for name in ("departure_time", "arrival_time", "next_departure_time")),
          "maximum_clock_minute": max(timed[name].max() for name in ("departure_time", "arrival_time", "next_departure_time")),
          "timed_steps_outside_day": timed.filter(pl.any_horizontal([~pl.col(name).is_between(0, 1440)
              for name in ("departure_time", "arrival_time", "next_departure_time")])).height,
          "zero_minute_activities": timed.filter(pl.col("duration_per_pers") == 0).height,
          "minute_mobile_modeled_choices": minute_mobile.height,
          "baseline_feasible_mobile_choices": baseline_choices.filter(pl.col("mode_seq_id") != 0).height,
          "timing_seconds": timing_seconds, "section_build_seconds": section_build_seconds,
          "profile_scoring_seconds": score_seconds, "plan_attachment_seconds": plan_seconds,
          "work_seconds": work_seconds, "peak_rss_before_comparison": rss_before_validation}), flush=True)

    cache.update(timing_plans=timing_plans, timed=timed, trip=trip.drop("token"),
                 activity=activity.drop("token"), sections=sections, references=references, scores=dense_utilities,
                 feasible=dense_feasible, known=dense_known)
    print("MINUTE_COUNTS", json.dumps({"new_timing_plans": new_timing.height,
          "reused_timing_plans": timing_plans.height - new_timing.height,
          "new_sections": sections.height - previous_sections, "total_sections": sections.height,
          "profile_sections_calculated": section_evaluations, "atom_input": args.atom_input, "compiled_kernel": bool(kernel),
          "unresolved_slots": int((~dense_known).sum())}), flush=True)

# Both batches use the same outer-step inputs. Reuse requires equal values for
# every lookup key already seen and equal physical rows for shared plan IDs.
folders = [args.folder / "batch-1", args.folder / "batch-2"]
a, b = [pl.read_parquet(folder / "step-scores.parquet") for folder in folders]
score_keys = ["country", "csp", "activity", "from", "to", "mode", "utility_profile_id"]
score_values = ["cost", "time", "is_feasible", "min_activity_time", "activity_utility_scale"]
common = a.join(b, on=score_keys, suffix="_next")
if common.height != a.height or common.filter(pl.any_horizontal(
    [~pl.col(name).eq_missing(pl.col(name + "_next")) for name in score_values])).height:
    raise ValueError("Scoring inputs changed between batches; reuse is unsafe.")
first, second = [pl.read_parquet(folder / "physical.parquet") for folder in folders]
physical_keys = PLAN_COLUMNS + ["seq_step_index"]
shared_plans = first.select(PLAN_COLUMNS).unique().join(second.select(PLAN_COLUMNS).unique(), on=PLAN_COLUMNS)
first_shared = first.join(shared_plans, on=PLAN_COLUMNS)
second_shared = second.join(shared_plans, on=PLAN_COLUMNS)
shared = first_shared.join(second_shared, on=physical_keys, suffix="_next")
if shared.height != first_shared.height or shared.height != second_shared.height:
    raise ValueError("A shared physical plan changed length between batches.")
if (folders[0] / "settings.json").read_text() != (folders[1] / "settings.json").read_text():
    raise ValueError("Timing settings changed between batches.")
physical_values = [name for name in first.columns if name not in physical_keys]
if shared.filter(pl.any_horizontal([~pl.col(name).eq_missing(pl.col(name + "_next"))
                                     for name in physical_values])).height:
    raise ValueError("A shared physical plan changed between batches.")
profiles = []
for folder in folders:
    m = pl.read_parquet(folder / "members.parquet")
    c = pl.read_parquet(folder / "contexts.parquet")
    profiles.append(m.join(c.select(unit + ["utility_profile_id"]), on=unit).select(profile_cols).unique())
profile_registry = pl.concat(profiles).unique().sort(profile_cols).with_row_index("profile")
del a, b, common, first, second, first_shared, second_shared, shared, shared_plans, profiles, m, c
cache = {}
for folder in folders:
    if args.strategy == "rebuild":
        cache = {}
    print("BATCH", folder.name, flush=True)
    run_batch(folder, cache, profile_registry)
stop.set()
thread.join()
