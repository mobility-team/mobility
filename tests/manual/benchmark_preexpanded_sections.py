"""Score captured candidates from physical steps before context expansion.

Usage: pixi run python tests/manual/benchmark_preexpanded_sections.py CAPTURE
       --strategy rigid|prefix

The existing evaluator's retained cost/parameter lookup is an input. Its
expanded rows, modeled mapping and final utilities are used only for validation.
This diagnostic does not yet calculate the lookup, assign new modeled schedule
IDs or replace candidate generation and behavioral transitions.
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
parser.add_argument("--strategy", choices=["rigid", "prefix"], required=True)
parser.add_argument("--constant", type=float, default=1.25)
parser.add_argument("--stream-profiles", action="store_true", help="Build and discard section scores one profile at a time")
parser.add_argument("--dense-census", action="store_true", help="Count possible section/profile table entries and exit")
args = parser.parse_args()
if args.dense_census and args.stream_profiles:
    raise ValueError("The dense census needs the all-profile membership count.")
unit = ["demand_group_id", "demand_subgroup_id"]
profile_columns = ["country", "csp", "utility_profile_id"]
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
profiles = members.select(profile_columns).unique(maintain_order=True).with_row_index("profile")
members = members.join(profiles, on=profile_columns, how="left", maintain_order="left")
travel_plans = (members.select(PLAN_COLUMNS + ["utility_profile_id"]).unique(maintain_order=True)
                .with_row_index("travel_plan"))
plan_classes = (members.select(PLAN_COLUMNS + ["utility_profile_id", "profile"]).unique(maintain_order=True)
                .join(travel_plans, on=PLAN_COLUMNS + ["utility_profile_id"], maintain_order="left"))
context_plans = (members.select(PLAN_KEY_COLS + ["profile", "utility_profile_id", "country", "csp"])
                 .join(plan_classes, on=PLAN_COLUMNS + ["utility_profile_id", "profile"], maintain_order="left"))
if context_plans.height != members.height or context_plans["travel_plan"].null_count():
    raise ValueError("Context memberships were lost while attaching shared physical plans.")
travel = scores.select(travel_keys + ["cost", "time", "distance", "is_feasible"]).unique()
if travel.select(travel_keys).n_unique() != travel.height:
    raise ValueError("The captured travel-profile key has conflicting cost or time inputs.")
activity_lookup = scores.select(activity_keys + ["min_activity_time", "activity_utility_scale"]).unique()
if activity_lookup.select(activity_keys).n_unique() != activity_lookup.height:
    raise ValueError("The captured activity key has conflicting utility inputs.")
load_seconds = perf_counter() - start

start = perf_counter()
timing_input = (travel_plans.join(physical, on=PLAN_COLUMNS)
                .join(travel, on=travel_keys, how="left")
                .rename({"utility_profile_id": "demand_group_id"})
                .with_columns(demand_subgroup_id=pl.lit(0, dtype=pl.UInt32)))
if timing_input["time"].null_count():
    raise ValueError("A candidate has no captured travel-time lookup.")
timed = (PlanScheduleUpdater().update_plan_timings(timing_input.lazy(), **settings)
         .with_columns(*[((pl.col(name).cast(pl.Float64) * 3600).round(0) / 3600)
                         .cast(pl.Float32).alias(name) for name in
                         ("departure_time", "arrival_time", "next_departure_time", "duration_per_pers")])
         .with_columns(step_feasible=(pl.col("is_feasible").fill_null(False)
                       & ((pl.col("next_departure_time") >= pl.col("arrival_time"))
                          | (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS)))))
         .collect(engine="streaming").rename({"demand_group_id": "utility_profile_id"})
         .drop("demand_subgroup_id"))
timing_seconds = perf_counter() - start

# Form rigid-bounded sections once per physical plan/travel profile. A rigid
# activity is its own atom; other consecutive trip/activity atoms form sections.
start = perf_counter()
trip_columns = ["utility_profile_id", "from", "to", "mode", "departure_time", "arrival_time", "time"]
activity_columns = ["to", "activity", "arrival_time", "next_departure_time", "duration_per_pers", "step_feasible"]
trip = timed.select(trip_columns).unique(maintain_order=True).with_row_index("trip")
activity = timed.select(activity_columns).unique(maintain_order=True).with_row_index("activity_atom")
rows = timed.join(trip, on=trip_columns, maintain_order="left").join(
    activity, on=activity_columns, maintain_order="left")
rigid = [name for name, value in settings["arrival_time_rigidity_by_activity"].items() if value == 1.0]
trip_rows = rows.select("travel_plan", (pl.col("seq_step_index").cast(pl.UInt32) * 2).alias("position"),
                        (pl.col("trip").cast(pl.UInt64) * 2).alias("token"), pl.lit(False).alias("anchor"))
activity_rows = rows.select("travel_plan", (pl.col("seq_step_index").cast(pl.UInt32) * 2 + 1).alias("position"),
                            (pl.col("activity_atom").cast(pl.UInt64) * 2 + 1).alias("token"),
                            pl.col("activity").cast(pl.String).is_in(rigid).alias("anchor"))
atoms = pl.concat([trip_rows, activity_rows]).sort("travel_plan", "position")
atoms = atoms.with_columns(previous_anchor=pl.col("anchor").shift(1).over("travel_plan").fill_null(True))
atoms = atoms.with_columns(boundary=(pl.col("anchor") | pl.col("previous_anchor")))
atoms = atoms.with_columns(section_in_plan=pl.col("boundary").cast(pl.UInt32).cum_sum().over("travel_plan"))
groups = atoms.group_by("travel_plan", "section_in_plan", maintain_order=True).agg(pl.col("token").alias("tokens"))
sections = groups.select("tokens").unique(maintain_order=True).with_row_index("section")
references = groups.join(sections, on="tokens", maintain_order="left").select("travel_plan", "section")
section_atoms = sections.explode("tokens").rename({"tokens": "token"})
plan_section_profiles = None if args.stream_profiles else (
    plan_classes.select("travel_plan", "profile").join(references, on="travel_plan")
    .select("profile", "section").unique())
prefix_nodes = prefix_paths = section_leaves = None
if args.strategy == "prefix":
    ordered = section_atoms.with_columns(pl.col("token").cum_count().over("section").alias("depth"))
    frontier = sections.select("section").with_columns(parent=pl.lit(0, dtype=pl.UInt32))
    nodes, paths = [], []
    next_node = 1
    for depth in sorted(ordered["depth"].unique().to_list()):
        layer = ordered.filter(pl.col("depth") == depth).join(frontier, on="section", maintain_order="left")
        distinct = (layer.select("parent", "token").unique(maintain_order=True)
                    .with_row_index("node", offset=next_node)
                    .with_columns(depth=pl.lit(depth, dtype=pl.UInt32)))
        assigned = layer.join(distinct, on=["parent", "token"], maintain_order="left")
        nodes.append(distinct.select("node", "parent", "token", "depth"))
        paths.append(assigned.select("section", "node"))
        frontier = assigned.select("section", pl.col("node").alias("parent"))
        next_node += distinct.height
    prefix_nodes = pl.concat(nodes).sort("node")
    prefix_paths = pl.concat(paths)
    section_leaves = prefix_paths.group_by("section", maintain_order=True).agg(pl.col("node").last())
    del ordered, frontier, nodes, paths, layer, distinct, assigned
del timing_input, rows, trip_rows, activity_rows, atoms, groups
section_seconds = perf_counter() - start

if args.dense_census:
    # Count an upper bound on compatible combinations. Activity-only sections
    # can use any profile; sections with trips must match their travel profile.
    # Missing activity/cost inputs may make some counted pairs unavailable.
    trip_tokens = trip.select((pl.col("trip").cast(pl.UInt64) * 2).alias("token"), "utility_profile_id")
    section_travel = (section_atoms.join(trip_tokens, on="token", how="inner")
                      .group_by("section").agg(pl.col("utility_profile_id").first(),
                                                pl.col("utility_profile_id").n_unique().alias("profiles")))
    if section_travel.filter(pl.col("profiles") > 1).height:
        raise ValueError("A section contains trips with incompatible travel profiles.")
    per_travel = profiles.group_by("utility_profile_id").len(name="eligible_profiles")
    sections_stats = (section_atoms.group_by("section").len(name="atoms")
                      .join(section_travel.select("section", "utility_profile_id"), on="section", how="left")
                      .join(per_travel, on="utility_profile_id", how="left")
                      .with_columns(pl.col("eligible_profiles").fill_null(profiles.height)))
    print("DENSE_SECTION_CENSUS", json.dumps({
        "physical_sections": sections.height, "scoring_profiles": profiles.height,
        "travel_profiles": profiles["utility_profile_id"].n_unique(),
        "all_section_profile_slots": sections.height * profiles.height,
        "travel_compatible_slots_upper_bound": int(sections_stats["eligible_profiles"].sum()),
        "travel_compatible_section_atom_rows_upper_bound": int(sections_stats.select(
            (pl.col("atoms").cast(pl.UInt64) * pl.col("eligible_profiles").cast(pl.UInt64)).sum()).item()),
        "activity_only_sections": sections.height - section_travel.height,
        "actually_requested_section_profiles": plan_section_profiles.height,
        "profile_counts_by_travel_profile": per_travel.sort("utility_profile_id").to_dicts(),
    }), flush=True)
    stop.set()
    thread.join()
    raise SystemExit(0)

# The lookup contains scores once per actual OD/mode/profile and activity
# destination/profile. Request only the atoms used by each section/profile.
trip = trip.with_columns((pl.col("trip").cast(pl.UInt64) * 2).alias("token"))
activity = activity.with_columns((pl.col("activity_atom").cast(pl.UInt64) * 2 + 1).alias("token"))


def score_sections(requested_sections):
    if args.strategy == "prefix":
        requests = (requested_sections.join(prefix_paths, on="section").select("profile", "node").unique()
                    .join(prefix_nodes, on="node"))
    else:
        requests = requested_sections.join(section_atoms, on="section")
    needed = requests.select("profile", "token").unique().join(profiles, on="profile")
    trip_scores = (needed.filter(pl.col("token") % 2 == 0)
                   .join(trip, on=["token", "utility_profile_id"]).join(travel, on=travel_keys)
                   .select("profile", "token", (-pl.col("cost")).alias("utility"),
                           pl.lit(0.0).alias("duration"), pl.col("time").fill_null(0.0).alias("travel"),
                           pl.lit(True).alias("feasible")))
    activity_scores = (needed.filter(pl.col("token") % 2 == 1).join(activity, on="token")
                       .join(activity_lookup, on=activity_keys)
                       .select("profile", "token", (pl.col("activity_utility_scale")
                               * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0))
                               .alias("utility"), pl.col("duration_per_pers").cast(pl.Float64).alias("duration"),
                               pl.lit(0.0).alias("travel"), pl.col("step_feasible").alias("feasible")))
    atom_scores = pl.concat([trip_scores, activity_scores], how="vertical_relaxed")
    if atom_scores.height != needed.height or atom_scores.select("profile", "token").n_unique() != needed.height:
        raise ValueError("Some required section/profile atoms do not have exactly one captured score.")
    inputs = requests.join(atom_scores, on=["profile", "token"], how="left")
    if inputs["utility"].null_count():
        raise ValueError("A requested section atom has no utility.")
    if args.strategy == "rigid":
        result = inputs.group_by("profile", "section").agg(
            pl.col("utility").sum(), pl.col("duration").sum(), pl.col("travel").sum(), pl.col("feasible").all())
    else:
        accumulated = []
        previous = None
        for depth in sorted(inputs["depth"].unique().to_list()):
            layer = inputs.filter(pl.col("depth") == depth)
            if previous is not None:
                parents = previous.select("profile", pl.col("node").alias("parent"),
                                          *[pl.col(name).alias("parent_" + name)
                                            for name in ("utility", "duration", "travel", "feasible")])
                layer = layer.join(parents, on=["profile", "parent"], how="left")
                if layer["parent_utility"].null_count():
                    raise ValueError("A prefix score has no scored parent.")
                layer = layer.with_columns(*[(pl.col(name) + pl.col("parent_" + name)).alias(name)
                                             for name in ("utility", "duration", "travel")],
                                           feasible=pl.col("feasible") & pl.col("parent_feasible"))
            previous = layer.select("profile", "node", "utility", "duration", "travel", "feasible")
            accumulated.append(previous)
        result = (requested_sections.join(section_leaves, on="section")
                  .join(pl.concat(accumulated), on=["profile", "node"])
                  .select("profile", "section", "utility", "duration", "travel", "feasible"))
    return result, requests.height, atom_scores.height


start = perf_counter()
request_count = atom_count = max_profile_scores = 0
if args.stream_profiles:
    class_parts = []
    for profile in profiles["profile"].to_list():
        local_classes = plan_classes.filter(pl.col("profile") == profile)
        if local_classes.is_empty():
            continue
        requested = (local_classes.select("travel_plan", "profile").join(references, on="travel_plan")
                     .select("profile", "section").unique())
        section_scores, local_requests, local_atoms = score_sections(requested)
        request_count += local_requests
        atom_count += local_atoms
        max_profile_scores = max(max_profile_scores, section_scores.height)
        class_parts.append(local_classes.join(references, on="travel_plan")
                           .join(section_scores, on=["profile", "section"])
                           .group_by("travel_plan", "profile").agg(
                               pl.col("utility").sum().alias("step_utility"), pl.col("duration").sum(),
                               pl.col("travel").sum(), pl.col("feasible").all().alias("result_feasible")))
    class_totals = pl.concat(class_parts)
    section_scoring_seconds = perf_counter() - start
    plan_start = perf_counter()
else:
    section_scores, request_count, atom_count = score_sections(plan_section_profiles)
    section_scoring_seconds = perf_counter() - start
    plan_start = perf_counter()
    class_totals = (plan_classes.join(references, on="travel_plan")
                    .join(section_scores, on=["profile", "section"])
                    .group_by("travel_plan", "profile").agg(
                        pl.col("utility").sum().alias("step_utility"), pl.col("duration").sum(),
                        pl.col("travel").sum(), pl.col("feasible").all().alias("result_feasible")))
home = pl.read_parquet(args.folder / "home-durations.parquet")
stay = (pl.read_parquet(args.folder / "utilities.parquet").filter(pl.col("mode_seq_id") == 0)
        .select(unit + [pl.col("utility").alias("stay_utility")]))
result = (context_plans.join(class_totals, on=["travel_plan", "profile"])
          .join(home, on=["country", "csp"]).join(stay, on=unit)
          .with_columns(home_hours=(24 - pl.col("duration") - pl.col("travel")).clip(0.0))
          .with_columns(result_utility=pl.col("step_utility") + pl.col("stay_utility") / args.constant
                        * (pl.col("home_hours") / (pl.col("mean_home_night_per_pers") * np.exp(-args.constant)))
                        .log().clip(0.0)))
plan_seconds = perf_counter() - plan_start
rss_before_validation = max(peak[0], process.memory_info().rss)

# Use the captured mapping only to compare the independent input-plan scores
# with the existing evaluator's modeled plan IDs; it is not used to score.
mapping = pl.read_parquet(args.folder / "modeled-mapping.parquet").rename(
    {"time_seq_id": "modeled_time_seq_id", "previous_time_seq_id": "time_seq_id"})
expected = (pl.read_parquet(args.folder / "utilities.parquet").filter(pl.col("mode_seq_id") != 0)
            .rename({"time_seq_id": "modeled_time_seq_id", "utility": "expected_utility",
                     "is_feasible": "expected_feasible"}))
comparison = (result.join(mapping, on=PLAN_KEY_COLS)
              .join(expected, on=[name for name in PLAN_KEY_COLS if name != "time_seq_id"]
                    + ["modeled_time_seq_id"]))
if comparison.height != members.height:
    raise ValueError(f"Only {comparison.height} of {members.height} input plans matched the captured outputs.")
if comparison.filter(pl.col("result_feasible") != pl.col("expected_feasible")).height:
    raise ValueError("Feasibility differs from the existing evaluator.")
max_error = (comparison.filter(pl.col("expected_feasible"))
             .select((pl.col("result_utility") - pl.col("expected_utility")).abs().max()).item())
if max_error > 1e-8:
    raise ValueError(f"Plan utility differs by {max_error}.")

# Verify every modeled clock/duration value against the existing evaluation.
reference = (pl.read_parquet(args.folder / "modeled-steps.parquet")
             .unique(subset=PLAN_KEY_COLS + ["seq_step_index"], keep="first")
             .rename({"time_seq_id": "modeled_time_seq_id"}))
expected_steps = (context_plans.select(PLAN_KEY_COLS + ["travel_plan"])
                  .join(mapping, on=PLAN_KEY_COLS)
                  .join(reference.select([name for name in PLAN_KEY_COLS if name != "time_seq_id"]
                                         + ["modeled_time_seq_id", "seq_step_index", "departure_time", "arrival_time",
                                            "next_departure_time", "duration_per_pers"]),
                        on=[name for name in PLAN_KEY_COLS if name != "time_seq_id"] + ["modeled_time_seq_id"]))
timed_columns = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
checked = expected_steps.join(timed.select(["travel_plan", "seq_step_index"] + timed_columns),
                              on=["travel_plan", "seq_step_index"], suffix="_new")
if (checked.height != pl.read_parquet(args.folder / "expanded-input.parquet").height
        or checked.filter(pl.any_horizontal([~pl.col(name).eq_missing(pl.col(name + "_new"))
                                            for name in timed_columns])).height):
    raise ValueError("Modeled timing or step coverage differs from the captured evaluator.")
stop.set()
thread.join()
print("PREEXPANDED_SECTION_RESULT", json.dumps({"strategy": args.strategy, "physical_steps": physical.height,
      "context_plans": members.height, "travel_plans": travel_plans.height, "timed_travel_steps": timed.height,
      "scoring_classes": plan_classes.height, "sections": sections.height,
      "physical_plan_section_references": references.height,
      "profile_sections": None if args.stream_profiles else plan_section_profiles.height,
      "stream_profiles": args.stream_profiles, "max_profile_section_scores": max_profile_scores,
      "prefix_nodes": None if prefix_nodes is None else prefix_nodes.height,
      "profile_section_or_prefix_rows": request_count, "profile_atom_scores": atom_count,
      "matched_context_plans": comparison.height, "matched_context_steps": checked.height,
      "max_feasible_utility_error": max_error, "load_and_lookup_seconds": load_seconds,
      "physical_timing_seconds": timing_seconds, "section_construction_seconds": section_seconds,
      "section_scoring_seconds": section_scoring_seconds, "plan_scoring_seconds": plan_seconds,
      "peak_process_rss_before_validation": rss_before_validation,
      "peak_process_rss_bytes_including_validation": peak[0]}), flush=True)
