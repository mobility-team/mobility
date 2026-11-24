"""Compare exact cuts of captured plans and score/aggregate their sections.

Usage: pixi run python tests/manual/benchmark_plan_sections.py TIMING CAPTURE
       HOME_DURATIONS OUTPUT --strategy rigid|prefix|atomic|repeated_pairs

This diagnostic begins with already-expanded modeled steps. It never creates
new plan combinations. Trip and activity atoms are shared by exact modeled
physical inputs; their scores are indexed separately by scoring profile.
"""
import argparse
import ctypes
import json
import threading
from pathlib import Path
from time import perf_counter

import numpy as np
import polars as pl
import psutil

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("timing", type=Path)
parser.add_argument("capture", type=Path)
parser.add_argument("home_durations", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--strategy", choices=["rigid", "prefix", "atomic", "repeated_pairs"], required=True)
parser.add_argument("--repetitions", type=int, default=3)
parser.add_argument("--constant", type=float, default=1.25)
parser.add_argument("--stream-profiles", action="store_true", help="Build and discard section scores one profile at a time")
parser.add_argument("--prefix-flow-direct", action="store_true", help="Use direct section/atom links for flows with prefix scoring")
parser.add_argument("--prefix-flow-array", choices=["add_at", "bincount", "sparse"],
                    help="Propagate prefix volumes using indexed arrays instead of per-level dataframe joins")
parser.add_argument("--prefix-flow-rust", type=Path, help="Path to the experimental compiled prefix flow kernel")
args = parser.parse_args()
if args.strategy == "prefix" and args.stream_profiles:
    raise ValueError("This first prefix comparison uses the all-profile scoring path.")
if args.prefix_flow_array and (args.strategy != "prefix" or args.prefix_flow_direct):
    raise ValueError("Array traversal requires prefix scoring and cannot be combined with direct flow links.")
if args.prefix_flow_rust and (args.strategy != "prefix" or args.prefix_flow_direct or args.prefix_flow_array):
    raise ValueError("Rust traversal requires prefix scoring without another traversal option.")
args.output.mkdir(parents=True, exist_ok=True)
process = psutil.Process()
stop = threading.Event()
peak = [process.memory_info().rss]


def monitor():
    while not stop.wait(0.01):
        peak[0] = max(peak[0], process.memory_info().rss)


thread = threading.Thread(target=monitor, daemon=True)
thread.start()
unit = ["demand_group_id", "demand_subgroup_id"]
plan_step = PLAN_KEY_COLS + ["seq_step_index"]
settings = json.loads((args.timing / "settings.json").read_text(encoding="utf-8"))
rigid_activities = [name for name, value in settings["arrival_time_rigidity_by_activity"].items() if value == 1.0]
trip_columns = ["from", "to", "mode", "departure_time", "arrival_time", "time"]
activity_columns = ["to", "activity", "arrival_time", "next_departure_time", "duration_per_pers", "step_feasible"]
start = perf_counter()
source = (pl.read_parquet(args.timing / "indexed-2.parquet")
          .unique(subset=plan_step, keep="first").sort(plan_step)
          .with_columns(step_feasible=(pl.col("is_feasible").fill_null(False)
                       & ((pl.col("next_departure_time") >= pl.col("arrival_time"))
                          | (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS))))))
contexts = source.select(unit).unique()
choices = (pl.scan_parquet(args.capture / "candidate-utility.parquet")
           .join(contexts.lazy(), on=unit, how="semi").collect(engine="streaming"))
mobile = choices.filter(pl.col("mode_seq_id") != 0).select(PLAN_KEY_COLS + ["plan_id", "utility", "is_feasible"])
plans = (source.select(PLAN_KEY_COLS).unique(maintain_order=True)
         .join(mobile, on=PLAN_KEY_COLS, how="left", maintain_order="left").with_row_index("plan"))
if plans.height != mobile.height or plans["plan_id"].null_count():
    raise ValueError("Captured modeled candidates and plan utilities do not match.")
profile_columns = ["country", "csp", "utility_profile_id"]
context_profiles = pl.read_parquet(args.capture / "contexts.parquet").select(unit + profile_columns)
profiles = context_profiles.select(profile_columns).unique(maintain_order=True).with_row_index("profile")
plan_profiles = (plans.select(["plan"] + unit).join(context_profiles, on=unit, maintain_order="left")
                 .join(profiles, on=profile_columns, maintain_order="left"))
trip = source.select(trip_columns).unique(maintain_order=True).with_row_index("trip")
activity = source.select(activity_columns).unique(maintain_order=True).with_row_index("activity_atom")
rows = (source.join(plans.select(PLAN_KEY_COLS + ["plan"]), on=PLAN_KEY_COLS, maintain_order="left")
        .join(plan_profiles.select("plan", "profile"), on="plan", maintain_order="left")
        .join(trip, on=trip_columns, maintain_order="left")
        .join(activity, on=activity_columns, maintain_order="left"))
if rows.height != source.height or rows["trip"].null_count() or rows["activity_atom"].null_count():
    raise ValueError("Cannot map every captured step to a physical trip and activity.")

# Even token IDs are trips and odd IDs are activities. Each captured step adds
# one of each; an anchor activity is a standalone section in the rigid cut.
trip_rows = rows.select("plan", "profile", (pl.col("seq_step_index").cast(pl.UInt32) * 2).alias("position"),
                        (pl.col("trip").cast(pl.UInt64) * 2).alias("token"), pl.lit(False).alias("anchor"))
activity_rows = rows.select("plan", "profile", (pl.col("seq_step_index").cast(pl.UInt32) * 2 + 1).alias("position"),
                            (pl.col("activity_atom").cast(pl.UInt64) * 2 + 1).alias("token"),
                            pl.col("activity").cast(pl.String).is_in(rigid_activities).alias("anchor"))
atoms = pl.concat([trip_rows, activity_rows]).sort("plan", "position")
atoms = atoms.with_columns(_previous_anchor=pl.col("anchor").shift(1).over("plan").fill_null(True))
atoms = atoms.with_columns(_boundary=(pl.col("anchor") | pl.col("_previous_anchor")))
atoms = atoms.with_columns(rigid_section=pl.col("_boundary").cast(pl.UInt32).cum_sum().over("plan"))

# Build utility records for a token/profile pair. Multiple source occurrences
# with the same pair must have identical scoring inputs in this fixed state.
def score_atoms(scoring_rows):
    trip_scores = scoring_rows.select("profile", (pl.col("trip").cast(pl.UInt64) * 2).alias("token"),
                                      (-pl.col("cost")).alias("utility"), pl.lit(0.0).alias("duration"),
                                      pl.col("time").fill_null(0.0).alias("travel"), pl.lit(True).alias("feasible"))
    activity_scores = scoring_rows.select(
        "profile", (pl.col("activity_atom").cast(pl.UInt64) * 2 + 1).alias("token"),
        (pl.col("activity_utility_scale")
         * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0)).alias("utility"),
        pl.col("duration_per_pers").cast(pl.Float64).alias("duration"), pl.lit(0.0).alias("travel"),
        pl.col("step_feasible").alias("feasible"))
    result = pl.concat([trip_scores, activity_scores], how="vertical_relaxed").unique()
    if result.select("profile", "token").n_unique() != result.height:
        raise ValueError("A physical token/profile has conflicting captured utility inputs.")
    return result


atom_scores = None if args.stream_profiles else score_atoms(rows)
input_seconds = perf_counter() - start

start = perf_counter()
if args.strategy in ("rigid", "prefix"):
    groups = atoms.group_by("plan", "profile", "rigid_section", maintain_order=True).agg(
        pl.col("token").alias("tokens")
    )
elif args.strategy == "atomic":
    groups = atoms.select("plan", "profile", pl.concat_list("token").alias("tokens"))
else:
    pairs = (atoms.with_columns(pl.col("token").shift(-1).over("plan").alias("next_token"),
                                pl.col("rigid_section").shift(-1).over("plan").alias("next_section"))
             .filter(pl.col("rigid_section") == pl.col("next_section"))
             .group_by("token", "next_token").agg(pl.col("plan").n_unique().alias("plan_count"))
             .filter(pl.col("plan_count") >= 2).select("token", "next_token"))
    candidates = (atoms.with_columns(pl.col("token").shift(-1).over("plan").alias("next_token"),
                                     pl.col("rigid_section").shift(-1).over("plan").alias("next_section"))
                  .join(pairs.with_columns(pl.lit(True).alias("reusable")),
                        on=["token", "next_token"], how="left", maintain_order="left")
                  .with_columns(reusable=(pl.col("reusable").fill_null(False)
                                           & (pl.col("rigid_section") == pl.col("next_section")).fill_null(False)))
                  .select("plan", "profile", "token", "reusable"))
    eligible = candidates["reusable"].to_numpy()
    starts = eligible & ~np.r_[False, eligible[:-1]]
    run_start = np.maximum.accumulate(np.where(starts, np.arange(len(eligible)), -1))
    take_pair = eligible & ((np.arange(len(eligible)) - run_start) % 2 == 0)
    group_id = np.arange(len(eligible), dtype=np.int64) - np.cumsum(np.r_[False, take_pair[:-1]])
    groups = candidates.with_columns(pl.Series("piece", group_id)).group_by(
        "plan", "profile", "piece", maintain_order=True
    ).agg(pl.col("token").alias("tokens"))
sections = groups.select("tokens").unique(maintain_order=True).with_row_index("section")
references = (groups.join(sections, on="tokens", maintain_order="left")
              .select("plan", "profile", "section"))
section_atoms = sections.explode("tokens").rename({"tokens": "token"})
profile_sections = None if args.stream_profiles else references.select("profile", "section").unique()
prefix_nodes = prefix_paths = section_leaves = None
if args.strategy == "prefix":
    # Each rigid-bounded section starts at the empty root. Prefixes are shared
    # across sections, independently of the preceding complete-plan history.
    ordered_atoms = section_atoms.with_columns(pl.col("token").cum_count().over("section").alias("depth"))
    frontier = sections.select("section").with_columns(parent=pl.lit(0, dtype=pl.UInt32))
    node_frames, path_frames = [], []
    next_node = 1
    for depth in sorted(ordered_atoms["depth"].unique().to_list()):
        layer = (ordered_atoms.filter(pl.col("depth") == depth)
                 .join(frontier, on="section", maintain_order="left"))
        distinct = (layer.select("parent", "token").unique(maintain_order=True)
                    .with_row_index("node", offset=next_node)
                    .with_columns(depth=pl.lit(depth, dtype=pl.UInt32)))
        assigned = layer.join(distinct, on=["parent", "token"], maintain_order="left")
        node_frames.append(distinct.select("node", "parent", "token", "depth"))
        path_frames.append(assigned.select("section", "node", "depth"))
        frontier = assigned.select("section", pl.col("node").alias("parent"))
        next_node += distinct.height
    prefix_nodes = pl.concat(node_frames).sort("node")
    prefix_paths = pl.concat(path_frames)
    section_leaves = (prefix_paths.sort("section", "depth").group_by("section", maintain_order=True)
                      .agg(pl.col("node").last().alias("node")))
    if section_leaves.height != sections.height:
        raise ValueError("Each rigid-bounded section must have one prefix leaf.")
    if args.prefix_flow_array or args.prefix_flow_rust:
        ordered_leaves = section_leaves.sort("section")
        if not np.array_equal(ordered_leaves["section"].to_numpy(), np.arange(sections.height)):
            raise ValueError("Section IDs must be contiguous for array traversal.")
        prefix_leaf_array = ordered_leaves["node"].to_numpy()
        prefix_parent_array = np.r_[np.uint32(0), prefix_nodes["parent"].to_numpy()]
        prefix_token_array = np.r_[np.uint64(0), prefix_nodes["token"].to_numpy()]
        node_depths = prefix_nodes["depth"].to_numpy()
        prefix_depth_ranges = []
        for depth in np.unique(node_depths):
            positions = np.flatnonzero(node_depths == depth)
            if positions[-1] - positions[0] + 1 != len(positions):
                raise ValueError("Prefix nodes must be ordered by depth.")
            prefix_depth_ranges.append((int(positions[0]) + 1, int(positions[-1]) + 2))
        if any(np.any(prefix_parent_array[first:last] >= first) for first, last in prefix_depth_ranges):
            raise ValueError("Each parent must precede its child depth.")
        if args.prefix_flow_rust:
            kernel = ctypes.CDLL(str(args.prefix_flow_rust)).propagate_prefix_flows
            u32_vector = np.ctypeslib.ndpointer(dtype=np.uint32, ndim=1, flags="C_CONTIGUOUS")
            u64_vector = np.ctypeslib.ndpointer(dtype=np.uint64, ndim=1, flags="C_CONTIGUOUS")
            f64_vector = np.ctypeslib.ndpointer(dtype=np.float64, ndim=1, flags="C_CONTIGUOUS")
            kernel.argtypes = [u32_vector, u64_vector, ctypes.c_size_t, u32_vector, ctypes.c_size_t,
                               u32_vector, f64_vector, ctypes.c_size_t, f64_vector, ctypes.c_size_t]
            kernel.restype = ctypes.c_int
build_seconds = perf_counter() - start

start = perf_counter()
plan_sequences = (references.group_by("plan", "profile", maintain_order=True)
                  .agg(pl.col("section").alias("section_sequence")))
physical_plans = (plan_sequences.select("section_sequence").unique(maintain_order=True)
                  .with_row_index("physical_plan"))
plan_membership = (plan_sequences.join(physical_plans, on="section_sequence", maintain_order="left")
                   .select("plan", "profile", "physical_plan"))
physical_plan_sections = (physical_plans.explode("section_sequence")
                          .rename({"section_sequence": "section"}))
physical_profile = plan_membership.select("profile", "physical_plan").unique()
factored_build_seconds = perf_counter() - start
factored_result = {"physical_plans": physical_plans.height,
                   "context_plan_memberships": plan_membership.height,
                   "profile_physical_plans": physical_profile.height,
                   "physical_plan_section_references": physical_plan_sections.height,
                   "build_seconds": factored_build_seconds,
                   "plan_structure_bytes": (plan_membership.estimated_size()
                                            + physical_plan_sections.estimated_size())}

section_score_inputs = section_scores = None
section_score_seconds = 0.0
if not args.stream_profiles:
    start = perf_counter()
    if args.strategy == "prefix":
        section_score_inputs = (profile_sections.join(prefix_paths, on="section")
                                .select("profile", "node").unique().join(prefix_nodes, on="node")
                                .join(atom_scores, on=["profile", "token"], how="left"))
        if section_score_inputs["utility"].null_count():
            raise ValueError("A prefix atom has no score for its profile.")
        accumulated = []
        previous = None
        for depth in sorted(section_score_inputs["depth"].unique().to_list()):
            layer = section_score_inputs.filter(pl.col("depth") == depth)
            if previous is not None:
                parent_scores = previous.select(
                    "profile", pl.col("node").alias("parent"),
                    *[pl.col(name).alias("parent_" + name)
                      for name in ("utility", "duration", "travel", "feasible")])
                layer = layer.join(parent_scores, on=["profile", "parent"], how="left")
                if layer["parent_utility"].null_count():
                    raise ValueError("A profile-specific prefix has no scored parent.")
                layer = layer.with_columns(
                    *[(pl.col(name) + pl.col("parent_" + name)).alias(name)
                      for name in ("utility", "duration", "travel")],
                    feasible=pl.col("feasible") & pl.col("parent_feasible"),
                )
            previous = layer.select("profile", "node", "utility", "duration", "travel", "feasible")
            accumulated.append(previous)
        prefix_scores = pl.concat(accumulated)
        section_scores = (profile_sections.join(section_leaves, on="section")
                          .join(prefix_scores, on=["profile", "node"])
                          .select("profile", "section", "utility", "duration", "travel", "feasible"))
    else:
        section_score_inputs = (profile_sections.join(section_atoms, on="section")
                                .join(atom_scores, on=["profile", "token"], how="left"))
        if section_score_inputs["utility"].null_count():
            raise ValueError("A section atom has no score for its profile.")
        section_scores = (section_score_inputs.group_by("profile", "section").agg(
            pl.col("utility").sum(), pl.col("duration").sum(), pl.col("travel").sum(), pl.col("feasible").all()
        ))
    section_score_seconds = perf_counter() - start

home = pl.read_parquet(args.home_durations).select("country", "csp", "mean_home_night_per_pers")
context_details = pl.read_parquet(args.capture / "contexts.parquet").select(unit + ["country", "csp"])
stay = choices.filter(pl.col("mode_seq_id") == 0).select(unit + [pl.col("utility").alias("stay_utility")])
home_parameters = (plans.select(["plan"] + unit).join(context_details, on=unit)
                   .join(home, on=["country", "csp"]).join(stay, on=unit)
                   .select("plan", "mean_home_night_per_pers", "stay_utility"))
expected = plans.select("plan", "utility", "is_feasible")
score_times = []
max_profile_scores = max_profile_evaluations = streamed_profile_count = 0
for repeat in range(args.repetitions):
    start = perf_counter()
    score_parts = []
    if args.stream_profiles:
        for profile in profiles["profile"].to_list():
            local_membership = plan_membership.filter(pl.col("profile") == profile)
            if local_membership.is_empty():
                continue
            local_refs = (physical_profile.filter(pl.col("profile") == profile)
                          .join(physical_plan_sections, on="physical_plan"))
            local_atom_scores = score_atoms(rows.filter(pl.col("profile") == profile))
            local_sections = local_refs.select("profile", "section").unique()
            local_inputs = (local_sections.join(section_atoms, on="section")
                            .join(local_atom_scores, on=["profile", "token"], how="left"))
            if local_inputs["utility"].null_count():
                raise ValueError("A section atom has no score for its profile.")
            local_scores = local_inputs.group_by("profile", "section").agg(
                pl.col("utility").sum(), pl.col("duration").sum(), pl.col("travel").sum(), pl.col("feasible").all())
            local_plan_scores = (local_refs.join(local_scores, on=["profile", "section"])
                                 .group_by("profile", "physical_plan").agg(
                                     pl.col("utility").sum().alias("step_utility"),
                                     pl.col("duration").sum(), pl.col("travel").sum(),
                                     pl.col("feasible").all().alias("result_feasible")))
            score_parts.append(local_membership.join(local_plan_scores, on=["profile", "physical_plan"]))
            max_profile_scores = max(max_profile_scores, local_sections.height)
            max_profile_evaluations = max(max_profile_evaluations, local_inputs.height)
        streamed_profile_count = len(score_parts)
        aggregated = pl.concat(score_parts)
    else:
        aggregated = (references.join(section_scores, on=["profile", "section"])
                  .group_by("plan").agg(pl.col("utility").sum().alias("step_utility"),
                                        pl.col("duration").sum(), pl.col("travel").sum(),
                                        pl.col("feasible").all().alias("result_feasible")))
    plan_score = (aggregated.join(home_parameters, on="plan").with_columns(
                      home_hours=(24 - pl.col("duration") - pl.col("travel")).clip(0.0))
                  .with_columns(result_utility=(pl.col("step_utility") + pl.col("stay_utility") / args.constant
                                * (pl.col("home_hours") / (pl.col("mean_home_night_per_pers") * np.exp(-args.constant)))
                                .log().clip(0.0))))
    score_times.append(perf_counter() - start)
    comparison = plan_score.join(expected, on="plan")
    if (comparison.height != plans.height
            or comparison.filter(pl.col("result_feasible") != pl.col("is_feasible")).height):
        raise ValueError("Section feasibility does not match captured plans.")
    errors = comparison.filter(pl.col("is_feasible")).select(
        (pl.col("result_utility") - pl.col("utility")).abs().max()).item()
    if errors > 1e-8:
        raise ValueError(f"Section utilities disagree with capture by {errors}.")

# Factor the ordered section sequence out of the context-specific plan rows.
# This models what a generator could retain directly; here its construction is
# still measured from already-expanded capture data.
if not args.stream_profiles:
    factored_score_times = []
    for repeat in range(args.repetitions):
        start = perf_counter()
        profile_plan_scores = (physical_profile.join(physical_plan_sections, on="physical_plan")
                               .join(section_scores, on=["profile", "section"])
                               .group_by("profile", "physical_plan").agg(
                                   pl.col("utility").sum().alias("step_utility"), pl.col("duration").sum(),
                                   pl.col("travel").sum(), pl.col("feasible").all().alias("result_feasible")))
        factored_plan_score = (plan_membership.join(profile_plan_scores, on=["profile", "physical_plan"])
                               .join(home_parameters, on="plan").with_columns(
                                   home_hours=(24 - pl.col("duration") - pl.col("travel")).clip(0.0))
                               .with_columns(result_utility=(
                                   pl.col("step_utility") + pl.col("stay_utility") / args.constant
                                   * (pl.col("home_hours")
                                      / (pl.col("mean_home_night_per_pers") * np.exp(-args.constant))).log().clip(0.0))))
        factored_score_times.append(perf_counter() - start)
        factored_comparison = factored_plan_score.join(expected, on="plan")
        if (factored_comparison.height != plans.height
                or factored_comparison.filter(pl.col("result_feasible") != pl.col("is_feasible")).height):
            raise ValueError("Factored physical-plan feasibility does not match the capture.")
        factored_error = factored_comparison.filter(pl.col("is_feasible")).select(
            (pl.col("result_utility") - pl.col("utility")).abs().max()).item()
        if factored_error > 1e-8:
            raise ValueError(f"Factored physical-plan utility error is {factored_error}.")
    factored_result["profile_and_plan_scoring_seconds"] = factored_score_times

unit_map = pl.read_parquet(args.capture / "unit-map.parquet")
allocation = (pl.read_parquet(args.capture / "allocated-plans.parquet").filter(pl.col("mode_seq_id") != 0)
              .drop("plan_id").join(unit_map, on=unit, how="left").drop(unit)
              .with_columns(demand_group_id=pl.col("_score_context_id"),
                            demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
              .join(contexts, on=unit, how="semi"))
if allocation.join(plans, on=PLAN_KEY_COLS, how="anti").height:
    raise ValueError("An allocated plan is absent from the section representation.")
plan_volume = (allocation.join(plans.select(PLAN_KEY_COLS + ["plan"]), on=PLAN_KEY_COLS)
               .group_by("plan").agg(pl.col("n_persons").sum().alias("persons")))
trip = trip.with_columns((pl.col("trip").cast(pl.UInt64) * 2).alias("token"))
activity = activity.with_columns((pl.col("activity_atom").cast(pl.UInt64) * 2 + 1).alias("token"))
flow_times = []
traversal_times = []
for repeat in range(args.repetitions):
    start = perf_counter()
    physical_volume = (plan_membership.join(plan_volume, on="plan").group_by("physical_plan")
                       .agg(pl.col("persons").sum()))
    section_volume = (physical_volume.join(physical_plan_sections, on="physical_plan")
                      .group_by("section").agg(pl.col("persons").sum()))
    traversal_start = perf_counter()
    if args.prefix_flow_rust:
        ids = np.ascontiguousarray(section_volume["section"].to_numpy(), dtype=np.uint32)
        weights = np.ascontiguousarray(section_volume["persons"].to_numpy(), dtype=np.float64)
        atom_volumes = np.zeros(int(prefix_token_array.max()) + 1, dtype=np.float64)
        result = kernel(prefix_parent_array, prefix_token_array, len(prefix_parent_array),
                        prefix_leaf_array, len(prefix_leaf_array), ids, weights, len(ids),
                        atom_volumes, len(atom_volumes))
        if result:
            raise ValueError(f"The prefix flow kernel rejected its inputs: {result}")
        used_tokens = np.flatnonzero(atom_volumes)
        atom_volume = pl.DataFrame({"token": used_tokens.astype(np.uint64), "persons": atom_volumes[used_tokens]})
    elif args.prefix_flow_array:
        active_leaves = prefix_leaf_array[section_volume["section"].to_numpy()]
        node_volume_array = np.bincount(active_leaves, weights=section_volume["persons"].to_numpy(),
                                        minlength=len(prefix_parent_array))
        for first, last in reversed(prefix_depth_ranges):
            if args.prefix_flow_array == "sparse":
                active = np.flatnonzero(node_volume_array[first:last]) + first
                np.add.at(node_volume_array, prefix_parent_array[active], node_volume_array[active])
            elif args.prefix_flow_array == "add_at":
                np.add.at(node_volume_array, prefix_parent_array[first:last], node_volume_array[first:last])
            else:
                parent_volume = np.bincount(prefix_parent_array[first:last], weights=node_volume_array[first:last],
                                            minlength=first)
                node_volume_array[:len(parent_volume)] += parent_volume
        if args.prefix_flow_array == "sparse":
            active = np.flatnonzero(node_volume_array[1:]) + 1
            atom_volumes = np.bincount(prefix_token_array[active].astype(np.int64), weights=node_volume_array[active])
        else:
            atom_volumes = np.bincount(prefix_token_array[1:].astype(np.int64), weights=node_volume_array[1:])
        used_tokens = np.flatnonzero(atom_volumes)
        atom_volume = pl.DataFrame({"token": used_tokens.astype(np.uint64), "persons": atom_volumes[used_tokens]})
    elif args.strategy == "prefix" and not args.prefix_flow_direct:
        node_volume = (section_volume.join(section_leaves, on="section")
                       .group_by("node").agg(pl.col("persons").sum()))
        contributions = []
        for depth in sorted(prefix_nodes["depth"].unique().to_list(), reverse=True):
            active = prefix_nodes.filter(pl.col("depth") == depth).join(node_volume, on="node")
            contributions.append(active.select("token", "persons"))
            parents = (active.filter(pl.col("parent") != 0)
                       .select(pl.col("parent").alias("node"), "persons"))
            if parents.height:
                node_volume = pl.concat([node_volume, parents]).group_by("node").agg(pl.col("persons").sum())
        atom_volume = pl.concat(contributions).group_by("token").agg(pl.col("persons").sum())
    else:
        atom_volume = (section_volume.join(section_atoms, on="section")
                       .group_by("token").agg(pl.col("persons").sum()))
    traversal_times.append(perf_counter() - traversal_start)
    od = (trip.join(atom_volume, on="token")
          .group_by("from", "to", "mode").agg(pl.col("persons").sum().alias("volume"))
          .sort("from", "to", "mode"))
    occupation = (activity.filter(pl.col("activity") != "home").join(atom_volume, on="token")
                  .group_by("to", "activity")
                  .agg((pl.col("persons") * pl.col("duration_per_pers").cast(pl.Float64)).sum().alias("occupation"))
                  .sort("to", "activity"))
    flow_times.append(perf_counter() - start)
if factored_result is not None:
    reference_section_volume = (references.join(plan_volume, on="plan")
                                .group_by("section").agg(pl.col("persons").sum()))
    factored_flow_times = []
    for repeat in range(args.repetitions):
        start = perf_counter()
        physical_volume = (plan_membership.join(plan_volume, on="plan").group_by("physical_plan")
                           .agg(pl.col("persons").sum()))
        factored_volume = (physical_volume.join(physical_plan_sections, on="physical_plan")
                           .group_by("section").agg(pl.col("persons").sum()))
        diff = (reference_section_volume.join(factored_volume, on="section", how="full", coalesce=True, suffix="_factored")
                .with_columns(pl.col("persons").fill_null(0.0), pl.col("persons_factored").fill_null(0.0))
                .select((pl.col("persons") - pl.col("persons_factored")).abs().max()).item())
        if diff is not None and diff > 1e-8:
            raise ValueError(f"Factored section volumes differ by {diff}.")
        factored_flow_times.append(perf_counter() - start)
    factored_result["section_volume_seconds_including_validation"] = factored_flow_times
od.write_parquet(args.output / "od.parquet")
occupation.write_parquet(args.output / "occupation.parquet")
stop.set()
thread.join()
print("SECTION_RESULT", json.dumps({"strategy": args.strategy, "contexts": contexts.height,
      "prefix_flow_direct": args.prefix_flow_direct,
      "prefix_flow_array": args.prefix_flow_array,
      "prefix_flow_rust": args.prefix_flow_rust is not None,
      "plans": plans.height, "steps": source.height, "trip_atoms": trip.height, "activity_atoms": activity.height,
      "sections": sections.height, "section_atom_references": section_atoms.height,
      "plan_section_references": references.height,
      "profile_section_scores": None if args.stream_profiles else profile_sections.height,
      "section_profile_atom_evaluations": None if args.stream_profiles else section_score_inputs.height,
      "streamed_profiles": streamed_profile_count, "max_streamed_section_scores": max_profile_scores,
      "max_streamed_atom_evaluations": max_profile_evaluations,
      "section_reference_bytes": references.estimated_size() + section_atoms.estimated_size(),
      "prefix_nodes": None if prefix_nodes is None else prefix_nodes.height,
      "prefix_profile_scores": None if prefix_nodes is None else section_score_inputs.height,
      "prefix_index_bytes": None if prefix_nodes is None else prefix_nodes.estimated_size() + section_leaves.estimated_size(),
      "factored_physical_plans": factored_result,
      "input_preparation_seconds": input_seconds, "section_build_seconds": build_seconds,
      "section_score_build_seconds": section_score_seconds, "plan_score_seconds": score_times,
      "flow_seconds": flow_times, "traversal_seconds": traversal_times, "max_feasible_utility_error": errors,
      "peak_process_rss_bytes": peak[0]}), flush=True)
