"""Measure retaining exact structural sections across two captured outer steps.

The section index uses reference inputs, never modeled times or cached scores.
It cuts immediately before each rigid activity, after the trip into it. The
actual retiming and utility calculation are not replaced by this diagnostic.
"""
import argparse
import json
from pathlib import Path
from time import perf_counter

import polars as pl

from mobility.trips.group_day_trips.plans.compact_candidate_steps import PLAN_COLUMNS

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--strategy", choices=["reuse", "rebuild"], required=True)
parser.add_argument("--output", type=Path, help="Save the plan and section references for the boundary check.")
args = parser.parse_args()
if args.output:
    args.output.mkdir(parents=True, exist_ok=True)

# These are reference inputs, not values calculated from current traffic.
step_columns = ["seq_step_index", "activity", "from", "to", "mode", "departure_time",
                "arrival_time", "next_departure_time", "duration_per_pers"]
trip_columns = ["activity", "from", "to", "mode", "departure_time", "arrival_time"]
activity_columns = ["activity", "to", "arrival_time", "next_departure_time", "duration_per_pers"]
cache = {}
for step in (1, 2):
    if args.strategy == "rebuild":
        cache = {}
    folder = args.folder / f"batch-{step}"
    physical = pl.read_parquet(folder / "physical.parquet")
    settings = json.loads((folder / "settings.json").read_text(encoding="utf-8"))
    rigidity = settings["activity_time_rigidity_by_activity"]
    reference_seconds = 0.0
    matched_reference_plans = 0
    if step == 2 and args.strategy == "reuse":
        # The actual prior evaluation maps reference IDs to modeled IDs. Use
        # that mapping to retain reference structure, but never substitute it
        # for the current timings or scoring inputs in evaluation.
        start = perf_counter()
        original = pl.read_parquet(args.folder / "batch-1" / "physical.parquet")
        mapping = (pl.read_parquet(args.folder / "batch-1" / "modeled-mapping.parquet")
                   .select(PLAN_COLUMNS + ["previous_time_seq_id"]).unique())
        if mapping.select(PLAN_COLUMNS).n_unique() != mapping.height:
            raise ValueError("Several prior references merged; this diagnostic cannot choose one.")
        matched_reference_plans = physical.select(PLAN_COLUMNS).unique().join(mapping, on=PLAN_COLUMNS).height
        source = original.rename({"time_seq_id": "previous_time_seq_id"}).rename(
            {name: "reference_" + name for name in step_columns if name != "seq_step_index"})
        joined = physical.join(mapping, on=PLAN_COLUMNS, how="left").join(
            source, on=[name for name in PLAN_COLUMNS if name != "time_seq_id"]
            + ["previous_time_seq_id", "seq_step_index"], how="left")
        if joined.height != physical.height:
            raise ValueError("Reference mapping multiplied candidate steps.")
        for name in ("activity", "from", "to", "mode"):
            if joined.filter(pl.col("previous_time_seq_id").is_not_null()
                             & ~pl.col(name).eq_missing(pl.col("reference_" + name))).height:
                raise ValueError("A retained plan changed its physical structure.")
        reference_columns = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
        physical = joined.with_columns(*[pl.coalesce("reference_" + name, name).alias(name)
                                         for name in reference_columns]).select(physical.columns)
        reference_seconds = perf_counter() - start
    if cache and cache["rigidity"] != rigidity:
        raise ValueError("Rigidity changed: structural boundaries must be rebuilt.")
    start = perf_counter()
    # Exact ordered contents identify a physical reference plan across steps;
    # changing local time or plan IDs alone cannot create a false match.
    plans = (physical.sort(PLAN_COLUMNS + ["seq_step_index"])
             .group_by(PLAN_COLUMNS, maintain_order=True)
             .agg(pl.struct(step_columns).alias("signature")))
    distinct = plans.select("signature").unique(maintain_order=True)
    if cache:
        new_plans = distinct.join(cache["plans"], on="signature", how="anti").with_row_index(
            "structural_plan", offset=cache["plans"].height)
        all_plans = pl.concat([cache["plans"], new_plans])
    else:
        new_plans = distinct.with_row_index("structural_plan")
        all_plans = new_plans
    membership = plans.join(all_plans, on="signature", maintain_order="left")
    plan_seconds = perf_counter() - start

    start = perf_counter()
    rows = new_plans.explode("signature").unnest("signature")
    trip_values = rows.select(trip_columns).unique(maintain_order=True)
    activity_values = rows.select(activity_columns).unique(maintain_order=True)
    if cache:
        new_trips = trip_values.join(cache["trips"], on=trip_columns, how="anti").with_row_index(
            "trip", offset=cache["trips"].height)
        new_activities = activity_values.join(cache["activities"], on=activity_columns, how="anti").with_row_index(
            "activity_atom", offset=cache["activities"].height)
        trips = pl.concat([cache["trips"], new_trips])
        activities = pl.concat([cache["activities"], new_activities])
    else:
        trips = trip_values.with_row_index("trip")
        activities = activity_values.with_row_index("activity_atom")
    rows = rows.join(trips, on=trip_columns).join(activities, on=activity_columns)
    trip_rows = rows.select("structural_plan", (pl.col("seq_step_index").cast(pl.UInt32) * 2).alias("position"),
                            (pl.col("trip").cast(pl.UInt64) * 2).alias("token"), pl.lit(False).alias("boundary"))
    activity_rows = rows.select("structural_plan", (pl.col("seq_step_index").cast(pl.UInt32) * 2 + 1).alias("position"),
                                (pl.col("activity_atom").cast(pl.UInt64) * 2 + 1).alias("token"),
                                pl.col("activity").cast(pl.String).is_in(
                                    [name for name, value in rigidity.items() if value == 1.0]).alias("boundary"))
    atoms = pl.concat([trip_rows, activity_rows]).sort("structural_plan", "position")
    atoms = atoms.with_columns(section_in_plan=pl.col("boundary").cast(pl.UInt32).cum_sum().over("structural_plan"))
    groups = atoms.group_by("structural_plan", "section_in_plan", maintain_order=True).agg("token")
    section_values = groups.select("token").unique(maintain_order=True)
    previous_sections = cache["sections"].height if cache else 0
    if cache:
        new_sections = section_values.join(cache["sections"], on="token", how="anti").with_row_index(
            "section", offset=previous_sections)
        sections = pl.concat([cache["sections"], new_sections])
    else:
        new_sections = section_values.with_row_index("section")
        sections = new_sections
    new_refs = groups.join(sections, on="token", maintain_order="left").select(
        "structural_plan", "section_in_plan", "section")
    refs = pl.concat([cache["refs"], new_refs]) if cache else new_refs
    build_seconds = perf_counter() - start

    # Reconstruct all new plans from their ordered sections and compare each
    # original token, including consecutive rigid activities and final home.
    expected = atoms.group_by("structural_plan", maintain_order=True).agg("token").sort("structural_plan")
    actual = (new_refs.sort("structural_plan", "section_in_plan").join(sections, on="section", maintain_order="left")
              .explode("token").group_by("structural_plan", maintain_order=True).agg("token").sort("structural_plan"))
    if not expected.equals(actual):
        raise ValueError("Section references do not reconstruct the physical plans.")
    if membership.height != plans.height or membership["structural_plan"].null_count():
        raise ValueError("A plan has no structural index entry.")
    if args.output:
        membership.select(PLAN_COLUMNS + ["structural_plan"]).write_parquet(args.output / f"plans-{step}.parquet")
        refs.write_parquet(args.output / f"references-{step}.parquet")
    print("STRUCTURAL_STEP", json.dumps({"strategy": args.strategy, "step": step,
          "physical_plan_keys": plans.height, "distinct_structural_plans": distinct.height,
          "new_structural_plans": new_plans.height, "reused_structural_plans": distinct.height - new_plans.height,
          "new_sections": new_sections.height, "retained_sections": previous_sections,
          "matched_reference_plans": matched_reference_plans,
          "section_count": sections.height, "new_plan_section_references": new_refs.height,
          "retained_plan_section_references": refs.height - new_refs.height,
          "reference_mapping_seconds": reference_seconds,
          "plan_identity_seconds": plan_seconds, "section_index_seconds": build_seconds,
          "total_seconds": reference_seconds + plan_seconds + build_seconds}), flush=True)
    cache = {"rigidity": rigidity, "plans": all_plans, "trips": trips, "activities": activities,
             "sections": sections, "refs": refs}
