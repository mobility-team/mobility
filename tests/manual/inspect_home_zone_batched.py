"""Benchmark direct home-zone utility ranking in batches, without plan search.

Uses the same cache-only Geneva probe as inspect_home_zone_pool.py. Optionally
set MOBILITY_HOME_BATCH_REFERENCE to an earlier folder containing per-context
ranking files; full rankings and top selections are compared by survey keys.
"""
import json
import os
import runpy
import tempfile
import threading
from pathlib import Path
from time import perf_counter

import polars as pl
import psutil

from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts
from mobility.transport.modes.choice.compute_subtour_mode_probabilities import modes_list_to_dict
from mobility.transport.modes.core.mode_values import get_mode_values

from home_zone_pool import build_home_zone_candidates_batch, prepare_home_zone_schedules, rank_home_zone_schedules


class CachedHomeCosts:
    """Reuse the fixed home-to-home cost rows during this ranking pass."""

    def __init__(self, costs, rows):
        self.modes = costs.modes
        self.rows = rows

    def get_profile_costs_by_od_and_mode(self, profiles, columns, *, od_pairs):
        return self.rows.join(od_pairs, on=["utility_profile_id", "from", "to"], how="semi")


def benchmark(coordinator, iteration, current_states, **kwargs):
    if len(current_states) != 1 or iteration != 1:
        raise ValueError("The batch probe expects one scenario at the first step.")
    run_key, state = next(iter(current_states.items()))
    run = coordinator.runs[run_key]
    state_asset = run.iteration_state_assets[0]
    costs = state_asset.transport_costs
    started = perf_counter()
    assignments, profiles = costs.get_utility_profiles(
        state.demand_groups.sort("demand_group_id", "demand_subgroup_id"), run.population.population_segments,
    )
    contexts = ScoringContexts.build(state.demand_groups, costs, run.population.population_segments,
                                     assignments=assignments)
    context_ids = [2946]
    for country in contexts.demand_groups.partition_by("country", maintain_order=True):
        ordered = country.sort("n_persons", "demand_group_id")["demand_group_id"].to_list()
        context_ids += [ordered[(2 * index + 1) * len(ordered) // 20] for index in range(10)]
    selected = (
        contexts.demand_groups if os.environ.get("MOBILITY_HOME_BATCH_ALL") == "1"
        else contexts.demand_groups.filter(pl.col("demand_group_id").is_in(context_ids))
    ).sort("demand_group_id")
    schedules = prepare_home_zone_schedules(state.survey_plans, state.survey_plan_steps, run.is_weekday)
    selection_path = os.environ.get("MOBILITY_HOME_BATCH_SEED_SELECTION")
    selected_schedules = pl.read_parquet(selection_path) if selection_path else None
    stay_home = contexts.collapse_stay_home(state.stay_home_plan)
    mode_properties = modes_list_to_dict(run.modes)
    modes = [name for name, props in mode_properties.items()
             if not props["is_return_mode"] and props["return_mode"] is None]
    pairs = selected.select("utility_profile_id", pl.col("home_zone_id").alias("from"),
                            pl.col("home_zone_id").alias("to")).unique()
    needed_profiles = {key: profiles[key] for key in selected["utility_profile_id"].unique().to_list()}
    cache_costs = os.environ.get("MOBILITY_HOME_BATCH_CACHE_COSTS") == "1"
    home_costs = costs.get_profile_costs_by_od_and_mode(
        needed_profiles, ["cost", "distance", "time"] if cache_costs else ["cost"], od_pairs=pairs,
    )
    ranking_costs = CachedHomeCosts(costs, home_costs) if cache_costs else costs
    available_modes = (
        home_costs.filter(pl.col("cost").is_finite() & pl.col("mode").is_in(modes))
        .select("utility_profile_id", pl.col("from").alias("home_zone_id"), "mode").unique()
        .with_columns(_mode_index=pl.col("mode").replace_strict({name: i for i, name in enumerate(modes)},
                                                                 return_dtype=pl.UInt32),
                      mode=pl.col("mode").cast(pl.Enum(get_mode_values(run.modes, "stay_home"))))
    )
    print("HOME_BATCH_SETUP", json.dumps({"contexts": selected.height, "seconds": perf_counter() - started,
                                          "schedules": schedules.height, "mode_rows": available_modes.height}), flush=True)
    reference = os.environ.get("MOBILITY_HOME_BATCH_REFERENCE")
    reference = Path(reference) if reference else None
    output = Path(tempfile.mkdtemp(prefix="mobility-home-batch-"))
    process = psutil.Process()
    peak = {"rss": process.memory_info().rss}
    stop = threading.Event()

    def observe():
        while not stop.wait(0.1):
            peak["rss"] = max(peak["rss"], process.memory_info().rss)

    monitor = threading.Thread(target=observe, daemon=True)
    monitor.start()
    try:
        for batch_size in [int(x) for x in os.environ.get("MOBILITY_HOME_BATCH_SIZES", "1,2,4").split(",")]:
            total_build = total_rank = 0.0
            total_rows = total_plans = 0
            mismatches = []
            best_rows = []
            limit = int(os.environ.get("MOBILITY_HOME_BATCH_TOP_K", "10"))
            for offset in range(0, selected.height, batch_size):
                batch = selected.slice(offset, batch_size)
                start = perf_counter()
                candidates = build_home_zone_candidates_batch(
                    survey_steps=state.survey_plan_steps, contexts=batch, opportunities=state.opportunities,
                    available_modes=available_modes, prepared_schedules=schedules,
                    selected_schedules=selected_schedules,
                )
                total_build += perf_counter() - start
                total_rows += candidates.height
                total_plans += candidates.select("demand_group_id", "activity_seq_id", "time_seq_id",
                                                  "dest_seq_id", "mode_seq_id").unique().height
                ids = batch["demand_group_id"].to_list()
                batch_home = stay_home.filter(pl.col("demand_group_id").is_in(ids))
                batch_profiles = {key: profiles[key] for key in batch["utility_profile_id"].unique().to_list()}
                start = perf_counter()
                rankings = rank_home_zone_schedules(
                    updater=state_asset.updater, candidates=candidates, context=batch, costs=ranking_costs,
                    population_segments=run.population.population_segments, utility_profiles=batch_profiles,
                    destination_saturation=state.destination_saturation, activity_dur=state.activity_dur,
                    home_night_dur=state.home_night_dur, stay_home=batch_home,
                    transport_zones=run.population.transport_zones,
                    resolved_activity_parameters=coordinator._resolved_activity_parameters(run, state_asset, 1),
                    activity_rigidity=coordinator._activity_time_rigidity(run, state_asset, 1),
                    parameters=run.parameters,
                )
                total_rank += perf_counter() - start
                best_rows.append(rankings.group_by("demand_group_id", "demand_subgroup_id", maintain_order=True)
                                 .head(limit))
                if reference is not None:
                    for context_id in ids:
                        if not (reference / f"ranking-{context_id}.parquet").exists():
                            continue
                        actual = rankings.filter(pl.col("demand_group_id") == context_id)
                        expected = pl.read_parquet(reference / f"ranking-{context_id}.parquet")
                        common = actual.join(expected, on=["activity_seq_id", "time_seq_id"], suffix="_reference",
                                             how="full", coalesce=True)
                        maxdiff = common.select((pl.col("best_utility") - pl.col("best_utility_reference")).abs().max()).item()
                        if (actual.height != expected.height or common.height != expected.height
                                or common["best_utility"].null_count() or common["best_utility_reference"].null_count()
                                or (maxdiff is not None and maxdiff > 1e-9)
                                or actual.head(10).join(expected.head(10), on=["activity_seq_id", "time_seq_id"]).height != 10):
                            mismatches.append({"context": context_id, "max_utility_difference": maxdiff})
                if (offset + batch_size) % 100 < batch_size:
                    print("HOME_BATCH_PROGRESS", json.dumps({"batch_size": batch_size,
                          "completed": min(offset + batch_size, selected.height), "total": selected.height,
                          "seconds": total_build + total_rank, "rss_peak_bytes": peak["rss"]}), flush=True)
                del candidates, rankings
            summary = {"batch_size": batch_size, "contexts": selected.height, "candidate_steps": total_rows,
                       "candidate_plans": total_plans, "build_seconds": total_build, "ranking_seconds": total_rank,
                       "rss_peak_bytes": peak["rss"], "mismatches": mismatches}
            print("HOME_BATCH_RESULT", json.dumps(summary), flush=True)
            (output / f"batch-{batch_size}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            pl.concat(best_rows).write_parquet(output / f"top-{limit}-batch-{batch_size}.parquet")
    finally:
        stop.set()
        monitor.join()
    print("HOME_BATCH_FILES", output, flush=True)
    raise SystemExit(0)


MultiScenarioRunAsset._advance_cohort_step = benchmark
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
