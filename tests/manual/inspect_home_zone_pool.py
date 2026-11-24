"""Capture the initial Geneva inputs needed to study home-zone plans.

This stops before candidate generation or simulation. Configure the Geneva
probe with its usual MOBILITY_PROBE_* environment variables.
"""
import json
import os
import runpy
import tempfile
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace

import polars as pl
import psutil

from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts, ScoringLookup, ScoringMemoryBudget
from mobility.trips.group_day_trips.plans.stable_key_index import add_ordered_key_hashes
from mobility.trips.group_day_trips.plans.time_sequences import TimeSequenceIndexCache
from mobility.transport.modes.core.mode_values import get_mode_values
from mobility.transport.modes.choice.compute_subtour_mode_probabilities import modes_list_to_dict

from home_zone_pool import build_home_zone_candidates, prepare_home_zone_schedules, rank_home_zone_schedules


def capture_initial_state(coordinator, iteration, current_states, **kwargs):
    if len(current_states) != 1 or iteration != 1:
        raise ValueError("The home-zone probe expects one scenario at the first step.")
    run_key, state = next(iter(current_states.items()))
    run = coordinator.runs[run_key]
    folder = Path(tempfile.mkdtemp(prefix="mobility-home-zone-pool-"))
    for name in ("survey_plans", "survey_plan_steps", "demand_groups", "opportunities", "activity_dur", "home_night_dur"):
        value = getattr(state, name)
        value.write_parquet(folder / f"{name}.parquet")
        print("HOME_POOL_INPUT", name, value.shape, value.estimated_size(), flush=True)
    print("HOME_POOL_MODES", json.dumps(get_mode_values(run.modes)), flush=True)
    print("HOME_POOL_FILES", folder, flush=True)
    if os.environ.get("MOBILITY_HOME_POOL_SCORE") == "1":
        score_one_context(coordinator, run, state, folder)
    raise SystemExit(0)


def score_one_context(coordinator, run, state, folder):
    """Use the normal evaluator on selected contexts without constructing search assets."""
    state_asset = run.iteration_state_assets[0]
    costs = state_asset.transport_costs
    assignments, profiles = costs.get_utility_profiles(
        state.demand_groups.sort("demand_group_id", "demand_subgroup_id"), run.population.population_segments,
    )
    contexts = ScoringContexts.build(
        state.demand_groups, costs, run.population.population_segments, assignments=assignments,
    )
    contexts.demand_groups.write_parquet(folder / "scoring-contexts.parquet")
    started = perf_counter()
    schedules = prepare_home_zone_schedules(state.survey_plans, state.survey_plan_steps, run.is_weekday)
    print("HOME_POOL_PREPARE", json.dumps({"seconds": perf_counter() - started, "schedules": schedules.height,
                                           "bytes": schedules.estimated_size()}), flush=True)
    time_index = None
    if os.environ.get("MOBILITY_HOME_POOL_TIME_INDEX_CACHE") == "1":
        started = perf_counter()
        time_columns = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
        catalog = state.survey_plan_steps.with_columns(
            ((pl.col("duration_per_pers").cast(pl.Float64) * 3600.0).round(0) / 3600.0)
            .cast(state.survey_plan_steps.schema["duration_per_pers"]).alias("duration_per_pers")
        )
        time_index = add_ordered_key_hashes(
            catalog.select(["time_seq_id", "seq_step_index", "activity"] + time_columns),
            group_cols=["time_seq_id"], order_col="seq_step_index",
            step_content=pl.struct(
                pl.col("activity").cast(pl.String),
                *[(pl.col(column).cast(pl.Float64) * 3600.0).round(0).cast(pl.Int64).alias(f"_{column}_seconds")
                  for column in time_columns],
            ), key_prefix="time_key",
        )
        print("HOME_POOL_TIME_INDEX", json.dumps({"seconds": perf_counter() - started,
                                                   "rows": time_index.height,
                                                   "bytes": time_index.estimated_size()}), flush=True)
    if os.environ.get("MOBILITY_HOME_POOL_COUNT_ALL") == "1":
        count_all_contexts(run, state, folder, costs, contexts, profiles, schedules)
    context_ids = [int(value) for value in os.environ.get("MOBILITY_HOME_POOL_CONTEXT", "2946").split(",")]
    if os.environ.get("MOBILITY_HOME_POOL_LARGEST_BY_COUNTRY") == "1":
        context_ids += (contexts.demand_groups.sort("n_persons", descending=True)
                        .unique(subset=["country"], keep="first", maintain_order=True)["demand_group_id"].to_list())
    sample_count = int(os.environ.get("MOBILITY_HOME_POOL_SAMPLE_PER_COUNTRY", "0"))
    if sample_count:
        for country in contexts.demand_groups.partition_by("country", maintain_order=True):
            ordered = country.sort("n_persons", "demand_group_id")["demand_group_id"].to_list()
            count = min(sample_count, len(ordered))
            context_ids += [ordered[(2 * index + 1) * len(ordered) // (2 * count)] for index in range(count)]
    print("HOME_POOL_CONTEXT_IDS", json.dumps(list(dict.fromkeys(context_ids))), flush=True)
    top_values = [value for value in os.environ.get("MOBILITY_HOME_POOL_TOP_K_VALUES", "all").split(",")
                  if value and value != "none"]
    limits = [None if value == "all" else int(value) for value in top_values]
    utility_values = [int(value) for value in os.environ.get("MOBILITY_HOME_POOL_UTILITY_K_VALUES", "").split(",") if value]
    if any(limit is not None and limit <= 0 for limit in limits + utility_values):
        raise ValueError("Every schedule limit must be positive.")
    available_mode_cache = {}
    for context_id in dict.fromkeys(context_ids):
        context_cache = {}
        for method, limit in [("survey", limit) for limit in limits] + [("utility", limit) for limit in utility_values]:
            output = folder if limit is None else folder / f"{'top' if method == 'survey' else method}-{limit}"
            output.mkdir(exist_ok=True)
            score_context(coordinator, run, state, output, state_asset, costs, contexts, profiles,
                          schedules, time_index, available_mode_cache, context_cache, context_id, limit, method)


def count_all_contexts(run, state, folder, costs, contexts, profiles, schedules):
    """Count the full pool by activity sets without expanding candidate plans."""
    started = perf_counter()
    eligible_modes = [name for name, props in modes_list_to_dict(run.modes).items()
                      if not props["is_return_mode"] and props["return_mode"] is None]
    pairs = contexts.demand_groups.select(
        "utility_profile_id", pl.col("home_zone_id").alias("from"), pl.col("home_zone_id").alias("to"),
    ).unique()
    home_costs = costs.get_profile_costs_by_od_and_mode(profiles, ["cost"], od_pairs=pairs)
    available_modes = (
        home_costs.filter(pl.col("cost").is_finite() & pl.col("mode").is_in(eligible_modes))
        .group_by("utility_profile_id", "from").agg(pl.col("mode").n_unique().alias("modes"))
        .rename({"from": "home_zone_id"})
    )
    step_counts = state.survey_plan_steps.group_by("activity_seq_id", "time_seq_id").len(name="steps")
    activity_sets = schedules.join(step_counts, on=["activity_seq_id", "time_seq_id"]).group_by(
        "country", "activities"
    ).agg(pl.len().alias("schedules"), pl.col("steps").sum().alias("steps"))
    local = (
        state.opportunities.filter(
            (pl.col("activity").cast(pl.String) != "home") & (pl.col("opportunity_capacity") > 0)
        ).group_by("to").agg(pl.col("activity").cast(pl.String).unique().sort().alias("local_activities"))
        .rename({"to": "home_zone_id"})
    )
    context_rows = contexts.demand_groups.with_columns(pl.col("country").cast(pl.String)).join(
        local, on="home_zone_id", how="left",
    ).with_columns(pl.col("local_activities").fill_null(pl.lit([], dtype=pl.List(pl.String))))
    counts = (
        context_rows.join(activity_sets, on="country")
        .filter((pl.col("activities").list.set_difference(pl.col("allowed_activities")).list.len() == 0)
                & (pl.col("activities").list.set_difference(pl.col("local_activities")).list.len() == 0))
        .group_by("demand_group_id", "home_zone_id", "utility_profile_id")
        .agg(pl.col("schedules", "steps").sum())
        .join(available_modes, on=["home_zone_id", "utility_profile_id"], how="left")
        .with_columns(pl.col("modes").fill_null(0))
        .with_columns(plans=pl.col("schedules").cast(pl.UInt64) * pl.col("modes"),
                      candidate_steps=pl.col("steps").cast(pl.UInt64) * pl.col("modes"))
    )
    counts.write_parquet(folder / "all-context-counts.parquet")
    print("HOME_POOL_ALL_COUNTS", json.dumps({"seconds": perf_counter() - started, "contexts": counts.height,
                                               "schedules": counts["schedules"].sum(),
                                               "plans": counts["plans"].sum(),
                                               "candidate_steps": counts["candidate_steps"].sum(),
                                               "mode_counts": counts.group_by("modes").len().sort("modes").to_dicts()}), flush=True)


def score_context(coordinator, run, state, folder, state_asset, costs, contexts, profiles, schedules, time_index,
                  available_mode_cache, context_cache, context_id, max_schedules, method):
    context = contexts.demand_groups.filter(pl.col("demand_group_id") == context_id)
    if context.height != 1:
        raise ValueError(f"Scoring context {context_id} was not found.")
    mode_properties = modes_list_to_dict(run.modes)
    modes = [name for name, props in mode_properties.items()
             if not props["is_return_mode"] and props["return_mode"] is None]
    if os.environ.get("MOBILITY_HOME_POOL_FILTER_UNAVAILABLE") == "1":
        started = perf_counter()
        profile_id = context["utility_profile_id"].item()
        zone = context["home_zone_id"].item()
        cache_key = (profile_id, zone)
        available = available_mode_cache.get(cache_key)
        cached = available is not None
        if available is None:
            pairs = pl.DataFrame({"utility_profile_id": [profile_id], "from": [zone], "to": [zone]},
                                 schema={"utility_profile_id": pl.UInt32, "from": pl.Int32, "to": pl.Int32})
            zone_costs = costs.get_profile_costs_by_od_and_mode(
                {profile_id: profiles[profile_id]}, ["cost"], od_pairs=pairs,
            )
            available = set(zone_costs.filter(pl.col("cost").is_finite())["mode"].cast(pl.String).to_list())
            available_mode_cache[cache_key] = available
        modes = [mode for mode in modes if mode in available]
        print("HOME_POOL_MODE_FILTER", json.dumps({"context": context_id, "seconds": perf_counter() - started,
                                                    "available_modes": modes, "cached": cached}), flush=True)
    print("HOME_POOL_CONTEXT", json.dumps(context.row(0, named=True), default=str), flush=True)
    print("HOME_POOL_DIRECT_MODES", json.dumps(modes), flush=True)
    started = perf_counter()
    if method == "utility" and "candidates" in context_cache:
        candidates = context_cache["candidates"]
    else:
        candidates = build_home_zone_candidates(
            survey_plans=state.survey_plans, survey_steps=state.survey_plan_steps,
            context=context, opportunities=state.opportunities, modes=modes, is_weekday=run.is_weekday,
            prepared_schedules=schedules, max_schedules=max_schedules if method == "survey" else None,
        ).with_columns(pl.col("mode").cast(pl.Enum(get_mode_values(run.modes, "stay_home"))))
        if method == "utility":
            context_cache["candidates"] = candidates
    print("HOME_POOL_BUILD", json.dumps({"context": context_id, "method": method, "top_k": max_schedules,
                                         "seconds": perf_counter() - started, "rows": candidates.height,
                                         "bytes": candidates.estimated_size(),
                                         "plans": candidates.select("activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id").unique().height,
                                         "rss_bytes": psutil.Process().memory_info().rss}), flush=True)
    current = contexts.collapse_allocations(state.current_plans).filter(pl.col("demand_group_id") == context_id)
    stay_home = contexts.collapse_stay_home(state.stay_home_plan).filter(pl.col("demand_group_id") == context_id)
    updater = state_asset.updater
    resolved_activities = coordinator._resolved_activity_parameters(run, state_asset, 1)
    activity_rigidity = coordinator._activity_time_rigidity(run, state_asset, 1)
    scoring_lookup = None
    if os.environ.get("MOBILITY_HOME_POOL_SCORE_CACHE") == "1":
        scoring_lookup = context_cache.setdefault("scoring_lookup", ScoringLookup(ScoringMemoryBudget(64 * 1024 * 1024)))
    if method == "utility":
        ranking = context_cache.get("ranking")
        if ranking is None:
            started = perf_counter()
            ranking = rank_home_zone_schedules(
                updater=updater, candidates=candidates, context=context, costs=costs,
                population_segments=run.population.population_segments, destination_saturation=state.destination_saturation,
                activity_dur=state.activity_dur, home_night_dur=state.home_night_dur, stay_home=stay_home,
                transport_zones=run.population.transport_zones, resolved_activity_parameters=resolved_activities,
                activity_rigidity=activity_rigidity, parameters=run.parameters, scoring_lookup=scoring_lookup,
            )
            context_cache["ranking"] = ranking
            print("HOME_POOL_UTILITY_RANK", json.dumps({"context": context_id, "seconds": perf_counter() - started,
                                                       "schedules": ranking.height}), flush=True)
            ranking.write_parquet(folder.parent / f"ranking-{context_id}.parquet")
        candidates = candidates.join(ranking.head(max_schedules).select("activity_seq_id", "time_seq_id"),
                                     on=["activity_seq_id", "time_seq_id"], how="semi")
    started = perf_counter()
    evaluation = updater.evaluate_candidate_pool(
        current_plans=current, current_plan_steps=None, candidate_plan_steps=None,
        shared_candidate_plan_steps=candidates, demand_groups=context, survey_plan_steps=state.survey_plan_steps,
        transport_costs=costs, destination_saturation=state.destination_saturation, activity_dur=state.activity_dur,
        home_night_dur=state.home_night_dur, stay_home_plan=stay_home,
        transport_zones=run.population.transport_zones, iteration=1,
        resolved_activity_parameters=resolved_activities,
        activity_time_rigidity_by_activity=activity_rigidity,
        destination_sequences=None, mode_sequences=SimpleNamespace(population_segments=run.population.population_segments),
        previous_plan_id_index=None, parameters=run.parameters,
        time_index_cache=(TimeSequenceIndexCache(schedules=state.survey_plan_steps, index=time_index)
                          if time_index is not None else None),
        scoring_lookup=scoring_lookup,
    )
    elapsed = perf_counter() - started
    allocation, _ = updater.estimate_mnl_allocation(
        evaluation, context, logit_scale=run.parameters.plan_update.transition_logit_scale, include_plan_steps=False,
    )
    evaluation.possible_plan_utility.write_parquet(folder / f"utility-{context_id}.parquet")
    allocation.write_parquet(folder / f"allocation-{context_id}.parquet")
    evaluation.time_mapping.write_parquet(folder / f"mapping-{context_id}.parquet")
    if (context_id == int(os.environ.get("MOBILITY_HOME_POOL_CONTEXT", "2946").split(",")[0])
            or os.environ.get("MOBILITY_HOME_POOL_ALL_STEPS") == "1"):
        evaluation.possible_plan_steps.collect(engine="streaming").write_parquet(folder / f"steps-{context_id}.parquet")
    print("HOME_POOL_SCORE", json.dumps({"context": context_id, "method": method, "top_k": max_schedules,
                                         "seconds": elapsed, "plans": evaluation.possible_plan_utility.height,
                                         "feasible": evaluation.possible_plan_utility["is_feasible"].sum(),
                                         "best_utility": evaluation.possible_plan_utility.filter(pl.col("is_feasible"))["utility"].max(),
                                         "mean_chosen_utility": allocation.select((pl.col("utility") * pl.col("n_persons")).sum() / pl.col("n_persons").sum()).item(),
                                         "rss_bytes": psutil.Process().memory_info().rss}), flush=True)
MultiScenarioRunAsset._advance_cohort_step = capture_initial_state
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
