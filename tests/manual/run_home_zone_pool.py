"""Run the Geneva probe with a previously ranked, experimental home pool.

The ranking and the run must use the same initial Geneva inputs. This is a
measurement harness, not a general simulation feature or a checkpoint format.
"""
import os
import runpy
from pathlib import Path
from time import perf_counter

import polars as pl

from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.trips.group_day_trips.plans.compact_candidate_steps import (
    MEMBERSHIP_COLUMNS, PLAN_COLUMNS, STEP_COLUMNS, ScoringCandidateSteps,
)
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts
from mobility.trips.group_day_trips.plans.shared_candidate_pool import SharedCandidatePool
from mobility.trips.group_day_trips.plans.stable_key_index import StableKeyIndex, add_ordered_key_hashes
from mobility.transport.modes.choice.compute_subtour_mode_probabilities import modes_list_to_dict
from mobility.transport.modes.core.mode_values import get_mode_values

from home_zone_pool import (build_factored_home_zone_candidates, build_home_zone_candidates_batch,
                            prepare_home_zone_schedules, rank_home_zone_schedules)


original_advance = MultiScenarioRunAsset._advance_cohort_step
original_build = SharedCandidatePool.build
home_candidates = None
home_units = None


def prepare_home_pool(coordinator, iteration, current_states, **kwargs):
    global home_candidates, home_units
    if home_candidates is None:
        if len(current_states) != 1 or iteration != 1:
            raise ValueError("The experimental home pool needs one scenario starting at step 1.")
        start = perf_counter()
        run_key, state = next(iter(current_states.items()))
        run = coordinator.runs[run_key]
        costs = run.iteration_state_assets[0].transport_costs
        assignments, profiles = costs.get_utility_profiles(
            state.demand_groups.sort("demand_group_id", "demand_subgroup_id"), run.population.population_segments,
        )
        contexts = ScoringContexts.build(state.demand_groups, costs, run.population.population_segments,
                                         assignments=assignments)
        selected = pl.read_parquet(os.environ["MOBILITY_HOME_RANKING"])
        if selected.select("demand_group_id").n_unique() != contexts.demand_groups.height:
            raise ValueError("The ranking does not cover this run's scoring contexts.")
        modes = [name for name, props in modes_list_to_dict(run.modes).items()
                 if not props["is_return_mode"] and props["return_mode"] is None]
        pairs = contexts.demand_groups.select("utility_profile_id", pl.col("home_zone_id").alias("from"),
                                               pl.col("home_zone_id").alias("to")).unique()
        home_costs = costs.get_profile_costs_by_od_and_mode(profiles, ["cost"], od_pairs=pairs)
        available = (
            home_costs.filter(pl.col("cost").is_finite() & pl.col("mode").is_in(modes))
            .select("utility_profile_id", pl.col("from").alias("home_zone_id"), "mode").unique()
            .with_columns(_mode_index=pl.col("mode").replace_strict(
                {name: i for i, name in enumerate(modes)}, return_dtype=pl.UInt32),
                mode=pl.col("mode").cast(pl.Enum(get_mode_values(run.modes, "stay_home"))))
        )
        build_inputs = dict(
            survey_steps=state.survey_plan_steps, contexts=contexts.demand_groups,
            opportunities=state.opportunities, available_modes=available,
            prepared_schedules=prepare_home_zone_schedules(state.survey_plans, state.survey_plan_steps, run.is_weekday),
            selected_schedules=selected,
        )
        if os.environ.get("MOBILITY_HOME_EXPANDED_BUILD") == "1":
            expanded = build_home_zone_candidates_batch(**build_inputs)
            home_candidates = (expanded.select(STEP_COLUMNS).unique(),
                               expanded.with_columns(pl.col("from").alias("home_zone_id"))
                               .select(MEMBERSHIP_COLUMNS + ["utility_profile_id", "home_zone_id"]).unique())
        else:
            home_candidates = build_factored_home_zone_candidates(**build_inputs)
        # Check the saved ranking against this run's actual first-step inputs.
        # A different profile or cost setup would make context IDs misleading.
        sample = contexts.demand_groups.sort("demand_group_id").head(4)
        ids = sample["demand_group_id"].to_list()
        physical, memberships = home_candidates
        sample_candidates = (memberships.filter(pl.col("demand_group_id").is_in(ids))
            .join(physical.with_columns(pl.col("from").alias("home_zone_id")),
                  on=PLAN_COLUMNS + ["home_zone_id"])
            .select(CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS
                    + ["utility_profile_id"]))
        scores = rank_home_zone_schedules(
            updater=run.iteration_state_assets[0].updater, candidates=sample_candidates, context=sample,
            costs=costs, population_segments=run.population.population_segments, utility_profiles=profiles,
            destination_saturation=state.destination_saturation, activity_dur=state.activity_dur,
            home_night_dur=state.home_night_dur,
            stay_home=contexts.collapse_stay_home(state.stay_home_plan).filter(pl.col("demand_group_id").is_in(ids)),
            transport_zones=run.population.transport_zones,
            resolved_activity_parameters=coordinator._resolved_activity_parameters(run, run.iteration_state_assets[0], 1),
            activity_rigidity=coordinator._activity_time_rigidity(run, run.iteration_state_assets[0], 1),
            parameters=run.parameters,
        )
        keys = ["demand_group_id", "demand_subgroup_id", "activity_seq_id", "time_seq_id"]
        check = selected.filter(pl.col("demand_group_id").is_in(ids)).join(scores, on=keys, how="full", coalesce=True)
        difference = check.select((pl.col("best_utility") - pl.col("best_utility_right")).abs().max()).item()
        if check["best_utility"].null_count() or check["best_utility_right"].null_count() or difference > 1e-7:
            raise ValueError(f"The saved home ranking does not match initial inputs: {difference}")
        home_units = contexts.unit_map
        print(f"HOME_POOL_PREPARED contexts={contexts.demand_groups.height} "
              f"physical_steps={physical.height} memberships={memberships.height} "
              f"seconds={perf_counter()-start:.2f}", flush=True)
    return original_advance(coordinator, iteration, current_states, **kwargs)


def attach_home_pool(builder):
    pool = original_build(builder)
    started = perf_counter()
    if home_candidates is None or len(builder.sources) != 1:
        raise ValueError("The experimental pool is only configured for one scenario.")
    run_key = builder.sources[0].run_key
    physical, memberships = home_candidates
    rows = physical.rename({"dest_seq_id": "_home_dest", "mode_seq_id": "_home_mode"})
    dest_rows = rows.select("from", "_home_dest", "seq_step_index", "to").unique()
    dest_keys = add_ordered_key_hashes(
        dest_rows, group_cols=["from", "_home_dest"], order_col="seq_step_index",
        step_content=pl.struct(pl.col("to")), key_prefix="destination_sequence_key",
    )
    dest_keys, dest_index = StableKeyIndex(
        ["destination_sequence_key_1", "destination_sequence_key_2"], "dest_seq_id",
    ).extend(dest_keys, pool.destination_sequence_indices[run_key])
    mode_rows = rows.select("_home_mode", "seq_step_index", "mode").unique()
    mode_keys = add_ordered_key_hashes(
        mode_rows, group_cols=["_home_mode"], order_col="seq_step_index",
        step_content=pl.struct(pl.col("mode").cast(pl.String)), key_prefix="mode_sequence_key",
    )
    mode_keys, mode_index = StableKeyIndex(
        ["mode_sequence_key_1", "mode_sequence_key_2"], "mode_seq_id",
    ).extend(mode_keys, pool.mode_sequence_indices[run_key])
    rows = (rows.join(dest_keys.select("from", "_home_dest", "dest_seq_id"), on=["from", "_home_dest"])
            .join(mode_keys.select("_home_mode", "mode_seq_id"), on="_home_mode")
            .drop("_home_dest", "_home_mode"))
    physical = rows.select(STEP_COLUMNS).unique()
    members = (memberships.rename({"dest_seq_id": "_home_dest", "mode_seq_id": "_home_mode"})
               .join(dest_keys.select(pl.col("from").alias("home_zone_id"), "_home_dest", "dest_seq_id"),
                     on=["home_zone_id", "_home_dest"])
               .join(mode_keys.select("_home_mode", "mode_seq_id"), on="_home_mode")
               .select(MEMBERSHIP_COLUMNS).lazy().drop("demand_subgroup_id")
               .rename({"demand_group_id": "_score_context_id"})
               .join(home_units.lazy(), on="_score_context_id").select(MEMBERSHIP_COLUMNS))
    home = ScoringCandidateSteps(steps=physical, membership=members)
    existing = pool.scoring_candidate_steps_by_run_key[run_key]
    pool.scoring_candidate_steps_by_run_key[run_key] = ScoringCandidateSteps(
        steps=pl.concat([existing.steps, home.steps], how="vertical_relaxed").unique(),
        membership=pl.concat([existing.membership, home.membership], how="vertical_relaxed"),
    )
    # Keep the ordinary row path complete if the factored path cannot be used.
    expanded = members.join(physical.lazy(), on=PLAN_COLUMNS).select(
        CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS,
    )
    candidate, active = pool.candidate_step_parts_by_run_key[run_key]
    pool.candidate_step_parts_by_run_key[run_key] = (
        pl.concat([candidate, expanded], how="vertical_relaxed"), active,
    )
    pool.destination_sequence_indices[run_key] = dest_index
    pool.mode_sequence_indices[run_key] = mode_index
    print(f"HOME_POOL_ATTACHED step={builder.iteration} physical_steps={physical.height} "
          f"seconds={perf_counter()-started:.2f}", flush=True)
    return pool


MultiScenarioRunAsset._advance_cohort_step = prepare_home_pool
SharedCandidatePool.build = attach_home_pool
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
