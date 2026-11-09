from __future__ import annotations


import json
import logging
import pathlib
from dataclasses import dataclass, replace
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import simulation_step, trace_simulation_run, trace_simulation_step

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import get_current_asset_resolver
from mobility.runtime.assets.input_hashing import hash_inputs
from mobility.activities.activity import (
    resolve_activity_arrival_time_rigidity,
    resolve_activity_parameters,
)
from mobility.trips.group_day_trips.core.progress import get_group_day_trips_progress
from mobility.trips.group_day_trips.iterations.iteration_assets import IterationSeedsAsset
from mobility.trips.group_day_trips.plans.activity_sequences import ActivitySequences
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
from mobility.trips.group_day_trips.plans.demand_subgroups import DEMAND_UNIT_COLS
from mobility.trips.group_day_trips.plans.mode_sequence_search.mode_sequences import ModeSequences
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.shared_candidate_pool import (
    ScenarioCandidateSource,
    SharedCandidatePoolAsset,
    SharedCandidatePoolResult,
)
from mobility.trips.group_day_trips.plans.generation_requests import (
    AdaptiveGenerationScheduler,
    allocation_effect_by_zone,
    build_generation_targets,
)
from mobility.trips.group_day_trips.core.multi_scenario import (
    CANDIDATE_ID_COLS,
    InnerLoopConvergence,
    add_candidate_ids,
    represented_od_mode_flows,
)
from mobility.trips.group_day_trips.core.run_state import RunState
from mobility.transport.costs.path.shared_congested_costs import prepare_shared_congested_costs
from mobility.trips.group_day_trips.iterations.iteration_assets import IterationTransportCostsAsset


@dataclass(frozen=True)
class CoordinatedRunResult:
    """Per-run outer state, behavior and latest inner estimates."""

    states_by_run_key: dict[int, list[Any]]
    final_states_by_run_key: dict[int, RunState]
    mnl_od_mode_flows_by_run_key: dict[int, pl.DataFrame]


class MultiScenarioRunAsset(InMemoryAsset):
    """Coordinate scenarios and sensitivity cases for one replication."""

    def __init__(self, runs: dict[int, Any]) -> None:
        if not runs:
            raise ValueError("A coordinated simulation needs at least one scenario run.")
        if any(not isinstance(run_key, int) for run_key in runs):
            raise TypeError("Coordinated runs must be keyed by integer run IDs.")
        if len({run.replication for run in runs.values()}) != 1:
            raise ValueError("A coordinator must contain exactly one replication.")
        self.runs = dict(runs)
        first_run = next(iter(self.runs.values()))
        self.run_keys = list(self.runs)
        # Existing scheduler helpers use this name, but the values are integer
        # run IDs; human-readable scenario names stay on each Run.
        self.scenarios = self.run_keys
        self.run_keys_by_group: dict[tuple[str, int], list[int]] = {}
        for run_key, run in self.runs.items():
            group = (run.sensitivity_case_id, run.replication)
            self.run_keys_by_group.setdefault(group, []).append(run_key)
        self.base_run_key_by_group = {
            group: next(
                (
                    run_key
                    for run_key in run_keys
                    if self.runs[run_key].scenario == "default"
                ),
                run_keys[0],
            )
            for group, run_keys in self.run_keys_by_group.items()
        }
        self.parameters = first_run.parameters
        self._cached_result: CoordinatedRunResult | None = None
        self._is_running = False
        super().__init__(
            {
                # The result now keeps the last regular inner estimate instead
                # of running a separate candidate search after the outer loop.
                "version": 11,
                "run_contexts": [
                    {
                        "run_key": run_key,
                        "scenario": run.scenario,
                        "sensitivity_case": run.sensitivity_case_id,
                        "replication": run.replication,
                        "run_context_hash": run.run_context.inputs_hash,
                    }
                    for run_key, run in self.runs.items()
                ],
                "adaptive_search_parameters": self.parameters.adaptive_search,
                "maximum_outer_iterations": self.parameters.run.n_steps,
            }
        )
        self._checkpoint_cohort_hash = hash_inputs(
            {
                "outer_iteration_policy": "fixed_count",
                "candidate_history_policy": "per_run_v1",
                "behavior_evaluation_policy": "reuse_inner_v1",
                "inner_attachment_policy": "scoring_contexts_v1",
                "runs": tuple(
                    (
                        run_key,
                        run.iterations.run_inputs_hash,
                    )
                    for run_key, run in self.runs.items()
                ),
            }
        )
        self._prepare_checkpoint_resume()

    @trace_simulation_run
    def get(self, *args: Any, **kwargs: Any) -> CoordinatedRunResult:
        """Resolve every run in the cohort and keep its finished state in memory."""
        if self._cached_result is not None:
            return self._cached_result
        if self._is_running:
            raise RuntimeError("The coordinated scenario loop cannot call itself recursively.")
        self._is_running = True
        try:
            self._cached_result = super().get(*args, **kwargs)
            return self._cached_result
        finally:
            self._is_running = False

    def create_and_get_asset(self) -> CoordinatedRunResult:
        """Run the nested candidate and behavior loops in lockstep."""
        resolver = get_current_asset_resolver()
        if resolver is not None:
            resolver.retain_assets_shared_by_roots(
                [
                    state_asset
                    for run in self.runs.values()
                    for state_asset in run.iteration_state_assets
                ]
            )

        states_by_run_key = {run_key: [] for run_key in self.run_keys}
        current_state_assets = {
            run_key: run.initial_iteration_state
            for run_key, run in self.runs.items()
        }
        current_states = {
            run_key: run.initial_iteration_state.get()
            for run_key, run in self.runs.items()
        }
        for run_key, state in current_states.items():
            run = self.runs[run_key]
            logging.info(
                "Coordinated run input | scenario=%s | sensitivity=%s | replication=%d | "
                "population_sample_size=%s | steps=%d",
                run.scenario,
                run.sensitivity_case_id,
                run.replication,
                run.population.parameters.sample_size,
                self.parameters.run.n_steps,
            )
            self._log_plan_distribution(
                stage="initial state",
                scenario=run.scenario,
                iteration=0,
                demand_groups=state.demand_groups,
                plans=state.current_plans,
                plan_steps=state.current_plan_steps,
                level=logging.INFO,
            )
        diagnostics: list[dict[str, Any]] = []
        last_inner_results: dict[int, Any] | None = None
        actual_outer_iterations = 0

        for iteration in range(1, self.parameters.run.n_steps + 1):
            with simulation_step("simulation step", phase="outer", step=iteration):
                get_group_day_trips_progress().iteration_step(
                    iteration,
                    "coordinated inner candidate search",
                )
                state_assets = {
                    run_key: self.runs[run_key].iteration_state_assets[iteration - 1]
                    for run_key in self.run_keys
                }
                # Build compatible scenario path costs together before candidate
                # generation requests them one scenario at a time.
                if len(state_assets) > 1:
                    prepare_shared_congested_costs(
                        [state_asset.transport_costs for state_asset in state_assets.values()]
                    )
                inner_result = self._run_inner_loop(
                    state_assets=state_assets,
                    fixed_states=current_states,
                    iteration=iteration,
                    max_search_batches=self.parameters.adaptive_search.max_search_batches,
                    flow_tolerance=self.parameters.adaptive_search.inner_flow_tolerance,
                    base_gap_tolerance=self.parameters.adaptive_search.inner_base_gap_tolerance,
                    required_passes=self.parameters.adaptive_search.inner_consecutive_passes,
                    initial_pool=None,
                )
                last_inner_results = inner_result
                diagnostics.extend(inner_result["diagnostics"])

                # Inner allocations measure search stability. Behavioral transitions
                # always start from the preceding outer distribution.
                for run_key, state_asset in state_assets.items():
                    state_asset.coordinated_candidate_plan_steps = (
                        inner_result["candidate_plan_steps"][run_key]
                    )
                    state_asset.coordinated_evaluation = inner_result.get("evaluations", {}).get(run_key)
                    state_asset.coordinated_current_plans = current_states[run_key].current_plans
                    state_asset.coordinated_current_plan_steps = current_states[run_key].current_plan_steps
                    state_asset.coordinated_plan_id_index = inner_result["plan_id_indices"][run_key]
                    state_asset.coordinated_destination_sequence_index = (
                        inner_result["pool"].destination_sequence_indices[run_key]
                    )
                    state_asset.coordinated_mode_sequence_index = (
                        inner_result["pool"].mode_sequence_indices[run_key]
                    )
                    state_asset.coordinated_rng_state = inner_result["rng_states"][run_key]
                    state_asset.coordinated_time_sequence_steps = inner_result["time_sequence_steps"][run_key]

                next_states = {}
                for run_key in self.run_keys:
                    run = self.runs[run_key]
                    with simulation_step(
                        "outer behavior state",
                        scenario=run.scenario,
                        replication=run.replication,
                        sensitivity_case=run.sensitivity_case_id,
                    ):
                        next_states[run_key] = state_assets[run_key].get()
                        state_assets[run_key].coordinated_evaluation = None
                inner_result.get("evaluations", {}).clear()
                for run_key, state in next_states.items():
                    run = self.runs[run_key]
                    self._log_plan_distribution(
                        stage="after behavior update",
                        scenario=run.scenario,
                        iteration=iteration,
                        demand_groups=state.demand_groups,
                        plans=state.current_plans,
                        plan_steps=state.current_plan_steps,
                    )
                self._write_checkpoint_manifest(iteration)
                actual_outer_iterations = iteration
                for run_key, state in next_states.items():
                    # Keep asset handles for historical output. The resolver can
                    # spill the large plan tables instead of pinning every state
                    # in this cohort result.
                    states_by_run_key[run_key].append(state_assets[run_key])

                outer_stop_reason = (
                    "completed_steps"
                    if iteration == self.parameters.run.n_steps
                    else "step_continues"
                )
                for run_key in self.run_keys:
                    run = self.runs[run_key]
                    convergence = inner_result["convergence_by_run_key"][run_key]
                    diagnostics.append(
                        {
                            "_run_key": run_key,
                            "scenario": run.scenario,
                            "sensitivity_case": run.sensitivity_case_id,
                            "replication": run.replication,
                            "phase": "outer",
                            "outer_step": iteration,
                            "search_batch": None,
                            "search_batches": inner_result["search_batches"],
                            "generation_effort_used": inner_result["generation_effort_used"],
                            "activity_profile_cell_count": (
                                inner_result["activity_profile_cell_count_by_scenario"][run_key]
                            ),
                            "covered_activity_profile_cell_count": (
                                inner_result["covered_activity_profile_cell_count_by_scenario"][run_key]
                            ),
                            "uncovered_activity_profile_cell_count": max(
                                0,
                                inner_result["activity_profile_cell_count_by_scenario"][run_key]
                                - inner_result["covered_activity_profile_cell_count_by_scenario"][run_key],
                            ),
                            "unique_pool_size": len(inner_result["pool"].candidate_ids),
                            "infeasible_candidate_count": inner_result["infeasible_counts"][run_key],
                            "missing_cost_row_candidate_count": inner_result["missing_cost_row_counts"][run_key],
                            "invalid_cost_row_candidate_count": inner_result["invalid_cost_row_counts"][run_key],
                            "flow_change": convergence.flow_changes.get(run_key),
                            "base_gap_change": convergence.base_gap_changes.get(run_key),
                            "inner_converged": convergence.converged,
                            "stop_reason": outer_stop_reason,
                        }
                    )

                current_states = next_states
                current_state_assets = state_assets

        if not actual_outer_iterations or last_inner_results is None:
            raise RuntimeError("The coordinated run completed no outer iterations.")

        # Congestion costs describe the final behavioral allocation. Candidate
        # search remains at the costs fixed at the start of the last outer
        # iteration, so the MNL diagnostic below is intentionally pre-update.
        final_iteration = actual_outer_iterations + 1
        final_cost_assets = {
            run_key: self._final_transport_cost_asset(
                self.runs[run_key],
                final_iteration,
                current_state_assets[run_key],
            )
            for run_key in self.run_keys
        }

        if len(final_cost_assets) > 1:
            prepare_shared_congested_costs(list(final_cost_assets.values()))
        final_states: dict[int, RunState] = {}
        for run_key in self.run_keys:
            run = self.runs[run_key]
            final_cost_asset = final_cost_assets[run_key]
            run.coordinated_final_transport_cost_asset = final_cost_asset
            final_state = replace(
                current_states[run_key],
                costs=final_cost_asset.get_costs_by_od(["cost", "distance"]),
            )
            scenario_diagnostics = pl.DataFrame(
                [
                    {key: value for key, value in row.items() if key != "_run_key"}
                    for row in diagnostics
                    if row["_run_key"] == run_key
                ],
                infer_schema_length=None,
            )
            final_state.adaptive_search_diagnostics = scenario_diagnostics
            final_states[run_key] = final_state
            self._log_plan_distribution(
                stage="final behavioral allocation",
                scenario=run.scenario,
                iteration=final_iteration,
                demand_groups=final_state.demand_groups,
                plans=final_state.current_plans,
                plan_steps=final_state.current_plan_steps,
                level=logging.INFO,
            )

        return CoordinatedRunResult(
            states_by_run_key=states_by_run_key,
            final_states_by_run_key=final_states,
            mnl_od_mode_flows_by_run_key={
                run_key: represented_od_mode_flows(last_inner_results["plan_steps"][run_key])
                for run_key in self.run_keys
            },
        )

    def _prepare_checkpoint_resume(self) -> None:
        """Resume only when every scenario has the same complete cohort checkpoint."""
        if not self.parameters.outputs.checkpoint_each_step:
            return

        resume_iterations = [
            run.iterations.get_resume_iteration(self.parameters.run.n_steps)
            for run in self.runs.values()
        ]
        if (
            not resume_iterations
            or resume_iterations[0] is None
            or len(set(resume_iterations)) != 1
        ):
            self._clear_resume_checkpoint_loaders()
            return

        resume_iteration = int(resume_iterations[0])
        manifest_path = self._checkpoint_manifest_path(resume_iteration)
        all_scenario_checkpoints_ready = all(
            state_asset.resume_checkpoint_loader is not None
            for run in self.runs.values()
            for state_asset in run.iteration_state_assets
            if state_asset.iteration <= resume_iteration
        )
        if not manifest_path.exists() or not all_scenario_checkpoints_ready:
            logging.info(
                "Coordinated checkpoints are incomplete or belong to a different "
                "scenario cohort; recomputing every scenario from the start."
            )
            self._clear_resume_checkpoint_loaders()
            return

        logging.info(
            "Resuming the coordinated scenario cohort from step %s.",
            resume_iteration,
        )

    def _clear_resume_checkpoint_loaders(self) -> None:
        """Prevent one scenario from resuming without its cohort partners."""
        for run in self.runs.values():
            for state_asset in run.iteration_state_assets:
                state_asset.resume_checkpoint_loader = None

    def _checkpoint_manifest_path(self, iteration: int) -> pathlib.Path:
        """Return the cohort marker written after all scenario states are saved."""
        checkpoint_folder = (
            next(iter(self.runs.values())).iterations.base_folder
            / "coordinated-checkpoints"
        )
        return checkpoint_folder / (
            f"{self._checkpoint_cohort_hash}_iteration_{iteration}.json"
        )

    @trace_simulation_step("checkpoint manifest")
    def _write_checkpoint_manifest(self, iteration: int) -> None:
        """Mark an outer iteration resumable only after every scenario completes."""
        if not self.parameters.outputs.checkpoint_each_step:
            return
        manifest_path = self._checkpoint_manifest_path(iteration)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(
                {
                    "cohort_hash": self._checkpoint_cohort_hash,
                    "iteration": iteration,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def remove_checkpoint_manifests(self) -> None:
        """Remove this scenario cohort's completion markers."""
        manifest_folder = self._checkpoint_manifest_path(1).parent
        if manifest_folder.exists():
            for manifest_path in manifest_folder.glob(
                f"{self._checkpoint_cohort_hash}_iteration_*.json"
            ):
                manifest_path.unlink()

    @trace_simulation_step("inner loop")
    def _run_inner_loop(
        self,
        *,
        state_assets: dict[int, Any],
        fixed_states: dict[int, RunState],
        iteration: int,
        max_search_batches: int,
        flow_tolerance: float,
        base_gap_tolerance: float,
        required_passes: int,
        initial_pool: SharedCandidatePoolResult | None,
        cost_assets: dict[int, Any] | None = None,
        generation_previous_states: dict[int, Any] | None = None,
        phase: str = "inner",
    ) -> dict[int, Any]:
        """Expand and evaluate the shared pool while outer state stays fixed."""
        adaptive = self.parameters.adaptive_search
        generation_targets = {}
        for scenario in self.scenarios:
            run = self.runs[scenario]
            state_asset = state_assets[scenario]
            schedule_pool_asset = state_asset.activity_sequences.activity_schedule_pool
            schedule_pool = schedule_pool_asset.get() if schedule_pool_asset is not None else None
            transport_costs = (
                cost_assets[scenario]
                if cost_assets is not None
                else state_asset.transport_costs
            )
            generation_targets[scenario] = build_generation_targets(
                demand_groups=fixed_states[scenario].demand_groups,
                current_plans=fixed_states[scenario].current_plans,
                activity_schedule_pool=schedule_pool,
                transport_costs=transport_costs,
                population_segments=run.population.population_segments,
                parameters=run.parameters,
                iteration=iteration,
            )
        scheduler = AdaptiveGenerationScheduler(
            targets_by_scenario=generation_targets,
        )
        pool = initial_pool
        previous_catalogue = None
        if pool is None:
            previous_candidates = {
                scenario: fixed_states[scenario].candidate_plan_steps
                for scenario in self.scenarios
            }
            previous_destination_indices = {
                scenario: fixed_states[scenario].destination_sequence_index
                for scenario in self.scenarios
            }
            previous_mode_indices = {
                scenario: fixed_states[scenario].mode_sequence_index
                for scenario in self.scenarios
            }
        else:
            previous_catalogue = pool.candidate_catalogue
            previous_candidates = {} if previous_catalogue is not None else pool.candidate_plan_steps_by_run_key
            previous_destination_indices = pool.destination_sequence_indices
            previous_mode_indices = pool.mode_sequence_indices

        plan_id_indices = {
            scenario: fixed_states[scenario].plan_id_index
            for scenario in self.scenarios
        }
        time_sequence_steps = {scenario: fixed_states[scenario].schedule_steps for scenario in self.scenarios}
        evaluated_candidates: dict[int, pl.DataFrame | pl.LazyFrame] = {}
        final_evaluations = {}
        convergence_by_group = {
            group: InnerLoopConvergence(
                flow_tolerance=flow_tolerance,
                base_gap_tolerance=base_gap_tolerance,
                required_consecutive_passes=required_passes,
            )
            for group in self.run_keys_by_group
        }
        population_by_scenario = {
            scenario: self._represented_population(fixed_states[scenario])
            for scenario in self.scenarios
        }
        allocations: dict[int, pl.DataFrame] = {}
        plan_steps: dict[int, pl.DataFrame] = {}
        rng_states: dict[int, object] = {}
        infeasible_counts = {scenario: 0 for scenario in self.scenarios}
        missing_cost_row_counts = {scenario: 0 for scenario in self.scenarios}
        invalid_cost_row_counts = {scenario: 0 for scenario in self.scenarios}
        batch_diagnostics: list[dict[str, Any]] = []
        batch_index = 0
        stop_reason = "max_search_batches"
        final_convergence_by_run_key = {}
        covered_cells_by_scenario: dict[int, set[tuple[int, str]]] = {
            scenario: set() for scenario in self.scenarios
        }
        covered_profiles_by_scenario: dict[int, set[tuple[int, str, int]]] = {
            scenario: set() for scenario in self.scenarios
        }
        recent_effect_by_scenario: dict[int, dict[int, float]] = {
            scenario: {} for scenario in self.scenarios
        }
        scenario_gap_by_scenario: dict[int, float] = {
            scenario: 0.0 for scenario in self.scenarios
        }
        previous_allocations: dict[int, pl.DataFrame | None] = {
            scenario: None for scenario in self.scenarios
        }

        while batch_index < max_search_batches:
            with simulation_step("search batch", search_batch=batch_index + 1):
                generation_requests = scheduler.select_requests(
                    batch_index=batch_index,
                    covered_cells_by_scenario=covered_cells_by_scenario,
                    covered_profiles_by_scenario=covered_profiles_by_scenario,
                    recent_effect_by_scenario=recent_effect_by_scenario,
                    scenario_gap_by_scenario=scenario_gap_by_scenario,
                )
                if (
                    batch_index > 0
                    and not any(frame.height for frame in generation_requests.values())
                ):
                    stop_reason = "generation_targets_exhausted"
                    break
                sources, source_by_scenario, seeds_by_scenario = self._build_candidate_sources(
                    state_assets=state_assets,
                    iteration=iteration,
                    batch_index=batch_index,
                    generation_requests_by_scenario=generation_requests,
                    cost_assets=cost_assets,
                    generation_previous_states=generation_previous_states,
                )
                pool_asset = SharedCandidatePoolAsset(
                    sources=sources,
                    iteration=iteration,
                    max_shared_candidates=adaptive.max_shared_candidates,
                    previous_candidates_by_run_key=previous_candidates,
                    previous_catalogue=previous_catalogue,
                    previous_destination_indices=previous_destination_indices,
                    previous_mode_indices=previous_mode_indices,
                    previous_time_sequence_steps=time_sequence_steps,
                    compact=True,
                )
                pool = pool_asset.get()
                # Only the latest pool is needed by the next batch. Keeping every
                # pool asset in the resolver also keeps its previous input tables.
                resolver = get_current_asset_resolver()
                if resolver is not None:
                    resolver.release_in_memory_asset(pool_asset)
                del pool_asset
                generated_sequences_this_batch = pool.generated_mode_sequences_by_run_key
                for scenario in self.scenarios:
                    source = source_by_scenario[scenario]
                    run = self.runs[scenario]
                    for generated_asset in (
                        source.mode_sequences,
                        source.destination_sequences,
                        source.destination_sequences.activity_sequences,
                    ):
                        resolver = get_current_asset_resolver()
                        if resolver is not None:
                            resolver.release_in_memory_asset(generated_asset)
                    rng_states[scenario] = seeds_by_scenario[scenario].get()[
                        "rng_state_after_sampling"
                    ]

                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        logging.debug(
                            "Plan generation batch | phase=%s | step=%d | search_batch=%d | "
                            "scenario=%s | generated_mode_sequences=%d | shared_unique_plans=%d",
                            phase,
                            iteration,
                            batch_index + 1,
                            run.scenario,
                            generated_sequences_this_batch[scenario],
                            len(pool.candidate_ids),
                        )

                flows_by_scenario = {}
                for scenario in self.scenarios:
                    run = self.runs[scenario]
                    with simulation_step("scenario evaluation", scenario=run.scenario):
                        state = fixed_states[scenario]
                        state_asset = state_assets[scenario]
                        transport_costs = (
                            cost_assets[scenario]
                            if cost_assets is not None
                            else state_asset.transport_costs
                        )
                        evaluation = state_asset.updater.evaluate_candidate_pool(
                            current_plans=state.current_plans,
                            current_plan_steps=state.current_plan_steps,
                            candidate_plan_steps=state.candidate_plan_steps,
                            shared_candidate_plan_steps=pool.candidate_plan_steps_by_run_key[scenario],
                            demand_groups=state.demand_groups,
                            survey_plan_steps=pool.time_sequence_steps_by_run_key[scenario],
                            transport_costs=transport_costs,
                            destination_saturation=state.destination_saturation,
                            activity_dur=state.activity_dur,
                            home_night_dur=state.home_night_dur,
                            stay_home_plan=state.stay_home_plan,
                            transport_zones=run.population.transport_zones,
                            iteration=iteration,
                            resolved_activity_parameters=self._resolved_activity_parameters(
                                run,
                                state_asset,
                                iteration,
                            ),
                            arrival_time_rigidity_by_activity=self._arrival_time_rigidity(
                                run,
                                state_asset,
                                iteration,
                            ),
                            destination_sequences=source_by_scenario[scenario].destination_sequences,
                            mode_sequences=source_by_scenario[scenario].mode_sequences,
                            previous_plan_id_index=plan_id_indices[scenario],
                            parameters=run.parameters,
                            compact_contexts=True,
                        )
                        evaluation_demand_groups = (
                            evaluation.scoring_contexts.demand_groups
                            if evaluation.scoring_contexts is not None else state.demand_groups
                        )
                        possible_plan_utility = evaluation.possible_plan_utility
                        if logging.getLogger().isEnabledFor(logging.DEBUG):
                            feasible_plans = possible_plan_utility.filter(pl.col("is_feasible"))
                            non_stay_home_plans = possible_plan_utility.filter(
                                pl.col("mode_seq_id") != 0
                            )
                            feasible_non_stay_home_plans = feasible_plans.filter(
                                pl.col("mode_seq_id") != 0
                            )
                            non_stay_home_demand_unit_keys = (
                                non_stay_home_plans.select(DEMAND_UNIT_COLS).unique()
                            )
                            feasible_non_stay_home_demand_unit_keys = (
                                feasible_non_stay_home_plans.select(DEMAND_UNIT_COLS).unique()
                            )
                            non_stay_home_demand_units = non_stay_home_demand_unit_keys.height
                            feasible_non_stay_home_demand_units = (
                                feasible_non_stay_home_demand_unit_keys.height
                            )
                            non_stay_home_units_without_feasible_plans = (
                                non_stay_home_demand_unit_keys
                                .join(
                                    feasible_non_stay_home_demand_unit_keys,
                                    on=DEMAND_UNIT_COLS,
                                    how="anti",
                                )
                                .height
                            )
                            total_demand_units = (
                                evaluation_demand_groups.select(DEMAND_UNIT_COLS).unique().height
                            )
                        infeasible_plan_details = (
                            possible_plan_utility
                            .filter((~pl.col("is_feasible")) & (pl.col("mode_seq_id") != 0))
                            .select(
                                PLAN_KEY_COLS
                                + ["missing_cost_row_count", "invalid_cost_row_count"]
                            )
                            .unique()
                        )
                        if logging.getLogger().isEnabledFor(logging.DEBUG):
                            infeasible_plan_variants = (
                                infeasible_plan_details
                                .select(PLAN_KEY_COLS)
                                .unique()
                                .height
                            )
                            missing_cost_plan_variants = (
                                infeasible_plan_details
                                .filter(pl.col("missing_cost_row_count") > 0)
                                .select(PLAN_KEY_COLS)
                                .unique()
                                .height
                            )
                            invalid_cost_plan_variants = (
                                infeasible_plan_details
                                .filter(pl.col("invalid_cost_row_count") > 0)
                                .select(PLAN_KEY_COLS)
                                .unique()
                                .height
                            )
                        if infeasible_plan_details.height:
                            candidate_keys = (
                                add_candidate_ids(evaluation.candidate_plan_steps)
                                .select(PLAN_KEY_COLS + CANDIDATE_ID_COLS)
                                .unique()
                            )
                            infeasible_candidates = (
                                candidate_keys
                                .join(infeasible_plan_details, on=PLAN_KEY_COLS, how="inner")
                                .group_by(CANDIDATE_ID_COLS)
                                .agg(
                                    missing_cost_row_count=pl.col("missing_cost_row_count").max(),
                                    invalid_cost_row_count=pl.col("invalid_cost_row_count").max(),
                                )
                            )
                            infeasible_counts[scenario] = infeasible_candidates.height
                            missing_cost_row_counts[scenario] = (
                                infeasible_candidates
                                .filter(pl.col("missing_cost_row_count") > 0)
                                .select(CANDIDATE_ID_COLS)
                                .unique()
                                .height
                            )
                            invalid_cost_row_counts[scenario] = (
                                infeasible_candidates
                                .filter(pl.col("invalid_cost_row_count") > 0)
                                .select(CANDIDATE_ID_COLS)
                                .unique()
                                .height
                            )
                        else:
                            infeasible_counts[scenario] = 0
                            missing_cost_row_counts[scenario] = 0
                            invalid_cost_row_counts[scenario] = 0

                        if logging.getLogger().isEnabledFor(logging.DEBUG):
                            logging.debug(
                                "Plan candidate evaluation | phase=%s | step=%d | search_batch=%d | "
                                "scenario=%s | candidate_plan_step_rows=%d | "
                                "plan_utility_rows_including_stay_home=%d | "
                                "feasible_plan_utility_rows_including_stay_home=%d | "
                                "non_stay_home_plan_variants=%d | feasible_non_stay_home_plan_variants=%d | "
                                "infeasible_non_stay_home_plan_variants=%d | "
                                "missing_cost_plan_variants=%d | invalid_cost_plan_variants=%d | "
                                "infeasible_candidate_ids=%d | missing_cost_candidate_ids=%d | "
                                "invalid_cost_candidate_ids=%d | demand_units=%d | "
                                "demand_units_with_non_stay_home_plans=%d | "
                                "demand_units_with_feasible_non_stay_home_plans=%d | "
                                "demand_units_without_non_stay_home_plans=%d | "
                                "demand_units_with_non_stay_home_plans_but_none_feasible=%d",
                                phase,
                                iteration,
                                batch_index + 1,
                                run.scenario,
                                evaluation.candidate_plan_steps.height,
                                possible_plan_utility.height,
                                feasible_plans.height,
                                non_stay_home_plans.select(PLAN_KEY_COLS).unique().height,
                                feasible_non_stay_home_plans.select(PLAN_KEY_COLS).unique().height,
                                infeasible_plan_variants,
                                missing_cost_plan_variants,
                                invalid_cost_plan_variants,
                                infeasible_counts[scenario],
                                missing_cost_row_counts[scenario],
                                invalid_cost_row_counts[scenario],
                                total_demand_units,
                                non_stay_home_demand_units,
                                feasible_non_stay_home_demand_units,
                                max(0, total_demand_units - non_stay_home_demand_units),
                                non_stay_home_units_without_feasible_plans,
                            )
                        allocation, current_steps = state_asset.updater.estimate_mnl_allocation(
                            evaluation,
                            state.demand_groups,
                            logit_scale=run.parameters.plan_update.transition_logit_scale,
                        )
                        if evaluation.scoring_contexts is not None:
                            context_groups = evaluation.scoring_contexts.demand_groups
                            feasible_contexts = (
                                evaluation.possible_plan_utility
                                .filter(
                                    pl.col("is_feasible")
                                    & (pl.col("mode_seq_id") != 0)
                                )
                                .select(DEMAND_UNIT_COLS)
                                .unique()
                                .join(
                                    context_groups.select(
                                        DEMAND_UNIT_COLS
                                        + [
                                            "home_zone_id",
                                            "activity_profile_id",
                                            "utility_profile_id",
                                        ]
                                    ),
                                    on=DEMAND_UNIT_COLS,
                                    how="inner",
                                )
                                .select(
                                    "home_zone_id",
                                    "activity_profile_id",
                                    "utility_profile_id",
                                )
                                .unique()
                            )
                            covered_cells_by_scenario[scenario] = {
                                (int(row["home_zone_id"]), str(row["activity_profile_id"]))
                                for row in feasible_contexts.select(
                                    "home_zone_id", "activity_profile_id"
                                ).unique().iter_rows(named=True)
                            }
                            covered_profiles_by_scenario[scenario] = {
                                (
                                    int(row["home_zone_id"]),
                                    str(row["activity_profile_id"]),
                                    int(row["utility_profile_id"]),
                                )
                                for row in feasible_contexts.iter_rows(named=True)
                            }
                            recent_effect_by_scenario[scenario] = allocation_effect_by_zone(
                                previous_allocations[scenario],
                                allocation,
                                context_groups,
                            )
                            previous_allocations[scenario] = allocation.clone()
                        self._log_plan_distribution(
                            stage=f"{phase} allocation batch {batch_index + 1}",
                            scenario=scenario,
                            iteration=iteration,
                            demand_groups=evaluation_demand_groups,
                            plans=allocation,
                            plan_steps=current_steps,
                        )
                        allocations[scenario] = allocation
                        time_sequence_steps[scenario] = evaluation.time_sequence_steps
                        evaluated_candidates[scenario] = (
                            evaluation.scoring_contexts.expand_step_rows(evaluation.candidate_plan_steps)
                            if evaluation.scoring_contexts is not None else evaluation.candidate_plan_steps
                        )
                        final_evaluations[scenario] = evaluation
                        plan_steps[scenario] = current_steps
                        plan_id_indices[scenario] = (
                            state.plan_id_index if evaluation.scoring_contexts is not None else evaluation.plan_id_index
                        )
                        flows_by_scenario[scenario] = represented_od_mode_flows(current_steps)
                        # The retained candidates and allocations above are sufficient.
                        # Release scored step queries before evaluating the next scenario.
                        del evaluation, possible_plan_utility, evaluation_demand_groups

                final_convergence_by_run_key = {}
                for group, run_keys in self.run_keys_by_group.items():
                    group_flows = {
                        run_key: flows_by_scenario[run_key]
                        for run_key in run_keys
                    }
                    group_populations = {
                        run_key: population_by_scenario[run_key]
                        for run_key in run_keys
                    }
                    convergence = convergence_by_group[group].evaluate(
                        group_flows,
                        group_populations,
                        base_scenario=self.base_run_key_by_group[group],
                    )
                    for run_key in run_keys:
                        final_convergence_by_run_key[run_key] = convergence

                all_runs_converged = all(
                    convergence.converged
                    for convergence in final_convergence_by_run_key.values()
                )
                if all_runs_converged:
                    stop_reason = "inner_converged"
                elif batch_index + 1 >= max_search_batches:
                    stop_reason = "max_search_batches"
                else:
                    stop_reason = "continue_search"
                scenario_gap_by_scenario = {
                    run_key: float(
                        final_convergence_by_run_key[run_key]
                        .base_gap_changes.get(run_key)
                        or 0.0
                    )
                    for run_key in self.run_keys
                }

                for scenario in self.scenarios:
                    run = self.runs[scenario]
                    convergence = final_convergence_by_run_key[scenario]
                    requests = generation_requests[scenario]
                    request_reason_counts = {
                        reason: requests.filter(pl.col("request_reason") == reason).height
                        for reason in (
                            "activity_coverage",
                            "utility_profile_gap",
                            "recent_allocation_effect",
                            "scenario_flow_difference",
                            "exploration",
                        )
                    } if requests.height else {}
                    target_cell_count = generation_targets[scenario].activity_cells.height
                    covered_cell_count = len(covered_cells_by_scenario[scenario])
                    target_profile_cell_count = scheduler.utility_profile_cells_by_scenario[
                        scenario
                    ]
                    covered_profile_cell_count = len(covered_profiles_by_scenario[scenario])
                    batch_diagnostics.append(
                        {
                            "_run_key": scenario,
                            "scenario": run.scenario,
                            "sensitivity_case": run.sensitivity_case_id,
                            "replication": run.replication,
                            "phase": phase,
                            "outer_step": iteration if phase == "inner" else None,
                            "search_batch": batch_index + 1,
                            "generated_mode_sequences": generated_sequences_this_batch[scenario],
                            "generation_request_count": requests.height,
                            "requested_utility_profile_count": (
                                requests["utility_profile_id"].n_unique()
                                if requests.height
                                else 0
                            ),
                            "activity_coverage_request_count": request_reason_counts.get(
                                "activity_coverage", 0
                            ),
                            "utility_profile_gap_request_count": request_reason_counts.get(
                                "utility_profile_gap", 0
                            ),
                            "recent_effect_request_count": request_reason_counts.get(
                                "recent_allocation_effect", 0
                            ),
                            "scenario_difference_request_count": request_reason_counts.get(
                                "scenario_flow_difference", 0
                            ),
                            "exploration_request_count": request_reason_counts.get(
                                "exploration", 0
                            ),
                            "generation_effort": int(
                                requests["generation_effort"].sum() or 0
                            ),
                            "generation_effort_used": scheduler.effort_used,
                            "activity_profile_cell_count": target_cell_count,
                            "covered_activity_profile_cell_count": covered_cell_count,
                            "uncovered_activity_profile_cell_count": max(
                                0, target_cell_count - covered_cell_count
                            ),
                            "utility_profile_cell_count": target_profile_cell_count,
                            "covered_utility_profile_cell_count": covered_profile_cell_count,
                            "uncovered_utility_profile_cell_count": max(
                                0, target_profile_cell_count - covered_profile_cell_count
                            ),
                            "unique_pool_size": len(pool.candidate_ids),
                            "infeasible_candidate_count": infeasible_counts[scenario],
                            "missing_cost_row_candidate_count": missing_cost_row_counts[scenario],
                            "invalid_cost_row_candidate_count": invalid_cost_row_counts[scenario],
                            "flow_change": convergence.flow_changes.get(scenario),
                            "base_gap_change": convergence.base_gap_changes.get(scenario),
                            "inner_consecutive_passes": convergence.consecutive_passes,
                            "inner_converged": convergence.converged,
                            "stop_reason": stop_reason,
                        }
                    )

                previous_catalogue = pool.candidate_catalogue
                previous_candidates = {} if previous_catalogue is not None else pool.candidate_plan_steps_by_run_key
                previous_destination_indices = pool.destination_sequence_indices
                previous_mode_indices = pool.mode_sequence_indices
                batch_index += 1
                if stop_reason != "continue_search":
                    break

        if pool is None or not final_convergence_by_run_key:
            raise RuntimeError("The shared candidate loop did not evaluate a candidate pool.")

        if batch_diagnostics and stop_reason != "continue_search":
            for row in batch_diagnostics[-len(self.scenarios):]:
                row["stop_reason"] = stop_reason

        return {
            "pool": pool,
            "time_sequence_steps": time_sequence_steps,
            "candidate_plan_steps": evaluated_candidates,
            "evaluations": final_evaluations,
            "allocations": allocations,
            "plan_steps": plan_steps,
            "plan_id_indices": plan_id_indices,
            "rng_states": rng_states,
            "diagnostics": batch_diagnostics,
            "search_batches": batch_index,
            "infeasible_counts": infeasible_counts,
            "missing_cost_row_counts": missing_cost_row_counts,
            "invalid_cost_row_counts": invalid_cost_row_counts,
            "convergence_by_run_key": final_convergence_by_run_key,
            "converged": all(
                convergence.converged
                for convergence in final_convergence_by_run_key.values()
            ),
            "stop_reason": stop_reason,
            "generation_effort_used": scheduler.effort_used,
            "activity_profile_cell_count_by_scenario": {
                scenario: generation_targets[scenario].activity_cells.height
                for scenario in self.scenarios
            },
            "covered_activity_profile_cell_count_by_scenario": {
                scenario: len(covered_cells_by_scenario[scenario])
                for scenario in self.scenarios
            },
            "utility_profile_cell_count_by_scenario": {
                scenario: scheduler.utility_profile_cells_by_scenario[scenario]
                for scenario in self.scenarios
            },
            "covered_utility_profile_cell_count_by_scenario": {
                scenario: len(covered_profiles_by_scenario[scenario])
                for scenario in self.scenarios
            },
        }

    @staticmethod
    def _log_plan_distribution(
        *,
        stage: str,
        scenario: str,
        iteration: int,
        demand_groups: pl.DataFrame,
        plans: pl.DataFrame,
        plan_steps: pl.DataFrame | None,
        level: int = logging.DEBUG,
    ) -> None:
        """Log compact population and plan totals without writing extra tables."""
        if not logging.getLogger().isEnabledFor(level):
            return
        represented_people = float(demand_groups["n_persons"].sum() or 0.0)
        allocated_people = float(plans["n_persons"].sum() or 0.0)
        active_plans = (
            plans.filter(pl.col("mode_seq_id") != 0)
            if "mode_seq_id" in plans.columns
            else plans.head(0)
        )
        stay_home_plans = (
            plans.filter(pl.col("mode_seq_id") == 0)
            if "mode_seq_id" in plans.columns
            else plans.head(0)
        )
        demand_unit_count = demand_groups.select(DEMAND_UNIT_COLS).unique().height
        allocated_demand_units = (
            plans.select(DEMAND_UNIT_COLS).unique().height
            if plans.height
            else 0
        )
        unique_plan_count = (
            plans.select(PLAN_KEY_COLS).unique().height
            if plans.height
            else 0
        )
        trip_step_summary: list[dict[str, Any]] = []
        plan_step_count = 0
        if plan_steps is not None:
            plan_step_count = plan_steps.height
            if {"activity_seq_id", "mode", "n_persons"}.issubset(plan_steps.columns):
                trip_step_summary = (
                    plan_steps
                    .filter(pl.col("activity_seq_id") != 0)
                    .group_by("mode")
                    .agg(
                        plan_step_rows=pl.len(),
                        represented_trip_steps=pl.col("n_persons").cast(pl.Float64).sum(),
                    )
                    .sort("mode")
                    .to_dicts()
                )

        logging.log(
            level,
            "Plan allocation summary | stage=%s | scenario=%s | step=%d | "
            "demand_units=%d | allocated_demand_units=%d | represented_people=%.3f | "
            "allocated_people=%.3f | population_gap=%.3f | plan_allocations=%d | "
            "unique_plans=%d | active_plan_allocations=%d | active_people=%.3f | "
            "stay_home_allocations=%d | stay_home_people=%.3f | plan_step_rows=%d | "
            "trip_steps_by_mode=%s",
            stage,
            scenario,
            iteration,
            demand_unit_count,
            allocated_demand_units,
            represented_people,
            allocated_people,
            allocated_people - represented_people,
            plans.height,
            unique_plan_count,
            active_plans.height,
            float(active_plans["n_persons"].sum() or 0.0),
            stay_home_plans.height,
            float(stay_home_plans["n_persons"].sum() or 0.0),
            plan_step_count,
            trip_step_summary,
        )

    def _build_candidate_sources(
        self,
        *,
        state_assets: dict[int, Any],
        iteration: int,
        batch_index: int,
        generation_requests_by_scenario: dict[int, pl.DataFrame],
        cost_assets: dict[int, Any] | None,
        generation_previous_states: dict[int, Any] | None,
    ) -> tuple[
        list[ScenarioCandidateSource],
        dict[int, ScenarioCandidateSource],
        dict[int, IterationSeedsAsset],
    ]:
        """Build scenario generation assets that all read the same outer state."""
        sources: list[ScenarioCandidateSource] = []
        source_by_scenario: dict[int, ScenarioCandidateSource] = {}
        seeds_by_scenario: dict[int, IterationSeedsAsset] = {}
        shared_activity_sampling_tables: dict = {}
        for scenario in self.scenarios:
            run = self.runs[scenario]
            state_asset = state_assets[scenario]
            previous_state = (
                state_asset.previous_state
                if generation_previous_states is None
                else generation_previous_states[scenario]
            )
            transport_costs = (
                cost_assets[scenario]
                if cost_assets is not None
                else state_asset.transport_costs
            )

            if batch_index == 0 and cost_assets is None:
                seeds = state_asset.seeds
            else:
                seeds = IterationSeedsAsset(
                    previous_state=previous_state,
                    iteration=iteration,
                    batch_index=batch_index,
                )
            generation_requests = generation_requests_by_scenario[scenario]
            activity_sequences = ActivitySequences(
                is_weekday=run.is_weekday,
                iteration=iteration,
                previous_state=previous_state,
                seed_asset=seeds,
                activity_schedule_pool=state_asset.activity_sequences.activity_schedule_pool,
                generation_requests=generation_requests,
                parameters=run.parameters,
                shared_sampling_tables=shared_activity_sampling_tables,
            )
            destination_sequences = DestinationSequences(
                is_weekday=run.is_weekday,
                iteration=iteration,
                previous_state=previous_state,
                seed_asset=seeds,
                activity_sequences=activity_sequences,
                generation_requests=generation_requests,
                activities=run.activities,
                scenario=run.scenario,
                sensitivity_case=run.sensitivity_case,
                transport_zones=run.population.transport_zones,
                transport_costs=transport_costs,
                population_segments=run.population.population_segments,
                parameters=run.parameters,
            )
            mode_sequences = ModeSequences(
                is_weekday=run.is_weekday,
                iteration=iteration,
                previous_state=previous_state,
                destination_sequences=destination_sequences,
                generation_requests=generation_requests,
                transport_costs=transport_costs,
                population_segments=run.population.population_segments,
                parameters=run.parameters,
            )
            seeds_by_scenario[scenario] = seeds
            source = ScenarioCandidateSource(
                run_key=scenario,
                scenario=run.scenario,
                run=run,
                previous_state=previous_state,
                destination_sequences=destination_sequences,
                mode_sequences=mode_sequences,
                iteration=iteration,
            )
            sources.append(source)
            source_by_scenario[scenario] = source
        return sources, source_by_scenario, seeds_by_scenario

    @staticmethod
    def _represented_population(state: RunState) -> float:
        """Return the represented number of people in a scenario state."""
        return float(state.demand_groups.select(pl.col("n_persons").sum()).item() or 0.0)

    def _final_transport_cost_asset(
        self,
        run: Any,
        iteration: int,
        previous_state: Any,
    ) -> Any:
        """Return costs updated for the final behavioral allocation."""
        if iteration <= len(run.iteration_transport_cost_assets):
            return run.iteration_transport_cost_assets[iteration - 1]
        return IterationTransportCostsAsset(
            is_weekday=run.is_weekday,
            iteration=iteration,
            transport_costs=run.transport_costs,
            scenario=run.scenario,
            sensitivity_case=run.sensitivity_case,
            previous_state=previous_state,
            n_steps_per_cost_update=run.parameters.run.n_steps_per_cost_update,
        )

    @staticmethod
    def _resolved_activity_parameters(
        run: Any,
        state_asset: Any,
        iteration: int,
    ) -> dict[str, Any]:
        """Return activity settings for the scenario and iteration being scored."""
        if iteration == state_asset.iteration:
            return state_asset.inputs["resolved_activity_parameters"]
        return resolve_activity_parameters(
            run.activities,
            iteration,
            scenario=run.scenario,
            sensitivity_case=run.sensitivity_case,
        )

    @staticmethod
    def _arrival_time_rigidity(
        run: Any,
        state_asset: Any,
        iteration: int,
    ) -> dict[str, float]:
        """Return schedule timing rigidity for the scenario and iteration."""
        if iteration == state_asset.iteration:
            return state_asset.inputs["arrival_time_rigidity_by_activity"]
        return resolve_activity_arrival_time_rigidity(
            run.activities,
            iteration,
            scenario=run.scenario,
            sensitivity_case=run.sensitivity_case,
        )
