from __future__ import annotations


import json
from contextlib import nullcontext
import logging
import math
from dataclasses import replace
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import simulation_step, trace_simulation_step, trace_simulation_run

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.graph import asset_graph_key
from mobility.runtime.assets.resolver import get_current_asset_resolver
from mobility.runtime.assets.input_hashing import hash_inputs
from mobility.activities.activity import (
    resolve_activity_time_rigidity,
    resolve_activity_parameters,
)
from mobility.trips.group_day_trips.core.progress import get_group_day_trips_progress
from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from mobility.trips.group_day_trips.iterations.iteration_assets import IterationSeedsAsset
from mobility.trips.group_day_trips.plans.activity_sequences import ActivitySequences
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences, TravelOptionsCache
from mobility.trips.group_day_trips.plans.demand_subgroups import DEMAND_UNIT_COLS
from mobility.trips.group_day_trips.plans.compact_candidate_steps import CompactCandidateSteps, PLAN_COLUMNS
from mobility.trips.group_day_trips.plans.mode_sequence_search.mode_sequences import ModeSequences
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.plans.factored_plan_scoring import SectionScoreCache
from mobility.trips.group_day_trips.plans.scoring_contexts import (
    ScoringContexts, ScoringLookup, ScoringMemoryBudget,
)
from mobility.trips.group_day_trips.plans.time_sequences import TimeSequenceIndexCache
from mobility.trips.group_day_trips.plans.shared_candidate_pool import (
    ScenarioCandidateSource,
    SharedCandidatePool,
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
    candidate_content_ids,
    allocation_od_mode_flows,
    flow_change_index,
    flow_change_breakdown,
    represented_od_mode_flows,
)
from mobility.trips.group_day_trips.core.run_state import RunState
from mobility.trips.group_day_trips.core.coordinated_run_result import CoordinatedRunResult, SimulationRequirements
from mobility.trips.group_day_trips.core.cohort_checkpoints import CohortCheckpoints
from mobility.transport.costs.path.shared_congested_costs import prepare_shared_congested_costs
from mobility.trips.group_day_trips.iterations.iteration_assets import IterationTransportCostsAsset


class MultiScenarioRunAsset(CohortCheckpoints, InMemoryAsset):
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
        self._scoring_context_cache = {}
        self._section_structure_cache = {}
        self._is_running = False
        super().__init__(
            {
                # Congestion costs and state identities follow the whole search.
                "version": 37,
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
                "candidate_history_policy": "physical_plan_age_per_replication_v3",
                "behavior_evaluation_policy": "score_current_pool_v2",
                "inner_attachment_policy": "scoring_contexts_v1",
                "checkpoint_manifest_version": 4,
                "runs": tuple(
                    (
                        run_key,
                        run.iterations.run_inputs_hash,
                    )
                    for run_key, run in self.runs.items()
                ),
            }
        )

    def get(self, *args: Any, **kwargs: Any) -> CoordinatedRunResult:
        """Resolve every run in the cohort within the current execution."""
        if self._is_running:
            raise RuntimeError("The coordinated scenario loop cannot call itself recursively.")
        self._is_running = True
        try:
            return super().get(*args, **kwargs)
        finally:
            self._scoring_context_cache.clear()
            self._section_structure_cache.clear()
            for run in self.runs.values():
                for state_asset in run.iteration_state_assets:
                    self._clear_step_inputs(state_asset)
            self._is_running = False

    def create_and_get_asset(self, *, step_consumer=None, requirements=None) -> CoordinatedRunResult:
        """Advance the cohort and supply requested completed steps."""
        if requirements is None:
            requirements = SimulationRequirements(
                steps=None if step_consumer is not None else frozenset(),
                final_outputs=True, mnl_flows=True, flow_change=True,
            )
            if step_consumer is not None:
                consumer = step_consumer
                step_consumer = lambda iteration, result: consumer(iteration)
        return self._run_steps(resolver=get_current_asset_resolver(), step_consumer=step_consumer,
                               requirements=requirements)

    @staticmethod
    def _clear_step_inputs(state_asset: Any) -> None:
        """Drop search inputs after their last consumer has finished."""
        for name in (
            "coordinated_candidate_plan_steps", "coordinated_evaluation",
            "coordinated_current_plans", "coordinated_current_plan_steps",
            "coordinated_plan_id_index", "coordinated_destination_sequence_index",
            "coordinated_mode_sequence_index", "coordinated_rng_state",
            "coordinated_time_sequence_steps",
        ):
            setattr(state_asset, name, None)

    def _finalize_cohort(
        self,
        iteration: int,
        current_states: dict[int, RunState],
        current_state_assets: dict[int, Any],
    ) -> dict[int, RunState]:
        """Finish post-update costs while the final step is live."""
        final_cost_assets = {
            run_key: self._final_transport_cost_asset(
                self.runs[run_key], iteration + 1, current_state_assets[run_key]
            )
            for run_key in self.run_keys
        }
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
            final_states[run_key] = final_state
            self._log_plan_distribution(
                stage="final behavioral allocation",
                scenario=run.scenario,
                iteration=iteration + 1,
                demand_groups=final_state.demand_groups,
                plans=final_state.current_plans,
                plan_steps=final_state.current_plan_steps,
                level=logging.INFO,
            )

        return final_states

    def _advance_cohort_step(
        self, iteration: int, current_states: dict[int, RunState], *, keep_inner_flows: bool = False,
        track_flow_change: bool = False,
    ) -> tuple[dict[int, Any], dict[int, RunState], dict[str, Any]]:
        """Advance the whole cohort from one fixed pre-step state."""
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
            keep_inner_flows=keep_inner_flows,
            track_flow_change=track_flow_change,
        )
        # The completed evaluations own the data needed by the outer update.
        # Drop the pool's attachment queries and active-step tables before
        # advancing scenarios; only its IDs, indices and catalogue are used.
        inner_result["pool"] = replace(
            inner_result["pool"], candidate_plan_steps_by_run_key={},
            candidate_step_parts_by_run_key=None, scoring_candidate_steps_by_run_key=None,
        )
        # Fresh MNL flows are an optional diagnostic; convergence uses the
        # reachable allocation from the fixed outer state instead.
        inner_result.pop("flows_by_scenario")
        inner_result["mnl_od_mode_flows"] = inner_result.pop("mnl_flows_by_scenario")
        # Inner allocations measure search stability. Behavioral transitions
        # always start from the preceding outer distribution.
        for run_key, state_asset in state_assets.items():
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
        try:
            for run_key in self.run_keys:
                run = self.runs[run_key]
                state_assets[run_key].coordinated_evaluation = inner_result.get("evaluations", {}).pop(
                    run_key, None
                )
                with simulation_step(
                    "outer behavior state",
                    scenario=run.scenario,
                    replication=run.replication,
                    sensitivity_case=run.sensitivity_case_id,
                ):
                    next_states[run_key] = state_assets[run_key].get()
                    state_assets[run_key].coordinated_evaluation = None
        finally:
            # Evaluations can contain large scoring tables. They are useful
            # only while this attempt applies the shared search to the cohort.
            for state_asset in state_assets.values():
                state_asset.coordinated_evaluation = None
            inner_result.get("evaluations", {}).clear()
        return state_assets, next_states, inner_result

    @trace_simulation_run
    def _run_steps(self, *, resolver, step_consumer, requirements) -> CoordinatedRunResult:
        """Advance and restore simulation steps for the requested outputs."""
        required_iterations = requirements.steps
        needs_final_outputs = requirements.final_outputs
        needs_mnl_flows = requirements.mnl_flows
        track_flow_change = requirements.flow_change
        if step_consumer is None:
            step_consumer = lambda iteration, result: None
        resume_iteration = 0
        if self.parameters.outputs.checkpoint_each_step:
            resume_iteration = self._prepare_checkpoint_resume(needs_mnl_flows=needs_mnl_flows)
            for run in self.runs.values():
                for asset in run.iteration_state_assets:
                    asset._checkpoint_saved = False
                    asset._loaded_checkpoint = False
        last_step = self.parameters.run.n_steps
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
        mnl_od_mode_flows: dict[int, pl.DataFrame] | None = None
        actual_outer_iterations = 0
        final_states: dict[int, RunState] | None = None
        previous_activity_flows: dict[int, pl.DataFrame] = {}

        for iteration in range(1, self.parameters.run.n_steps + 1):
            with simulation_step("simulation step", phase="outer", step=iteration):
                if iteration <= resume_iteration:
                    manifest = json.loads(self._checkpoint_manifest_path(iteration).read_text(encoding="utf-8"))
                    diagnostics.extend(manifest["diagnostics"])
                    # The latest checkpoint supplies the state for continued
                    # simulation. Earlier ones are only needed by consumers
                    # that explicitly requested those steps.
                    if (iteration < resume_iteration and required_iterations is not None
                            and iteration not in required_iterations):
                        continue
                    state_assets = {
                        run_key: run.iteration_state_assets[iteration - 1]
                        for run_key, run in self.runs.items()
                    }
                    next_states = {run_key: asset.get() for run_key, asset in state_assets.items()}
                    if track_flow_change:
                        previous_activity_flows = {
                            run_key: represented_od_mode_flows(state.current_plan_steps, include_activity=True)
                            for run_key, state in next_states.items()
                        }
                    if iteration > 1:
                        for previous_asset in current_state_assets.values():
                            if resolver is not None:
                                resolver.release_in_memory_asset(previous_asset)
                            self._clear_step_inputs(previous_asset)
                    current_states = next_states
                    current_state_assets = state_assets
                    actual_outer_iterations = iteration
                    if iteration == last_step:
                        mnl_od_mode_flows = ({
                            int(key): pl.read_parquet(item["path"])
                            for key, item in manifest["mnl_flows"].items()
                        } if needs_mnl_flows else {})
                        if needs_mnl_flows and not mnl_od_mode_flows:
                            raise RuntimeError("Checkpoint is missing final inner-flow results.")
                        final_states = (self._finalize_cohort(iteration, current_states, current_state_assets)
                                        if needs_final_outputs else current_states)
                        for run_key, state in final_states.items():
                            state.adaptive_search_diagnostics = pl.DataFrame(
                                [{key: value for key, value in row.items() if key != "_run_key"}
                                 for row in diagnostics if row["_run_key"] == run_key], infer_schema_length=None,
                            )
                    step_result = CoordinatedRunResult(
                        final_states_by_run_key=final_states or {},
                        mnl_od_mode_flows_by_run_key=mnl_od_mode_flows or {},
                        current_iteration=iteration,
                        current_states_by_run_key=current_states,
                    )
                    previous_coordinator = resolver.simulation_coordinator if resolver is not None else None
                    previous_step_result = resolver.simulation_step_result if resolver is not None else None
                    try:
                        if required_iterations is None or iteration in required_iterations:
                            with (resolver.pin_in_memory_assets(list(state_assets.values()))
                                  if resolver is not None else nullcontext()):
                                if resolver is not None:
                                    resolver.simulation_coordinator = self
                                    resolver.simulation_step_result = step_result
                                step_consumer(iteration, step_result)
                    finally:
                        if resolver is not None:
                            resolver.simulation_coordinator = previous_coordinator
                            resolver.simulation_step_result = previous_step_result
                    continue
                if self.parameters.outputs.checkpoint_each_step:
                    # Later states depend on this step. Invalidate their
                    # markers before any files can be rewritten.
                    self._invalidate_checkpoint_manifests_from(iteration)
                state_assets, next_states, inner_result = self._advance_cohort_step(
                    iteration, current_states,
                    keep_inner_flows=iteration == last_step and (needs_mnl_flows or self.parameters.outputs.checkpoint_each_step),
                    **({"track_flow_change": True} if track_flow_change else {}),
                )
                diagnostics.extend(inner_result["diagnostics"])

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
                actual_outer_iterations = iteration
                outer_stop_reason = (
                    "completed_steps"
                    if iteration == self.parameters.run.n_steps
                    else "step_continues"
                )
                for run_key in self.run_keys:
                    run = self.runs[run_key]
                    convergence = inner_result["convergence_by_run_key"][run_key]
                    outer_flow_change = None
                    outer_breakdowns = {}
                    if track_flow_change:
                        current_flows = represented_od_mode_flows(
                            next_states[run_key].current_plan_steps, include_activity=True,
                        )
                        previous_flows = previous_activity_flows.get(run_key)
                        if previous_flows is not None:
                            outer_flow_change = flow_change_index(previous_flows, current_flows)
                        outer_breakdowns = {
                            category: flow_change_breakdown(previous_flows, current_flows, category)
                            for category in ("mode", "activity")
                        }
                        previous_activity_flows[run_key] = current_flows
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
                            "scored_candidate_scope": inner_result["scored_candidate_scope_by_scenario"][run_key],
                            "infeasible_candidate_count": inner_result["infeasible_counts"][run_key],
                            "missing_cost_row_candidate_count": inner_result["missing_cost_row_counts"][run_key],
                            "invalid_cost_row_candidate_count": inner_result["invalid_cost_row_counts"][run_key],
                            "flow_change": convergence.flow_changes.get(run_key),
                            "flow_change_index": outer_flow_change,
                            "flow_change_by_mode": outer_breakdowns.get("mode"),
                            "flow_change_by_activity": outer_breakdowns.get("activity"),
                            "base_gap_change": convergence.base_gap_changes.get(run_key),
                            "inner_converged": convergence.converged,
                            "stop_reason": outer_stop_reason,
                        }
                    )

                self._write_checkpoint_manifest(
                    iteration,
                    diagnostics=[row for row in diagnostics if row["outer_step"] == iteration],
                    mnl_flows=inner_result["mnl_od_mode_flows"] if iteration == last_step else None,
                )

                # This diagnostic describes the final search estimate, before
                # the behavioral update and its post-update transport costs.
                if iteration == last_step:
                    mnl_od_mode_flows = inner_result["mnl_od_mode_flows"]
                if iteration > 1:
                    # This step has finished using the preceding state, and
                    # its requested outputs were reduced during that step.
                    for run_key, previous_asset in current_state_assets.items():
                        if resolver is not None:
                            resolver.release_in_memory_asset(previous_asset)
                        self._clear_step_inputs(previous_asset)
                current_states = next_states
                current_state_assets = state_assets
                del inner_result
                if iteration == last_step:
                    if needs_final_outputs:
                        final_states = self._finalize_cohort(
                            iteration, current_states, current_state_assets
                        )
                    else:
                        final_states = current_states
                    for run_key, state in final_states.items():
                        state.adaptive_search_diagnostics = pl.DataFrame(
                            [
                                {key: value for key, value in row.items() if key != "_run_key"}
                                for row in diagnostics if row["_run_key"] == run_key
                            ],
                            infer_schema_length=None,
                        )
                # Result assets read the current state without recursively
                # starting a second cohort execution.
                step_result = CoordinatedRunResult(
                    final_states_by_run_key=final_states or {},
                    mnl_od_mode_flows_by_run_key=mnl_od_mode_flows or {},
                    current_iteration=iteration,
                    current_states_by_run_key=current_states,
                )
                previous_coordinator = resolver.simulation_coordinator if resolver is not None else None
                previous_step_result = resolver.simulation_step_result if resolver is not None else None
                try:
                    if required_iterations is None or iteration in required_iterations:
                        with (
                            resolver.pin_in_memory_assets(list(state_assets.values()))
                            if resolver is not None else nullcontext()
                        ):
                            if resolver is not None:
                                resolver.simulation_coordinator = self
                                resolver.simulation_step_result = step_result
                            step_consumer(iteration, step_result)
                finally:
                    if resolver is not None:
                        resolver.simulation_coordinator = previous_coordinator
                        resolver.simulation_step_result = previous_step_result

        if not actual_outer_iterations or mnl_od_mode_flows is None or final_states is None:
            raise RuntimeError("The coordinated run completed no outer iterations.")

        return CoordinatedRunResult(
            final_states_by_run_key=final_states,
            mnl_od_mode_flows_by_run_key=mnl_od_mode_flows,
            current_iteration=actual_outer_iterations,
        )

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
        keep_inner_flows: bool = True,
        track_flow_change: bool = False,
    ) -> dict[int, Any]:
        """Expand and evaluate the shared pool while outer state stays fixed."""
        generate_candidates = (
            self.parameters.behavior_change.scope_at(iteration) != BehaviorChangeScope.NO_TRANSITIONS
        )
        if not generate_candidates:
            max_search_batches = 1
        # A single batch cannot be compared with a preceding batch.
        estimate_allocation = (
            max_search_batches > 1
            or track_flow_change
            or logging.getLogger().isEnabledFor(logging.DEBUG)
        )
        adaptive = self.parameters.adaptive_search
        # Decide what each run can generate from the state at the start of
        # this step. Every search batch uses these same targets.
        generation_targets = self._build_generation_targets(
            fixed_states, state_assets, cost_assets, iteration,
        )
        scheduler = AdaptiveGenerationScheduler(
            targets_by_scenario=generation_targets,
        )
        has_future_generation = any(
            targets.requests.filter(pl.col("generation_effort") > 0).height
            for targets in generation_targets.values()
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
        final_evaluations = {}
        # Contexts depend on the fixed pre-step demand and scenario costs, not
        # on the candidate pool. Keep one per scenario only for this inner loop.
        scoring_setup_by_run_key = {}
        compact_plan_indices_by_run_key = {}
        time_index_caches = {run_key: TimeSequenceIndexCache() for run_key in self.run_keys}
        # Bound retained scoring lookups across the whole scenario cohort.
        scoring_memory = ScoringMemoryBudget(max_bytes=512 * 1024 * 1024)
        scoring_lookups = self._build_scoring_lookups(
            fixed_states, state_assets, cost_assets, iteration, scoring_memory,
        )
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
        previous_activity_flows: dict[int, pl.DataFrame] = {}
        forced_scoring_contexts = {}
        scored_candidate_scope = {scenario: "all" for scenario in self.scenarios}
        destination_probabilities = {
            scenario: {} for scenario in self.scenarios
        } if max_search_batches > 1 else None
        # Costs stay fixed within this inner search. Bound retained OD tables
        # across the entire scenario cohort and release them after this step.
        travel_options_cache = (
            TravelOptionsCache(max_bytes=256 * 1024 * 1024)
            if max_search_batches > 1 and self.parameters.destination_sequences.sampling_method == "utility"
            else None
        )

        while batch_index < max_search_batches:
            with simulation_step("search batch", search_batch=batch_index + 1):
                batch_flow_changes: dict[int, float | None] = {}
                batch_flow_breakdowns: dict[int, dict[str, list[dict]]] = {}
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
                if batch_index > 0:
                    # The preceding results are needed if search stops, but
                    # this batch will replace them. Keep only the catalogue,
                    # indices and allocation summaries used to generate it.
                    final_evaluations.clear()
                    pool = None
                sources, source_by_scenario, seeds_by_scenario = self._build_candidate_sources(
                    state_assets=state_assets,
                    iteration=iteration,
                    batch_index=batch_index,
                    generation_requests_by_scenario=generation_requests,
                    cost_assets=cost_assets,
                    generation_previous_states=generation_previous_states,
                    destination_probabilities=destination_probabilities,
                    travel_options_cache=travel_options_cache,
                )
                pool_builder = SharedCandidatePool(
                    sources=sources,
                    iteration=iteration,
                    max_shared_candidates=adaptive.max_shared_candidates,
                    max_inactive_age=(
                        self.parameters.plan_update.max_inactive_age
                        if batch_index == 0 and iteration > self.parameters.plan_update.n_warmup_steps
                        else None
                    ),
                    previous_candidates_by_run_key=previous_candidates,
                    previous_catalogue=previous_catalogue,
                    previous_destination_indices=previous_destination_indices,
                    previous_mode_indices=previous_mode_indices,
                    previous_time_sequence_steps=time_sequence_steps,
                    compact=True,
                    build_catalogue=batch_index + 1 < max_search_batches,
                    generate_candidates=generate_candidates,
                    time_index_caches=time_index_caches,
                )
                pool = pool_builder.build()
                del pool_builder
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
                    rng_states[scenario] = (
                        seeds_by_scenario[scenario].get()["rng_state_after_sampling"]
                        if generate_candidates else fixed_states[scenario].rng_state
                    )

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

                another_batch_guaranteed = (
                    batch_index < required_passes
                    and batch_index + 1 < max_search_batches and has_future_generation
                )
                evaluation_groups = self._group_candidate_evaluations(
                    pool=pool, states=fixed_states, state_assets=state_assets,
                    sources=source_by_scenario, cost_assets=cost_assets, iteration=iteration,
                    scoring_setups=scoring_setup_by_run_key,
                    compact_indices=compact_plan_indices_by_run_key,
                    forced_contexts=forced_scoring_contexts,
                    restrict_scoring=batch_index > 0 and another_batch_guaranteed,
                )
                group_by_scenario = {
                    scenario: group_index
                    for group_index, members in enumerate(evaluation_groups)
                    for scenario in members
                }
                # Consume matching scenarios together so only one complete
                # evaluation is retained while it is reused.
                ordered_scenarios = [scenario for members in evaluation_groups for scenario in members]
                current_group = None
                reusable_evaluation = None
                reusable_infeasible_counts = None
                reusable_completed = {}
                flows_by_scenario = {}
                for scenario in ordered_scenarios:
                    run = self.runs[scenario]
                    group_index = group_by_scenario[scenario]
                    if group_index != current_group:
                        reusable_evaluation = None
                        reusable_infeasible_counts = None
                        reusable_completed.clear()
                        current_group = group_index
                    with simulation_step("scenario evaluation", scenario=run.scenario):
                        state = fixed_states[scenario]
                        state_asset = state_assets[scenario]
                        transport_costs = (
                            cost_assets[scenario]
                            if cost_assets is not None
                            else state_asset.transport_costs
                        )
                        scoring_setup = scoring_setup_by_run_key[scenario]
                        candidate_parts = (pool.candidate_step_parts_by_run_key or {}).get(scenario)
                        scope = run.parameters.behavior_change.scope_at(iteration)
                        # Before enough comparisons exist to stop, and while a
                        # later generation request is guaranteed, the complete
                        # pool remains available for a final full evaluation.
                        restrict_scoring = (
                            batch_index > 0 and another_batch_guaranteed
                            and scope != BehaviorChangeScope.FULL_REPLANNING
                        )
                        scored_candidate_scope[scenario] = "reachable" if restrict_scoring else "all"
                        if reusable_evaluation is None:
                            evaluation = state_asset.updater.evaluate_candidate_pool(
                                current_plans=state.current_plans,
                                current_plan_steps=state.current_plan_steps,
                                candidate_plan_steps=state.candidate_plan_steps,
                                shared_candidate_plan_steps=(
                                    candidate_parts[0] if candidate_parts is not None
                                    else pool.candidate_plan_steps_by_run_key[scenario]
                                ),
                                shared_active_plan_steps=(candidate_parts[1] if candidate_parts is not None else None),
                                factored_candidate_steps=(
                                    pool.scoring_candidate_steps_by_run_key or {}
                                ).get(scenario),
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
                                activity_time_rigidity_by_activity=self._activity_time_rigidity(
                                    run,
                                    state_asset,
                                    iteration,
                                ),
                                destination_sequences=source_by_scenario[scenario].destination_sequences,
                                mode_sequences=source_by_scenario[scenario].mode_sequences,
                                previous_plan_id_index=plan_id_indices[scenario],
                                parameters=run.parameters,
                                compact_contexts=True,
                                scoring_contexts=scoring_setup[0],
                                compact_current_plans=scoring_setup[1],
                                compact_stay_home=scoring_setup[2],
                                compact_plan_id_index=compact_plan_indices_by_run_key.get(scenario),
                                scoring_lookup=scoring_lookups[scenario],
                                time_index_cache=time_index_caches[scenario],
                                reachable_scoring_scope=scope if restrict_scoring else None,
                                forced_scoring_contexts=forced_scoring_contexts.get(scenario),
                            )
                            if len(evaluation_groups[group_index]) > 1:
                                reusable_evaluation = evaluation
                        else:
                            evaluation = reusable_evaluation
                        if batch_index == 0 and scope != BehaviorChangeScope.FULL_REPLANNING:
                            active = evaluation.current_plans
                            if scope != BehaviorChangeScope.NO_TRANSITIONS:
                                active = active.filter(pl.col("mode_seq_id") != 0)
                            forced_scoring_contexts[scenario] = (
                                active.select(PLAN_KEY_COLS)
                                .join(
                                    evaluation.possible_plan_utility.select(PLAN_KEY_COLS + ["is_feasible"]),
                                    on=PLAN_KEY_COLS, how="left",
                                )
                                .filter(~pl.col("is_feasible").fill_null(False))
                                .select(DEMAND_UNIT_COLS).unique()
                            )
                        # Section evaluations use a fresh per-batch temporary
                        # index; only the outer demand-unit index is persistent.
                        compact_plan_indices_by_run_key[scenario] = (
                            None if evaluation.shared_plan_ids is not None else evaluation.plan_id_index
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
                        infeasible_plan_details = None
                        if reusable_infeasible_counts is None or logging.getLogger().isEnabledFor(logging.DEBUG):
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
                        if reusable_infeasible_counts is None:
                            reusable_infeasible_counts = self._count_infeasible_candidates(
                                infeasible_plan_details, evaluation.candidate_plan_steps,
                            )
                        (
                            infeasible_counts[scenario],
                            missing_cost_row_counts[scenario],
                            invalid_cost_row_counts[scenario],
                        ) = reusable_infeasible_counts
                        del infeasible_plan_details

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
                                (evaluation.candidate_plan_steps.lazy().select(pl.len()).collect().item()
                                 if isinstance(evaluation.candidate_plan_steps, CompactCandidateSteps)
                                 else evaluation.candidate_plan_steps.height),
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
                        allocation, current_steps, reachable_ids = (None, None, None)
                        if estimate_allocation:
                            update = run.parameters.plan_update
                            need_reachable_ids = (
                                (batch_index + 1 >= max_search_batches
                                 or batch_index >= required_passes or not has_future_generation)
                                and (not keep_inner_flows or evaluation.shared_step_details is not None)
                                and not (update.enable_transition_distance_model
                                         and math.isfinite(update.transition_distance_threshold))
                            )
                            estimate = state_asset.updater.estimate_reachable_allocation(
                                evaluation, state.current_plans, iteration,
                                run.parameters.behavior_change.scope_at(iteration),
                                transport_zones=run.population.transport_zones,
                                transition_distance_threshold=update.transition_distance_threshold,
                                enable_transition_distance_model=update.enable_transition_distance_model,
                                transition_revision_probability=update.transition_revision_probability,
                                transition_logit_scale=update.transition_logit_scale,
                                transition_utility_pruning_delta=update.transition_utility_pruning_delta,
                                min_transition_utility_gain=update.min_transition_utility_gain,
                                transition_distance_friction=update.transition_distance_friction,
                                plan_embedding_dimension_weights=update.plan_embedding_dimension_weights,
                                min_plan_persons=update.min_plan_persons,
                                plan_pruning_start_step=update.plan_pruning_start_step,
                                include_reachable_ids=need_reachable_ids,
                            )
                            if need_reachable_ids:
                                allocation, reachable_ids = estimate
                            else:
                                allocation = estimate
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                current_steps = state_asset.updater.get_current_plan_steps(
                                    allocation, evaluation.possible_plan_steps.lazy(),
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
                            if allocation is not None:
                                recent_effect_by_scenario[scenario] = allocation_effect_by_zone(
                                    previous_allocations[scenario], allocation, context_groups,
                                )
                                previous_allocations[scenario] = allocation.clone()
                        if allocation is not None:
                            self._log_plan_distribution(
                                stage=f"{phase} allocation batch {batch_index + 1}",
                                scenario=scenario,
                                iteration=iteration,
                                demand_groups=evaluation_demand_groups,
                                plans=allocation,
                                plan_steps=current_steps,
                            )
                        time_sequence_steps[scenario] = evaluation.time_sequence_steps
                        plan_id_indices[scenario] = (
                            state.plan_id_index if evaluation.scoring_contexts is not None else evaluation.plan_id_index
                        )
                        if allocation is None:
                            # The first convergence check reports no comparison;
                            # no later check or flow consumer can use this value.
                            flows_by_scenario[scenario] = pl.DataFrame()
                        elif current_steps is not None:
                            flows_by_scenario[scenario] = represented_od_mode_flows(current_steps)
                        else:
                            flows_by_scenario[scenario] = allocation_od_mode_flows(
                                allocation, evaluation.possible_plan_steps,
                                shared_plan_ids=evaluation.shared_plan_ids, shared_steps=evaluation.shared_steps,
                            )
                        if track_flow_change and allocation is not None:
                            activity_flows = allocation_od_mode_flows(
                                allocation, evaluation.possible_plan_steps, include_activity=True,
                                shared_plan_ids=evaluation.shared_plan_ids, shared_steps=evaluation.shared_steps,
                            )
                            previous = previous_activity_flows.get(scenario)
                            batch_flow_changes[scenario] = (
                                None if previous is None else flow_change_index(previous, activity_flows)
                            )
                            batch_flow_breakdowns[scenario] = {
                                category: flow_change_breakdown(previous, activity_flows, category)
                                for category in ("mode", "activity")
                            }
                            previous_activity_flows[scenario] = activity_flows
                        # A completed evaluation can remain live while other
                        # scenarios are scored. Store its history in factored
                        # form before retaining it when this may be the last batch.
                        if (batch_index + 1 >= max_search_batches
                                or batch_index >= required_passes or not has_future_generation):
                            completed = reusable_completed.get(scope)
                            if completed is None:
                                update = run.parameters.plan_update
                                completed = self._compact_completed_candidate_history(
                                    evaluation, state_asset.updater, scope,
                                    keep_all_steps=keep_inner_flows,
                                    reachable_ids=reachable_ids,
                                    rigid_activities={
                                        activity for activity, rigidity in
                                        self._activity_time_rigidity(run, state_asset, iteration).items()
                                        if rigidity == 1.0
                                    },
                                    transition_parameters={
                                        "transport_zones": run.population.transport_zones,
                                        "transition_distance_threshold": update.transition_distance_threshold,
                                        "enable_transition_distance_model": update.enable_transition_distance_model,
                                        "transition_revision_probability": update.transition_revision_probability,
                                        "transition_logit_scale": update.transition_logit_scale,
                                        "transition_utility_pruning_delta": update.transition_utility_pruning_delta,
                                        "min_transition_utility_gain": update.min_transition_utility_gain,
                                        "transition_distance_friction": update.transition_distance_friction,
                                        "plan_embedding_dimension_weights": update.plan_embedding_dimension_weights,
                                    },
                                )
                                if len(evaluation_groups[group_index]) > 1:
                                    reusable_completed[scope] = completed
                            evaluation = completed
                        final_evaluations[scenario] = evaluation
                        completed = None
                        # Only the convergence summary and next-batch allocation
                        # are needed after this scenario's inner evaluation.
                        del evaluation, possible_plan_utility, evaluation_demand_groups, allocation, current_steps, candidate_parts

                reusable_evaluation = None
                reusable_completed.clear()
                final_convergence_by_run_key = self._evaluate_convergence(
                    flows_by_scenario, population_by_scenario, convergence_by_group,
                )

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
                            "scored_candidate_scope": scored_candidate_scope[scenario],
                            "infeasible_candidate_count": infeasible_counts[scenario],
                            "missing_cost_row_candidate_count": missing_cost_row_counts[scenario],
                            "invalid_cost_row_candidate_count": invalid_cost_row_counts[scenario],
                            "flow_change": convergence.flow_changes.get(scenario),
                            "flow_change_index": batch_flow_changes.get(scenario),
                            "flow_change_by_mode": batch_flow_breakdowns.get(scenario, {}).get("mode"),
                            "flow_change_by_activity": batch_flow_breakdowns.get(scenario, {}).get("activity"),
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

        if travel_options_cache is not None:
            travel_options_cache.clear()

        if batch_diagnostics and stop_reason != "continue_search":
            for row in batch_diagnostics[-len(self.scenarios):]:
                row["stop_reason"] = stop_reason

        # Only the final candidate set is exposed as the fresh-MNL diagnostic.
        # Wait until search stops, so earlier batches never compute it.
        mnl_flows_by_scenario = {}
        if keep_inner_flows:
            for scenario in self.scenarios:
                evaluation = final_evaluations[scenario]
                allocation, _ = state_assets[scenario].updater.estimate_mnl_allocation(
                    evaluation, fixed_states[scenario].demand_groups,
                    logit_scale=self.runs[scenario].parameters.plan_update.transition_logit_scale,
                    include_plan_steps=False,
                )
                mnl_flows_by_scenario[scenario] = allocation_od_mode_flows(
                    allocation, evaluation.possible_plan_steps,
                    shared_plan_ids=evaluation.shared_plan_ids, shared_steps=evaluation.shared_steps,
                )
                del allocation

        # Retain section identities, never their scores, across cost updates.
        # Scores belong to one fixed scenario and outer-step input set.
        self._section_structure_cache = {
            run_key: lookup.section_cache.sections
            for run_key, lookup in scoring_lookups.items()
            if lookup is not None and lookup.section_cache is not None
            and lookup.section_cache.sections is not None
        }
        return {
            "pool": pool,
            "time_sequence_steps": time_sequence_steps,
            "evaluations": final_evaluations,
            "flows_by_scenario": flows_by_scenario,
            "mnl_flows_by_scenario": mnl_flows_by_scenario,
            "plan_id_indices": plan_id_indices,
            "rng_states": rng_states,
            "diagnostics": batch_diagnostics,
            "search_batches": batch_index,
            "infeasible_counts": infeasible_counts,
            "missing_cost_row_counts": missing_cost_row_counts,
            "invalid_cost_row_counts": invalid_cost_row_counts,
            "scored_candidate_scope_by_scenario": scored_candidate_scope,
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

    def _evaluate_convergence(self, flows, populations, convergence_by_group):
        """Check each scenario group against its own reference and population."""
        convergence_by_run = {}
        for group, run_keys in self.run_keys_by_group.items():
            convergence = convergence_by_group[group].evaluate(
                {run_key: flows[run_key] for run_key in run_keys},
                {run_key: populations[run_key] for run_key in run_keys},
                base_scenario=self.base_run_key_by_group[group],
            )
            for run_key in run_keys:
                convergence_by_run[run_key] = convergence
        return convergence_by_run

    @staticmethod
    def _count_infeasible_candidates(infeasible_plan_details, candidates):
        """Count distinct infeasible physical plans and their cost problems."""
        if infeasible_plan_details.height:
            if isinstance(candidates, CompactCandidateSteps):
                needed = infeasible_plan_details.select(PLAN_KEY_COLS).lazy()
                member_keys = candidates.memberships_for_plans(needed).select(PLAN_KEY_COLS).unique()
                physical = candidates.physical_steps(needed).collect(engine="streaming")
                physical_ids = candidate_content_ids(physical, PLAN_COLUMNS)
                candidate_keys = (member_keys.join(physical_ids.lazy(), on=PLAN_COLUMNS)
                                  .collect(engine="streaming"))
                del needed, member_keys, physical, physical_ids
            else:
                candidate_keys = (
                    add_candidate_ids(candidates)
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
            infeasible_count = infeasible_candidates.height
            # Grouping above already leaves one row per physical candidate.
            missing_cost_count = infeasible_candidates.filter(pl.col("missing_cost_row_count") > 0).height
            invalid_cost_count = infeasible_candidates.filter(pl.col("invalid_cost_row_count") > 0).height
        else:
            infeasible_count = 0
            missing_cost_count = 0
            invalid_cost_count = 0
        return infeasible_count, missing_cost_count, invalid_cost_count

    def _build_generation_targets(self, fixed_states, state_assets, cost_assets, iteration):
        """Find the activity and transport profiles each run can generate."""
        targets = {}
        for run_key in self.scenarios:
            run = self.runs[run_key]
            state_asset = state_assets[run_key]
            schedule_pool_asset = state_asset.activity_sequences.activity_schedule_pool
            schedule_pool = schedule_pool_asset.get() if schedule_pool_asset is not None else None
            transport_costs = (
                cost_assets[run_key]
                if cost_assets is not None
                else state_asset.transport_costs
            )
            targets[run_key] = build_generation_targets(
                demand_groups=fixed_states[run_key].demand_groups,
                current_plans=fixed_states[run_key].current_plans,
                activity_schedule_pool=schedule_pool,
                transport_costs=transport_costs,
                population_segments=run.population.population_segments,
                parameters=run.parameters,
                iteration=iteration,
            )
        return targets

    def _group_candidate_evaluations(
        self, *, pool, states, state_assets, sources, cost_assets, iteration,
        scoring_setups, compact_indices, forced_contexts, restrict_scoring,
    ):
        """Group scenarios whose complete candidate-evaluation inputs match."""
        groups = []
        for run_key in self.scenarios:
            run = self.runs[run_key]
            state = states[run_key]
            state_asset = state_assets[run_key]
            source = sources[run_key]
            costs = cost_assets[run_key] if cost_assets is not None else state_asset.transport_costs
            if run_key not in scoring_setups:
                schedule_pool = getattr(state_asset.activity_sequences, "activity_schedule_pool", None)
                contexts = self._get_scoring_contexts(
                    run_key, state.demand_groups, costs, source.mode_sequences.population_segments,
                    demand_identity=asset_graph_key(schedule_pool) if isinstance(schedule_pool, Asset) else None,
                )
                scoring_setups[run_key] = (
                    contexts, contexts.collapse_allocations(state.current_plans),
                    contexts.collapse_stay_home(state.stay_home_plan),
                )
            contexts = scoring_setups[run_key][0]
            parts = (pool.candidate_step_parts_by_run_key or {}).get(run_key)
            shared_steps = parts[0] if parts is not None else pool.candidate_plan_steps_by_run_key[run_key]
            active_steps = parts[1] if parts is not None else None
            factored = (pool.scoring_candidate_steps_by_run_key or {}).get(run_key)
            history = state.candidate_plan_steps
            if isinstance(history, CompactCandidateSteps):
                history_key = (id(history.steps), id(history.membership), id(history.unit_map), history.columns)
            else:
                history_key = id(history)
            updater = state_asset.updater
            # Custom updaters can carry their own state, so do not group them.
            updater_key = (
                type(updater), type(updater.schedule_updater)
            ) if type(updater) is PlanUpdater and type(updater.schedule_updater) is PlanScheduleUpdater else ("custom", run_key)
            scope = run.parameters.behavior_change.scope_at(iteration)
            restricted_scope = scope if restrict_scoring and scope != BehaviorChangeScope.FULL_REPLANNING else None
            # Candidate and state tables are fixed during this batch. Sharing
            # their existing objects avoids comparing full tables; settings
            # and cost assets still have to match for the scores to match.
            inputs = (
                updater_key, id(shared_steps), id(active_steps), id(factored), history_key,
                id(state.current_plans), id(state.current_plan_steps), id(state.demand_groups),
                id(state.destination_saturation), id(state.activity_dur), id(state.home_night_dur),
                id(state.stay_home_plan), id(pool.time_sequence_steps_by_run_key[run_key]),
                asset_graph_key(costs) if isinstance(costs, Asset) else id(costs),
                id(run.population.transport_zones), id(contexts), id(compact_indices.get(run_key)),
                restricted_scope, id(forced_contexts.get(run_key)) if restricted_scope is not None else None,
                source.mode_sequences.population_segments, run.parameters.plan_update,
                self._resolved_activity_parameters(run, state_asset, iteration),
                self._activity_time_rigidity(run, state_asset, iteration),
            )
            for previous, members in groups:
                if previous == inputs:
                    members.append(run_key)
                    break
            else:
                groups.append((inputs, [run_key]))
        return [members for _, members in groups]

    def _build_scoring_lookups(
        self, states, state_assets, cost_assets, iteration, budget,
    ):
        """Share step scores while their inputs are fixed and identical."""
        groups = []
        for run_key in self.run_keys:
            run = self.runs[run_key]
            state = states[run_key]
            state_asset = state_assets[run_key]
            costs = cost_assets[run_key] if cost_assets is not None else state_asset.transport_costs
            cost_key = asset_graph_key(costs) if isinstance(costs, Asset) else id(costs)
            # These tables are fixed for the outer step. Requiring the same
            # objects avoids comparing or hashing their potentially large data.
            inputs = (
                cost_key, id(state.demand_groups), id(state.activity_dur),
                id(state.destination_saturation), id(run.population.transport_zones),
                run.population.population_segments,
                self._resolved_activity_parameters(run, state_asset, iteration),
                run.parameters.plan_update.min_activity_time_constant,
                run.parameters.plan_update.use_destination_shadow_prices,
            )
            for previous_inputs, members in groups:
                if previous_inputs == inputs:
                    members.append(run_key)
                    break
            else:
                groups.append((inputs, [run_key]))
        lookups = {}
        previous_sections = getattr(self, "_section_structure_cache", {})
        self._section_structure_cache = {}
        for _, members in groups:
            # Even a single batch uses the compact lookup while it is being
            # evaluated; retaining it afterwards is merely an optimization.
            lookup = ScoringLookup(budget)
            sections = next((previous_sections[key] for key in members if key in previous_sections), None)
            # Leave most of the cohort budget for fresh trip and section
            # scores. Old section identities are an optional speed cache.
            if sections is not None and sections.estimated_size() <= budget.max_bytes // 4:
                retained = budget.retain(None, sections)
                if retained is not None:
                    lookup.section_cache = SectionScoreCache(sections=retained, budget=budget)
                    logging.debug("Reusing %d timed section identities for runs %s", retained.height, members)
            lookups.update({run_key: lookup for run_key in members})
        return lookups

    @trace_simulation_step("reuse or build scoring contexts")
    def _get_scoring_contexts(
        self, run_key, demand_groups, transport_costs, population_segments, *, demand_identity=None,
    ):
        """Reuse contexts only for the same demand and declared cost inputs."""
        key = (
            demand_identity if demand_identity is not None else ("table", id(demand_groups)),
            asset_graph_key(transport_costs), hash_inputs({"population_segments": population_segments}),
        ) if isinstance(transport_costs, Asset) else None
        if key is not None:
            for saved_key, saved_demand, saved_segments, contexts in self._scoring_context_cache.values():
                if key == saved_key and (demand_identity is not None or saved_demand is demand_groups):
                    self._scoring_context_cache[run_key] = (key, demand_groups, population_segments, contexts)
                    return contexts
        assignments, _ = transport_costs.get_utility_profiles(
            demand_groups.sort(DEMAND_UNIT_COLS), population_segments,
        )
        contexts = ScoringContexts.build(
            demand_groups, transport_costs, population_segments, assignments=assignments,
        )
        if key is not None:
            # Retaining the inputs also prevents their object IDs being reused.
            self._scoring_context_cache[run_key] = (key, demand_groups, population_segments, contexts)
        return contexts

    @staticmethod
    @trace_simulation_step("compact completed candidate history")
    def _compact_completed_candidate_history(
        evaluation, updater, scope, *, keep_all_steps, transition_parameters, rigid_activities=None,
        reachable_ids=None,
    ):
        """Keep only plan details needed after a scenario has been scored."""
        history = evaluation.candidate_plan_steps
        if isinstance(history, CompactCandidateSteps):
            history = history.with_sections(rigid_activities)
        else:
            history = CompactCandidateSteps.from_frame(history, prefer_compact=True, rigid_activities=rigid_activities)
        steps = evaluation.possible_plan_steps.lazy()
        needs_distances = (
            transition_parameters["enable_transition_distance_model"]
            and math.isfinite(transition_parameters["transition_distance_threshold"])
        )
        needed_ids = None
        if (not keep_all_steps or evaluation.shared_step_details is not None) and not needs_distances:
            if reachable_ids is None:
                choices = (
                    evaluation.possible_plan_utility
                    .join(evaluation.time_sequence_steps.select("time_seq_id", "reference_time_seq_id").unique(),
                          on="time_seq_id", how="left")
                    .with_columns(reference_time_seq_id=pl.col("reference_time_seq_id").fill_null(pl.col("time_seq_id")))
                )
                # Include eligible targets even if their movement probability is zero.
                reachable_ids = updater.build_allowed_plan_transitions(
                    evaluation.current_plans, choices.lazy(), scope,
                    transition_utility_pruning_delta=transition_parameters["transition_utility_pruning_delta"],
                    transition_logit_scale=transition_parameters["transition_logit_scale"],
                    min_transition_utility_gain=transition_parameters["min_transition_utility_gain"],
                ).select(pl.col("plan_id_trans").alias("plan_id")).unique().collect(engine="streaming")
            needed_ids = pl.concat([
                evaluation.current_plans.select("plan_id"),
                reachable_ids,
            ]).unique()
            if evaluation.shared_step_details is None:
                steps = steps.join(needed_ids.lazy(), on="plan_id", how="semi")
        if evaluation.shared_step_details is not None and needed_ids is not None:
            steps = evaluation.shared_step_details.for_storage(needed_ids)
        else:
            steps = steps.select(
                PLAN_KEY_COLS + [
                    "plan_id", "seq_step_index", "activity", "from", "to", "mode", "distance", "time",
                    "duration_per_pers", "departure_time", "arrival_time", "next_departure_time",
                ]
            ).collect(engine="streaming")
        return replace(
            evaluation, candidate_plan_steps=history, possible_plan_steps=steps,
            shared_step_details=None,
            shared_plan_ids=evaluation.shared_plan_ids if keep_all_steps else None,
            shared_steps=evaluation.shared_steps if keep_all_steps else None,
        )

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
        destination_probabilities: dict[int, dict[int, pl.DataFrame]] | None,
        travel_options_cache: TravelOptionsCache | None,
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
                sampling_probabilities=(
                    destination_probabilities[scenario]
                    if destination_probabilities is not None else None
                ),
                travel_options_cache=travel_options_cache,
                travel_options_run_key=scenario,
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
    def _activity_time_rigidity(
        run: Any,
        state_asset: Any,
        iteration: int,
    ) -> dict[str, float]:
        """Return schedule timing rigidity for the scenario and iteration."""
        if iteration == state_asset.iteration:
            return state_asset.inputs["activity_time_rigidity_by_activity"]
        return resolve_activity_time_rigidity(
            run.activities,
            iteration,
            scenario=run.scenario,
            sensitivity_case=run.sensitivity_case,
        )
