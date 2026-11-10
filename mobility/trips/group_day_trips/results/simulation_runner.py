from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from contextlib import nullcontext
from dataclasses import dataclass
import logging
from typing import Any

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.graph import asset_graph_key, build_asset_graph
from mobility.trips.group_day_trips.core.performance_trace import simulation_step, trace_simulation_run
from mobility.trips.group_day_trips.core.run_state import RunState

from .assets.metric import Metric, MetricLaneResult
from .assets.scoped_tables import ScopedRunTable, release_scoped_run_tables
from .result_table import ResultTable


@dataclass(frozen=True)
class CoordinatedRunResult:
    """Per-run outer state, behavior and latest inner estimates."""

    final_states_by_run_key: dict[int, RunState]
    mnl_od_mode_flows_by_run_key: dict[int, pl.DataFrame]
    current_iteration: int = 0
    current_state_assets: dict[int, Any] | None = None


class SimulationRunner:
    """Resolve requested result assets together, advancing through iterations once."""

    def __init__(
        self,
        *,
        requests: Sequence[Metric | ScopedRunTable],
        resolver,
    ) -> None:
        self.requests = self._unique_requests(requests)
        self.resolver = resolver

    @staticmethod
    def validate_external_inputs(runs) -> None:
        """Check every member of the cohort, even for filtered results."""
        checked = set()
        for run in runs:
            coordinator = getattr(run, "coordinated_run_asset", None)
            members = getattr(coordinator, "runs", {}).values() if coordinator else [run]
            for member in members:
                if id(member) in checked:
                    continue
                checked.add(id(member))
                validate = getattr(member, "_validate_external_inputs", None)
                if callable(validate):
                    validate()

    @staticmethod
    def _unique_requests(
        requests: Sequence[Metric | ScopedRunTable],
    ) -> list[Metric | ScopedRunTable]:
        """Include declared metric inputs once, before the metrics that use them."""
        seen = set()
        ordered = []

        def add(request):
            key = asset_graph_key(request)
            if key in seen:
                return
            seen.add(key)
            if isinstance(request, Metric):
                for dependency in request.inputs["dependencies"]:
                    if isinstance(dependency, Metric) and dependency.results is request.results:
                        add(dependency)
            ordered.append(request)

        for request in requests:
            add(request)
        return ordered

    @staticmethod
    def _declared_result_inputs(metric: Metric) -> tuple[Metric | ScopedRunTable, ...] | None:
        """Find result inputs when the remaining dependencies are independent files."""
        inputs = []
        for asset in metric.inputs["dependencies"]:
            if isinstance(asset, (Metric, ScopedRunTable)) and asset.results is metric.results:
                inputs.append(asset)
            elif not isinstance(asset, FileAsset) or any(
                isinstance(node, Metric) or not isinstance(node, FileAsset)
                for node in build_asset_graph(asset, include_node_data=False).nodes
            ):
                return None
        covered_outputs = {
            asset_graph_key(output)
            for item in inputs
            for output in item.planned_outputs
        }
        if any(asset_graph_key(output) not in covered_outputs for output in metric.planned_outputs):
            return None
        return tuple(inputs) if metric.inputs["dependencies"] else None

    @staticmethod
    def _capturable_tables(metric: Metric) -> tuple[ScopedRunTable, ...] | None:
        """Find declared tables when every other dependency is file-backed."""
        inputs = SimulationRunner._declared_result_inputs(metric)
        if inputs is None or any(isinstance(item, Metric) for item in inputs):
            return None
        return inputs

    @staticmethod
    def _metric_is_cached(metric: Metric) -> bool:
        """Check the metric and its file dependencies without rebuilding them."""
        graph = build_asset_graph(
            metric, include_root=True, file_assets_only=True, include_node_data=False,
        )
        return all(not asset.is_update_needed() for asset in graph.nodes)

    @staticmethod
    def _uses_initial_state_only(request) -> bool:
        """Whether a request is known to need no behavior simulation."""
        if isinstance(request, Metric) and request.compute_iteration is None:
            inputs = SimulationRunner._declared_result_inputs(request)
            return inputs is not None and all(
                isinstance(item, Metric) and SimulationRunner._metric_is_cached(item)
                or SimulationRunner._uses_initial_state_only(item)
                for item in inputs
            )
        return (
            isinstance(request, ScopedRunTable)
            and request.table_name in {"demand_groups", "opportunities"}
            and all(
                hasattr(getattr(context.run, "coordinated_run_asset", None), "_advance_cohort_step")
                for context in request.run_contexts
            )
        )

    @staticmethod
    def _final_requirements(requests, last_step: int) -> tuple[bool, bool]:
        """Find whether the requested results need final costs and inner flows."""
        required_requests = []
        remaining = list(requests)
        pending_keys = {asset_graph_key(request) for request in requests}
        seen = set()
        while remaining:
            request = remaining.pop()
            key = asset_graph_key(request)
            if key in seen:
                continue
            seen.add(key)
            inputs = (
                SimulationRunner._declared_result_inputs(request)
                if isinstance(request, Metric) and request.compute_iteration is None
                else None
            )
            if inputs is None:
                required_requests.append(request)
            else:
                for item in inputs:
                    if isinstance(item, Metric) and asset_graph_key(item) not in pending_keys:
                        if SimulationRunner._metric_is_cached(item):
                            continue
                    remaining.append(item)
        # Earlier-step consumers cannot read final costs or final diagnostics.
        # Unknown consumers and direct final-state requests stay conservative.
        needs_final_outputs = not requests or any(
            not (
                isinstance(request, ScopedRunTable)
                and (
                    request.table_name in {"plans", "demand_groups", "opportunities", "iteration_metrics", "mnl_od_mode_flows"}
                    or request.table_name in {"plan_steps", "costs"} and not request.uses_last_iteration
                )
                or isinstance(request, Metric)
                and request.compute_lane is not None
                and request.finish_lanes is not None
                and request.selected_iterations
                and all(step < last_step for step in request.selected_iterations)
            )
            for request in required_requests
        )
        # Final flow diagnostics describe the last inner estimate and are not
        # needed for ordinary final-state tables or metrics.
        needs_mnl_flows = not requests or any(
            not (
                isinstance(request, ScopedRunTable)
                and request.table_name in {
                    "plans", "plan_steps", "costs", "demand_groups", "opportunities", "iteration_metrics",
                    "transitions", "adaptive_search_diagnostics", "mnl_od_mode_flows",
                }
                or isinstance(request, Metric)
                and request.compute_lane is not None
                and request.finish_lanes is not None
            )
            or any(
                getattr(output, "table_name", None) == "mnl_od_mode_flows"
                for output in request.planned_outputs
            )
            for request in required_requests
        )
        return needs_final_outputs, needs_mnl_flows

    @trace_simulation_run
    def _run_cohort(self, coordinator, step_consumer) -> CoordinatedRunResult:
        """Run the nested candidate and behavior loops in lockstep."""
        resolver = self.resolver
        if step_consumer is None:
            step_consumer = lambda iteration: None
        if coordinator.parameters.outputs.checkpoint_each_step:
            # Runs can be executed again after their previous transient state
            # is released. A saved flag belongs to one execution, not the run
            # object, and files selected for resume may have changed meanwhile.
            if any(
                asset.resume_checkpoint_loader is not None
                for run in coordinator.runs.values()
                for asset in run.iteration_state_assets
            ):
                coordinator._prepare_checkpoint_resume()
            for run in coordinator.runs.values():
                for asset in run.iteration_state_assets:
                    asset._checkpoint_saved = False
                    asset._loaded_checkpoint = False
        last_step = coordinator.parameters.run.n_steps
        needs_final_outputs, needs_mnl_flows = self._final_requirements(self.requests, last_step)
        current_state_assets = {
            run_key: run.initial_iteration_state
            for run_key, run in coordinator.runs.items()
        }
        current_states = {
            run_key: run.initial_iteration_state.get()
            for run_key, run in coordinator.runs.items()
        }
        for run_key, state in current_states.items():
            run = coordinator.runs[run_key]
            logging.info(
                "Coordinated run input | scenario=%s | sensitivity=%s | replication=%d | "
                "population_sample_size=%s | steps=%d",
                run.scenario,
                run.sensitivity_case_id,
                run.replication,
                run.population.parameters.sample_size,
                coordinator.parameters.run.n_steps,
            )
            coordinator._log_plan_distribution(
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

        for iteration in range(1, coordinator.parameters.run.n_steps + 1):
            with simulation_step("simulation step", phase="outer", step=iteration):
                state_assets, next_states, inner_result = coordinator._advance_cohort_step(
                    iteration, current_states,
                    keep_inner_flows=iteration == last_step and needs_mnl_flows,
                )
                diagnostics.extend(inner_result["diagnostics"])

                for run_key, state in next_states.items():
                    run = coordinator.runs[run_key]
                    coordinator._log_plan_distribution(
                        stage="after behavior update",
                        scenario=run.scenario,
                        iteration=iteration,
                        demand_groups=state.demand_groups,
                        plans=state.current_plans,
                        plan_steps=state.current_plan_steps,
                    )
                coordinator._write_checkpoint_manifest(iteration)
                actual_outer_iterations = iteration
                outer_stop_reason = (
                    "completed_steps"
                    if iteration == coordinator.parameters.run.n_steps
                    else "step_continues"
                )
                for run_key in coordinator.run_keys:
                    run = coordinator.runs[run_key]
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
                        coordinator._clear_step_inputs(previous_asset)
                current_states = next_states
                current_state_assets = state_assets
                del inner_result
                if iteration == last_step:
                    if needs_final_outputs:
                        final_states = coordinator._finalize_cohort(
                            iteration, current_states, current_state_assets, diagnostics
                        )
                    else:
                        final_states = current_states
                # Result assets read the current state without recursively
                # starting a second cohort execution.
                coordinator._active_result = CoordinatedRunResult(
                    final_states_by_run_key=final_states or {},
                    mnl_od_mode_flows_by_run_key=mnl_od_mode_flows or {},
                    current_iteration=iteration,
                    current_state_assets=current_state_assets,
                )
                try:
                    with (
                        resolver.pin_in_memory_assets(list(state_assets.values()))
                        if resolver is not None else nullcontext()
                    ):
                        step_consumer(iteration)
                finally:
                    coordinator._active_result = None

        if not actual_outer_iterations or mnl_od_mode_flows is None or final_states is None:
            raise RuntimeError("The coordinated run completed no outer iterations.")

        return CoordinatedRunResult(
            final_states_by_run_key=final_states,
            mnl_od_mode_flows_by_run_key=mnl_od_mode_flows,
            current_iteration=actual_outer_iterations,
        )

    def execute_final_state(self, run):
        """Run the complete cohort while retaining the requested final state."""
        coordinator = run.coordinated_run_asset
        self.validate_external_inputs([run])
        self.resolver.shared_asset_keys.discard(asset_graph_key(coordinator))
        self.resolver.release_in_memory_asset(coordinator)
        result = coordinator.get(step_consumer=lambda iteration: None, runner=self)
        self.resolver.shared_asset_keys.discard(asset_graph_key(coordinator))
        self.resolver.release_in_memory_asset(coordinator)
        return result.final_states_by_run_key[run.coordinated_run_key]

    def execute(self) -> dict[tuple, pl.DataFrame | pl.LazyFrame | ResultTable]:
        """Cache-check result assets, run shared iteration work, and return values."""
        self.validate_external_inputs(
            context.run for request in self.requests for context in request.results.run_contexts
        )
        values_by_request: dict[tuple, pl.DataFrame | pl.LazyFrame | ResultTable] = {}
        pending = []

        # Check persistent metrics before loading iteration state. Raw tables
        # are always materialized because they are transient result assets.
        for request in self.requests:
            metric_dependencies = (
                isinstance(request, Metric)
                and any(isinstance(asset, Metric) and asset.results is request.results
                        for asset in request.inputs["dependencies"])
            )
            if metric_dependencies:
                # Preparing this graph now could execute a requested upstream
                # metric before its other consumers have joined the batch.
                # A completely valid graph can still be returned immediately.
                if self._metric_is_cached(request):
                    values_by_request[asset_graph_key(request)] = self.resolver.get(request)
                else:
                    pending.append(request)
                continue
            self.resolver.prepare_upstream_assets(request)
            if isinstance(request, Metric) and not request.is_update_needed():
                request_key = asset_graph_key(request)
                values_by_request[request_key] = self.resolver.get(request)
            else:
                pending.append(request)

        # A declared file-only metric has no simulation consumer. Resolve it
        # separately so it does not force other requests onto a fallback path.
        remaining = []
        for request in pending:
            if (isinstance(request, Metric) and request.compute_iteration is None
                    and self._capturable_tables(request) == ()):
                values_by_request[asset_graph_key(request)] = self.resolver.get(request)
            else:
                remaining.append(request)
        pending = remaining
        if not pending:
            return values_by_request

        # Cached outputs have no remaining consumers in this execution. They
        # must not cause final costs or diagnostics to be built for other work.
        self.requests = pending

        # Capture declared tables when all other dependencies are independent
        # file assets. Other dependencies keep the conservative path.
        fallback_metrics = [
            request for request in pending
            if isinstance(request, Metric) and request.compute_iteration is None
        ]
        table_dependencies = {}
        table_backed_metrics = set()
        for metric in fallback_metrics:
            dependencies = self._declared_result_inputs(metric)
            tables = (
                [item for item in dependencies if isinstance(item, ScopedRunTable)]
                if dependencies is not None else []
            )
            if tables:
                table_backed_metrics.add(asset_graph_key(metric))
                for asset in tables:
                    table_dependencies.setdefault(asset_graph_key(asset), asset)

        # Demand groups and opportunities come from each run's initial state.
        # Their step labels do not require running the behavior simulation.
        if all(self._uses_initial_state_only(request) for request in pending):
            tables = {}
            result_keys = set()
            completed = False
            try:
                for request in [
                    *[request for request in pending if isinstance(request, ScopedRunTable)],
                    *table_dependencies.values(),
                ]:
                    key = asset_graph_key(request)
                    if key in tables:
                        continue
                    table = ResultTable()
                    tables[key] = table
                    for iteration in request.selected_iterations:
                        table._append(request.compute_iteration(iteration))
                for key, dependency in table_dependencies.items():
                    self.resolver.in_memory_values[key] = tables[key]._scan()
                    self.resolver.in_memory_assets[key] = dependency
                for request in pending:
                    key = asset_graph_key(request)
                    if isinstance(request, Metric):
                        values_by_request[key] = self.resolver.get(request)
                    else:
                        values_by_request[key] = tables[key]
                        result_keys.add(key)
                completed = True
                return values_by_request
            finally:
                for key in table_dependencies:
                    self.resolver.in_memory_values.pop(key, None)
                    self.resolver.in_memory_assets.pop(key, None)
                for key, table in tables.items():
                    if not completed or key not in result_keys:
                        table.close()

        planned_outputs = [
            output
            for request in pending
            for output in request.planned_outputs
        ]
        self.resolver.retain_assets_shared_by_roots(planned_outputs)

        iteration_requests = [
            request
            for request in pending
            if request.compute_iteration is not None
        ]
        selected_iterations = sorted(
            {
                iteration
                for request in iteration_requests
                for iteration in request.selected_iterations
            }
        )
        metric_values: dict[tuple, list[pl.DataFrame]] = defaultdict(list)
        lane_values: dict[tuple, dict[int, list]] = defaultdict(lambda: defaultdict(list))
        table_values: dict[tuple, ResultTable] = {}
        dependency_values: dict[tuple, ResultTable] = {}
        installed_dependencies = set()
        live_consumed = set()
        scoped_tables = [
            request
            for request in pending
            if isinstance(request, ScopedRunTable)
        ]
        completed = False

        try:
            # Reduce each supported metric to per-seed totals while the lane
            # is live; raw tables write their selected rows at the same time.
            # Unknown dependencies are resolved separately and do not prevent
            # the supported requests from sharing their cohort execution.
            live_requests = [
                request for request in pending
                if (
                    (isinstance(request, ScopedRunTable)
                     and request.table_name in {
                         "plans", "plan_steps", "costs", "demand_groups", "opportunities", "iteration_metrics", "transitions",
                         "adaptive_search_diagnostics", "mnl_od_mode_flows",
                     })
                    or (isinstance(request, Metric)
                        and request.compute_lane is not None
                        and request.finish_lanes is not None)
                    or asset_graph_key(request) in table_backed_metrics
                )
            ]
            if live_requests:
                contexts = live_requests[0].results.run_contexts
                coordinators = []
                for context in contexts:
                    coordinator = getattr(context.run, "coordinated_run_asset", None)
                    if not any(coordinator is item for item in coordinators):
                        coordinators.append(coordinator)
                if coordinators and all(
                    coordinator is not None
                    and hasattr(coordinator, "_advance_cohort_step")
                    and all(
                        1 <= iteration <= coordinator.parameters.run.n_steps
                        for request in [*live_requests, *table_dependencies.values()]
                        for iteration in request.selected_iterations
                    )
                    for coordinator in coordinators
                ):
                    for coordinator in coordinators:
                        # A previous request in the same resolver must not
                        # turn this request into a read of discarded history.
                        self.resolver.shared_asset_keys.discard(asset_graph_key(coordinator))
                        self.resolver.release_in_memory_asset(coordinator)
                        def consume_lane_step(iteration):
                            previous_coordinator = getattr(self.resolver, "result_coordinator", None)
                            previous_tables = getattr(self.resolver, "lane_scoped_tables", None)
                            self.resolver.result_coordinator = coordinator
                            self.resolver.lane_scoped_tables = []
                            try:
                                for request in live_requests:
                                    if asset_graph_key(request) in table_backed_metrics:
                                        continue
                                    if iteration not in request.selected_iterations:
                                        continue
                                    request_key = asset_graph_key(request)
                                    if isinstance(request, ScopedRunTable):
                                        value = request.compute_iteration(
                                            iteration, coordinator=coordinator
                                        )
                                        if request_key not in table_values:
                                            table_values[request_key] = ResultTable()
                                        table_values[request_key]._append(value)
                                    else:
                                        value = request.compute_lane(iteration)
                                        if not isinstance(value, MetricLaneResult):
                                            raise TypeError("A replication result must contain metric partials.")
                                        lane_values[request_key][iteration].append(value)
                                for key, dependency in table_dependencies.items():
                                    if iteration in dependency.selected_iterations:
                                        # A requested table with the same identity already
                                        # owns this fragment for the caller.
                                        if key in table_values:
                                            continue
                                        if key not in dependency_values:
                                            dependency_values[key] = ResultTable()
                                        dependency_values[key]._append(
                                            dependency.compute_iteration(iteration, coordinator=coordinator)
                                        )
                            finally:
                                used_tables = self.resolver.lane_scoped_tables
                                self.resolver.result_coordinator = previous_coordinator
                                self.resolver.lane_scoped_tables = previous_tables
                                release_scoped_run_tables(iteration, [*scoped_tables, *used_tables])

                        coordinator.get(step_consumer=consume_lane_step, runner=self)
                        self.resolver.shared_asset_keys.discard(asset_graph_key(coordinator))
                        self.resolver.release_in_memory_asset(coordinator)
                    for request in live_requests:
                        if isinstance(request, Metric) and asset_graph_key(request) not in table_backed_metrics:
                            request_key = asset_graph_key(request)
                            metric_values[request_key].extend(
                                request.finish_lanes(lane_values[request_key][iteration])
                                for iteration in request.selected_iterations
                            )
                    live_consumed.update(asset_graph_key(request) for request in live_requests)
                    if table_dependencies.keys() - (dependency_values.keys() | table_values.keys()):
                        raise RuntimeError("The simulation runner produced no rows for a metric dependency.")

            for iteration in selected_iterations:
                try:
                    for request in iteration_requests:
                        if iteration not in request.selected_iterations:
                            continue

                        request_key = asset_graph_key(request)
                        if request_key in live_consumed:
                            continue
                        value = request.compute_iteration(iteration)
                        if isinstance(request, ScopedRunTable):
                            if request_key not in table_values:
                                table_values[request_key] = ResultTable()
                            table_values[request_key]._append(value)
                            continue

                        if isinstance(value, pl.LazyFrame):
                            value = value.collect(engine="streaming")
                        if not isinstance(value, pl.DataFrame):
                            raise TypeError(
                                "A result iteration must return a Polars "
                                "DataFrame or LazyFrame."
                            )
                        metric_values[request_key].append(value)
                finally:
                    # All metric and table consumers have used this iteration.
                    release_scoped_run_tables(iteration, scoped_tables)

            for key in table_dependencies.keys() & (dependency_values.keys() | table_values.keys()):
                table = table_values[key] if key in table_values else dependency_values[key]
                self.resolver.in_memory_values[key] = table._scan()
                self.resolver.in_memory_assets[key] = table_dependencies[key]
                installed_dependencies.add(key)

            for request in iteration_requests:
                request_key = asset_graph_key(request)
                if isinstance(request, ScopedRunTable):
                    if request_key not in table_values:
                        raise RuntimeError(
                            "The simulation runner produced no values for "
                            f"{request.__class__.__name__}."
                        )
                    values_by_request[request_key] = table_values[request_key]
                    continue

                values = metric_values[request_key]
                if not values:
                    raise RuntimeError(
                        "The simulation runner produced no values for "
                        f"{request.__class__.__name__}."
                    )
                request.set_precomputed_value(
                    values[0]
                    if len(values) == 1
                    else pl.concat(values, how="diagonal_relaxed")
                )

                values_by_request[request_key] = self.resolver.get(request)

            # Derived metrics can depend on the iteration metrics above. Write
            # those first so resolving a derived metric does not rerun them.
            for metric in fallback_metrics:
                try:
                    values_by_request[asset_graph_key(metric)] = self.resolver.get(metric)
                finally:
                    metric.clear_precomputed_value()

            completed = True
            return values_by_request
        finally:
            for key in installed_dependencies:
                self.resolver.in_memory_values.pop(key, None)
                self.resolver.in_memory_assets.pop(key, None)
            for table in dependency_values.values():
                table.close()
            if not completed:
                for table in table_values.values():
                    table.close()
            for request in iteration_requests:
                if isinstance(request, Metric):
                    request.clear_precomputed_value()
