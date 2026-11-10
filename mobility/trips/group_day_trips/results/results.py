from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from mobility.runtime.assets.graph import asset_graph_key, build_asset_graph
from mobility.runtime.assets.resolver import (
    asset_resolution_context,
)
from mobility.runtime.parameter_values import DEFAULT_SCENARIO, SensitivityCase
from mobility.runtime.scenarios import Scenarios
from mobility.runtime.temporary import managed_temporary_disk_usage, temporary_disk_limit

from .assets.metric import Metric
from .assets.scoped_tables import ScopedRunTable
from .diagnostics import GroupDayTripsResultDiagnostics
from .simulation_runner import SimulationRunner
from .metrics import GroupDayTripsResultMetrics
from .tables import GroupDayTripsResultTables

IterationSelector = str | int | list[int] | range


@dataclass(frozen=True)
class ResultRun:
    """One concrete run included in a result set."""

    run: object
    scenario: str
    sensitivity_case: SensitivityCase | None
    sensitivity_case_id: str
    day_type: str
    replication: int


class GroupDayTripsResults:
    """Analysis entry point for one or more scenarios, one day type, and one or more runs."""

    def __init__(
        self,
        *,
        run: Callable,
        day_type: str,
        scenarios: str | list[str] | tuple[str, ...] | None,
        n_replications: int,
        scenario_manifest: Scenarios | None = None,
        sensitivity_cases: list[SensitivityCase | None] | None = None,
        replication: int | None = None,
        replications: list[int] | range | None = None,
    ) -> None:
        """Store the selected result scope and expose analysis namespaces."""
        if replication is not None and replications is not None:
            raise ValueError("Use either `replication` or `replications`, not both.")

        self._run = run
        self.day_type = str(day_type)
        self.scenarios = self._validate_scenarios(scenarios)
        self.scenario_manifest = scenario_manifest
        if self.scenario_manifest is not None:
            self.scenario_manifest.validate_requested(self.scenarios)
        self.n_replications = int(n_replications)
        self.sensitivity_cases = (
            [None]
            if sensitivity_cases is None
            else list(sensitivity_cases)
        )
        if not self.sensitivity_cases:
            raise ValueError("GroupDayTripsResults needs at least one sensitivity case.")
        self.replications = self._validate_replications(
            replication=replication,
            replications=replications,
        )
        self._run_contexts: list[ResultRun] | None = None

        self.tables = GroupDayTripsResultTables(self)
        self.metrics = GroupDayTripsResultMetrics(self)
        self.diagnostics = GroupDayTripsResultDiagnostics(self)

    def get_all(self, **requests: Metric | ScopedRunTable) -> dict[str, Any]:
        """Resolve named metric and table requests together."""
        if not requests:
            return {}

        self._validate_requests(requests)

        # A result batch owns its transient execution; it must not release
        # unrelated values held by a surrounding asset-resolution context.
        with asset_resolution_context() as resolver:
            try:
                values_by_request = SimulationRunner(
                    requests=list(requests.values()),
                    resolver=resolver,
                ).execute()
            finally:
                resolver.release_transient_values()

        return {
            name: values_by_request[asset_graph_key(request)]
            for name, request in requests.items()
        }

    def _validate_requests(self, requests: dict[str, Metric | ScopedRunTable]) -> None:
        """Check that all named requests belong to this result scope."""
        for name, request in requests.items():
            if not isinstance(request, (Metric, ScopedRunTable)):
                raise TypeError(
                    f"Request {name!r} should be a Metric or ScopedRunTable, "
                    f"received {request.__class__.__name__}."
                )
            if request.results is not self:
                raise ValueError(
                    f"Request {name!r} belongs to a different "
                    "GroupDayTripsResults object."
                )

    def explain(self, **requests: Metric | ScopedRunTable) -> dict[str, Any]:
        """Describe requested work and check existing files without running it."""
        self._validate_requests(requests)
        SimulationRunner.validate_external_inputs(context.run for context in self.run_contexts)
        lanes = {}
        for context in self.run_contexts:
            coordinator = getattr(context.run, "coordinated_run_asset", None)
            runs = getattr(coordinator, "runs", {}).values() if coordinator else [context.run]
            members = [
                {"scenario": getattr(run, "scenario", context.scenario),
                 "sensitivity_case": getattr(run, "sensitivity_case_id", context.sensitivity_case_id)}
                for run in runs
            ]
            if not members:
                members = [{"scenario": context.scenario, "sensitivity_case": context.sensitivity_case_id}]
            lane = lanes.setdefault(context.replication, {
                "replication": context.replication,
                "seed": getattr(context.run.parameters.run, "seed", None),
                "cohort": members,
                "configured_steps": int(context.run.parameters.run.n_steps),
            })
            for member in members:
                if member not in lane["cohort"]:
                    lane["cohort"].append(member)

        outputs = {}
        for name, request in requests.items():
            is_metric = isinstance(request, Metric)
            # Match the resolver's dependency check, but never rebuild a file.
            graph = build_asset_graph(
                request, include_root=is_metric, file_assets_only=True,
                include_node_data=False,
            )
            cached = is_metric and all(not asset.is_update_needed() for asset in graph.nodes)
            outputs[name] = {
                "type": "metric" if is_metric else "table",
                "name": request.inputs["name"] if is_metric else request.table_name,
                "steps": list(request.selected_iterations),
                "columns": None if is_metric or request.columns is None else list(request.columns),
                "query": request.inputs.get("query") if is_metric else None,
                "cache": ("valid" if cached else "needs computation") if is_metric else "transient",
                "inputs": sorted({
                    getattr(asset, "table_name", asset.__class__.__name__)
                    for asset in request.planned_outputs
                }),
                "storage": "metric cache" if is_metric else "temporary files owned by the result",
            }
        needs_run = any(item["cache"] != "valid" for item in outputs.values())
        needs_simulation = any(
            outputs[name]["cache"] != "valid"
            and not SimulationRunner._uses_initial_state_only(request)
            for name, request in requests.items()
        )
        pending = [request for name, request in requests.items() if outputs[name]["cache"] != "valid"]
        for lane in lanes.values():
            final_costs, inner_flows = (
                SimulationRunner._final_requirements(pending, lane["configured_steps"])
                if needs_simulation else (False, False)
            )
            lane["final_costs"] = "planned" if final_costs else "not needed"
            lane["inner_flow_diagnostics"] = "planned" if inner_flows else "not needed"
            lane["computations"] = (
                [
                    "Prepare inputs and initial cohort state",
                    "Advance the cohort through configured steps, using valid checkpoints when available",
                ]
                if needs_simulation else []
            )
            lane["step_outputs"] = {
                step: [
                    name for name, request in requests.items()
                    if outputs[name]["cache"] != "valid"
                    and step in request.selected_iterations
                ]
                for step in sorted({
                    step for name, request in requests.items()
                    if outputs[name]["cache"] != "valid"
                    for step in request.selected_iterations
                })
            }
        return {
            "day_type": self.day_type,
            "lanes": list(lanes.values()),
            "outputs": outputs,
            "simulation": (
                "Required; valid checkpoints may supply saved steps. "
                "All configured steps and cohort members are included."
                if needs_simulation else (
                    "No simulation is required; requested inputs are available without advancing the cohort."
                    if needs_run else "No simulation is required for these cached outputs."
                )
            ),
            "temporary_storage": {
                "managed_bytes": managed_temporary_disk_usage(),
                "limit_bytes": temporary_disk_limit(),
                "note": (
                    "Result sizes are not estimated. Polars spill and some external-tool files "
                    "are not tracked. Existing path-cost caches are outside the temporary limit. "
                    "Path-cost and shared congestion outputs are checked after R finishes writing; "
                    "map frames are checked after rendering."
                ),
            },
        }

    @property
    def run_contexts(self) -> list[ResultRun]:
        """Return the concrete runs included in this result set."""
        if self._run_contexts is None:
            self._run_contexts = [
                ResultRun(
                    run=self._run(
                        self.day_type,
                        scenario=scenario,
                        sensitivity_case=sensitivity_case,
                        replication=replication,
                    ),
                    scenario=scenario,
                    sensitivity_case=sensitivity_case,
                    sensitivity_case_id=(
                        "base"
                        if sensitivity_case is None
                        else sensitivity_case.case_id
                    ),
                    day_type=self.day_type,
                    replication=replication,
                )
                for scenario in self.scenarios
                for sensitivity_case in self.sensitivity_cases
                for replication in self.replications
            ]
        return self._run_contexts

    @property
    def first_run(self):
        """Return the first concrete run in the result set."""
        return self.run_contexts[0].run

    def uses_last_iteration(self, iterations: IterationSelector = "last") -> bool:
        """Return whether a result query uses normal final run outputs."""
        parsed_iterations = self._validate_iterations(iterations)
        return parsed_iterations == "last"

    def selected_iterations(self, iterations: IterationSelector = "last") -> list[int]:
        """Return concrete iteration numbers for one result query."""
        parsed_iterations = self._validate_iterations(iterations)
        if parsed_iterations == "last":
            return [self.last_iteration]
        if parsed_iterations == "all":
            return list(range(1, self.last_iteration + 1))
        return list(parsed_iterations)

    @property
    def last_iteration(self) -> int:
        """Return the configured maximum outer iteration count without running the model."""
        try:
            return max(
                int(run_context.run.parameters.run.n_steps)
                for run_context in self.run_contexts
            )
        except AttributeError as exc:
            raise TypeError(
                "Iteration-scoped results need runs with parameters.run.n_steps."
            ) from exc

    @staticmethod
    def _last_iteration_for_run(run) -> int:
        """Return the number of outer states completed by one run context."""
        coordinated_run_asset = getattr(run, "coordinated_run_asset", None)
        if coordinated_run_asset is not None:
            if (
                hasattr(coordinated_run_asset, "_advance_cohort_step")
                and not getattr(coordinated_run_asset, "_is_running", False)
            ):
                return int(run.parameters.run.n_steps)
            result = coordinated_run_asset.get()
            return result.current_iteration
        return int(run.parameters.run.n_steps)

    def has_multiple_iterations(self, iterations: IterationSelector = "last") -> bool:
        """Return whether one result query contains several iterations."""
        return len(self.selected_iterations(iterations)) > 1

    @property
    def scenario_titles(self) -> dict[str, str]:
        """Return display titles for selected scenarios."""
        if self.scenario_manifest is None:
            return {}
        return {
            scenario: self.scenario_manifest.get(scenario).display_title
            for scenario in self.scenarios
        }

    @property
    def survey_reference_plan_steps(self):
        """Return the survey-weighted plan-step asset for this result scope."""
        try:
            return self.first_run._get_expected_diagnostics_inputs().survey_reference_plan_steps
        except AttributeError as exc:
            raise TypeError(
                "Survey diagnostics need runs that expose population-weighted survey plan steps."
            ) from exc

    @property
    def unsupported_survey_demand_mass(self) -> float:
        """Return demand weight in segments with no supporting survey observations."""
        return self.survey_reference_plan_steps.unsupported_demand_mass

    @property
    def transport_zones(self):
        """Return the transport-zone asset used by this result scope."""
        try:
            return self.first_run.population.transport_zones
        except AttributeError as exc:
            raise TypeError("Survey diagnostics need runs with a population transport-zone asset.") from exc

    @property
    def surveys(self):
        """Return the surveys used by this result scope."""
        try:
            return self.first_run.surveys
        except AttributeError as exc:
            raise TypeError("Survey-based metrics need runs that expose their surveys.") from exc

    @property
    def is_weekday(self) -> bool:
        """Return whether this result scope is for weekday runs."""
        return self.day_type == "weekday"

    def _validate_replications(
        self,
        *,
        replication: int | None,
        replications: list[int] | range | None,
    ) -> list[int]:
        """Return a validated list of replication indices."""
        if replication is not None:
            replications = [replication]
        if replications is None:
            replications = range(self.n_replications)

        selected_replications = [int(replication) for replication in replications]
        if len(selected_replications) != len(set(selected_replications)):
            raise ValueError("GroupDayTripsResults.replications should not contain duplicates.")
        invalid_replications = [
            replication
            for replication in selected_replications
            if replication < 0 or replication >= self.n_replications
        ]
        if invalid_replications:
            raise ValueError(
                "GroupDayTripsResults.replications should contain replication "
                "indices between 0 and n_replications - 1. "
                f"Received {invalid_replications} with "
                f"n_replications={self.n_replications}."
            )
        if not selected_replications:
            raise ValueError("GroupDayTripsResults needs at least one replication.")
        return selected_replications

    def _validate_scenarios(
        self,
        scenarios: str | list[str] | tuple[str, ...] | None,
    ) -> list[str]:
        """Return validated scenario names included in this result set."""
        if scenarios is None:
            selected_scenarios = [DEFAULT_SCENARIO]
        elif isinstance(scenarios, str):
            selected_scenarios = [scenarios]
        elif isinstance(scenarios, (list, tuple)):
            selected_scenarios = list(scenarios)
        else:
            raise TypeError(
                "GroupDayTripsResults.scenarios should be None, one scenario name, "
                "or a list of scenario names."
            )

        if not selected_scenarios:
            raise ValueError("GroupDayTripsResults needs at least one scenario.")
        if not all(isinstance(scenario, str) for scenario in selected_scenarios):
            raise TypeError("GroupDayTripsResults.scenarios should only contain scenario names.")

        duplicate_scenarios = sorted(
            scenario
            for scenario in set(selected_scenarios)
            if selected_scenarios.count(scenario) > 1
        )
        if duplicate_scenarios:
            raise ValueError(
                "GroupDayTripsResults.scenarios should not contain duplicate names. "
                f"Received duplicates: {duplicate_scenarios}."
            )

        return selected_scenarios

    def _validate_iterations(
        self,
        iterations: IterationSelector,
    ) -> str | list[int]:
        """Return a validated iteration selector."""
        if isinstance(iterations, str):
            if iterations in {"last", "all"}:
                return iterations
            raise ValueError(
                'steps should be "last", "all", one step number, '
                "or a list/range of step numbers."
            )
        if isinstance(iterations, int):
            selected_iterations = [iterations]
        elif isinstance(iterations, range):
            selected_iterations = list(iterations)
        elif isinstance(iterations, list):
            selected_iterations = list(iterations)
        else:
            raise TypeError(
                'steps should be "last", "all", one step number, '
                "or a list/range of step numbers."
            )

        if not selected_iterations:
            raise ValueError("GroupDayTripsResults needs at least one step.")
        if not all(type(iteration) is int for iteration in selected_iterations):
            raise TypeError("steps should only contain integer step numbers.")
        invalid_iterations = [
            iteration
            for iteration in selected_iterations
            if iteration < 1
        ]
        if invalid_iterations:
            raise ValueError(
                "steps should contain positive step numbers. "
                f"Received {invalid_iterations}."
            )
        duplicate_iterations = sorted(
            iteration
            for iteration in set(selected_iterations)
            if selected_iterations.count(iteration) > 1
        )
        if duplicate_iterations:
            raise ValueError(
                "steps should not contain duplicate values. "
                f"Received duplicates: {duplicate_iterations}."
            )
        unavailable = [
            iteration for iteration in selected_iterations
            if iteration > self.last_iteration
        ]
        if unavailable:
            raise ValueError(
                f"steps cannot exceed the configured maximum of {self.last_iteration}. "
                f"Received {unavailable}."
            )
        return selected_iterations
