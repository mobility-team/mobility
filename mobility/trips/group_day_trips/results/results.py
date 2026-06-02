from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from mobility.runtime.parameter_values import DEFAULT_SCENARIO
from mobility.runtime.scenarios import Scenarios

from .diagnostics import GroupDayTripsResultDiagnostics
from .metrics import GroupDayTripsResultMetrics
from .tables import GroupDayTripsResultTables

IterationSelector = str | int | list[int] | range


@dataclass(frozen=True)
class ResultRun:
    """One concrete run included in a result set."""

    run: object
    scenario: str
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
        self.replications = self._validate_replications(
            replication=replication,
            replications=replications,
        )
        self._run_contexts: list[ResultRun] | None = None

        self.tables = GroupDayTripsResultTables(self)
        self.metrics = GroupDayTripsResultMetrics(self)
        self.diagnostics = GroupDayTripsResultDiagnostics(self)

    @property
    def run_contexts(self) -> list[ResultRun]:
        """Return the concrete runs included in this result set."""
        if self._run_contexts is None:
            self._run_contexts = [
                ResultRun(
                    run=self._run(
                        self.day_type,
                        scenario=scenario,
                        replication=replication,
                    ),
                    scenario=scenario,
                    day_type=self.day_type,
                    replication=replication,
                )
                for scenario in self.scenarios
                for replication in self.replications
            ]
        return self._run_contexts

    @property
    def first_run(self):
        """Return the first concrete run in the result set."""
        return self.run_contexts[0].run

    def uses_last_iteration(self, iterations: IterationSelector = "last") -> bool:
        """Return whether a result query uses normal final run outputs."""
        return self._validate_iterations(iterations) == "last"

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
        """Return the configured last model iteration."""
        try:
            return int(self.first_run.parameters.run.n_iterations)
        except AttributeError as exc:
            raise TypeError(
                "Iteration-scoped results need runs with parameters.run.n_iterations."
            ) from exc

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
            return self.first_run._get_expected_diagnostics_inputs().population_weighted_plan_steps
        except AttributeError as exc:
            raise TypeError(
                "Survey diagnostics need runs that expose population-weighted survey plan steps."
            ) from exc

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
                'iterations should be "last", "all", one iteration number, '
                "or a list/range of iteration numbers."
            )
        if isinstance(iterations, int):
            selected_iterations = [iterations]
        elif isinstance(iterations, range):
            selected_iterations = list(iterations)
        elif isinstance(iterations, list):
            selected_iterations = list(iterations)
        else:
            raise TypeError(
                'iterations should be "last", "all", one iteration number, '
                "or a list/range of iteration numbers."
            )

        if not selected_iterations:
            raise ValueError("GroupDayTripsResults needs at least one iteration.")
        if not all(isinstance(iteration, int) for iteration in selected_iterations):
            raise TypeError("iterations should only contain integer iteration numbers.")
        invalid_iterations = [
            iteration
            for iteration in selected_iterations
            if iteration < 1
        ]
        if invalid_iterations:
            raise ValueError(
                "iterations should contain positive iteration numbers. "
                f"Received {invalid_iterations}."
            )
        duplicate_iterations = sorted(
            iteration
            for iteration in set(selected_iterations)
            if selected_iterations.count(iteration) > 1
        )
        if duplicate_iterations:
            raise ValueError(
                "iterations should not contain duplicate values. "
                f"Received duplicates: {duplicate_iterations}."
            )
        return selected_iterations
