from __future__ import annotations

from ..evaluation.iteration_metrics import IterationMetricsHistory
from .diagnostics import RunDiagnostics
from .metrics import RunMetrics
from .transitions import RunTransitions


class RunResults:
    """Run-scoped analysis root that exposes grouped metrics, transitions, and diagnostics."""

    def __init__(
        self,
        *,
        inputs_hash,
        is_weekday: bool,
        transport_zones,
        demand_groups,
        plan_steps,
        opportunities,
        costs,
        population_weighted_plan_steps,
        transitions,
        surveys,
        modes,
        parameters,
        run,
        expected_calibration_plan_steps=None,
        observed_calibration_plan_steps=None,
        iteration_metrics: IterationMetricsHistory | None = None,
        expected_entropy_plan_steps=None,
        observed_entropy_plan_steps=None,
        defer_run_tables: bool = False,
    ) -> None:
        """Store cached run outputs and expose grouped analysis helpers for one run."""
        self.inputs_hash = inputs_hash
        self.is_weekday = is_weekday
        self.transport_zones = transport_zones
        self._defer_run_tables = defer_run_tables
        if defer_run_tables:
            self._population_weighted_plan_steps_asset = population_weighted_plan_steps
        else:
            self.demand_groups = demand_groups
            self.plan_steps = plan_steps
            self.opportunities = opportunities
            self.costs = costs
            self.population_weighted_plan_steps = population_weighted_plan_steps
            self.transition_events = transitions
        self.surveys = surveys
        self.modes = modes
        self.parameters = parameters
        self.run = run
        self.expected_calibration_plan_steps = expected_calibration_plan_steps
        self.observed_calibration_plan_steps = observed_calibration_plan_steps
        if not defer_run_tables:
            self.iteration_metrics_store = iteration_metrics
        self.expected_entropy_plan_steps = expected_entropy_plan_steps
        self.observed_entropy_plan_steps = observed_entropy_plan_steps

        self.metrics = RunMetrics(self)
        self.transitions = RunTransitions(self)
        self.diagnostics = RunDiagnostics(self)

    def __getattr__(self, name):
        """Resolve a direct-run output only when an analysis asks for it."""
        if self.__dict__.get("_defer_run_tables", False):
            table_names = {
                "demand_groups": "demand_groups",
                "plan_steps": "plan_steps",
                "opportunities": "opportunities",
                "costs": "costs",
                "transition_events": "transitions",
            }
            if name in table_names:
                return self.run.table(table_names[name]).lazy()
            if name == "iteration_metrics_store":
                return IterationMetricsHistory(self.run.table("iteration_metrics").lazy())
            if name == "population_weighted_plan_steps":
                return self._population_weighted_plan_steps_asset.get()
        raise AttributeError(f"{type(self).__name__!s} has no attribute {name!r}")

    @property
    def period(self) -> str:
        """Return the string period label expected by plotting methods."""
        return "weekdays" if self.is_weekday else "weekends"
