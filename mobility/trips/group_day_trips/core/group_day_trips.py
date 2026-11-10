from enum import StrEnum
import logging
import warnings

from mobility.runtime.parameter_values import DEFAULT_SCENARIO, SensitivityCase
from mobility.runtime.scenarios import Scenarios
from mobility.transport.costs.transport_costs import TransportCosts
from ..results import GroupDayTripsResults
from ..sensitivity import (
    GroupDayTripsSensitivityAnalysis,
    collect_group_day_trips_sensitivity_cases,
)
from .parameters import GroupDayTripsParameters
from .run import Run
from .multi_scenario_run import MultiScenarioRunAsset
from mobility.activities import Activity, HomeActivity, OtherActivity
from mobility.surveys import SurveyPlanAssets, select_surveys_for_population
from mobility.surveys.mobility_survey import MobilitySurvey
from mobility.population import Population
from mobility.transport.modes.core.transport_mode import TransportMode


class DayType(StrEnum):
    """Day type supported by grouped day-trip runs."""

    WEEKDAY = "weekday"
    WEEKEND = "weekend"


class GroupDayTripsExperiment:
    """A fixed selection of scenarios, sensitivity cases and replications."""

    def __init__(self, setup, day_type, scenarios, sensitivity_cases, replication, replications):
        self.setup = setup
        self.day_type = DayType(day_type)
        self.parameters = setup.parameters.model_copy(deep=True) if hasattr(setup.parameters, "model_copy") else setup.parameters
        self._runs = {}
        self._coordinators = {}
        self._results = GroupDayTripsResults(
            run=self.run,
            day_type=self.day_type.value,
            scenarios=scenarios,
            scenario_manifest=setup.scenarios,
            sensitivity_cases=sensitivity_cases,
            n_replications=self.parameters.run.n_replications,
            replication=replication,
            replications=replications,
        )
        available = collect_group_day_trips_sensitivity_cases(setup)
        if len({"base" if case is None else case.case_id for case in available}) != len(available):
            raise ValueError("Sensitivity case IDs must be unique within an experiment.")
        if any(case not in available for case in self._results.sensitivity_cases):
            raise ValueError("The requested sensitivity case is not part of this experiment's model setup.")
        case_ids = ["base" if case is None else case.case_id for case in self._results.sensitivity_cases]
        if len(set(case_ids)) != len(case_ids):
            raise ValueError("Sensitivity cases must not be repeated in an experiment.")
        self.scenarios = tuple(self._results.scenarios)
        self.sensitivity_cases = tuple(self._results.sensitivity_cases)
        self.replications = tuple(self._results.replications)
        behavior = getattr(self.parameters, "behavior_change", None)
        for phase in getattr(behavior, "phases", None) or []:
            if phase.start_step > self.parameters.run.n_steps:
                warnings.warn(
                    f"Behavior phase {phase.scope.value!r} starts at step {phase.start_step}, "
                    f"after the final simulation step {self.parameters.run.n_steps}.",
                    stacklevel=3,
                )
        for change in setup.scenarios.changes:
            for scenario in self.scenarios:
                points = change.iteration_points_by_scenario.get(scenario, ())
                unreachable = [step for step in points if step > self.parameters.run.n_steps]
                if unreachable:
                    warnings.warn(
                        f"Scenario {scenario!r} changes {change.path} at steps {unreachable}, "
                        f"after the final simulation step {self.parameters.run.n_steps}.",
                        stacklevel=3,
                    )

    def describe(self):
        """Describe the selected scope without constructing simulation assets."""
        return {
            "day_type": self.day_type.value,
            "scenarios": list(self.scenarios),
            "sensitivity_cases": ["base" if case is None else case.case_id for case in self.sensitivity_cases],
            "replications": {i: self.parameters.run.seed_for_replication(i) for i in self.replications},
            "effort": self.parameters.effort,
            "parameters": self.parameters.model_dump(mode="python"),
        }

    def results(self, *, scenarios=None, sensitivity_cases=None, replications=None):
        """Return a result view without changing the experiment's cohort."""
        if scenarios is None and sensitivity_cases is None and replications is None:
            return self._results
        view = GroupDayTripsResults(
            run=self.run, day_type=self.day_type.value,
            scenarios=self.scenarios if scenarios is None else scenarios,
            scenario_manifest=self.setup.scenarios,
            sensitivity_cases=self.sensitivity_cases if sensitivity_cases is None else sensitivity_cases,
            n_replications=self.parameters.run.n_replications,
            replications=self.replications if replications is None else replications,
        )
        if (not set(view.scenarios).issubset(self.scenarios)
                or not set(view.replications).issubset(self.replications)
                or any(case not in self.sensitivity_cases for case in view.sensitivity_cases)):
            raise ValueError("A result view can only select members of its experiment.")
        return view

    def run(self, day_type, *, scenario=None, sensitivity_case=None, replication=0):
        """Return a member of this fixed experiment, building its lane on demand."""
        scenario = DEFAULT_SCENARIO if scenario is None else scenario
        if (DayType(day_type) != self.day_type or scenario not in self.scenarios
                or sensitivity_case not in self.sensitivity_cases
                or replication not in self.replications):
            raise ValueError("The requested run is not part of this experiment.")
        if replication not in self._coordinators:
            self._build_lane(replication)
        case_id = "base" if sensitivity_case is None else sensitivity_case.case_id
        return self._runs[(scenario, case_id, replication)]

    def _build_lane(self, replication):
        setup = self.setup
        scenarios = sorted(self.scenarios)
        cases = sorted(self.sensitivity_cases, key=lambda case: "" if case is None else case.case_id)
        # Include full case values, not just their names, in checkpoint identity.
        cohort = tuple((scenario, case) for case in cases for scenario in scenarios)
        logging.info(
            "Experiment lane | day_type=%s | scenarios=%s | sensitivity_cases=%s | replication=%s | seed=%s | steps=%s",
            self.day_type.value, scenarios,
            ["base" if case is None else case.case_id for case in cases], replication,
            self.parameters.run.seed_for_replication(replication), self.parameters.run.n_steps,
        )
        survey_assets = SurveyPlanAssets(surveys=setup.surveys, activities=setup.activities, modes=setup.modes)
        costs = TransportCosts(setup.modes)
        runs = {}
        for run_id, (scenario, case) in enumerate(cohort):
            parameters = self.parameters.with_replication(replication)
            run = Run(
                population=setup.population, transport_costs=costs, activities=setup.activities,
                modes=setup.modes, surveys=setup.surveys, survey_plan_assets=survey_assets,
                parameters=parameters, is_weekday=self.day_type is DayType.WEEKDAY,
                enabled=self.day_type is DayType.WEEKDAY or parameters.periods.simulate_weekend,
                scenario=scenario, sensitivity_case=case, replication=replication,
                cohort_identity=cohort,
            )
            run.coordinated_run_key = run_id
            case_id = "base" if case is None else case.case_id
            self._runs[(scenario, case_id, replication)] = run
            runs[run_id] = run
        coordinator = MultiScenarioRunAsset(runs)
        for run in runs.values():
            run.coordinated_run_asset = coordinator
        self._coordinators[replication] = coordinator


class PopulationGroupDayTrips:
    """Top-level setup for grouped day-trip simulations.

    This object stores the population, transport modes, activities, surveys,
    and grouped day-trip parameters. Each result request defines the scenarios
    and replications that share a candidate search. Model execution happens
    when the requested results or a run are resolved.
    """

    def __init__(
        self,
        population: Population,
        modes: list[TransportMode] | None = None,
        activities: list[Activity] | None = None,
        surveys: list[MobilitySurvey] | None = None,
        parameters: GroupDayTripsParameters | None = None,
        scenarios: Scenarios | None = None,
    ):
        """Create a grouped day-trips simulation setup.

        The setup checks that the required model inputs are present and keeps
        them together until a concrete run is requested. This keeps scenario
        values and replication seeds unresolved until `run(...)` selects the
        context to use. Omitting `scenario` selects the default scenario.

        Args:
            population: Population object containing demand groups and transport
                zones.
            modes: Available transport modes. Must contain at least one
                `TransportMode`.
            activities: Activities used to build and spatialize schedules.
                Must contain at least one `Activity` and include `HomeActivity`
                and `OtherActivity`.
            surveys: Mobility surveys providing empirical activity programs. Must
                contain at least one `MobilitySurvey`.
            parameters: Grouped day-trip settings. If omitted, default settings
                are used.
            scenarios: Optional scenario manifest used to document scenario names
                and validate scenario-varying parameter values. It is required
                when any `ParameterValue` uses a non-default scenario.

        Raises:
            ValueError: If modes, activities, or surveys are empty, or if required
                activities are missing.
            TypeError: If any element of modes, activities, or surveys has an
                incorrect type.
        """
        self._validate_modes(modes)
        self._validate_activities(activities)
        self._validate_surveys(surveys)

        if parameters is None:
            parameters = GroupDayTripsParameters()

        self.population = population
        self.modes = modes
        self.activities = activities
        self.surveys = select_surveys_for_population(population, surveys)
        self.parameters = parameters
        self.scenarios = Scenarios.for_setup(
            scenarios,
            modes=self.modes,
            activities=self.activities,
            parameters=self.parameters,
        )
        self._runs: dict[tuple[str, str, int, DayType], Run] = {}
        self._experiments: list[GroupDayTripsExperiment] = []

    def run(self, day_type, *, scenario=None, sensitivity_case=None, replication=0):
        """Return a run in a one-member experiment."""
        experiment = self.experiment(
            day_type, scenarios=[DEFAULT_SCENARIO if scenario is None else scenario],
            sensitivity_cases=[sensitivity_case], replication=replication,
        )
        return experiment.run(day_type, scenario=scenario, sensitivity_case=sensitivity_case, replication=replication)

    def experiment(
        self, day_type=DayType.WEEKDAY, *, scenarios=None, sensitivity_cases=None,
        replication=None, replications=None,
    ):
        """Define the contexts that share a candidate search for each seed."""
        experiment = GroupDayTripsExperiment(self, day_type, scenarios, sensitivity_cases, replication, replications)
        self._experiments.append(experiment)
        return experiment

    def results(
        self,
        day_type: str | DayType = DayType.WEEKDAY,
        *,
        scenarios: str | list[str] | tuple[str, ...] | None = None,
        sensitivity_cases: list[SensitivityCase | None] | None = None,
        replication: int | None = None,
        replications: list[int] | range | None = None,
    ) -> GroupDayTripsResults:
        """Return result helpers for one day type, scenario set, and replication set.

        Omitting `scenarios` selects the default scenario named `"default"`.
        Omitting `replication` and `replications` selects all stochastic
        replications declared in `parameters.run`.
        """
        return self.experiment(
            day_type, scenarios=scenarios, sensitivity_cases=sensitivity_cases,
            replication=replication, replications=replications,
        ).results()

    def sensitivity(
        self,
        *,
        scenarios: str | list[str] | tuple[str, ...] | None = None,
    ):
        """Return sensitivity-analysis helpers for selected scenarios."""
        self.scenarios.validate_requested(Scenarios.selected_names(scenarios))
        return GroupDayTripsSensitivityAnalysis(
            setup=self,
            scenarios=Scenarios.selected_names(scenarios),
        )

    def remove(self):
        """Remove cached run outputs for this setup.

        This removes all runs that can be inferred from the setup: the default
        scenario, scenarios found in parameter values, configured replications,
        enabled day types, and any runs already built in this Python session.
        """
        for experiment in self._experiments:
            for run in experiment._runs.values():
                run.remove()
        scenarios = {DEFAULT_SCENARIO}
        scenarios.update(self.scenarios.names)
        scenarios.update(scenario for scenario, _, _, _ in self._runs)

        replications = set(range(self.parameters.run.n_replications))
        replications.update(replication for _, _, replication, _ in self._runs)

        sensitivity_cases = [None]
        for run in self._runs.values():
            sensitivity_case = getattr(run, "sensitivity_case", None)
            if sensitivity_case is not None and sensitivity_case not in sensitivity_cases:
                sensitivity_cases.append(sensitivity_case)

        day_types = {DayType.WEEKDAY}
        if self.parameters.periods.simulate_weekend:
            day_types.add(DayType.WEEKEND)
        day_types.update(day_type for _, _, _, day_type in self._runs)

        for scenario in scenarios:
            for sensitivity_case in sensitivity_cases:
                for replication in replications:
                    for day_type in day_types:
                        self.run(
                            day_type,
                            scenario=scenario,
                            sensitivity_case=sensitivity_case,
                            replication=replication,
                        ).remove()

        self._runs = {}
        self._experiments = []

    def _validate_activities(self, activities: list[Activity] | None) -> None:
        """Check that activity inputs are present and usable."""
        if not activities:
            raise ValueError("PopulationGroupDayTrips needs at least one activity in `activities`.")

        for activity in activities:
            if not isinstance(activity, Activity):
                raise TypeError(
                    "PopulationGroupDayTrips activities argument should be a list of `Activity` "
                    f"instances, but received one object of class {type(activity)}."
                )

        if not any(isinstance(a, OtherActivity) for a in activities):
            raise ValueError("PopulationGroupDayTrips `activities` argument should contain an `OtherActivity`.")

        if not any(isinstance(a, HomeActivity) for a in activities):
            raise ValueError("PopulationGroupDayTrips `activities` argument should contain a `HomeActivity`.")

    def _validate_modes(self, modes: list[TransportMode] | None) -> None:
        """Check that transport mode inputs are present and usable."""
        if not modes:
            raise ValueError("PopulationGroupDayTrips needs at least one mode in `modes`.")

        for mode in modes:
            if not isinstance(mode, TransportMode):
                raise TypeError(
                    "PopulationGroupDayTrips modes argument should be a list of `TransportMode` "
                    f"instances, but received one object of class {type(mode)}."
                )

    def _validate_surveys(self, surveys: list[MobilitySurvey] | None) -> None:
        """Check that survey inputs are present and usable."""
        if not surveys:
            raise ValueError("PopulationGroupDayTrips needs at least one survey in `surveys`.")

        for survey in surveys:
            if not isinstance(survey, MobilitySurvey):
                raise TypeError(
                    "PopulationGroupDayTrips surveys argument should be a list of `MobilitySurvey` "
                    f"instances, but received one object of class {type(survey)}."
                )
