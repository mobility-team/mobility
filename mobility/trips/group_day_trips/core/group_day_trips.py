from enum import StrEnum

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


class PopulationGroupDayTrips:
    """Top-level setup for grouped day-trip simulations.

    This object stores the population, transport modes, activities, surveys,
    and grouped day-trip parameters. The first run request creates one
    coordinated weekday or weekend run for every configured scenario. Model
    execution happens through any returned scenario view, for example with
    `population_trips.run("weekday").get()`. If no scenario is given, the
    returned view uses the default scenario named `"default"`.
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
        self._coordinated_runs: dict[DayType, MultiScenarioRunAsset] = {}

    def run(
        self,
        day_type: str | DayType,
        *,
        scenario: str | None = None,
        sensitivity_case: SensitivityCase | None = None,
        replication: int = 0,
    ) -> Run:
        """Return one scenario view of a coordinated weekday or weekend run.

        The first request builds all configured scenarios for the same day type,
        sensitivity case, and replication. Calling `get()` on any returned view
        runs that full scenario cohort together. The view selects which
        scenario's state and outputs are returned. Omitting `scenario` selects
        the default scenario named `"default"`.

        Args:
            day_type: `"weekday"` or `"weekend"`.
            scenario: Scenario name to resolve scenario-varying values. If
                omitted, `"default"` is used.
            replication: Stochastic replication index. Replication seeds are
                taken from `parameters.run`.

        Returns:
            Run: Concrete run for the selected day type and context.
        """
        day_type = DayType(day_type)
        scenario = DEFAULT_SCENARIO if scenario is None else scenario
        sensitivity_case_id = "base" if sensitivity_case is None else sensitivity_case.case_id
        self.scenarios.validate_requested([scenario])
        key = (scenario, sensitivity_case_id, replication, day_type)
        if day_type not in self._coordinated_runs:
            survey_plan_assets = SurveyPlanAssets(
                surveys=self.surveys,
                activities=self.activities,
                modes=self.modes,
            )
            transport_costs = TransportCosts(self.modes)
            is_weekday = day_type is DayType.WEEKDAY
            sensitivity_cases = collect_group_day_trips_sensitivity_cases(self)
            if sensitivity_case not in sensitivity_cases:
                sensitivity_cases.append(sensitivity_case)

            cohort_runs = {}
            next_run_key = 0
            for cohort_case in sensitivity_cases:
                cohort_case_id = "base" if cohort_case is None else cohort_case.case_id
                for cohort_replication in range(self.parameters.run.n_replications):
                    parameters = self.parameters.with_replication(cohort_replication)
                    for scenario_name in self.scenarios.names:
                        run_key = (
                            scenario_name,
                            cohort_case_id,
                            cohort_replication,
                            day_type,
                        )
                        if run_key not in self._runs:
                            self._runs[run_key] = Run(
                                population=self.population,
                                transport_costs=transport_costs,
                                activities=self.activities,
                                modes=self.modes,
                                surveys=self.surveys,
                                survey_plan_assets=survey_plan_assets,
                                parameters=parameters,
                                is_weekday=is_weekday,
                                enabled=is_weekday or parameters.periods.simulate_weekend,
                                scenario=scenario_name,
                                sensitivity_case=cohort_case,
                                replication=cohort_replication,
                            )
                        run = self._runs[run_key]
                        run.coordinated_run_key = next_run_key
                        cohort_runs[next_run_key] = run
                        next_run_key += 1

            coordinated_run = MultiScenarioRunAsset(cohort_runs)
            self._coordinated_runs[day_type] = coordinated_run
            for cohort_run in cohort_runs.values():
                cohort_run.coordinated_run_asset = coordinated_run
        return self._runs[key]

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
        day_type = DayType(day_type)
        self.scenarios.validate_requested(Scenarios.selected_names(scenarios))
        return GroupDayTripsResults(
            run=self.run,
            day_type=day_type.value,
            scenarios=scenarios,
            scenario_manifest=self.scenarios,
            sensitivity_cases=sensitivity_cases,
            n_replications=self.parameters.run.n_replications,
            replication=replication,
            replications=replications,
        )

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
        self._coordinated_runs = {}

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
