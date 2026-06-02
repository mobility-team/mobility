from enum import StrEnum

from mobility.runtime.parameter_values import DEFAULT_SCENARIO
from mobility.runtime.scenarios import Scenarios
from mobility.transport.costs.transport_costs import TransportCosts
from .parameters import GroupDayTripsParameters
from .run import Run
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
    and grouped day-trip parameters. It creates concrete weekday and weekend
    runs on demand when a day type, scenario, and stochastic replication are
    selected. Model execution happens through the returned run, for example
    with `population_trips.run("weekday").get()`. If no scenario is given, the
    setup uses the package default scenario named `"default"`.
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
        self._runs: dict[tuple[str, int, DayType], Run] = {}

    def run(
        self,
        day_type: str | DayType,
        *,
        scenario: str | None = None,
        replication: int = 0,
    ) -> Run:
        """Return one weekday or weekend run.

        The run is built on first access, then reused if the same `day_type`,
        `scenario`, and `replication` are requested again. Omitting `scenario`
        selects the default scenario named `"default"`. Plain parameter values
        are shared by all scenarios. Scenario-varying `ParameterValue` objects
        must define the requested scenario.

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
        self.scenarios.validate_requested([scenario])
        key = (scenario, replication, day_type)
        if key not in self._runs:
            parameters = self.parameters.with_replication(replication)
            survey_plan_assets = SurveyPlanAssets(
                surveys=self.surveys,
                activities=self.activities,
                modes=self.modes,
            )
            transport_costs = TransportCosts(self.modes)
            is_weekday = day_type is DayType.WEEKDAY
            self._runs[key] = Run(
                population=self.population,
                transport_costs=transport_costs,
                activities=self.activities,
                modes=self.modes,
                surveys=self.surveys,
                survey_plan_assets=survey_plan_assets,
                parameters=parameters,
                is_weekday=is_weekday,
                enabled=is_weekday or parameters.periods.simulate_weekend,
                scenario=scenario,
            )
        return self._runs[key]

    def remove(self):
        """Remove cached run outputs for this setup.

        This removes all runs that can be inferred from the setup: the default
        scenario, scenarios found in parameter values, configured replications,
        enabled day types, and any runs already built in this Python session.
        """
        scenarios = {DEFAULT_SCENARIO}
        scenarios.update(self.scenarios.names)
        scenarios.update(scenario for scenario, _, _ in self._runs)

        replications = set(range(self.parameters.run.n_replications))
        replications.update(replication for _, replication, _ in self._runs)

        day_types = {DayType.WEEKDAY}
        if self.parameters.periods.simulate_weekend:
            day_types.add(DayType.WEEKEND)
        day_types.update(day_type for _, _, day_type in self._runs)

        for scenario in scenarios:
            for replication in replications:
                for day_type in day_types:
                    self.run(
                        day_type,
                        scenario=scenario,
                        replication=replication,
                    ).remove()

        self._runs = {}

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
