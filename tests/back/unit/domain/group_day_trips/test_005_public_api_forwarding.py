import warnings
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from mobility.runtime.parameter_values import DEFAULT_SCENARIO, ParameterValue
from mobility.runtime.scenarios import Scenario, Scenarios
from mobility.trips.group_day_trips import BehaviorChangePhase, BehaviorChangeScope, PopulationGroupDayTrips
from mobility.trips.group_day_trips.core.parameters import (
    GroupDayTripsActivitySequenceParameters,
    GroupDayTripsBehaviorChangeParameters,
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsModeSequenceParameters,
    GroupDayTripsParameters,
    GroupDayTripsPeriodParameters,
    GroupDayTripsPlanUpdateParameters,
    GroupDayTripsRunParameters,
)
from mobility.trips.group_day_trips.core import group_day_trips as group_day_trips_module


class _FakeRun:
    removed_runs = []

    def __init__(self, *, parameters, is_weekday, enabled, survey_plan_assets, scenario=None, **kwargs):
        self.parameters = parameters
        self.is_weekday = is_weekday
        self.enabled = enabled
        self.survey_plan_assets = survey_plan_assets
        self.scenario = scenario
        self.replication = kwargs.get("replication")
        base = Path("cache") / ("weekday" if is_weekday else "weekend")
        self.cache_path = {
            "plan_steps": base / "plan_steps.parquet",
            "opportunities": base / "opportunities.parquet",
            "costs": base / "costs.parquet",
            "chains": base / "chains.parquet",
            "transitions": base / "transitions.parquet",
            "demand_groups": base / "demand_groups.parquet",
        }

    def remove(self):
        self.removed_runs.append((self.scenario, self.parameters.run.seed, self.is_weekday))


class _FakeSurveyPlanAssets:
    def __init__(self, *, surveys, activities, modes):
        self.surveys = surveys
        self.activities = activities
        self.modes = modes


class _FakeSurvey:
    def __init__(self, country: str):
        self.inputs = {"parameters": SimpleNamespace(country=country)}


def _patch_group_day_trips_wrapper(monkeypatch):
    """Replace heavy grouped day-trip dependencies with small test doubles."""
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_modes", lambda self, modes: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_activities", lambda self, activities: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_surveys", lambda self, surveys: None)
    monkeypatch.setattr(group_day_trips_module, "Run", _FakeRun)
    monkeypatch.setattr(group_day_trips_module, "TransportCosts", lambda modes: SimpleNamespace(modes=modes))
    monkeypatch.setattr(group_day_trips_module, "SurveyPlanAssets", _FakeSurveyPlanAssets)


def _make_population(*countries: str):
    rows = [{"local_admin_unit_id": f"{country}001"} for country in countries]
    study_area = SimpleNamespace(get=lambda: pd.DataFrame(rows))
    transport_zones = SimpleNamespace(
        study_area=study_area,
        get_study_area_countries=lambda: sorted(countries),
    )
    return SimpleNamespace(transport_zones=transport_zones)


def test_group_day_trips_wrapper_uses_explicit_nested_parameters(monkeypatch):
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_modes", lambda self, modes: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_activities", lambda self, activities: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_surveys", lambda self, surveys: None)
    monkeypatch.setattr(group_day_trips_module, "Run", _FakeRun)
    monkeypatch.setattr(group_day_trips_module, "TransportCosts", lambda modes: SimpleNamespace(modes=modes))
    monkeypatch.setattr(group_day_trips_module, "SurveyPlanAssets", _FakeSurveyPlanAssets)

    wrapper = PopulationGroupDayTrips(
        population=_make_population("fr"),
        modes=[object()],
        activities=[object()],
        surveys=[_FakeSurvey("fr")],
        parameters=GroupDayTripsParameters(
            run=GroupDayTripsRunParameters(
                n_iterations=4,
                n_replications=4,
                seeds=[0, 1, 3, 4],
            ),
            periods=GroupDayTripsPeriodParameters(simulate_weekend=False),
            behavior_change=GroupDayTripsBehaviorChangeParameters(
                phases=[
                    BehaviorChangePhase(start_iteration=2, scope=BehaviorChangeScope.MODE_REPLANNING),
                ],
            ),
            activity_sequences=GroupDayTripsActivitySequenceParameters(k_activity_sequences=5),
            destination_sequences=GroupDayTripsDestinationSequenceParameters(
                k_destination_sequences=6,
                refresh_active_mode_alternatives=True,
            ),
            mode_sequences=GroupDayTripsModeSequenceParameters(use_rust_mode_sequence_search=True),
            plan_update=GroupDayTripsPlanUpdateParameters(
                n_warmup_iterations=2,
                max_inactive_age=3,
                transition_revision_probability=0.4,
                transition_logit_scale=0.75,
                use_destination_shadow_prices=True,
                min_transition_utility_gain=0.1,
                plan_probability_pruning_retained_share=0.995,
                plan_probability_pruning_min_iteration=3,
                enable_transition_distance_model=True,
                transition_distance_threshold=8.0,
                transition_distance_friction=1.5,
                plan_embedding_dimension_weights=[1.0, 2.0, 3.0],
            ),
        ),
    )

    weekday_run = wrapper.run("weekday", replication=2)
    weekend_run = wrapper.run("weekend", replication=2)
    parameters = weekday_run.parameters

    assert parameters.run.n_iterations == 4
    assert parameters.activity_sequences.k_activity_sequences == 5
    assert parameters.destination_sequences.k_destination_sequences == 6
    assert parameters.plan_update.n_warmup_iterations == 2
    assert parameters.plan_update.max_inactive_age == 3
    assert parameters.destination_sequences.refresh_active_mode_alternatives is True
    assert parameters.plan_update.transition_revision_probability == 0.4
    assert parameters.plan_update.transition_logit_scale == 0.75
    assert parameters.plan_update.use_destination_shadow_prices is True
    assert parameters.plan_update.min_transition_utility_gain == 0.1
    assert parameters.plan_update.plan_probability_pruning_retained_share == 0.995
    assert parameters.plan_update.plan_probability_pruning_min_iteration == 3
    assert parameters.run.n_replications == 1
    assert parameters.run.seeds is None
    assert parameters.mode_sequences.use_rust_mode_sequence_search is True
    assert parameters.plan_update.enable_transition_distance_model is True
    assert parameters.plan_update.transition_distance_threshold == 8.0
    assert parameters.plan_update.transition_distance_friction == 1.5
    assert parameters.plan_update.plan_embedding_dimension_weights == [1.0, 2.0, 3.0]
    assert parameters.behavior_change.phases == [
        BehaviorChangePhase(start_iteration=2, scope=BehaviorChangeScope.MODE_REPLANNING)
    ]
    assert [survey.inputs["parameters"].country for survey in weekday_run.survey_plan_assets.surveys] == ["fr"]
    assert [survey.inputs["parameters"].country for survey in weekend_run.survey_plan_assets.surveys] == ["fr"]
    assert weekend_run.enabled is False
    assert weekday_run.replication == 2
    assert weekend_run.replication == 2
    assert "iteration_metrics" not in weekday_run.cache_path
    assert "iteration_metrics" not in weekend_run.cache_path


def test_group_day_trips_wrapper_raises_when_population_country_has_no_matching_survey(monkeypatch):
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_modes", lambda self, modes: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_activities", lambda self, activities: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_surveys", lambda self, surveys: None)
    monkeypatch.setattr(group_day_trips_module, "Run", _FakeRun)
    monkeypatch.setattr(group_day_trips_module, "TransportCosts", lambda modes: SimpleNamespace(modes=modes))
    monkeypatch.setattr(group_day_trips_module, "SurveyPlanAssets", _FakeSurveyPlanAssets)

    with pytest.raises(ValueError, match="Missing survey coverage for: be"):
        PopulationGroupDayTrips(
            population=_make_population("fr", "be"),
            modes=[object()],
            activities=[object()],
            surveys=[_FakeSurvey("fr")],
        )


def test_group_day_trips_wrapper_warns_when_some_surveys_will_not_be_used(monkeypatch):
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_modes", lambda self, modes: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_activities", lambda self, activities: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_surveys", lambda self, surveys: None)
    monkeypatch.setattr(group_day_trips_module, "Run", _FakeRun)
    monkeypatch.setattr(group_day_trips_module, "TransportCosts", lambda modes: SimpleNamespace(modes=modes))
    monkeypatch.setattr(group_day_trips_module, "SurveyPlanAssets", _FakeSurveyPlanAssets)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        wrapper = PopulationGroupDayTrips(
            population=_make_population("fr"),
            modes=[object()],
            activities=[object()],
            surveys=[_FakeSurvey("fr"), _FakeSurvey("be")],
        )

    assert len(caught) == 1
    assert "will not be used" in str(caught[0].message)
    weekday_run = wrapper.run("weekday")
    assert [survey.inputs["parameters"].country for survey in weekday_run.survey_plan_assets.surveys] == ["fr"]


def test_group_day_trips_wrapper_exposes_iteration_metrics_when_child_runs_provide_them(monkeypatch):
    class _FakeRunWithIterationMetrics(_FakeRun):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.cache_path["iteration_metrics"] = self.cache_path["plan_steps"].with_name("iteration_metrics.parquet")

    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_modes", lambda self, modes: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_activities", lambda self, activities: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_surveys", lambda self, surveys: None)
    monkeypatch.setattr(group_day_trips_module, "Run", _FakeRunWithIterationMetrics)
    monkeypatch.setattr(group_day_trips_module, "TransportCosts", lambda modes: SimpleNamespace(modes=modes))
    monkeypatch.setattr(group_day_trips_module, "SurveyPlanAssets", _FakeSurveyPlanAssets)

    wrapper = PopulationGroupDayTrips(
        population=_make_population("fr"),
        modes=[object()],
        activities=[object()],
        surveys=[_FakeSurvey("fr")],
        parameters=GroupDayTripsParameters(
            periods=GroupDayTripsPeriodParameters(simulate_weekend=True),
        ),
    )

    weekday_run = wrapper.run("weekday")
    weekend_run = wrapper.run("weekend")

    assert "iteration_metrics" in weekday_run.cache_path
    assert "iteration_metrics" in weekend_run.cache_path


def test_parameters_seed_for_replication_uses_default_run_index_seeds():
    parameters = GroupDayTripsRunParameters(n_replications=3)

    assert parameters.seed_for_replication(0) == 0
    assert parameters.seed_for_replication(1) == 1
    assert parameters.seed_for_replication(2) == 2


def test_parameters_seed_for_replication_uses_explicit_seeds():
    parameters = GroupDayTripsRunParameters(n_replications=4, seeds=[0, 1, 3, 4])

    assert parameters.seed_for_replication(0) == 0
    assert parameters.seed_for_replication(2) == 3
    assert parameters.seed_for_replication(3) == 4


def test_parameters_for_replication_returns_single_replication_parameters():
    parameters = GroupDayTripsRunParameters(n_replications=4, seeds=[0, 1, 3, 4])

    replication_parameters = parameters.with_replication(2)

    assert replication_parameters.seed == 3
    assert replication_parameters.n_replications == 1
    assert replication_parameters.seeds is None


def test_parameters_raise_when_replication_seed_count_does_not_match():
    with pytest.raises(ValueError, match="one seed per replication"):
        GroupDayTripsRunParameters(n_replications=2, seeds=[0])


def test_scenarios_manifest_adds_default_and_checks_names():
    scenarios = Scenarios([
        Scenario(
            name="saleve_jura",
            title="Saleve-Jura RER",
            description="Adds the Saleve-Jura RER service.",
            reference="default",
        )
    ])

    assert scenarios.names == ["default", "saleve_jura"]
    assert scenarios.get("saleve_jura").display_title == "Saleve-Jura RER"

    with pytest.raises(ValueError, match="duplicate names"):
        Scenarios([Scenario(name="project"), Scenario(name="project")])

    with pytest.raises(TypeError, match="list of Scenario"):
        Scenarios(Scenario(name="project"))

    with pytest.raises(ValueError, match="Missing references"):
        Scenarios([Scenario(name="project", reference="missing")])


def test_group_day_trips_documents_scenarios_and_changes(monkeypatch):
    _patch_group_day_trips_wrapper(monkeypatch)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        wrapper = PopulationGroupDayTrips(
            population=_make_population("fr"),
            modes=[
                ParameterValue.by_scenario_and_iteration(
                    default="base",
                    saleve_jura={
                        1: "base",
                        5: "project",
                    },
                )
            ],
            activities=[object()],
            surveys=[_FakeSurvey("fr")],
            parameters=GroupDayTripsParameters(
                run=GroupDayTripsRunParameters(n_iterations=4),
            ),
            scenarios=Scenarios([
                Scenario(
                    name="saleve_jura",
                    title="Saleve-Jura RER",
                    description="Adds the Saleve-Jura RER service from iteration 5.",
                    reference="default",
                )
            ]),
        )

    assert wrapper.scenarios.names == ["default", "saleve_jura"]
    assert wrapper.scenarios.changes[0].path == "setup['modes'][0]"
    assert "Scenario 'saleve_jura' changes setup['modes'][0] at iterations [5]" in str(
        caught[0].message
    )

    description = wrapper.scenarios.describe()

    assert "saleve_jura" in description
    assert "Title: Saleve-Jura RER" in description
    assert "Reference: default" in description
    assert "- setup['modes'][0]: iterations 1, 5" in description


def test_group_day_trips_rejects_undeclared_parameter_scenario(monkeypatch):
    _patch_group_day_trips_wrapper(monkeypatch)

    with pytest.raises(ValueError, match="pass it as `scenarios=`"):
        PopulationGroupDayTrips(
            population=_make_population("fr"),
            modes=[ParameterValue.by_scenario(default="base", saleve_jura="project")],
            activities=[object()],
            surveys=[_FakeSurvey("fr")],
        )

    with pytest.raises(ValueError, match="Missing declarations: \\['saleve_jura'\\]"):
        PopulationGroupDayTrips(
            population=_make_population("fr"),
            modes=[ParameterValue.by_scenario(default="base", saleve_jura="project")],
            activities=[object()],
            surveys=[_FakeSurvey("fr")],
            scenarios=Scenarios([Scenario(name="default")]),
        )


def test_group_day_trips_warns_when_declared_scenario_changes_nothing(monkeypatch):
    _patch_group_day_trips_wrapper(monkeypatch)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        PopulationGroupDayTrips(
            population=_make_population("fr"),
            modes=[object()],
            activities=[object()],
            surveys=[_FakeSurvey("fr")],
            scenarios=Scenarios([Scenario(name="unused_project")]),
        )

    assert "do not change any ParameterValue" in str(caught[0].message)
    assert "unused_project" in str(caught[0].message)


def test_group_day_trips_rejects_unknown_requested_scenario(monkeypatch):
    _patch_group_day_trips_wrapper(monkeypatch)
    wrapper = PopulationGroupDayTrips(
        population=_make_population("fr"),
        modes=[object()],
        activities=[object()],
        surveys=[_FakeSurvey("fr")],
    )

    with pytest.raises(ValueError, match="Missing scenarios: \\['typo'\\]"):
        wrapper.run("weekday", scenario="typo")


def test_group_day_trips_run_can_select_scenario_and_replication(monkeypatch):
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_modes", lambda self, modes: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_activities", lambda self, activities: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_surveys", lambda self, surveys: None)
    monkeypatch.setattr(group_day_trips_module, "Run", _FakeRun)
    monkeypatch.setattr(group_day_trips_module, "TransportCosts", lambda modes: SimpleNamespace(modes=modes))
    monkeypatch.setattr(group_day_trips_module, "SurveyPlanAssets", _FakeSurveyPlanAssets)

    wrapper = PopulationGroupDayTrips(
        population=_make_population("fr"),
        modes=[ParameterValue.by_scenario(baseline="base", saleve_jura="project")],
        activities=[object()],
        surveys=[_FakeSurvey("fr")],
        scenarios=Scenarios([
            Scenario(name="baseline"),
            Scenario(name="saleve_jura"),
        ]),
        parameters=GroupDayTripsParameters(
            run=GroupDayTripsRunParameters(n_replications=4, seeds=[0, 1, 3, 4]),
        ),
    )

    default_run = wrapper.run("weekday", replication=2)

    weekday_run = wrapper.run("weekday", scenario="saleve_jura", replication=2)

    assert default_run.scenario == DEFAULT_SCENARIO
    assert weekday_run.parameters.run.seed == 3
    assert weekday_run.parameters.run.n_replications == 1
    assert weekday_run.parameters.run.seeds is None
    assert weekday_run.scenario == "saleve_jura"


def test_group_day_trips_remove_clears_all_known_scenario_replications(monkeypatch):
    _FakeRun.removed_runs = []
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_modes", lambda self, modes: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_activities", lambda self, activities: None)
    monkeypatch.setattr(PopulationGroupDayTrips, "_validate_surveys", lambda self, surveys: None)
    monkeypatch.setattr(group_day_trips_module, "Run", _FakeRun)
    monkeypatch.setattr(group_day_trips_module, "TransportCosts", lambda modes: SimpleNamespace(modes=modes))
    monkeypatch.setattr(group_day_trips_module, "SurveyPlanAssets", _FakeSurveyPlanAssets)

    wrapper = PopulationGroupDayTrips(
        population=_make_population("fr"),
        modes=[ParameterValue.by_scenario(default="base", saleve_jura="project")],
        activities=[object()],
        surveys=[_FakeSurvey("fr")],
        scenarios=Scenarios([Scenario(name="saleve_jura")]),
        parameters=GroupDayTripsParameters(
            run=GroupDayTripsRunParameters(n_replications=2, seeds=[0, 10]),
            periods=GroupDayTripsPeriodParameters(simulate_weekend=True),
        ),
    )

    wrapper.remove()

    assert set(_FakeRun.removed_runs) == {
        ("default", 0, True),
        ("default", 0, False),
        ("default", 10, True),
        ("default", 10, False),
        ("saleve_jura", 0, True),
        ("saleve_jura", 0, False),
        ("saleve_jura", 10, True),
        ("saleve_jura", 10, False),
    }
