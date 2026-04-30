import warnings
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from mobility.trips.group_day_trips import BehaviorChangePhase, BehaviorChangeScope, PopulationGroupDayTrips
from mobility.trips.group_day_trips.core import group_day_trips as group_day_trips_module


class _FakeRun:
    def __init__(self, *, parameters, is_weekday, enabled, survey_plan_assets, **kwargs):
        self.parameters = parameters
        self.is_weekday = is_weekday
        self.enabled = enabled
        self.survey_plan_assets = survey_plan_assets
        base = Path("cache") / ("weekday" if is_weekday else "weekend")
        self.cache_path = {
            "plan_steps": base / "plan_steps.parquet",
            "opportunities": base / "opportunities.parquet",
            "costs": base / "costs.parquet",
            "chains": base / "chains.parquet",
            "transitions": base / "transitions.parquet",
            "demand_groups": base / "demand_groups.parquet",
        }


class _FakeSurveyPlanAssets:
    def __init__(self, *, surveys, activities, modes):
        self.surveys = surveys
        self.activities = activities
        self.modes = modes


class _FakeSurvey:
    def __init__(self, country: str):
        self.inputs = {"parameters": SimpleNamespace(country=country)}


def _make_population(*countries: str):
    rows = [{"local_admin_unit_id": f"{country}001"} for country in countries]
    study_area = SimpleNamespace(get=lambda: pd.DataFrame(rows))
    transport_zones = SimpleNamespace(study_area=study_area)
    return SimpleNamespace(transport_zones=transport_zones)


def test_group_day_trips_wrapper_forwards_new_parameters(monkeypatch):
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
        n_iterations=4,
        k_activity_sequences=5,
        k_destination_sequences=6,
        n_warmup_iterations=2,
        max_inactive_age=3,
        transition_revision_probability=0.4,
        transition_logit_scale=0.75,
        enable_transition_distance_model=True,
        transition_distance_threshold=8.0,
        transition_distance_friction=1.5,
        plan_embedding_dimension_weights=[1.0, 2.0, 3.0],
        behavior_change_phases=[
            BehaviorChangePhase(start_iteration=2, scope=BehaviorChangeScope.MODE_REPLANNING),
        ],
        simulate_weekend=False,
    )

    parameters = wrapper.weekday_run.parameters

    assert parameters.n_iterations == 4
    assert parameters.k_activity_sequences == 5
    assert parameters.k_destination_sequences == 6
    assert parameters.n_warmup_iterations == 2
    assert parameters.max_inactive_age == 3
    assert parameters.transition_revision_probability == 0.4
    assert parameters.transition_logit_scale == 0.75
    assert parameters.enable_transition_distance_model is True
    assert parameters.transition_distance_threshold == 8.0
    assert parameters.transition_distance_friction == 1.5
    assert parameters.plan_embedding_dimension_weights == [1.0, 2.0, 3.0]
    assert parameters.behavior_change_phases == [
        BehaviorChangePhase(start_iteration=2, scope=BehaviorChangeScope.MODE_REPLANNING)
    ]
    assert wrapper.weekday_run.survey_plan_assets is wrapper.weekend_run.survey_plan_assets
    assert wrapper.survey_plan_assets is wrapper.weekday_run.survey_plan_assets
    assert wrapper.survey_plan_assets.surveys == wrapper.weekday_run.survey_plan_assets.surveys
    assert wrapper.weekend_run.enabled is False


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
    assert [survey.inputs["parameters"].country for survey in wrapper.survey_plan_assets.surveys] == ["fr"]
