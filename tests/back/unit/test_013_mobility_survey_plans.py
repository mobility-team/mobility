from dataclasses import dataclass

import pandas as pd
import polars as pl
from mobility.runtime.assets.asset import Asset
from mobility.surveys import MobilitySurveyPlans, MobilitySurveyPlanSteps

@dataclass
class _SurveyParams:
    survey_name: str
    country: str


@dataclass
class _ActivityOrModeParams:
    name: str | None
    survey_ids: list[str]


class _HashableStubAsset(Asset):
    def __init__(self, *, name: str | None = None, survey_ids: list[str] | None = None, is_anchor: bool = False):
        self.name = name
        self.is_anchor = is_anchor
        super().__init__({"parameters": _ActivityOrModeParams(name=name, survey_ids=survey_ids or [])})

    def get(self):
        return self

    def get_cached_hash(self):
        return self.inputs_hash


class _StubSurvey(Asset):
    def __init__(self, cached):
        self._cached = cached
        super().__init__({"parameters": _SurveyParams(survey_name="stub-survey", country="fr")})

    def get(self):
        return self._cached

    def create_and_get_asset(self):
        return self._cached

    def get_cached_hash(self):
        return self.inputs_hash

    def is_update_needed(self):
        return False


def _make_activity(name: str, survey_ids: list[str]):
    asset = _HashableStubAsset(name=name, survey_ids=survey_ids, is_anchor=(name == "home"))
    asset.name = name
    return asset


def _make_mode(name: str, survey_ids: list[str]):
    return _HashableStubAsset(name=name, survey_ids=survey_ids)


def test_survey_plan_assets_return_weighted_step_level_plans():
    survey = _StubSurvey(
        {
            "days_trip": pd.DataFrame(
                {
                    "day_id": [10, 20],
                    "day_of_week": [1, 1],
                    "pondki": [1.0, 3.0],
                    "city_category": ["urban", "urban"],
                    "csp": ["A", "A"],
                    "n_cars": [1, 1],
                }
            ),
            "short_trips": pd.DataFrame(
                {
                    "day_id": [10, 10, 20, 20],
                    "individual_id": [1, 1, 2, 2],
                    "daily_trip_index": [1, 2, 1, 2],
                    "departure_time": [8 * 3600, 17 * 3600, 8 * 3600, 17 * 3600],
                    "arrival_time": [9 * 3600, 18 * 3600, 9 * 3600, 18 * 3600],
                    "motive": ["1", "2", "1", "2"],
                    "mode_id": ["car", "walk", "car", "walk"],
                    "distance": [10.0, 10.0, 10.0, 10.0],
                }
            ),
        }
    )

    activities = [
        _make_activity("work", ["1"]),
        _make_activity("home", ["2"]),
    ]
    modes = [
        _make_mode("car", ["car"]),
        _make_mode("walk", ["walk"]),
    ]

    plan_steps_asset = MobilitySurveyPlanSteps(survey=survey, activities=activities, modes=modes)
    plan_steps = plan_steps_asset.get().sort(["activity_seq_id", "time_seq_id", "seq_step_index"])
    plans = MobilitySurveyPlans(plan_steps=plan_steps_asset).get().sort(["activity_seq_id", "time_seq_id"])

    assert plans.select(["activity_seq_id", "time_seq_id"]).n_unique() == 1
    assert plan_steps["activity"].to_list() == ["work", "home"]
    assert plan_steps["mode"].to_list() == ["car", "walk"]
    assert plan_steps["plan_weight_mass"].to_list() == [4.0, 4.0]
    assert plans["p_plan"].to_list() == [1.0]
    assert "plan_weight_mass" not in plans.columns
