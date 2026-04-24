import pandas as pd
import polars as pl

from mobility.surveys.mobility_survey import MobilitySurvey


class _StubSurvey(MobilitySurvey):
    def __init__(self, cached):
        self._cached = cached

    def get(self):
        return self._cached

    def create_and_get_asset(self):
        return self._cached


def _make_activity(name: str, survey_ids: list[str]):
    return type(
        "ActivityStub",
        (),
        {"name": name, "inputs": {"parameters": type("Params", (), {"survey_ids": survey_ids})()}},
    )()


def _make_mode(name: str, survey_ids: list[str]):
    return type(
        "ModeStub",
        (),
        {"inputs": {"parameters": type("Params", (), {"name": name, "survey_ids": survey_ids})()}},
    )()


def test_get_plans_probability_returns_weighted_step_level_plans():
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

    plans = survey.get_plans_probability(activities, modes).sort(["survey_plan_id", "seq_step_index"])

    assert plans["survey_plan_id"].unique().to_list() == [0, 1]
    assert plans["activity"].to_list() == ["work", "home", "work", "home"]
    assert plans["mode"].to_list() == ["car", "walk", "car", "walk"]
    assert plans.filter(pl.col("survey_plan_id") == 0)["p_plan"].unique().to_list() == [0.25]
    assert plans.filter(pl.col("survey_plan_id") == 1)["p_plan"].unique().to_list() == [0.75]
    assert "pondki" not in plans.columns
    assert "mode_seq" not in plans.columns
    assert "max_seq_step_index" not in plans.columns
