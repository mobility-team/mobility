import numpy as np
import pandas as pd
import pytest
from scipy.stats import gamma

from mobility.surveys.france import EMPMobilitySurvey, ENTDMobilitySurvey
from mobility.surveys.mobility_survey import MobilitySurveyParameters
from mobility.surveys.zero_duration_activities import (
    _correct_zero_duration_activities,
    _fit_five_minute_corrections,
    _interval_probability,
    _prepare_activity_durations,
    _zero_interval_mean,
)


def _survey_trips(*, one_minute_resolution: bool = False) -> pd.DataFrame:
    rows = []
    gaps = [0] * 8 + [5] * 8 + [10] * 8 + [20] * 8 + [40] * 8
    for day_id, gap_minutes in enumerate(gaps, start=1):
        first_departure = 8 * 3600 + (
            60 if one_minute_resolution else 0
        )
        rows.extend(
            [
                {
                    "day_id": day_id,
                    "individual_id": day_id,
                    "daily_trip_index": 1,
                    "departure_time": first_departure,
                    "arrival_time": 9 * 3600,
                    "motive": "2.20",
                    "pondki": 1.0,
                },
                {
                    "day_id": day_id,
                    "individual_id": day_id,
                    "daily_trip_index": 2,
                    "departure_time": 9 * 3600 + gap_minutes * 60,
                    "arrival_time": 11 * 3600,
                    "motive": "1.1",
                    "pondki": 1.0,
                },
            ]
        )
    return pd.DataFrame(rows).set_index("day_id")


def test_interval_probability_matches_gamma_cdf_difference() -> None:
    shape = 0.7
    scale = 12.0
    observed = np.asarray([0.0, 5.0, 20.0])
    half_width = np.asarray([5.0, 5.0, 5.0])

    actual = _interval_probability(
        observed,
        half_width,
        shape,
        scale,
    )
    lower = np.maximum(0.0, observed - half_width)
    upper = observed + half_width
    expected = gamma.cdf(upper, shape, scale=scale) - gamma.cdf(
        lower,
        shape,
        scale=scale,
    )

    assert actual == pytest.approx(expected, rel=1e-9, abs=1e-12)


def test_zero_interval_mean_stays_inside_observation_interval() -> None:
    assert 0.0 < _zero_interval_mean(0.6, 20.0, 1) < 1.0
    assert 0.0 < _zero_interval_mean(0.6, 20.0, 5) < 5.0


def test_public_emp_flag_is_disabled_by_default_and_can_be_enabled() -> None:
    assert not EMPMobilitySurvey().inputs[
        "parameters"
    ].correct_zero_durations
    assert EMPMobilitySurvey(correct_zero_durations=True).inputs[
        "parameters"
    ].correct_zero_durations
    parameters = MobilitySurveyParameters(
        survey_name="fr-EMP-2019",
        country="fr",
        correct_zero_durations=True,
    )
    assert EMPMobilitySurvey(parameters=parameters).inputs[
        "parameters"
    ].correct_zero_durations


def test_entd_rejects_zero_duration_correction() -> None:
    with pytest.raises(
        ValueError,
        match="until its parser exposes the survey's trip clock times",
    ):
        ENTDMobilitySurvey(correct_zero_durations=True)


def test_five_minute_correction_is_fitted_from_the_supplied_survey() -> None:
    prepared, _ = _prepare_activity_durations(
        _survey_trips(),
    )

    corrections = _fit_five_minute_corrections(prepared)

    assert set(corrections) == {"2.20"}
    assert 1 <= corrections["2.20"] <= 5


def test_public_survey_method_persists_a_known_correction(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        "mobility.surveys.zero_duration_activities."
        "_fit_five_minute_corrections",
        lambda _prepared: {"2.20": 4},
    )
    trips = _survey_trips().drop(columns="pondki")
    days = pd.DataFrame(
        {
            "day_id": range(1, 41),
            "pondki": np.ones(40),
        }
    ).set_index("day_id")
    survey = EMPMobilitySurvey()
    survey.cache_path["short_trips"] = tmp_path / "short_trips.parquet"
    survey.cache_path["days_trip"] = tmp_path / "days_trip.parquet"
    trips.to_parquet(survey.cache_path["short_trips"])
    days.to_parquet(survey.cache_path["days_trip"])

    survey.correct_zero_durations()

    corrected = pd.read_parquet(survey.cache_path["short_trips"])
    first_trip = corrected.loc[1].iloc[0]
    second_trip = corrected.loc[1].iloc[1]

    assert first_trip["arrival_time"] == 9 * 3600 - 2 * 60
    assert second_trip["departure_time"] == 9 * 3600 + 2 * 60


def test_one_minute_diaries_need_no_fitted_distribution() -> None:
    trips = _survey_trips(one_minute_resolution=True).iloc[:4]

    corrected = _correct_zero_duration_activities(trips)
    second_trip = corrected.loc[1].iloc[1]

    assert second_trip["departure_time"] == 9 * 3600 + 30


def test_clock_shifts_cannot_make_adjacent_travel_times_negative(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "mobility.surveys.zero_duration_activities."
        "_fit_five_minute_corrections",
        lambda _prepared: {"2.20": 4},
    )
    trips = pd.DataFrame(
        {
            "day_id": [1, 1],
            "individual_id": [1, 1],
            "daily_trip_index": [1, 2],
            "departure_time": [9 * 3600, 9 * 3600],
            "arrival_time": [9 * 3600, 9 * 3600 + 5 * 60],
            "motive": ["2.20", "1.1"],
        }
    )
    corrected = _correct_zero_duration_activities(trips)

    assert corrected.loc[0, "arrival_time"] == 9 * 3600
    assert corrected.loc[1, "departure_time"] == 9 * 3600 + 4 * 60
    assert (
        corrected["arrival_time"] >= corrected["departure_time"]
    ).all()


def test_consecutive_corrections_share_the_middle_trip_capacity() -> None:
    trips = pd.DataFrame(
        {
            "day_id": [1, 1, 1],
            "individual_id": [1, 1, 1],
            "daily_trip_index": [1, 2, 3],
            "departure_time": [
                8 * 3600 + 50 * 60,
                9 * 3600,
                9 * 3600 + 20,
            ],
            "arrival_time": [
                9 * 3600,
                9 * 3600 + 20,
                9 * 3600 + 10 * 60,
            ],
            "motive": ["2.20", "4.1", "1.1"],
        }
    )

    corrected = _correct_zero_duration_activities(trips)

    assert (
        corrected["arrival_time"] >= corrected["departure_time"]
    ).all()
    assert corrected.loc[1, "departure_time"] > corrected.loc[0, "arrival_time"]
    assert corrected.loc[2, "departure_time"] > corrected.loc[1, "arrival_time"]


def test_emp_creation_calls_enabled_correction(
    monkeypatch,
) -> None:
    survey = EMPMobilitySurvey(correct_zero_durations=True)
    correction_calls = []
    monkeypatch.setattr(survey, "download_survey_data", lambda _path: None)
    monkeypatch.setattr(survey, "parse_survey_data", lambda _path: None)
    monkeypatch.setattr(
        survey,
        "correct_zero_durations",
        lambda: correction_calls.append(True),
    )
    monkeypatch.setattr(survey, "get_cached_asset", lambda: {"ready": True})

    result = survey.create_and_get_asset()

    assert correction_calls == [True]
    assert result == {"ready": True}
