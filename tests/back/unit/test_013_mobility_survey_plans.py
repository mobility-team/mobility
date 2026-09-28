from dataclasses import dataclass

import pandas as pd
import polars as pl
from polars.testing import assert_frame_equal
from mobility.runtime.assets.asset import Asset
from mobility.surveys import MobilitySurveyPlans, MobilitySurveyPlanSteps, SurveyPlanAssets
from mobility.surveys.programme_pooling import ProgrammePoolingParameters
from mobility.trips.group_day_trips.evaluation.trip_pattern_distribution import (
    build_trip_pattern_distribution,
)
from mobility.trips.group_day_trips.evaluation.population_weighted_plan_steps import (
    _weight_reference_observations,
    PopulationWeightedSurveyReferenceSteps,
)

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
    def __init__(self, cached, survey_name="stub-survey"):
        self._cached = cached
        super().__init__({"parameters": _SurveyParams(survey_name=survey_name, country="fr")})

    def get(self):
        return self._cached

    def create_and_get_asset(self):
        return self._cached

    def get_cached_hash(self):
        return self.inputs_hash

    def is_update_needed(self):
        return False


class _StubStudyArea:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    def get(self) -> pd.DataFrame:
        return self.frame


class _StubPopulation(Asset):
    def __init__(self, population_groups_path: str, study_area: pd.DataFrame) -> None:
        self.population_groups_path = population_groups_path
        self.transport_zones = type(
            "StubTransportZones",
            (),
            {"study_area": _StubStudyArea(study_area)},
        )()
        super().__init__({"version": 1, "population_groups_path": population_groups_path})

    def get(self):
        return {"population_groups": self.population_groups_path}

    def get_cached_hash(self):
        return self.inputs_hash
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


def test_survey_plan_steps_truncate_to_ten_trips_then_force_last_trip_home():
    survey = _StubSurvey(
        {
            "days_trip": pd.DataFrame(
                {
                    "day_id": [10, 20],
                    "day_of_week": [1, 2],
                    "pondki": [1.0, 1.0],
                    "city_category": ["urban", "urban"],
                    "csp": ["A", "A"],
                    "n_cars": [1, 1],
                }
            ),
            "short_trips": pd.DataFrame(
                {
                    "day_id": [10] * 12 + [20] * 2,
                    "individual_id": [1] * 12 + [2] * 2,
                    "daily_trip_index": list(range(1, 13)) + [1, 2],
                    "departure_time": [i * 3600 for i in range(12)] + [8 * 3600, 17 * 3600],
                    "arrival_time": [(i + 0.5) * 3600 for i in range(12)] + [9 * 3600, 18 * 3600],
                    "motive": ["1"] * 12 + ["1", "1"],
                    "mode_id": ["car"] * 14,
                    "distance": [10.0] * 14,
                }
            ),
        }
    )

    activities = [
        _make_activity("work", ["1"]),
        _make_activity("home", ["2"]),
    ]
    modes = [_make_mode("car", ["car"])]

    plan_steps_asset = MobilitySurveyPlanSteps(survey=survey, activities=activities, modes=modes)
    plan_steps = plan_steps_asset._prepare_survey_plans()

    truncated_day = plan_steps.filter(pl.col("day_id") == 10).sort("seq_step_index")
    short_day = plan_steps.filter(pl.col("day_id") == 20).sort("seq_step_index")

    assert truncated_day["seq_step_index"].to_list() == list(range(1, 11))
    assert truncated_day["activity"].to_list() == ["work"] * 9 + ["home"]
    assert truncated_day["step_count"].to_list() == [10] * 10

    assert short_day["seq_step_index"].to_list() == [1, 2]
    assert short_day["activity"].to_list() == ["work", "home"]
    assert short_day["step_count"].to_list() == [2, 2]


def test_survey_plan_steps_does_not_pool_travel_time_across_respondent_days():
    survey = _StubSurvey(
        {
            "days_trip": pd.DataFrame(
                {
                    "day_id": [10, 20],
                    "day_of_week": [1, 1],
                    "pondki": [1.0, 1.0],
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
                    "departure_time": [1 * 3600, 12 * 3600, 1 * 3600, 12 * 3600],
                    "arrival_time": [7.5 * 3600, 18.5 * 3600, 7.5 * 3600, 18.5 * 3600],
                    "motive": ["1", "2", "1", "2"],
                    "mode_id": ["car"] * 4,
                    "distance": [10.0] * 4,
                }
            ),
        }
    )
    activities = [
        _make_activity("work", ["1"]),
        _make_activity("home", ["2"]),
    ]
    modes = [_make_mode("car", ["car"])]

    plan_steps = MobilitySurveyPlanSteps(
        survey=survey,
        activities=activities,
        modes=modes,
    )._prepare_survey_plans()

    assert plan_steps["day_id"].unique().sort().to_list() == [10, 20]
    assert (
        plan_steps.group_by("day_id")
        .agg(day_travel_time=pl.col("travel_time").sum())
        .sort("day_id")["day_travel_time"]
        .to_list()
        == [13.0, 13.0]
    )


def test_reference_mode_share_keeps_original_weights_for_identical_timed_plans(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path / "project"))
    survey = _StubSurvey(
        {
            "days_trip": pd.DataFrame(
                {
                    "day_id": [10, 20],
                    "day_of_week": [1, 1],
                    "pondki": [6.0, 4.0],
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
                    "motive": ["9.91", "1.1", "9.91", "1.1"],
                    "mode_id": ["car", "car", "train", "train"],
                    "distance": [10.0, 10.0, 10.0, 10.0],
                }
            ),
        }
    )
    activities = [_make_activity("work", ["9.91"]), _make_activity("home", ["1.1"])]
    modes = [_make_mode("car", ["car"]), _make_mode("train", ["train"])]
    plan_steps_asset = MobilitySurveyPlanSteps(
        survey=survey,
        activities=activities,
        modes=modes,
    )

    sampling_steps = plan_steps_asset.create_and_get_asset()
    reference_steps = plan_steps_asset.get_reference_observations().with_columns(
        survey_name=pl.lit("test"),
        country=pl.lit("fr"),
    )
    weighted_steps = _weight_reference_observations(reference_steps)
    mode_shares = (
        weighted_steps
        .group_by("mode")
        .agg(weight=pl.col("p_reference").sum())
        .with_columns(share=pl.col("weight") / pl.col("weight").sum())
        .sort("mode")
    )

    assert reference_steps.select("activity_seq_id", "time_seq_id").n_unique() == 1
    first_trip_mode = (
        sampling_steps
        .filter(pl.col("seq_step_index") == 1)["mode"]
        .cast(pl.String)
        .to_list()
    )
    assert first_trip_mode == ["car"]
    assert sampling_steps["plan_weight_mass"].unique().to_list() == [10.0]
    assert mode_shares.select("mode").to_series().to_list() == ["car", "train"]
    assert [round(value, 6) for value in mode_shares["share"]] == [0.6, 0.4]
    reference_patterns = build_trip_pattern_distribution(
        reference_steps.lazy().with_columns(
            n_persons=pl.col("pondki"),
            activity_seq_id=pl.lit(1),
        )
    )
    assert [round(value, 6) for value in reference_patterns["probability"]] == [0.6, 0.4]

    population_groups_path = tmp_path / "population_groups.parquet"
    pl.DataFrame(
        {
            "local_admin_unit_id": ["lau1", "lau1"],
            "transport_zone_id": [101, 102],
            "socio_pro_category": ["A", "B"],
            "weight": [100.0, 40.0],
            "n_cars": [1, 1],
        }
    ).write_parquet(population_groups_path)
    population = _StubPopulation(
        str(population_groups_path),
        pd.DataFrame(
            {
                "local_admin_unit_id": ["lau1"],
                "country": ["fr"],
                "urban_unit_category": ["urban"],
                "geometry": [None],
            }
        ),
    )
    survey_plan_assets = SurveyPlanAssets(
        surveys=[survey],
        activities=activities,
        modes=modes,
    )
    weighted_reference = PopulationWeightedSurveyReferenceSteps(
        population=population,
        survey_plan_assets=survey_plan_assets,
        is_weekday=True,
    )

    assert weighted_reference.assets_missing()
    population_weighted_steps = weighted_reference.get().collect()
    assert not weighted_reference.assets_missing()
    assert weighted_reference.unsupported_demand_mass == 40.0
    expanded_mode_weights = (
        population_weighted_steps
        .group_by("mode")
        .agg(weight=pl.col("n_persons").sum())
        .with_columns(share=pl.col("weight") / pl.col("weight").sum())
        .sort("mode")
    )
    assert [round(value, 6) for value in expanded_mode_weights["share"]] == [0.6, 0.4]


def test_emp_donors_exclude_whole_incomplete_days_without_repair(tmp_path, monkeypatch):
    """Only the original valid home-to-home day survives all donor checks."""
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    rows = []
    for day in range(1, 8):
        count = 11 if day == 5 else 2
        for index in range(1, count + 1):
            rows.append({
                "day_id": day, "individual_id": day,
                "daily_trip_index": 1 if day == 7 else index,
                "departure_time": index * 3600.0,
                "arrival_time": (index + 0.5) * 3600.0 if day != 3 else 0.0,
                "previous_motive": "home" if index == 1 and day != 6 else "work",
                "motive": "home" if index == count and day != 2 else "work",
                "mode_id": "walk", "distance": 1.0,
            })
    survey = _StubSurvey({
        "days_trip": pd.DataFrame({
            "day_id": list(range(1, 8)), "day_of_week": [1] * 7,
            "pondki": [1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0],
            "city_category": ["R"] * 7, "csp": ["fr-3"] * 7, "n_cars": ["1"] * 7,
        }),
        "short_trips": pd.DataFrame(rows),
    }, survey_name="fr-EMP-2019")
    asset = MobilitySurveyPlanSteps(
        survey=survey, activities=[_make_activity("home", ["home"]), _make_activity("work", ["work"])],
        modes=[_make_mode("walk", ["walk"])],
    )
    steps = asset.get_reference_observations().sort("seq_step_index")
    assert steps["day_id"].unique().to_list() == [1]
    assert steps["departure_time"].to_list() == [1.0, 2.0]
    assert steps["arrival_time"].to_list() == [1.5, 2.5]


def test_disabled_emp_pooling_keeps_legacy_programmes_and_priors(tmp_path, monkeypatch):
    """Keep repaired diaries for default sampling, but never borrow them."""
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    rows = []
    for day, trip_count in [(1, 2), (2, 2), (3, 12)]:
        for index in range(1, trip_count + 1):
            rows.append({
                "day_id": day, "individual_id": day, "daily_trip_index": index,
                "departure_time": (index + day - 1) * 3600.0,
                "arrival_time": (index + day - 0.5) * 3600.0,
                "previous_motive": "home" if index == 1 else "work",
                "motive": "home" if index == trip_count and day != 2 else "work",
                "mode_id": "walk", "distance": 1.0,
            })
    data = {
        "days_trip": pd.DataFrame({
            "day_id": [1, 2, 3], "day_of_week": [1] * 3, "pondki": [1.0, 2.0, 3.0],
            "city_category": ["R"] * 3, "csp": ["fr-3"] * 3, "n_cars": ["1"] * 3,
        }),
        "short_trips": pd.DataFrame(rows),
    }
    activities = [_make_activity("home", ["home"]), _make_activity("work", ["work"])]
    modes = [_make_mode("walk", ["walk"])]
    # The non-EMP stub exercises unchanged legacy preparation on the same data.
    legacy = SurveyPlanAssets(surveys=[_StubSurvey(data)], activities=activities, modes=modes)
    emp = SurveyPlanAssets(
        surveys=[_StubSurvey(data, survey_name="fr-EMP-2019")], activities=activities, modes=modes,
    )
    targets = pl.DataFrame({
        "country": ["fr"], "is_weekday": [True], "city_category": ["R"], "csp": ["fr-3"], "n_cars": ["1"],
    })
    sampled = emp.get_sampling_plans(targets, ProgrammePoolingParameters())
    assert_frame_equal(sampled.drop("survey_name"), legacy.get_plans().drop("survey_name"), check_exact=True)
    assert sampled.height == 3
    assert sampled.sort("p_plan")["p_plan"].to_list() == [1 / 6, 2 / 6, 3 / 6]
    assert emp.get_plan_steps().group_by("time_seq_id").len()["len"].sort().to_list() == [2, 2, 10]

    # The complete-day reference and enabled pooling use only the first day.
    reference_asset = emp.get_reference_plan_assets_by_survey()[0]["plan_steps"]
    reference = reference_asset.get().sort("seq_step_index")
    assert reference["day_id"].unique().to_list() == [1]
    pooled = emp.get_sampling_plans(targets, ProgrammePoolingParameters(enabled=True))
    assert pooled.height == 1
    assert pooled["time_seq_id"][0] == reference["time_seq_id"][0]
    assert pooled["p_plan"][0] == 1.0
    assert_frame_equal(reference_asset.get().sort("seq_step_index"), reference, check_exact=True)

    # Pooling never replaces legacy duration or opportunity priors.
    for getter in ["get_mean_activity_durations", "get_mean_home_night_durations", "get_activity_demand_per_pers"]:
        expected = getattr(legacy, getter)()
        actual = getattr(emp, getter)()
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns), check_exact=True)
