import polars as pl
import pytest

from mobility.trips.group_day_trips.plans.plan_initializer import PlanInitializer


def test_population_weighted_survey_summaries_use_one_asset_with_correct_aggregations():
    """Check that runtime survey summaries come from one weighted step asset.

    In plain language: the same weighted survey step table should drive both
    activity-duration utilities and opportunity-demand totals, but with two
    different aggregations:

    - mean activity duration is averaged across activity occurrences
    - activity demand per person is summed across all represented persons
    """
    initializer = PlanInitializer()
    population_weighted_plan_steps = pl.DataFrame(
        {
            "country": ["fr"] * 7,
            "home_zone_id": [1] * 7,
            "city_category": ["urban"] * 7,
            "csp": ["A"] * 7,
            "n_cars": ["1"] * 7,
            "activity_seq_id": [1, 1, 2, 2, 2, 2, 2],
            "time_seq_id": [1, 1, 2, 2, 2, 2, 2],
            "seq_step_index": [0, 1, 0, 1, 2, 3, 4],
            "activity": ["work", "home", "work", "shopping", "shopping", "other", "home"],
            "duration_per_pers": [8.0, 16.0, 4.0, 1.0, 1.0, 1.0, 17.0],
            "n_persons": [6.0, 6.0, 4.0, 4.0, 4.0, 4.0, 4.0],
        }
    ).lazy()
    demand_groups = pl.DataFrame(
        {
            "country": ["fr"],
            "home_zone_id": [1],
            "city_category": ["urban"],
            "csp": ["A"],
            "n_cars": ["1"],
            "n_persons": [10.0],
        }
    )
    mean_activity_durations, mean_home_night_durations, activity_demand_per_pers = (
        initializer.get_survey_duration_summaries(
            population_weighted_plan_steps,
            demand_groups,
        )
    )

    mean_activity_duration_by_activity = {
        row["activity"]: row["mean_duration_per_pers"]
        for row in mean_activity_durations.to_dicts()
    }
    assert mean_activity_duration_by_activity["work"] == pytest.approx((8.0 * 6.0 + 4.0 * 4.0) / 10.0)
    assert mean_activity_duration_by_activity["shopping"] == pytest.approx(1.0)
    assert mean_activity_duration_by_activity["other"] == pytest.approx(1.0)

    assert mean_home_night_durations["mean_home_night_per_pers"].to_list() == pytest.approx([120.0 / 3600.0])

    activity_demand_by_activity = {
        row["activity"]: row["duration_per_pers"]
        for row in activity_demand_per_pers.to_dicts()
    }
    assert activity_demand_by_activity["work"] == pytest.approx((8.0 * 6.0 + 4.0 * 4.0) / 10.0)
    assert activity_demand_by_activity["shopping"] == pytest.approx((1.0 * 4.0 + 1.0 * 4.0) / 10.0)
    assert activity_demand_by_activity["other"] == pytest.approx((1.0 * 4.0) / 10.0)
    assert activity_demand_by_activity["home"] == pytest.approx((16.0 * 6.0 + 17.0 * 4.0) / 10.0)
