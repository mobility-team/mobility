import polars as pl
import pytest
from types import SimpleNamespace

from mobility.activities import OtherActivity
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
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


def test_survey_summaries_are_stable_when_input_rows_are_reordered():
    steps = pl.DataFrame({
        "country": ["fr"] * 8, "home_zone_id": [1] * 8,
        "city_category": ["urban"] * 8, "csp": ["A"] * 8,
        "n_cars": ["1"] * 8, "activity_seq_id": [1, 1, 2, 2, 3, 3, 4, 4],
        "time_seq_id": [1, 1, 2, 2, 3, 3, 4, 4],
        "seq_step_index": [0, 1] * 4, "activity": ["work", "home"] * 4,
        "duration_per_pers": [3.0, 1.0, 4.0, 1.0, 5.0, 1.0, 6.0, 1.0],
        "n_persons": [1e12, 1e12, 0.0001, 0.0001, 0.0001, 0.0001, 0.0002, 0.0002],
    })
    groups = pl.DataFrame({
        "country": ["fr"] * 4, "home_zone_id": [1, 2, 3, 4],
        "city_category": ["urban"] * 4, "csp": ["A"] * 4,
        "n_cars": ["1"] * 4, "n_persons": [1e12, 0.0001, 0.0001, 0.0002],
    })
    initializer = PlanInitializer()
    forward = initializer.get_survey_duration_summaries(steps.lazy(), groups)
    reverse = initializer.get_survey_duration_summaries(steps.reverse().lazy(), groups.reverse())

    for left, right in zip(forward, reverse):
        keys = [name for name in left.columns if name not in {
            "mean_duration_per_pers", "mean_home_night_per_pers", "duration_per_pers"
        }]
        assert left.sort(keys).equals(right.sort(keys))


def test_opportunity_capacities_are_stable_when_input_rows_are_reordered():
    groups = pl.DataFrame({
        "country": ["fr"] * 4,
        "n_persons": [1e12, 0.0001, 0.0001, 0.0002],
    })
    demand = pl.DataFrame({
        "country": ["fr"],
        "activity": pl.Series(["work"], dtype=pl.Enum(["work"])),
        "duration_per_pers": [2.0],
    })
    parameters = SimpleNamespace(
        sink_saturation_coeff=1.0, destination_soft_capacity_factor=1.0,
        destination_sampling_overload_gamma=1.0, destination_sampling_min_attraction_factor=0.0,
    )
    opportunities = pl.DataFrame({"to": [1, 2, 3, 4], "n_opp": [1e12, 0.0001, 0.0001, 0.0002]})

    def calculate(group_rows, opportunity_rows):
        activity = SimpleNamespace(
            name="work", has_opportunities=True, inputs={"parameters": parameters},
            get_opportunities=lambda _: opportunity_rows,
        )
        return PlanInitializer().get_opportunities(demand, group_rows, [activity], None).sort("to")

    assert calculate(groups, opportunities).equals(calculate(groups.reverse(), opportunities.reverse()))


def test_population_opportunities_are_stable_when_input_rows_are_reordered(tmp_path):
    class PopulationSource(InMemoryAsset):
        def __init__(self, path):
            super().__init__({"path": str(path)})
            self.path = path

        def get(self):
            return {"population_groups": self.path}

    population = pl.DataFrame({
        "transport_zone_id": [1, 1, 1, 1],
        "weight": [1e12, 0.0001, 0.0001, 0.0002],
    })
    values = []
    for index, rows in enumerate((population, population.reverse())):
        path = tmp_path / f"population-{index}.parquet"
        rows.write_parquet(path)
        source = PopulationSource(path)
        values.append(OtherActivity(population=source).get_opportunities(None))
    assert values[0].equals(values[1])
