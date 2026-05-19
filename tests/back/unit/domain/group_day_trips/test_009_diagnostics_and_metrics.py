from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest

from mobility.trips.group_day_trips.core.diagnostics import RunDiagnostics
from mobility.trips.group_day_trips.core.results import RunResults
from mobility.trips.group_day_trips.evaluation.iteration_metrics import (
    IterationMetricsBuilder,
    IterationMetricsHistory,
)
from mobility.trips.group_day_trips.evaluation.calibration_plan_steps import to_calibration_plan_steps
from mobility.trips.group_day_trips.evaluation.model_entropy import ModelEntropy
from mobility.trips.group_day_trips.evaluation.model_loss import ModelLoss
from mobility.trips.group_day_trips.evaluation.model_trip_count_loss import (
    ModelTripCountLoss,
    build_trip_count_distribution,
)
from mobility.trips.group_day_trips.evaluation.public_transport_network_evaluation import (
    PublicTransportNetworkEvaluation,
)
from mobility.trips.group_day_trips.evaluation.routing_evaluation import RoutingEvaluation
from mobility.trips.group_day_trips.evaluation.travel_costs_evaluation import (
    TravelCostsEvaluation,
)
from mobility.trips.group_day_trips.evaluation.car_traffic_evaluation import (
    CarTrafficEvaluation,
)
from mobility.trips.group_day_trips.evaluation.trip_pattern_distribution import (
    build_trip_pattern_distribution,
)


class _FrameAsset:
    def __init__(self, frame: pl.DataFrame, *, lazy: bool = True):
        self.frame = frame
        self.lazy = lazy

    def get(self):
        return self.frame.lazy() if self.lazy else self.frame


def _make_transport_zones():
    return SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2"],
                "local_admin_unit_id": ["fr001", "fr001"],
                "is_inner_zone": [True, True],
                "geometry": [None, None],
            }
        ),
        study_area=SimpleNamespace(
            get=lambda: pd.DataFrame(
                {
                    "local_admin_unit_id": ["fr001"],
                    "country": ["fr"],
                    "geometry": [None],
                }
            )
        ),
    )


def _make_results_for_metrics() -> RunResults:
    plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [1, 1],
            "home_zone_id": ["z1", "z1"],
            "activity": ["work", "shop"],
            "mode": ["car", "walk"],
            "time": [0.5, 0.25],
            "distance": [5.0, 1.0],
            "n_persons": [2.0, 1.0],
        }
    )
    population_weighted_plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [1, 1],
            "home_zone_id": ["z1", "z1"],
            "country": ["fr", "fr"],
            "activity": ["work", "shop"],
            "mode": ["car", "walk"],
            "travel_time": [0.5, 0.25],
            "distance": [5.0, 1.0],
            "n_persons": [2.0, 1.0],
        }
    )
    demand_groups = pl.DataFrame(
        {
            "home_zone_id": ["z1"],
            "n_persons": [3.0],
        }
    )
    return RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=_make_transport_zones(),
        demand_groups=demand_groups.lazy(),
        plan_steps=plan_steps.lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=population_weighted_plan_steps.lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=SimpleNamespace(),
    )


def test_metrics_aggregate_and_travel_indicators_by_match_reference_when_inputs_match():
    results = _make_results_for_metrics()

    aggregate = results.metrics.aggregate()
    by_mode = results.metrics.travel_indicators_by(variable="mode")
    by_time_bin = results.metrics.travel_indicators_by(variable="time_bin")

    assert aggregate.height == 3
    assert aggregate["delta"].to_list() == pytest.approx([0.0, 0.0, 0.0])
    assert by_mode["delta"].to_list() == pytest.approx([0.0] * by_mode.height)
    assert by_time_bin["delta"].to_list() == pytest.approx([0.0] * by_time_bin.height)


def test_metrics_evaluation_wrappers_pass_run_results(monkeypatch):
    results = _make_results_for_metrics()
    seen = {}

    def _capture(label):
        def fake_get(self, *args, **kwargs):
            seen[label] = self.results
            return label

        return fake_get

    monkeypatch.setattr(CarTrafficEvaluation, "get", _capture("car_traffic"))
    monkeypatch.setattr(TravelCostsEvaluation, "get", _capture("travel_costs"))
    monkeypatch.setattr(RoutingEvaluation, "get", _capture("routing"))
    monkeypatch.setattr(
        PublicTransportNetworkEvaluation,
        "get",
        _capture("public_transport_network"),
    )

    assert results.metrics.car_traffic() == "car_traffic"
    assert results.metrics.travel_costs([]) == "travel_costs"
    assert results.metrics.routing(pl.DataFrame()) == "routing"
    assert results.metrics.public_transport_network() == "public_transport_network"
    assert seen == {
        "car_traffic": results,
        "travel_costs": results,
        "routing": results,
        "public_transport_network": results,
    }


def test_opportunity_occupation_includes_full_capacity_stock_once_per_destination_activity():
    """Check that diagnostics include the full opportunity stock once.

    In plain language: if one destination/activity pair receives duration from
    several resident-scope segments, aggregated opportunity capacity should
    still match the underlying unique destination supply. Unused destinations
    should remain visible with zero duration so summed capacity reflects the
    full stock, not only occupied sinks.
    """
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": [1, 2, 10, 11],
                "is_inner_zone": [True, False, True, False],
                "geometry": [None, None, None, None],
            }
        ),
        study_area=SimpleNamespace(get=lambda: None),
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame({"n_persons": [5.0]}).lazy(),
        plan_steps=pl.DataFrame(
            {
                "activity_seq_id": [1, 1],
                "home_zone_id": [1, 2],
                "activity": ["work", "work"],
                "to": [10, 10],
                "duration": [2.0, 3.0],
            }
        ).lazy(),
        opportunities=pl.DataFrame(
            {
                "to": [10, 11],
                "activity": ["work", "work"],
                "opportunity_capacity": [100.0, 50.0],
            }
        ).lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=SimpleNamespace(),
    )

    occupation = results.metrics.opportunity_occupation(inner_zone_residents_only=False)
    unused_destination = occupation.filter(pl.col("transport_zone_id") == 11)
    totals = occupation.group_by("activity").agg(
        duration=pl.col("duration").sum(),
        opportunity_capacity=pl.col("opportunity_capacity").sum(),
    )

    assert unused_destination["duration"].to_list() == pytest.approx([0.0])
    assert unused_destination["opportunity_occupation"].to_list() == pytest.approx([0.0])
    assert totals["duration"].to_list() == pytest.approx([5.0])
    assert totals["opportunity_capacity"].to_list() == pytest.approx([150.0])


def test_opportunity_occupation_plot_path_masks_outliers_and_plots(monkeypatch):
    class _GeoFrame(pd.DataFrame):
        @property
        def _constructor(self):
            return _GeoFrame

        def to_crs(self, _epsg):
            return self

    transport_zones = SimpleNamespace(
        get=lambda: _GeoFrame(
            {
                "transport_zone_id": [1, 10],
                "local_admin_unit_id": ["fr001", "fr001"],
                "is_inner_zone": [True, True],
                "geometry": [None, None],
            }
        ),
        study_area=SimpleNamespace(get=lambda: None),
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame({"home_zone_id": [1], "n_persons": [2.0]}).lazy(),
        plan_steps=pl.DataFrame(
            {
                "activity_seq_id": [1],
                "home_zone_id": [1],
                "activity": ["work"],
                "to": [10],
                "duration": [2.0],
            }
        ).lazy(),
        opportunities=pl.DataFrame(
            {
                "to": [10],
                "activity": ["work"],
                "opportunity_capacity": [100.0],
            }
        ).lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=SimpleNamespace(),
    )
    seen = {}

    def fake_mask_outliers(series):
        seen["masked_values"] = series.to_list()
        return series + 1.0

    def fake_plot_map(tz, variable, plot_activity):
        seen["plot_variable"] = variable
        seen["plot_activity"] = plot_activity
        seen["plotted_values"] = tz[variable].to_list()

    monkeypatch.setattr(results.metrics, "mask_outliers", fake_mask_outliers)
    monkeypatch.setattr(results.metrics, "plot_map", fake_plot_map)

    occupation = results.metrics.opportunity_occupation(
        plot_activity="work",
        mask_outliers=True,
        inner_zone_residents_only=True,
    )

    assert occupation["opportunity_occupation"].to_list() == pytest.approx([0.02])
    assert seen["plot_variable"] == "opportunity_occupation"
    assert seen["plot_activity"] == "work"
    assert sorted(seen["masked_values"]) == pytest.approx([0.0, 0.02])
    assert sorted(seen["plotted_values"]) == pytest.approx([1.0, 1.02])


def test_trip_count_by_demand_group_plot_path_masks_outliers_and_plots(monkeypatch):
    class _GeoFrame(pd.DataFrame):
        @property
        def _constructor(self):
            return _GeoFrame

        def to_crs(self, _epsg):
            return self

    transport_zones = SimpleNamespace(
        get=lambda: _GeoFrame(
            {
                "transport_zone_id": ["z1", "z2"],
                "local_admin_unit_id": ["fr001", "fr001"],
                "is_inner_zone": [True, True],
                "geometry": [None, None],
            }
        ),
        study_area=SimpleNamespace(get=lambda: None),
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame(
            {
                "home_zone_id": ["z1"],
                "csp": ["A"],
                "n_cars": ["1"],
                "n_persons": [4.0],
            }
        ).lazy(),
        plan_steps=pl.DataFrame(
            {
                "activity_seq_id": [1, 2],
                "home_zone_id": ["z1", "z1"],
                "csp": ["A", "A"],
                "n_cars": ["1", "1"],
                "n_persons": [2.0, 1.0],
            }
        ).lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=SimpleNamespace(),
    )
    seen = {}

    def fake_mask_outliers(series):
        seen["masked_values"] = series.to_list()
        return series + 1.0

    def fake_plot_map(tz, variable):
        seen["plot_variable"] = variable
        seen["plotted_values"] = tz[variable].to_list()

    monkeypatch.setattr(results.metrics, "mask_outliers", fake_mask_outliers)
    monkeypatch.setattr(results.metrics, "plot_map", fake_plot_map)

    trip_count = results.metrics.trip_count_by_demand_group(plot=True, mask_outliers=True)

    assert trip_count["n_trips"].to_list() == pytest.approx([3.0])
    assert trip_count["n_trips_per_person"].to_list() == pytest.approx([0.75])
    assert seen["plot_variable"] == "n_trips_per_person"
    assert sorted(seen["masked_values"]) == pytest.approx([0.0, 0.75])
    assert sorted(seen["plotted_values"]) == pytest.approx([1.0, 1.75])


def test_metric_per_person_plot_and_wrapper_methods(monkeypatch):
    class _GeoFrame(pd.DataFrame):
        @property
        def _constructor(self):
            return _GeoFrame

        def to_crs(self, _epsg):
            return self

    transport_zones = SimpleNamespace(
        get=lambda: _GeoFrame(
            {
                "transport_zone_id": ["z1", "z2"],
                "local_admin_unit_id": ["fr001", "fr001"],
                "is_inner_zone": [True, True],
                "geometry": [None, None],
            }
        ),
        study_area=SimpleNamespace(get=lambda: None),
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame(
            {
                "home_zone_id": ["z1"],
                "csp": ["A"],
                "n_cars": ["1"],
                "n_persons": [4.0],
            }
        ).lazy(),
        plan_steps=pl.DataFrame(
            {
                "activity_seq_id": [1],
                "home_zone_id": ["z1"],
                "csp": ["A"],
                "n_cars": ["1"],
                "from": ["z1"],
                "to": ["z2"],
                "mode": ["car"],
                "n_persons": [2.0],
            }
        ).lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame(
            {
                "from": ["z1"],
                "to": ["z2"],
                "mode": ["car"],
                "distance": [10.0],
                "time": [1.5],
                "cost": [3.0],
                "ghg_emissions_per_trip": [5.0],
            }
        ).lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=SimpleNamespace(),
    )
    seen = {}

    def fake_mask_outliers(series):
        seen["masked_values"] = series.to_list()
        return series + 1.0

    def fake_plot_map(tz, variable, **kwargs):
        seen.setdefault("plot_calls", []).append((variable, tz[variable].to_list(), kwargs))

    monkeypatch.setattr(results.metrics, "mask_outliers", fake_mask_outliers)
    monkeypatch.setattr(results.metrics, "plot_map", fake_plot_map)

    metric = results.metrics.metric_per_person("distance", plot=True, mask_outliers=True)

    assert metric["distance"].to_list() == pytest.approx([20.0])
    assert metric["distance_per_person"].to_list() == pytest.approx([5.0])
    assert sorted(seen["masked_values"]) == pytest.approx([0.0, 5.0])
    assert seen["plot_calls"][0][0] == "distance_per_person"
    assert sorted(seen["plot_calls"][0][1]) == pytest.approx([1.0, 6.0])

    wrapper_calls = []

    def fake_metric_per_person(metric_name, *args, **kwargs):
        wrapper_calls.append((metric_name, args, kwargs))
        return metric_name

    monkeypatch.setattr(results.metrics, "metric_per_person", fake_metric_per_person)

    assert results.metrics.distance_per_person(plot=False) == "distance"
    assert results.metrics.ghg_per_person(plot=False) == "ghg_emissions_per_trip"
    assert results.metrics.time_per_person(plot=False) == "time"
    assert results.metrics.cost_per_person(plot=False) == "cost"
    assert [call[0] for call in wrapper_calls] == [
        "distance",
        "ghg_emissions_per_trip",
        "time",
        "cost",
    ]


def test_plot_map_builds_choropleth_without_opening_backend(monkeypatch):
    class _GeoFrame(pd.DataFrame):
        @property
        def _constructor(self):
            return _GeoFrame

        def to_json(self, *args, **kwargs):
            return super().to_json(*args, **kwargs)

    tz = _GeoFrame(
        {
            "transport_zone_id": ["z1"],
            "geometry": [None],
            "value": [1.5],
        }
    )
    results = _make_results_for_metrics()
    seen = {}

    class _FakeFig:
        def update_geos(self, **kwargs):
            seen["geos"] = kwargs

        def update_layout(self, **kwargs):
            seen["layout"] = kwargs

        def show(self, plot_method):
            seen["plot_method"] = plot_method

    def fake_choropleth(*args, **kwargs):
        seen["choropleth_kwargs"] = kwargs
        return _FakeFig()

    monkeypatch.setattr("mobility.trips.group_day_trips.core.metrics.px.choropleth", fake_choropleth)

    results.metrics.plot_map(
        tz,
        value="value",
        activity="Work",
        plot_method="json",
        color_continuous_scale="RdBu_r",
        color_continuous_midpoint=0.0,
    )

    assert seen["choropleth_kwargs"]["locations"] == "transport_zone_id"
    assert seen["choropleth_kwargs"]["color"] == "value"
    assert seen["choropleth_kwargs"]["title"] == "Work"
    assert seen["choropleth_kwargs"]["subtitle"] == "Work"
    assert seen["plot_method"] == "json"


def test_metric_per_person_compare_with_and_plot_delta(tmp_path, monkeypatch):
    class _GeoFrame(pd.DataFrame):
        @property
        def _constructor(self):
            return _GeoFrame

        def to_crs(self, _epsg):
            return self

    transport_zones = SimpleNamespace(
        get=lambda: _GeoFrame(
            {
                "transport_zone_id": ["z1", "z2"],
                "local_admin_unit_id": ["fr001", "fr001"],
                "is_inner_zone": [True, True],
                "geometry": [None, None],
            }
        ),
        study_area=SimpleNamespace(get=lambda: None),
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame(
            {
                "home_zone_id": ["z1"],
                "csp": ["A"],
                "n_cars": ["1"],
                "n_persons": [4.0],
            }
        ).lazy(),
        plan_steps=pl.DataFrame(
            {
                "activity_seq_id": [1],
                "home_zone_id": ["z1"],
                "csp": ["A"],
                "n_cars": ["1"],
                "from": ["z1"],
                "to": ["z2"],
                "mode": ["car"],
                "n_persons": [2.0],
            }
        ).lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame(
            {
                "from": ["z1"],
                "to": ["z2"],
                "mode": ["car"],
                "distance": [10.0],
                "time": [1.5],
                "cost": [3.0],
                "ghg_emissions_per_trip": [5.0],
            }
        ).lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=SimpleNamespace(),
    )

    weekday_plan_steps = tmp_path / "weekday_plan_steps.parquet"
    weekday_costs = tmp_path / "weekday_costs.parquet"
    pl.DataFrame(
        {
            "activity_seq_id": [1],
            "home_zone_id": ["z1"],
            "csp": ["A"],
            "n_cars": ["1"],
            "from": ["z1"],
            "to": ["z2"],
            "mode": ["car"],
            "n_persons": [2.0],
        }
    ).write_parquet(weekday_plan_steps)
    pl.DataFrame(
        {
            "from": ["z1"],
            "to": ["z2"],
            "mode": ["car"],
            "distance": [8.0],
            "time": [1.0],
            "cost": [2.0],
            "ghg_emissions_per_trip": [4.0],
        }
    ).write_parquet(weekday_costs)

    compare_with = SimpleNamespace(
        cache_path={
            "weekday_plan_steps": weekday_plan_steps,
            "weekday_costs": weekday_costs,
        },
        get=lambda: "loaded",
    )
    seen = {}

    def fake_mask_outliers(series):
        seen.setdefault("masked_values", []).append(series.to_list())
        return series + 1.0

    def fake_plot_map(tz, variable, **kwargs):
        seen.setdefault("plot_calls", []).append((variable, tz[variable].to_list(), kwargs))

    monkeypatch.setattr(results.metrics, "mask_outliers", fake_mask_outliers)
    monkeypatch.setattr(results.metrics, "plot_map", fake_plot_map)

    metric = results.metrics.metric_per_person(
        "distance",
        compare_with=compare_with,
        plot=True,
        plot_delta=True,
        mask_outliers=True,
    )

    assert metric["distance"].to_list() == pytest.approx([20.0])
    assert metric["distance_per_person"].to_list() == pytest.approx([5.0])
    assert metric["distance_comp"].to_list() == pytest.approx([16.0])
    assert metric["distance_per_person_comp"].to_list() == pytest.approx([4.0])
    assert metric["delta"].to_list() == pytest.approx([1.0])
    assert len(seen["plot_calls"]) == 2
    assert seen["plot_calls"][0][0] == "distance_per_person"
    assert seen["plot_calls"][1][0] == "delta"
    assert sorted(seen["masked_values"][0]) == pytest.approx([0.0, 5.0])
    assert sorted(seen["masked_values"][1]) == pytest.approx([0.0, 1.0])
    assert seen["plot_calls"][1][2] == {
        "color_continuous_scale": "RdBu_r",
        "color_continuous_midpoint": 0,
    }


def test_plot_od_flows_returns_not_implemented_for_unhandled_options():
    results = _make_results_for_metrics()

    assert results.metrics.plot_od_flows(level_of_detail=0) is NotImplemented
    assert results.metrics.plot_od_flows(level_of_detail=2) is NotImplemented
    assert results.metrics.plot_od_flows(activity="work") is NotImplemented


def test_plot_od_flows_count_mode_returns_mode_counts():
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=_make_transport_zones(),
        demand_groups=pl.DataFrame({"home_zone_id": ["z1"], "n_persons": [3.0]}).lazy(),
        plan_steps=pl.DataFrame(
            {
                "activity_seq_id": [1, 2, 3],
                "from": ["z1", "z1", "z1"],
                "to": ["z2", "z2", "z2"],
                "mode": ["car", None, "walk"],
                "n_persons": [2.0, 1.0, 1.0],
            }
        ).lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=SimpleNamespace(),
    )

    count_modes = results.metrics.plot_od_flows(mode="count")

    assert set(count_modes.index.to_list()) == {"car", "unknown_mode", "walk"}


def test_plot_modal_share_public_transport_filters_modes_and_returns_share(monkeypatch):
    class _FakeAxes:
        def set_axis_off(self):
            return None

    class _FakeGeoDataFrame:
        def __init__(self, frame):
            self.frame = frame

        def fillna(self, _value):
            return self

        def plot(self, *_args, **_kwargs):
            return _FakeAxes()

    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=SimpleNamespace(
            get=lambda: pd.DataFrame(
                {
                    "transport_zone_id": ["z1", "z2"],
                    "geometry": [None, None],
                }
            ),
            study_area=SimpleNamespace(get=lambda: None),
        ),
        demand_groups=pl.DataFrame({"home_zone_id": ["z1"], "n_persons": [3.0]}).lazy(),
        plan_steps=pl.DataFrame(
            {
                "from": ["z1", "z1", "z2"],
                "to": ["z2", "z2", "z1"],
                "mode": [
                    "walk/public_transport/walk",
                    "car",
                    "walk/public_transport/walk",
                ],
                "n_persons": [2.0, 1.0, 3.0],
            }
        ).lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=SimpleNamespace(),
    )
    seen = {}
    monkeypatch.setattr("mobility.trips.group_day_trips.core.metrics.gpd.GeoDataFrame", _FakeGeoDataFrame)
    monkeypatch.setattr("mobility.trips.group_day_trips.core.metrics.plt.show", lambda: seen.setdefault("show_called", True))
    monkeypatch.setattr("mobility.trips.group_day_trips.core.metrics.plt.title", lambda value: seen.setdefault("title", value))

    mode_share = results.metrics.plot_modal_share(zone="origin", mode="public_transport")

    assert seen["show_called"] is True
    assert "Public transport share per origin transport zone" in seen["title"]
    assert set(mode_share["mode"].unique().tolist()) == {"public_transport"}


def test_mask_outliers_replaces_extreme_values_with_nan():
    results = _make_results_for_metrics()
    series = pd.Series([1.0, 1.1, 0.9, 100.0])

    masked = results.metrics.mask_outliers(series)

    assert pd.isna(masked.iloc[-1])
    assert masked.iloc[0] == pytest.approx(1.0)


def test_calibration_plan_steps_keep_stay_home_as_a_distribution_state():
    raw_plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [1, 0],
            "activity": ["work", "home"],
            "mode": ["car", "stay_home"],
            "distance": [10.0, 0.0],
            "time": [0.5, 0.0],
            "n_persons": [2.0, 3.0],
        }
    ).with_columns(
        activity=pl.col("activity").cast(pl.Enum(["home", "work"])),
        mode=pl.col("mode").cast(pl.Enum(["car", "stay_home"])),
    )

    calibration_steps = to_calibration_plan_steps(raw_plan_steps).collect()

    assert calibration_steps.sort("mode").to_dict(as_series=False) == {
        "activity": ["work", "stay_home"],
        "distance_bin": ["[10, 20)", "stay_home"],
        "time_bin": ["[30, 45)", "stay_home"],
        "mode": ["car", "stay_home"],
        "distance": [10.0, 0.0],
        "time": [0.5, 0.0],
        "n_persons": [2.0, 3.0],
    }


def test_model_loss_summary_history_and_validation():
    expected = pl.DataFrame(
        {
            "activity": ["work", "shop"],
            "distance_bin": ["(1, 5]", "(0, 1]"],
            "time_bin": ["[60, 1000000000)", "[60, 1000000000)"],
            "mode": ["car", "walk"],
            "distance": [10.0, 1.0],
            "time": [2.0, 1.0],
            "n_persons": [2.0, 1.0],
        }
    )
    observed = pl.DataFrame(
        {
            "activity": ["work", "shop"],
            "distance_bin": ["(1, 5]", "(0, 1]"],
            "time_bin": ["[60, 1000000000)", "[60, 1000000000)"],
            "mode": ["car", "walk"],
            "distance": [8.0, 1.0],
            "time": [1.5, 2.0],
            "n_persons": [3.0, 1.0],
        }
    )
    history_store = IterationMetricsHistory(
        pl.DataFrame(
            {
                "iteration": [1],
                "total_loss": [0.3],
                "trip_count_loss": [0.08],
                "activity_loss": [0.04],
                "distance_bin_loss": [0.05],
                "time_bin_loss": [0.06],
                "mode_loss": [0.07],
                "observed_entropy": [0.7],
                "mean_utility": [1.0],
                "mean_trip_count": [2.0],
                "mean_travel_time": [1.5],
                "mean_travel_distance": [12.0],
            }
        ).lazy()
    )
    loss = ModelLoss(
        expected_plan_steps=_FrameAsset(expected, lazy=False),
        observed_plan_steps=_FrameAsset(observed),
        history=history_store,
    )

    summary = loss.summary()
    history = loss.history()
    history_row = loss.history_row(iteration=2, plan_steps=observed)

    assert summary.height == 1
    assert summary["total_loss"].unique().item() > 0.0
    assert history.columns == [
        "iteration",
        "total_loss",
        "trip_count_loss",
        "activity_loss",
        "distance_bin_loss",
        "time_bin_loss",
        "mode_loss",
    ]
    assert history_row["total_loss"] > 0.0
    assert history_row["activity_loss"] >= 0.0
    assert history_row["distance_bin_loss"] >= 0.0
    assert history_row["time_bin_loss"] >= 0.0
    assert history_row["mode_loss"] >= 0.0

    with pytest.raises(ValueError, match="ModelLoss expects canonical calibration plan steps"):
        loss.comparison(
            plan_steps=pl.DataFrame(
                {
                    "activity": ["work"],
                    "distance": [10.0],
                    "time": [2.0],
                    "n_persons": [1.0],
                }
            )
        )


def test_model_loss_is_based_on_distribution_shares_not_global_scale():
    expected = pl.DataFrame(
        {
            "activity": ["work", "shop", "stay_home"],
            "distance_bin": ["(1, 5]", "(0, 1]", "stay_home"],
            "time_bin": ["[60, 1000000000)", "[60, 1000000000)", "stay_home"],
            "mode": ["car", "walk", "stay_home"],
            "distance": [10.0, 1.0, 0.0],
            "time": [2.0, 1.0, 0.0],
            "n_persons": [2.0, 1.0, 1.0],
        }
    )
    observed = pl.DataFrame(
        {
            "activity": ["work", "shop", "stay_home"],
            "distance_bin": ["(1, 5]", "(0, 1]", "stay_home"],
            "time_bin": ["[60, 1000000000)", "[60, 1000000000)", "stay_home"],
            "mode": ["car", "walk", "stay_home"],
            "distance": [10.0, 1.0, 0.0],
            "time": [2.0, 1.0, 0.0],
            "n_persons": [1.0, 2.0, 1.0],
        }
    )

    scaled_expected = expected.with_columns(n_persons=pl.col("n_persons") * 10.0)
    scaled_observed = observed.with_columns(n_persons=pl.col("n_persons") * 10.0)

    loss = ModelLoss(expected_plan_steps=_FrameAsset(expected, lazy=False))
    scaled_loss = ModelLoss(expected_plan_steps=_FrameAsset(scaled_expected, lazy=False))

    assert loss.total_loss(plan_steps=observed) == pytest.approx(
        scaled_loss.total_loss(plan_steps=scaled_observed)
    )


def test_model_loss_ignores_stay_home_when_comparing_trip_distributions():
    """Stay-home is a person state, not a trip class in the trip-count loss."""
    expected = pl.DataFrame(
        {
            "activity": ["work"],
            "distance_bin": ["(1, 5]"],
            "time_bin": ["[60, 1000000000)"],
            "mode": ["car"],
            "distance": [10.0],
            "time": [2.0],
            "n_persons": [2.0],
        }
    )
    observed = pl.DataFrame(
        {
            "activity": ["work", "stay_home"],
            "distance_bin": ["(1, 5]", "stay_home"],
            "time_bin": ["[60, 1000000000)", "stay_home"],
            "mode": ["car", "stay_home"],
            "distance": [10.0, 0.0],
            "time": [2.0, 0.0],
            "n_persons": [2.0, 3.0],
        }
    )

    comparison = ModelLoss(expected_plan_steps=_FrameAsset(expected, lazy=False)).comparison(plan_steps=observed)

    assert comparison["activity"].to_list() == ["work"]
    assert comparison["observed_total"].to_list() == pytest.approx([2.0])
    assert comparison["expected_total"].to_list() == pytest.approx([2.0])


def test_model_trip_count_loss_uses_cleaned_survey_steps_and_immobility():
    """The zero-trip mass comes from survey immobility, not calibration step rows."""
    expected_mobile_steps = pl.DataFrame(
        {
            "country": ["fr", "fr", "fr", "fr", "fr", "fr"],
            "home_zone_id": ["z1", "z1", "z2", "z2", "z2", "z2"],
            "city_category": ["dense", "dense", "dense", "dense", "dense", "dense"],
            "csp": ["A", "A", "A", "A", "A", "A"],
            "n_cars": [1, 1, 1, 1, 1, 1],
            "activity_seq_id": [1, 1, 2, 2, 2, 2],
            "time_seq_id": [10, 10, 20, 20, 20, 20],
            "seq_step_index": [1, 2, 1, 2, 3, 4],
            "n_persons": [10.0, 10.0, 5.0, 5.0, 5.0, 5.0],
        }
    )
    survey = SimpleNamespace(
        inputs={"parameters": SimpleNamespace(country="fr")},
        get=lambda: {
            "p_immobility": pd.DataFrame(
                {"immobility_weekday": [0.2], "immobility_weekend": [0.3]},
                index=pd.Index(["A"], name="csp"),
            )
        },
    )
    observed_steps = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2],
            "activity_seq_id": [1, 1, 0],
            "time_seq_id": [10, 10, 0],
            "dest_seq_id": [10, 10, 0],
            "mode_seq_id": [10, 10, 0],
            "seq_step_index": [1, 2, 0],
            "n_persons": [2.0, 2.0, 3.0],
        }
    )

    distribution = build_trip_count_distribution(
        expected_mobile_steps,
        immobility_probabilities=pl.DataFrame({"country": ["fr"], "csp": ["A"], "p_immobility": [0.2]}),
    )
    loss = ModelTripCountLoss(
        expected_plan_steps=_FrameAsset(expected_mobile_steps),
        surveys=[survey],
        is_weekday=True,
        observed_plan_steps=_FrameAsset(observed_steps),
    )

    expected_by_bin = dict(zip(distribution["trip_count_bin"], distribution["n_persons"]))
    comparison = loss.comparison()

    assert expected_by_bin["0"] == pytest.approx(3.0)
    assert expected_by_bin["2"] == pytest.approx(8.0)
    assert expected_by_bin["4"] == pytest.approx(4.0)
    assert comparison["expected_total"].max() == pytest.approx(15.0)
    assert comparison["observed_total"].max() == pytest.approx(5.0)
    assert loss.total_loss() >= 0.0


def test_model_entropy_and_trip_pattern_distribution_cover_mobile_and_stay_home_patterns():
    raw_plan_steps = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2],
            "activity_seq_id": [1, 1, 0],
            "time_seq_id": [1, 1, 0],
            "seq_step_index": [1, 2, 0],
            "activity": ["work", "shop", "home"],
            "distance": [2.0, 0.5, 0.0],
            "mode": ["car", "walk", "stay_home"],
            "n_persons": [2.0, 2.0, 1.0],
        }
    )
    distribution = build_trip_pattern_distribution(raw_plan_steps)
    expected_distribution = pl.DataFrame(
        {
            "trip_pattern": distribution["trip_pattern"].to_list(),
            "n_persons": [1.0, 2.0],
            "probability": [1.0 / 3.0, 2.0 / 3.0],
        }
    )
    observed_distribution = pl.DataFrame(
        {
            "trip_pattern": distribution["trip_pattern"].to_list(),
            "n_persons": [2.0, 1.0],
            "probability": [2.0 / 3.0, 1.0 / 3.0],
        }
    )
    history_store = IterationMetricsHistory(
        pl.DataFrame(
            {
                "iteration": [1],
                "total_loss": [0.3],
                "trip_count_loss": [0.08],
                "activity_loss": [0.04],
                "distance_bin_loss": [0.05],
                "time_bin_loss": [0.06],
                "mode_loss": [0.07],
                "observed_entropy": [0.7],
                "mean_utility": [1.0],
                "mean_trip_count": [2.0],
                "mean_travel_time": [1.5],
                "mean_travel_distance": [12.0],
            }
        ).lazy()
    )
    entropy = ModelEntropy(
        expected_plan_steps=_FrameAsset(expected_distribution),
        observed_plan_steps=_FrameAsset(observed_distribution),
        history=history_store,
    )

    comparison = entropy.comparison()
    summary = entropy.summary(plan_steps=raw_plan_steps)
    history = entropy.history()
    history_row = entropy.history_row(iteration=2, plan_steps=raw_plan_steps)

    assert set(distribution["trip_pattern"].to_list()) == {"stay_home", distribution["trip_pattern"][0]}
    assert distribution["probability"].sum() == pytest.approx(1.0)
    assert comparison.height == 2
    assert set(summary.columns) == {"observed_entropy", "expected_entropy", "entropy_gap"}
    assert history.columns == ["iteration", "observed_entropy"]
    assert history_row["iteration"] == 2
    assert history_row["observed_entropy"] >= 0.0


def test_iteration_metrics_builder_rebuilds_history_and_run_diagnostics_exposes_views():
    class FakeLoss:
        def history_row(self, *, iteration, plan_steps):
            return {
                "iteration": iteration,
                "total_loss": float(iteration),
                "activity_loss": 0.1,
                "distance_bin_loss": 0.2,
                "time_bin_loss": 0.3,
                "mode_loss": 0.4,
            }

    class FakeTripCountLoss:
        def history_row(self, *, iteration, plan_steps):
            return {
                "iteration": iteration,
                "trip_count_loss": 0.5,
            }

    class FakeEntropy:
        def history_row(self, *, iteration, plan_steps):
            return {
                "iteration": iteration,
                "observed_entropy": float(iteration) / 10.0,
            }

    current_plans = pl.DataFrame({"utility": [1.0, 3.0], "n_persons": [1.0, 3.0]})
    current_plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [1, 0],
            "time": [1.0, 0.0],
            "distance": [10.0, 0.0],
            "n_persons": [2.0, 2.0],
            "activity": ["work", "home"],
            "mode": ["car", "stay_home"],
        }
    )
    builder = IterationMetricsBuilder(
        model_loss=FakeLoss(),
        model_trip_count_loss=FakeTripCountLoss(),
        model_entropy=FakeEntropy(),
    )

    row = builder.history_row(
        iteration=2,
        current_plans=current_plans,
        current_plan_steps=current_plan_steps,
        destination_saturation=pl.DataFrame(
            {
                "opportunity_occupation": [100.0, 80.0],
                "opportunity_capacity": [50.0, 100.0],
                "destination_soft_capacity_factor": [1.25, 1.25],
            }
        ),
    )

    class FakeIteration:
        def __init__(self, state):
            self._state = state

        def load_state(self):
            return self._state

    fake_state = SimpleNamespace(
        current_plans=current_plans,
        current_plan_steps=current_plan_steps,
        destination_saturation=pl.DataFrame(),
    )
    fake_iterations = SimpleNamespace(iteration=lambda _: FakeIteration(fake_state))
    rebuilt = builder.rebuild_history(iterations=fake_iterations, resume_from_iteration=2)
    history_store = IterationMetricsHistory(IterationMetricsHistory.from_records([row]).lazy())
    diagnostics = RunDiagnostics(
        SimpleNamespace(
            expected_calibration_plan_steps=object(),
            observed_calibration_plan_steps=object(),
            expected_entropy_plan_steps=object(),
            observed_entropy_plan_steps=object(),
            population_weighted_plan_steps=object(),
            surveys=[],
            is_weekday=True,
            plan_steps=object(),
            iteration_metrics_store=history_store,
        )
    )

    assert row["total_loss"] == pytest.approx(2.5)
    assert row["trip_count_loss"] == pytest.approx(0.5)
    assert row["mean_utility"] == pytest.approx(2.5)
    assert row["mean_trip_count"] == pytest.approx(0.5)
    assert row["mean_travel_time"] == pytest.approx(0.5)
    assert row["mean_travel_distance"] == pytest.approx(5.0)
    assert row["excess_occupation_share"] == pytest.approx((100.0 - 62.5) / 180.0)
    assert [record["iteration"] for record in rebuilt] == [1, 2]
    assert diagnostics.iteration_metrics()["iteration"].to_list() == [2]
    assert diagnostics.loss().history().columns == [
        "iteration",
        "total_loss",
        "trip_count_loss",
        "activity_loss",
        "distance_bin_loss",
        "time_bin_loss",
        "mode_loss",
    ]
    assert diagnostics.trip_count_loss().history().columns == ["iteration", "trip_count_loss"]
    assert diagnostics.entropy().history().columns == ["iteration", "observed_entropy"]
