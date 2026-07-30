from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest

from mobility.reports.theme import MOBILITY_COLORS
from mobility.trips.group_day_trips.core.diagnostics import RunDiagnostics
from mobility.trips.group_day_trips.core.parameters import (
    BehaviorChangePhase,
    BehaviorChangeScope,
    GroupDayTripsBehaviorChangeParameters,
    GroupDayTripsParameters,
)
from mobility.trips.group_day_trips.core.metrics import RunMetrics
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
            "seq_step_index": [1, 2],
            "activity": ["work", "shop"],
            "mode": ["car", "walk"],
            "time": [0.5, 0.25],
            "distance": [5.0, 1.0],
            "duration": [1.0, 0.25],
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
            "seq_step_index": [1, 2],
            "travel_time": [0.5, 0.25],
            "distance": [5.0, 1.0],
            "duration_per_pers": [0.5, 0.25],
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


def test_travel_indicators_by_can_plot_and_save_svg(tmp_path):
    results = _make_results_for_metrics()
    output_path = tmp_path / "travel-indicators.svg"

    comparison, fig = results.metrics.travel_indicators_by(
        variable="mode",
        plot=False,
        save_to_file=True,
        output_path=output_path,
        return_figure=True,
    )

    assert comparison.height > 0
    assert output_path.exists()
    assert fig.layout.barmode == "group"
    assert fig.layout.paper_bgcolor == MOBILITY_COLORS["background"]
    assert fig.layout.plot_bgcolor == MOBILITY_COLORS["background"]
    assert fig.layout.title.text is None
    assert fig.layout.xaxis.showgrid is False
    assert fig.layout.yaxis.gridcolor == MOBILITY_COLORS["grid"]
    assert {trace.name for trace in fig.data} == {"Survey", "Model"}


def test_activity_duration_distribution_weights_model_and_survey_durations():
    plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [1, 1, 1, 1, 1, 0],
            "time_seq_id": [10, 10, 10, 10, 10, 0],
            "home_zone_id": ["z1", "z1", "z1", "z1", "z1", "z1"],
            "seq_step_index": [1, 1, 2, 3, 4, 0],
            "activity": ["work", "work", "home", "shop", "home", "home"],
            "mode": ["car", "car", "walk", "walk", "car", "car"],
            "duration": [4.0, 4.0, 2.0, 1.0, 20.0, 120.0],
            "n_persons": [2.0, 1.0, 2.0, 1.0, 2.0, 5.0],
        }
    )
    survey_plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [1, 1, 1, 1, 1, 0],
            "time_seq_id": [10, 10, 10, 10, 10, 0],
            "home_zone_id": ["z1", "z1", "z1", "z1", "z1", "z1"],
            "seq_step_index": [1, 1, 2, 3, 4, 0],
            "activity": ["work", "work", "home", "shop", "home", "home"],
            "mode": ["car", "car", "walk", "walk", "car", "car"],
            "duration_per_pers": [2.0, 3.0, 1.0, 1.0, 10.0, 24.0],
            "n_persons": [1.0, 2.0, 2.0, 3.0, 2.0, 5.0],
        }
    )
    results = _make_results_for_metrics()
    results.plan_steps = plan_steps.lazy()
    results.population_weighted_plan_steps = survey_plan_steps.lazy()

    distribution = results.metrics.activity_duration_distribution(bin_width_minutes=60, plot=False)

    work_model = distribution.filter((pl.col("source") == "model") & (pl.col("activity") == "work"))
    assert work_model["duration_bin_start"].to_list() == pytest.approx([2.0, 4.0])
    assert work_model["weighted_visits"].to_list() == pytest.approx([2.0, 1.0])
    assert work_model["probability"].to_list() == pytest.approx([2.0 / 3.0, 1.0 / 3.0])

    probability_sums = distribution.group_by(["source", "activity"]).agg(pl.col("probability").sum())
    assert probability_sums["probability"].to_list() == pytest.approx([1.0] * probability_sums.height)

    home_model = distribution.filter((pl.col("source") == "model") & (pl.col("activity") == "home"))
    assert home_model["duration_bin_start"].to_list() == pytest.approx([1.0])


def test_activity_duration_distribution_plot_path_uses_activity_facets(monkeypatch):
    results = _make_results_for_metrics()
    seen = {}

    class FakeFigure:
        def update_yaxes(self, **kwargs):
            seen["yaxes"] = kwargs
            return self

        def update_xaxes(self, **kwargs):
            seen["xaxes"] = kwargs
            return self

        def show(self, renderer):
            seen["renderer"] = renderer

    def fake_line(data_frame, **kwargs):
        seen["data_frame"] = data_frame
        seen["kwargs"] = kwargs
        return FakeFigure()

    monkeypatch.setattr("mobility.trips.group_day_trips.core.metrics.px.line", fake_line)

    distribution = results.metrics.activity_duration_distribution(plot=True)

    assert not distribution.is_empty()
    assert seen["kwargs"]["facet_col"] == "activity"
    assert seen["kwargs"]["color"] == "source"
    assert seen["renderer"] == "browser"


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


def test_travel_costs_plot_uses_report_style_and_mode_colors(monkeypatch, tmp_path):
    class FakeIterationTransportCosts:
        def __init__(self, iteration):
            self.iteration = iteration

        def get_costs_by_od_and_mode(self, columns):
            seen["iteration"] = self.iteration
            seen["columns"] = columns
            return pl.DataFrame(
                {
                    "from": ["z1", "z1"],
                    "to": ["z2", "z2"],
                    "mode": ["car", "bicycle"],
                    "time": [0.5, 0.75],
                    "distance": [10.0, 8.0],
                }
            )

    class FakeFigure:
        def update_layout(self, **kwargs):
            seen.setdefault("layout_updates", []).append(kwargs)
            return self

        def update_xaxes(self, **kwargs):
            seen["xaxes"] = kwargs
            return self

        def update_yaxes(self, **kwargs):
            seen["yaxes"] = kwargs
            return self

        def show(self, renderer):
            seen["renderer"] = renderer

    def fake_scatter(data_frame, **kwargs):
        seen["scatter_frame"] = data_frame
        seen["scatter_kwargs"] = kwargs
        return FakeFigure()

    results = SimpleNamespace(
        parameters=SimpleNamespace(run=SimpleNamespace(n_iterations=3)),
        run=SimpleNamespace(
            iteration_transport_cost_assets=[
                FakeIterationTransportCosts(1),
                FakeIterationTransportCosts(2),
                FakeIterationTransportCosts(3),
            ],
        ),
        transport_zones=SimpleNamespace(get=lambda: pd.DataFrame()),
    )
    seen = {}
    evaluator = TravelCostsEvaluation(results)
    monkeypatch.setattr(
        evaluator,
        "convert_to_dataframe",
        lambda _ref_costs: pl.DataFrame(
            {
                "ref_index": [0, 1],
                "origin": ["A", "A"],
                "destination": ["B", "B"],
                "from": ["z1", "z1"],
                "to": ["z2", "z2"],
                "mode": ["car", "bicycle"],
                "time": [0.4, 0.8],
                "distance": [9.0, 7.5],
            }
        ),
    )
    monkeypatch.setattr(evaluator, "join_transport_zone_ids", lambda frame, _zones: frame)
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.evaluation.travel_costs_evaluation.px.scatter",
        fake_scatter,
    )

    output_path = tmp_path / "travel-costs.svg"
    comparison = evaluator.get(
        [],
        variable="time",
        plot=True,
        save_to_file=True,
        output_path=output_path,
    )

    assert comparison.height == 2
    assert output_path.exists()
    assert seen["iteration"] == 3
    assert seen["columns"] == ["time", "distance"]
    assert seen["scatter_kwargs"]["color"] == "mode"
    assert seen["scatter_kwargs"]["color_discrete_map"] == {
        "car": "#4D4D4D",
        "bicycle": "#0B5A66",
    }
    assert any(update.get("paper_bgcolor") == MOBILITY_COLORS["background"] for update in seen["layout_updates"])
    assert any(update.get("plot_bgcolor") == MOBILITY_COLORS["background"] for update in seen["layout_updates"])
    assert seen["xaxes"]["title_text"] == "Reference travel time (h)"
    assert seen["xaxes"]["showgrid"] is False
    assert seen["yaxes"]["title_text"] == "Model travel time (h)"
    assert seen["yaxes"]["gridcolor"] == MOBILITY_COLORS["grid"]
    assert seen["renderer"] == "browser"


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


def test_modal_share_evolution_by_iteration_returns_share_table(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(current_plan_steps=states[iteration])
            return SimpleNamespace(load_state=lambda: state)

    states = {
        1: pl.DataFrame(
            {
                "activity_seq_id": [1, 2],
                "mode": ["car", "walk"],
                "n_persons": [2.0, 1.0],
            }
        ),
        2: pl.DataFrame(
            {
                "activity_seq_id": [1, 2, 3],
                "mode": ["car", "walk", "bicycle"],
                "n_persons": [1.0, 2.0, 1.0],
            }
        ),
    }
    results = RunResults(
        inputs_hash="runhash",
        is_weekday=True,
        transport_zones=_make_transport_zones(),
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(run=SimpleNamespace(n_iterations=2)),
        run=SimpleNamespace(
            inputs_hash="runhash",
            is_weekday=True,
            cache_path={"plan_steps": tmp_path / "run" / "plan_steps.parquet"},
        ),
    )

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )

    modal_share = results.metrics.modal_share_evolution_by_iteration(
        modes=["walk", "bicycle", "car"],
        plot=False,
    )

    assert modal_share.to_dict(as_series=False) == {
        "iteration": [1, 1, 1, 2, 2, 2],
        "mode": ["walk", "bicycle", "car", "walk", "bicycle", "car"],
        "mode_label": ["Walk", "Bicycle", "Car", "Walk", "Bicycle", "Car"],
        "n_trips": pytest.approx([1.0, 0.0, 2.0, 2.0, 1.0, 1.0]),
        "modal_share": pytest.approx([1.0 / 3.0, 0.0, 2.0 / 3.0, 0.5, 0.25, 0.25]),
    }


def test_modal_share_evolution_by_iteration_groups_public_transport(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            return None

        def iteration(self, iteration):
            state = SimpleNamespace(current_plan_steps=plan_steps)
            return SimpleNamespace(load_state=lambda: state)

    plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [1, 2, 3],
            "mode": [
                "walk/public_transport/walk",
                "bicycle/public_transport/walk",
                "car",
            ],
            "n_persons": [2.0, 1.0, 1.0],
        }
    )
    results = RunResults(
        inputs_hash="runhash",
        is_weekday=True,
        transport_zones=_make_transport_zones(),
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(run=SimpleNamespace(n_iterations=1)),
        run=SimpleNamespace(
            inputs_hash="runhash",
            is_weekday=True,
            cache_path={"plan_steps": tmp_path / "run" / "plan_steps.parquet"},
        ),
    )

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )

    modal_share = results.metrics.modal_share_evolution_by_iteration(
        modes=["walk/public_transport/walk", "car"],
        plot=False,
    )

    assert modal_share.to_dict(as_series=False) == {
        "iteration": [1, 1],
        "mode": ["public_transport", "car"],
        "mode_label": ["Public transport", "Car"],
        "n_trips": pytest.approx([3.0, 1.0]),
        "modal_share": pytest.approx([0.75, 0.25]),
    }


def test_modal_share_evolution_by_iteration_can_use_inner_zones_only(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            return None

        def iteration(self, iteration):
            state = SimpleNamespace(current_plan_steps=plan_steps)
            return SimpleNamespace(load_state=lambda: state)

    plan_steps = pl.DataFrame(
        {
            "home_zone_id": ["z1", "z2", "outer"],
            "activity_seq_id": [1, 2, 3],
            "mode": ["walk", "car", "car"],
            "n_persons": [1.0, 1.0, 8.0],
        }
    )
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2", "outer"],
                "is_inner_zone": [True, True, False],
                "geometry": [None, None, None],
            }
        )
    )
    results = RunResults(
        inputs_hash="runhash",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(run=SimpleNamespace(n_iterations=1)),
        run=SimpleNamespace(
            inputs_hash="runhash",
            is_weekday=True,
            cache_path={"plan_steps": tmp_path / "run" / "plan_steps.parquet"},
        ),
    )

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )

    modal_share = results.metrics.modal_share_evolution_by_iteration(
        modes=["walk", "car"],
        inner_zones_only=True,
        plot=False,
    )

    assert modal_share.to_dict(as_series=False) == {
        "iteration": [1, 1],
        "mode": ["walk", "car"],
        "mode_label": ["Walk", "Car"],
        "n_trips": pytest.approx([1.0, 1.0]),
        "modal_share": pytest.approx([0.5, 0.5]),
    }


def test_modal_share_evolution_by_iteration_can_plot_and_save_svg(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            return None

        def iteration(self, iteration):
            state = SimpleNamespace(current_plan_steps=plan_steps)
            return SimpleNamespace(load_state=lambda: state)

    class FakeFigure:
        def show(self, renderer):
            seen["renderer"] = renderer

    plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [1, 2],
            "mode": ["car", "walk"],
            "n_persons": [1.0, 1.0],
        }
    )
    results = RunResults(
        inputs_hash="runhash",
        is_weekday=True,
        transport_zones=_make_transport_zones(),
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(run=SimpleNamespace(n_iterations=1)),
        run=SimpleNamespace(
            inputs_hash="runhash",
            is_weekday=True,
            cache_path={"plan_steps": tmp_path / "run" / "plan_steps.parquet"},
        ),
    )
    seen = {}

    def fake_plot(modal_share, *, width, height, chart_type):
        seen["plot"] = (modal_share, width, height, chart_type)
        return FakeFigure()

    def fake_save(modal_share, *, output_path, width, height, chart_type):
        seen["save"] = (modal_share, output_path, width, height, chart_type)

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )
    monkeypatch.setattr(results.metrics, "_plot_modal_share_evolution", fake_plot)
    monkeypatch.setattr(results.metrics, "_save_modal_share_evolution_svg", fake_save)

    modal_share, fig = results.metrics.modal_share_evolution_by_iteration(
        plot=True,
        plot_method="notebook",
        save_to_file=True,
        output_path=tmp_path / "modal-share.svg",
        width=700,
        height=420,
        chart_type="line",
        return_figure=True,
    )

    assert seen["renderer"] == "notebook"
    assert seen["plot"][1:] == (700, 420, "line")
    assert seen["save"][1:] == (tmp_path / "modal-share.svg", 700, 420, "line")
    assert modal_share["modal_share"].to_list() == pytest.approx([0.5, 0.5])
    assert isinstance(fig, FakeFigure)


def test_modal_share_delta_evolution_by_iteration_compares_two_scenarios(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(
                current_plan_steps=states[(self.run_inputs_hash, iteration)]
            )
            return SimpleNamespace(load_state=lambda: state)

    current_run = SimpleNamespace(
        inputs_hash="current",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "current" / "plan_steps.parquet"},
    )
    reference_run = SimpleNamespace(
        inputs_hash="reference",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "reference" / "plan_steps.parquet"},
    )
    states = {
        ("current", 1): pl.DataFrame(
            {
                "activity_seq_id": [1, 2],
                "mode": ["car", "walk/public_transport/walk"],
                "n_persons": [2.0, 2.0],
            }
        ),
        ("reference", 1): pl.DataFrame(
            {
                "activity_seq_id": [1, 2],
                "mode": ["car", "walk/public_transport/walk"],
                "n_persons": [3.0, 1.0],
            }
        ),
        ("current", 2): pl.DataFrame(
            {
                "activity_seq_id": [1, 2],
                "mode": ["car", "walk/public_transport/walk"],
                "n_persons": [1.0, 3.0],
            }
        ),
        ("reference", 2): pl.DataFrame(
            {
                "activity_seq_id": [1, 2],
                "mode": ["car", "walk/public_transport/walk"],
                "n_persons": [2.0, 2.0],
            }
        ),
    }
    results = RunResults(
        inputs_hash="current",
        is_weekday=True,
        transport_zones=_make_transport_zones(),
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(run=SimpleNamespace(n_iterations=2)),
        run=current_run,
    )

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )

    delta = results.metrics.modal_share_delta_evolution_by_iteration(
        SimpleNamespace(run=reference_run, demand_groups=pl.DataFrame().lazy()),
        modes=["public_transport", "car"],
        plot=False,
    )

    assert delta.to_dict(as_series=False) == {
        "iteration": [1, 1, 2, 2],
        "mode": ["public_transport", "car", "public_transport", "car"],
        "mode_label": ["Public transport", "Car", "Public transport", "Car"],
        "n_trips": pytest.approx([2.0, 2.0, 3.0, 1.0]),
        "n_trips_reference": pytest.approx([1.0, 3.0, 2.0, 2.0]),
        "n_trips_delta": pytest.approx([1.0, -1.0, 1.0, -1.0]),
        "modal_share": pytest.approx([0.5, 0.5, 0.75, 0.25]),
        "modal_share_reference": pytest.approx([0.25, 0.75, 0.5, 0.5]),
        "modal_share_delta": pytest.approx([0.25, -0.25, 0.25, -0.25]),
    }


def test_modal_share_delta_evolution_by_iteration_can_plot_and_save_svg(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(current_plan_steps=states[self.run_inputs_hash])
            return SimpleNamespace(load_state=lambda: state)

    class FakeFigure:
        def show(self, renderer):
            seen["renderer"] = renderer

    current_run = SimpleNamespace(
        inputs_hash="current",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "current" / "plan_steps.parquet"},
    )
    reference_run = SimpleNamespace(
        inputs_hash="reference",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "reference" / "plan_steps.parquet"},
    )
    states = {
        "current": pl.DataFrame(
            {
                "activity_seq_id": [1],
                "mode": ["car"],
                "n_persons": [1.0],
            }
        ),
        "reference": pl.DataFrame(
            {
                "activity_seq_id": [1],
                "mode": ["walk"],
                "n_persons": [1.0],
            }
        ),
    }
    results = RunResults(
        inputs_hash="current",
        is_weekday=True,
        transport_zones=_make_transport_zones(),
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(run=SimpleNamespace(n_iterations=1)),
        run=current_run,
    )
    seen = {}

    def fake_plot(delta, *, width, height):
        seen["plot"] = (delta, width, height)
        return FakeFigure()

    def fake_save(delta, *, output_path, width, height):
        seen["save"] = (delta, output_path, width, height)

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )
    monkeypatch.setattr(results.metrics, "_plot_modal_share_delta_evolution", fake_plot)
    monkeypatch.setattr(results.metrics, "_save_modal_share_delta_evolution_svg", fake_save)

    delta, fig = results.metrics.modal_share_delta_evolution_by_iteration(
        SimpleNamespace(run=reference_run, demand_groups=pl.DataFrame().lazy()),
        plot=True,
        plot_method="notebook",
        save_to_file=True,
        output_path=tmp_path / "modal-share-delta.svg",
        width=700,
        height=420,
        return_figure=True,
    )

    assert seen["renderer"] == "notebook"
    assert seen["plot"][1:] == (700, 420)
    assert seen["save"][1:] == (tmp_path / "modal-share-delta.svg", 700, 420)
    assert delta["modal_share_delta"].to_list() == pytest.approx([-1.0, 1.0])
    assert isinstance(fig, FakeFigure)


def test_car_modal_share_delta_by_iteration_uses_saved_home_zone_states(monkeypatch, tmp_path):
    """Compare car shares by resident zone for one saved iteration."""

    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash
            self.is_weekday = is_weekday
            self.base_folder = base_folder

        def iteration(self, iteration):
            state = SimpleNamespace(
                current_plan_steps=states[(self.run_inputs_hash, iteration)]
            )
            return SimpleNamespace(load_state=lambda: state)

    current_run = SimpleNamespace(
        inputs_hash="current",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "current" / "plan_steps.parquet"},
    )
    reference_run = SimpleNamespace(
        inputs_hash="reference",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "reference" / "plan_steps.parquet"},
    )
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2", "z3"],
                "is_inner_zone": [True, True, True],
                "geometry": [None, None, None],
            }
        )
    )
    current_demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1, 2],
            "home_zone_id": ["z1", "z2"],
        }
    )
    reference_demand_groups = pl.DataFrame(
        {
            "demand_group_id": [10, 20],
            "home_zone_id": ["z1", "z2"],
        }
    )
    states = {
        ("current", 3): pl.DataFrame(
            {
                "demand_group_id": [1, 1, 2],
                "activity_seq_id": [1, 2, 1],
                "mode": ["car", "walk", "walk"],
                "n_persons": [3.0, 1.0, 2.0],
            }
        ),
        ("reference", 3): pl.DataFrame(
            {
                "demand_group_id": [10, 10, 20],
                "activity_seq_id": [1, 2, 1],
                "mode": ["car", "walk", "car"],
                "n_persons": [1.0, 1.0, 2.0],
            }
        ),
    }
    results = RunResults(
        inputs_hash="current",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=current_demand_groups.lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=current_run,
    )
    reference_results = SimpleNamespace(
        run=reference_run,
        demand_groups=reference_demand_groups.lazy(),
    )

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )

    modal_share_delta = results.metrics.car_modal_share_delta_by_iteration(
        reference_results,
        iteration=3,
        plot=False,
    )

    assert modal_share_delta.to_dict(as_series=False) == {
        "transport_zone_id": ["z1", "z2", "z3"],
        "car_modal_share": pytest.approx([0.75, 0.0, 0.0]),
        "car_modal_share_reference": pytest.approx([0.5, 1.0, 0.0]),
        "car_modal_share_delta": pytest.approx([0.25, -1.0, 0.0]),
    }

    walk_modal_share_delta = results.metrics.modal_share_delta_by_iteration(
        reference_results,
        iteration=3,
        mode="walk",
        plot=False,
    )

    assert walk_modal_share_delta.to_dict(as_series=False) == {
        "transport_zone_id": ["z1", "z2", "z3"],
        "modal_share": pytest.approx([0.25, 1.0, 0.0]),
        "n_trips": pytest.approx([1.0, 2.0, 0.0]),
        "modal_share_reference": pytest.approx([0.5, 0.0, 0.0]),
        "n_trips_reference": pytest.approx([1.0, 0.0, 0.0]),
        "modal_share_delta": pytest.approx([-0.25, 1.0, 0.0]),
        "n_trips_delta": pytest.approx([0.0, 2.0, 0.0]),
        "mode": ["walk", "walk", "walk"],
    }


def test_modal_share_by_iteration_plots_one_scenario(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            return None

        def iteration(self, iteration):
            state = SimpleNamespace(current_plan_steps=plan_steps)
            return SimpleNamespace(load_state=lambda: state)

    class FakeFigure:
        def show(self, renderer):
            seen["renderer"] = renderer

    class FakeTransportZoneMaps:
        def __init__(self, transport_zones, **kwargs):
            seen["transport_zones"] = transport_zones
            seen["map_kwargs"] = kwargs

        def metric(self, values, **kwargs):
            seen["values"] = values
            seen["metric_kwargs"] = kwargs
            return FakeFigure()

    plan_steps = pl.DataFrame(
        {
            "home_zone_id": ["z1", "z1", "z2"],
            "activity_seq_id": [1, 2, 1],
            "mode": ["walk", "car", "walk"],
            "n_persons": [1.0, 1.0, 2.0],
        }
    )
    run = SimpleNamespace(
        inputs_hash="run",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "run" / "plan_steps.parquet"},
        population=SimpleNamespace(name="population"),
    )
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2", "z3"],
                "is_inner_zone": [True, True, False],
                "geometry": [None, None, None],
            }
        )
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=GroupDayTripsParameters(
            behavior_change=GroupDayTripsBehaviorChangeParameters(
                phases=[
                    BehaviorChangePhase(
                        start_iteration=2,
                        scope=BehaviorChangeScope.MODE_REPLANNING,
                    )
                ]
            )
        ),
        run=run,
    )
    seen = {}

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.TransportZoneMaps",
        FakeTransportZoneMaps,
    )

    modal_share, fig = results.metrics.modal_share_by_iteration(
        iteration=2,
        mode="walk",
        plot=True,
        plot_method="notebook",
        return_figure=True,
        inner_zones_only=True,
        color_range=0.8,
        labels=False,
        width=500,
        height=400,
    )

    assert seen["renderer"] == "notebook"
    assert seen["map_kwargs"] == {
        "population": run.population,
        "max_labels": 30,
        "simplify_tolerance": 50.0,
    }
    assert seen["metric_kwargs"]["value_column"] == "modal_share"
    assert seen["metric_kwargs"]["save_name"] == "weekdays-walk-modal-share-iteration-2-map"
    assert seen["metric_kwargs"]["inner_zones_only"] is True
    assert seen["metric_kwargs"]["labels"] is False
    assert seen["metric_kwargs"]["width"] == 500
    assert seen["metric_kwargs"]["height"] == 400
    assert seen["metric_kwargs"]["hover_columns"] == ["n_trips", "mode"]
    assert seen["metric_kwargs"]["legend_label"] == "Walk modal share"
    assert seen["metric_kwargs"]["frame_title"] == "Iteration 2\nMode replanning\nGlobal share: 75.00%"
    assert seen["metric_kwargs"]["classify"] is False
    assert seen["metric_kwargs"]["range_color"] == (0.0, 0.8)
    assert seen["metric_kwargs"]["colorbar_tickformat"] == ".0%"
    assert modal_share.to_dict(as_series=False) == {
        "transport_zone_id": ["z1", "z2", "z3"],
        "modal_share": pytest.approx([0.5, 1.0, 0.0]),
        "n_trips": pytest.approx([1.0, 2.0, 0.0]),
        "mode": ["walk", "walk", "walk"],
    }
    assert seen["values"]["modal_share"].to_list() == pytest.approx([0.5, 1.0, 0.0])
    assert isinstance(fig, FakeFigure)


def test_plot_metric_by_iteration_plots_average_distance(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            return None

        def iteration(self, iteration):
            state = SimpleNamespace(current_plan_steps=plan_steps)
            return SimpleNamespace(load_state=lambda: state)

    class FakeFigure:
        def show(self, renderer):
            seen["renderer"] = renderer

    class FakeTransportZoneMaps:
        def __init__(self, transport_zones, **kwargs):
            seen["transport_zones"] = transport_zones
            seen["map_kwargs"] = kwargs

        def metric(self, values, **kwargs):
            seen["values"] = values
            seen["metric_kwargs"] = kwargs
            return FakeFigure()

    plan_steps = pl.DataFrame(
        {
            "home_zone_id": ["z1", "z2", "z2"],
            "activity_seq_id": [1, 1, 0],
            "mode": ["car", "walk", "walk"],
            "n_persons": [2.0, 2.0, 2.0],
            "distance": [10.0, 8.0, 0.0],
            "time": [0.5, 0.4, 0.0],
        }
    )
    demand_groups = pl.DataFrame(
        {
            "home_zone_id": ["z1", "z2"],
            "n_persons": [2.0, 2.0],
        }
    )
    run = SimpleNamespace(
        inputs_hash="run",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "run" / "plan_steps.parquet"},
        population=SimpleNamespace(name="population"),
    )
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2", "z3"],
                "is_inner_zone": [True, True, False],
                "geometry": [None, None, None],
            }
        )
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=demand_groups.lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=run,
    )
    seen = {}

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.TransportZoneMaps",
        FakeTransportZoneMaps,
    )

    metric, fig = results.metrics.plot_metric_by_iteration(
        iteration=3,
        quantity="distance",
        plot=True,
        plot_method="notebook",
        return_figure=True,
        inner_zones_only=True,
        labels=False,
        width=500,
        height=400,
    )

    assert seen["renderer"] == "notebook"
    assert seen["map_kwargs"] == {
        "population": run.population,
        "max_labels": 30,
        "simplify_tolerance": 50.0,
    }
    assert seen["metric_kwargs"]["value_column"] == "value"
    assert seen["metric_kwargs"]["save_name"] == "weekdays-average-travel-distance-iteration-3-map"
    assert seen["metric_kwargs"]["inner_zones_only"] is True
    assert seen["metric_kwargs"]["labels"] is False
    assert seen["metric_kwargs"]["width"] == 500
    assert seen["metric_kwargs"]["height"] == 400
    assert seen["metric_kwargs"]["hover_columns"] == ["total", "population", "quantity"]
    assert seen["metric_kwargs"]["legend_label"] == "Average travel distance (km/pers.)"
    assert seen["metric_kwargs"]["frame_title"] == "Iteration 3\nFull replanning\nGlobal average: 9.00 km/pers."
    assert seen["metric_kwargs"]["classify"] is False
    assert seen["metric_kwargs"]["color_continuous_scale"][0] == [0.0, "#ffffcc"]
    assert seen["metric_kwargs"]["color_continuous_scale"][-1] == [1.0, "#800026"]
    assert seen["metric_kwargs"]["range_color"] == (8.0, 10.0)
    assert seen["metric_kwargs"]["colorbar_tickformat"] is None
    assert metric.to_dict(as_series=False) == {
        "transport_zone_id": ["z1", "z2", "z3"],
        "value": pytest.approx([10.0, 8.0, 0.0]),
        "total": pytest.approx([20.0, 16.0, 0.0]),
        "population": pytest.approx([2.0, 2.0, 0.0]),
        "quantity": ["distance", "distance", "distance"],
    }
    assert seen["values"]["value"].to_list() == pytest.approx([10.0, 8.0, 0.0])
    assert isinstance(fig, FakeFigure)


def test_plot_metric_color_range_can_clamp_outliers_by_quantile():
    metric = pl.DataFrame({"value": [1.0, 2.0, 3.0, 4.0, 100.0]})

    color_range = RunMetrics._positive_metric_color_range(
        metric,
        None,
        value_column="value",
        clamp_outliers=True,
        outlier_quantile=0.75,
    )

    assert color_range == pytest.approx((2.0, 4.0))


def test_mode_color_map_uses_report_mode_palette():
    colors = RunMetrics._mode_label_color_map(
        [
            "Car",
            "Carpool",
            "Car / public transport / walk",
            "Bicycle / public transport / walk",
            "Walk / public transport / walk",
            "Bicycle",
            "Walk",
        ]
    )

    assert colors == {
        "Car": "#4D4D4D",
        "Carpool": "#8C8C8C",
        "Car / public transport / walk": "#EF4B3E",
        "Bicycle / public transport / walk": "#D7191C",
        "Walk / public transport / walk": "#F06A5A",
        "Bicycle": "#0B5A66",
        "Walk": "#5F7F73",
    }


def test_car_modal_share_delta_by_iteration_plots_with_transport_zone_maps(
    monkeypatch,
    tmp_path,
):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(current_plan_steps=plan_steps)
            return SimpleNamespace(load_state=lambda: state)

    class FakeFigure:
        def show(self, renderer):
            seen["renderer"] = renderer

    class FakeTransportZoneMaps:
        def __init__(self, transport_zones, **kwargs):
            seen["transport_zones"] = transport_zones
            seen["map_kwargs"] = kwargs

        def metric(self, values, **kwargs):
            seen["values"] = values
            seen["metric_kwargs"] = kwargs
            return FakeFigure()

    plan_steps = pl.DataFrame(
        {
            "home_zone_id": ["z1", "z1"],
            "activity_seq_id": [1, 2],
            "mode": ["car", "walk"],
            "n_persons": [1.0, 1.0],
        }
    )
    run = SimpleNamespace(
        inputs_hash="run",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "run" / "plan_steps.parquet"},
        population=SimpleNamespace(name="population"),
    )
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1"],
                "is_inner_zone": [True],
                "geometry": [None],
            }
        )
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=run,
    )
    seen = {}

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.TransportZoneMaps",
        FakeTransportZoneMaps,
    )

    returned_delta, returned_fig = results.metrics.modal_share_delta_by_iteration(
        SimpleNamespace(run=run, demand_groups=pl.DataFrame().lazy()),
        iteration=1,
        mode="walk/public_transport/walk",
        plot=True,
        plot_method="notebook",
        return_figure=True,
        inner_zones_only=True,
        color_range=(-0.03, 0.04),
        labels=False,
        width=500,
        height=400,
    )

    assert seen["renderer"] == "notebook"
    assert seen["map_kwargs"] == {
        "population": run.population,
        "max_labels": 30,
        "simplify_tolerance": 50.0,
    }
    assert seen["metric_kwargs"]["value_column"] == "modal_share_delta"
    assert seen["metric_kwargs"]["save_name"] == "weekdays-walk-public-transport-walk-modal-share-delta-iteration-1-map"
    assert seen["metric_kwargs"]["save_to_file"] is False
    assert seen["metric_kwargs"]["inner_zones_only"] is True
    assert seen["metric_kwargs"]["labels"] is False
    assert seen["metric_kwargs"]["width"] == 500
    assert seen["metric_kwargs"]["height"] == 400
    assert seen["metric_kwargs"]["classify"] is False
    assert seen["metric_kwargs"]["color_continuous_midpoint"] == 0.0
    assert seen["metric_kwargs"]["range_color"] == (-0.03, 0.04)
    assert seen["metric_kwargs"]["colorbar_tickformat"] == ".0%"
    assert seen["metric_kwargs"]["hover_columns"] == [
        "modal_share",
        "modal_share_reference",
        "n_trips",
        "n_trips_reference",
    ]
    assert seen["metric_kwargs"]["legend_label"] == "Walk / public transport / walk modal share difference"
    assert seen["metric_kwargs"]["frame_title"] == "Iteration 1\nFull replanning\nGlobal delta: +0.00%"
    assert seen["values"]["modal_share_delta"].to_list() == pytest.approx([0.0])
    assert returned_delta["modal_share_delta"].to_list() == pytest.approx([0.0])
    assert returned_delta["mode"].to_list() == ["walk/public_transport/walk"]
    assert isinstance(returned_fig, FakeFigure)


def test_plot_delta_by_iteration_can_compare_mean_utility(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(
                current_plans=plans[self.run_inputs_hash],
                current_plan_steps=pl.DataFrame(),
            )
            return SimpleNamespace(load_state=lambda: state)

    current_run = SimpleNamespace(
        inputs_hash="current",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "current" / "plan_steps.parquet"},
    )
    reference_run = SimpleNamespace(
        inputs_hash="reference",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "reference" / "plan_steps.parquet"},
    )
    plans = {
        "current": pl.DataFrame(
            {
                "demand_group_id": [1, 2],
                "utility": [4.0, 1.0],
                "n_persons": [2.0, 1.0],
            }
        ),
        "reference": pl.DataFrame(
            {
                "demand_group_id": [10, 20],
                "utility": [3.0, 2.0],
                "n_persons": [2.0, 1.0],
            }
        ),
    }
    current_demand_groups = pl.DataFrame(
        {"demand_group_id": [1, 2], "home_zone_id": ["z1", "z2"]}
    )
    reference_demand_groups = pl.DataFrame(
        {"demand_group_id": [10, 20], "home_zone_id": ["z1", "z2"]}
    )
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2", "z3"],
                "is_inner_zone": [True, True, True],
                "geometry": [None, None, None],
            }
        )
    )
    results = RunResults(
        inputs_hash="current",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=current_demand_groups.lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=current_run,
    )

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )

    delta = results.metrics.plot_delta_by_iteration(
        SimpleNamespace(run=reference_run, demand_groups=reference_demand_groups.lazy()),
        iteration=1,
        value="mean_utility",
        plot=False,
    )

    assert delta.to_dict(as_series=False) == {
        "transport_zone_id": ["z1", "z2", "z3"],
        "value": pytest.approx([4.0, 1.0, 0.0]),
        "value_reference": pytest.approx([3.0, 2.0, 0.0]),
        "value_delta": pytest.approx([1.0, -1.0, 0.0]),
        "value_name": ["mean_utility", "mean_utility", "mean_utility"],
    }


def test_plot_delta_by_iteration_can_compare_ghg_emissions(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(
                current_plans=pl.DataFrame(),
                current_plan_steps=plan_steps[self.run_inputs_hash],
            )
            return SimpleNamespace(load_state=lambda: state)

    current_run = SimpleNamespace(
        inputs_hash="current",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "current" / "plan_steps.parquet"},
    )
    reference_run = SimpleNamespace(
        inputs_hash="reference",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "reference" / "plan_steps.parquet"},
    )
    plan_steps = {
        "current": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2"],
                "activity_seq_id": [1, 1],
                "from": ["a", "b"],
                "to": ["c", "d"],
                "mode": ["car", "walk"],
                "n_persons": [2.0, 1.0],
            }
        ).with_columns(pl.col("mode").cast(pl.Enum(["car", "walk"]))),
        "reference": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2"],
                "activity_seq_id": [1, 1],
                "from": ["a", "b"],
                "to": ["c", "d"],
                "mode": ["car", "walk"],
                "n_persons": [1.0, 1.0],
            }
        ).with_columns(pl.col("mode").cast(pl.Enum(["car", "walk"]))),
    }
    current_costs = pl.DataFrame(
        {
            "from": ["a", "b"],
            "to": ["c", "d"],
            "mode": ["car", "walk"],
            "ghg_emissions_per_trip": [5.0, 0.0],
        }
    )
    reference_costs = pl.DataFrame(
        {
            "from": ["a", "b"],
            "to": ["c", "d"],
            "mode": ["car", "walk"],
            "ghg_emissions_per_trip": [5.0, 2.0],
        }
    )
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2"],
                "is_inner_zone": [True, True],
                "geometry": [None, None],
            }
        )
    )
    results = RunResults(
        inputs_hash="current",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2"],
                "n_persons": [2.0, 2.0],
            }
        ).lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=current_costs.lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=current_run,
    )

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )

    delta = results.metrics.plot_delta_by_iteration(
        SimpleNamespace(
            run=reference_run,
            demand_groups=pl.DataFrame(
                {
                    "home_zone_id": ["z1", "z2"],
                    "n_persons": [2.0, 2.0],
                }
            ).lazy(),
            costs=reference_costs.lazy(),
        ),
        iteration=1,
        value="ghg_emissions",
        plot=False,
    )

    assert delta.to_dict(as_series=False) == {
        "transport_zone_id": ["z1", "z2"],
        "value": pytest.approx([5.0, 0.0]),
        "value_reference": pytest.approx([2.5, 1.0]),
        "value_delta": pytest.approx([2.5, -1.0]),
        "value_name": ["ghg_emissions", "ghg_emissions"],
    }


def test_plot_delta_by_iteration_can_compare_relative_ghg_emissions(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(
                current_plans=pl.DataFrame(),
                current_plan_steps=plan_steps[self.run_inputs_hash],
            )
            return SimpleNamespace(load_state=lambda: state)

    current_run = SimpleNamespace(
        inputs_hash="current",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "current" / "plan_steps.parquet"},
    )
    reference_run = SimpleNamespace(
        inputs_hash="reference",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "reference" / "plan_steps.parquet"},
    )
    plan_steps = {
        "current": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2"],
                "activity_seq_id": [1, 1],
                "from": ["a", "b"],
                "to": ["c", "d"],
                "mode": ["car", "walk"],
                "n_persons": [2.0, 1.0],
            }
        ),
        "reference": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2"],
                "activity_seq_id": [1, 1],
                "from": ["a", "b"],
                "to": ["c", "d"],
                "mode": ["car", "walk"],
                "n_persons": [1.0, 1.0],
            }
        ),
    }
    current_costs = pl.DataFrame(
        {
            "from": ["a", "b"],
            "to": ["c", "d"],
            "mode": ["car", "walk"],
            "ghg_emissions_per_trip": [5.0, 0.0],
        }
    )
    reference_costs = pl.DataFrame(
        {
            "from": ["a", "b"],
            "to": ["c", "d"],
            "mode": ["car", "walk"],
            "ghg_emissions_per_trip": [5.0, 2.0],
        }
    )
    demand_groups = pl.DataFrame(
        {
            "home_zone_id": ["z1", "z2"],
            "n_persons": [2.0, 2.0],
        }
    )
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2"],
                "is_inner_zone": [True, True],
                "geometry": [None, None],
            }
        )
    )
    results = RunResults(
        inputs_hash="current",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=demand_groups.lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=current_costs.lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=current_run,
    )

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )

    delta = results.metrics.plot_delta_by_iteration(
        SimpleNamespace(
            run=reference_run,
            demand_groups=demand_groups.lazy(),
            costs=reference_costs.lazy(),
        ),
        iteration=1,
        value="ghg_emissions",
        relative_delta=True,
        plot=False,
    )

    assert delta["value_delta"].to_list() == pytest.approx([1.0, -1.0])


def test_plot_delta_by_iteration_rejects_relative_modal_share(tmp_path):
    run = SimpleNamespace(
        inputs_hash="run",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "run" / "plan_steps.parquet"},
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=_make_transport_zones(),
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=run,
    )

    with pytest.raises(ValueError, match="relative_delta=True is not available"):
        results.metrics.plot_delta_by_iteration(
            SimpleNamespace(run=run, demand_groups=pl.DataFrame().lazy()),
            iteration=1,
            value="modal_share",
            relative_delta=True,
            plot=False,
        )


def test_plot_delta_by_iteration_plots_with_transport_zone_maps(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(
                current_plans=pl.DataFrame(
                    {
                        "home_zone_id": ["z1"],
                        "utility": [2.0],
                        "n_persons": [1.0],
                    }
                ),
                current_plan_steps=pl.DataFrame(),
            )
            return SimpleNamespace(load_state=lambda: state)

    class FakeFigure:
        def show(self, renderer):
            seen["renderer"] = renderer

    class FakeTransportZoneMaps:
        def __init__(self, transport_zones, **kwargs):
            seen["transport_zones"] = transport_zones
            seen["map_kwargs"] = kwargs

        def metric(self, values, **kwargs):
            seen["values"] = values
            seen["metric_kwargs"] = kwargs
            return FakeFigure()

    run = SimpleNamespace(
        inputs_hash="run",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "run" / "plan_steps.parquet"},
        population=SimpleNamespace(name="population"),
    )
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1"],
                "is_inner_zone": [True],
                "geometry": [None],
            }
        )
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=run,
    )
    seen = {}

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.TransportZoneMaps",
        FakeTransportZoneMaps,
    )

    delta, fig = results.metrics.plot_delta_by_iteration(
        SimpleNamespace(run=run, demand_groups=pl.DataFrame().lazy()),
        iteration=2,
        value="mean_utility",
        plot=True,
        plot_method="notebook",
        return_figure=True,
        inner_zones_only=True,
        color_range=2.0,
        labels=False,
        width=500,
        height=400,
    )

    assert seen["renderer"] == "notebook"
    assert seen["metric_kwargs"]["value_column"] == "value_delta"
    assert seen["metric_kwargs"]["legend_label"] == "Mean utility difference"
    assert seen["metric_kwargs"]["range_color"] == (-2.0, 2.0)
    assert seen["metric_kwargs"]["inner_zones_only"] is True
    assert seen["metric_kwargs"]["frame_title"] == "Iteration 2\nFull replanning\nGlobal delta: +0.000"
    assert delta["value_delta"].to_list() == pytest.approx([0.0])
    assert isinstance(fig, FakeFigure)


def test_plot_delta_by_iteration_can_auto_cap_outliers(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(
                current_plans=plans[self.run_inputs_hash],
                current_plan_steps=pl.DataFrame(),
            )
            return SimpleNamespace(load_state=lambda: state)

    class FakeTransportZoneMaps:
        def __init__(self, *args, **kwargs):
            return None

        def metric(self, values, **kwargs):
            seen["metric_kwargs"] = kwargs
            return SimpleNamespace(show=lambda _renderer: None)

    current_run = SimpleNamespace(
        inputs_hash="current",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "current" / "plan_steps.parquet"},
    )
    reference_run = SimpleNamespace(
        inputs_hash="reference",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "reference" / "plan_steps.parquet"},
    )
    plans = {
        "current": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2", "z3"],
                "utility": [2.0, 3.0, 20.0],
                "n_persons": [1.0, 1.0, 1.0],
            }
        ),
        "reference": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2", "z3"],
                "utility": [1.0, 2.0, 1.0],
                "n_persons": [1.0, 1.0, 1.0],
            }
        ),
    }
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2", "z3"],
                "is_inner_zone": [True, True, True],
                "geometry": [None, None, None],
            }
        )
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=current_run,
    )
    seen = {}

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.TransportZoneMaps",
        FakeTransportZoneMaps,
    )

    results.metrics.plot_delta_by_iteration(
        SimpleNamespace(run=reference_run, demand_groups=pl.DataFrame().lazy()),
        iteration=1,
        value="mean_utility",
        auto_cap_outliers=True,
        outlier_quantile=0.5,
        plot=True,
    )

    assert seen["metric_kwargs"]["range_color"] == pytest.approx((-1.0, 1.0))


def test_plot_delta_by_iteration_can_average_paired_seed_deltas(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(
                current_plans=plans[self.run_inputs_hash],
                current_plan_steps=pl.DataFrame(),
            )
            return SimpleNamespace(load_state=lambda: state)

    class FakeFigure:
        def show(self, renderer):
            seen["renderer"] = renderer

    class FakeTransportZoneMaps:
        def __init__(self, *args, **kwargs):
            return None

        def metric(self, values, **kwargs):
            seen["values"] = values
            seen["metric_kwargs"] = kwargs
            return FakeFigure()

    def run(name):
        return SimpleNamespace(
            inputs_hash=name,
            is_weekday=True,
            cache_path={"plan_steps": tmp_path / name / "plan_steps.parquet"},
        )

    def context(name):
        return SimpleNamespace(run=run(name), demand_groups=pl.DataFrame().lazy())

    plans = {
        "scenario-0": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2"],
                "utility": [3.0, 1.0],
                "n_persons": [1.0, 1.0],
            }
        ),
        "base-0": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2"],
                "utility": [1.0, 2.0],
                "n_persons": [1.0, 1.0],
            }
        ),
        "scenario-1": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2"],
                "utility": [5.0, 4.0],
                "n_persons": [1.0, 1.0],
            }
        ),
        "base-1": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2"],
                "utility": [1.0, 2.0],
                "n_persons": [1.0, 1.0],
            }
        ),
    }
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2"],
                "is_inner_zone": [True, True],
                "geometry": [None, None],
            }
        )
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=run("scenario-0"),
    )
    seen = {}

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.TransportZoneMaps",
        FakeTransportZoneMaps,
    )

    delta, fig = results.metrics.plot_delta_by_iteration(
        None,
        iteration=1,
        value="mean_utility",
        paired_runs=[
            (context("scenario-0"), context("base-0")),
            (context("scenario-1"), context("base-1")),
        ],
        plot=True,
        plot_method="notebook",
        return_figure=True,
    )

    assert seen["renderer"] == "notebook"
    assert delta["value_delta"].to_list() == pytest.approx([3.0, 0.5])
    assert delta["value"].to_list() == pytest.approx([4.0, 2.5])
    assert delta["value_reference"].to_list() == pytest.approx([1.0, 2.0])
    assert delta["n_pairs"].to_list() == [2, 2]
    assert seen["metric_kwargs"]["hover_columns"] == [
        "value",
        "value_reference",
        "value_delta_std",
        "n_pairs",
    ]
    assert seen["metric_kwargs"]["frame_title"] == "Iteration 1\nFull replanning\nGlobal delta: +1.750"
    assert isinstance(fig, FakeFigure)


def test_modal_share_delta_gifs_by_iteration_writes_one_gif_per_mode(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(current_plan_steps=states[self.run_inputs_hash])
            return SimpleNamespace(load_state=lambda: state)

    current_run = SimpleNamespace(
        inputs_hash="current",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "current" / "plan_steps.parquet"},
        population=SimpleNamespace(name="population"),
    )
    reference_run = SimpleNamespace(
        inputs_hash="reference",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "reference" / "plan_steps.parquet"},
    )
    states = {
        "current": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z1"],
                "activity_seq_id": [1, 2],
                "mode": ["car", "walk"],
                "n_persons": [1.0, 1.0],
            }
        ),
        "reference": pl.DataFrame(
            {
                "home_zone_id": ["z1", "z1"],
                "activity_seq_id": [1, 2],
                "mode": ["walk", "car"],
                "n_persons": [1.0, 1.0],
            }
        ),
    }
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["z1"],
                "is_inner_zone": [True],
                "geometry": [None],
            }
        )
    )
    results = RunResults(
        inputs_hash="runhash",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=current_run,
    )
    reference_results = SimpleNamespace(
        run=reference_run,
        demand_groups=pl.DataFrame().lazy(),
    )
    seen = {"frames": [], "gifs": []}

    def fake_save_frame(**kwargs):
        seen["frames"].append(kwargs)

    def fake_write_gif(frame_paths, output_path, duration_ms):
        seen["gifs"].append((frame_paths, output_path, duration_ms))

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )
    monkeypatch.setattr(results.metrics, "_save_modal_share_delta_frame", fake_save_frame)
    monkeypatch.setattr(results.metrics, "_write_gif", fake_write_gif)

    gif_paths = results.metrics.modal_share_delta_gifs_by_iteration(
        reference_results,
        modes=["walk", "car"],
        iterations=range(1, 3),
        value_type="n_trips_delta",
        inner_zones_only=True,
        color_range=0.25,
        labels=False,
        output_folder=tmp_path,
        duration_ms=250,
    )

    assert set(gif_paths) == {"walk", "car"}
    assert gif_paths["walk"].name == "runhash-weekdays-walk-n-trips-delta-iterations-1-2-map.gif"
    assert gif_paths["car"].name == "runhash-weekdays-car-n-trips-delta-iterations-1-2-map.gif"
    assert len(seen["frames"]) == 4
    assert len(seen["gifs"]) == 2
    assert all(frame["value_type"] == "n_trips_delta" for frame in seen["frames"])
    assert all(frame["inner_zones_only"] is True for frame in seen["frames"])
    assert all(frame["color_range"] == 0.25 for frame in seen["frames"])
    assert [frame["global_delta"] for frame in seen["frames"]] == pytest.approx([0.0, 0.0, 0.0, 0.0])
    assert all(gif_call[2] == 250 for gif_call in seen["gifs"])


def test_modal_share_delta_gif_global_delta_uses_inner_zone_scope(monkeypatch, tmp_path):
    class FakeIterations:
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            self.run_inputs_hash = run_inputs_hash

        def iteration(self, iteration):
            state = SimpleNamespace(current_plan_steps=states[self.run_inputs_hash])
            return SimpleNamespace(load_state=lambda: state)

    current_run = SimpleNamespace(
        inputs_hash="current",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "current" / "plan_steps.parquet"},
        population=None,
    )
    reference_run = SimpleNamespace(
        inputs_hash="reference",
        is_weekday=True,
        cache_path={"plan_steps": tmp_path / "reference" / "plan_steps.parquet"},
    )
    states = {
        "current": pl.DataFrame(
            {
                "home_zone_id": ["inner", "inner", "outer"],
                "activity_seq_id": [1, 2, 1],
                "mode": ["car", "walk", "walk"],
                "n_persons": [2.0, 2.0, 100.0],
            }
        ),
        "reference": pl.DataFrame(
            {
                "home_zone_id": ["inner", "inner", "outer"],
                "activity_seq_id": [1, 2, 1],
                "mode": ["car", "walk", "car"],
                "n_persons": [1.0, 3.0, 100.0],
            }
        ),
    }
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["inner", "outer"],
                "is_inner_zone": [True, False],
                "geometry": [None, None],
            }
        )
    )
    results = RunResults(
        inputs_hash="runhash",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=current_run,
    )
    seen = {"frames": []}

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.Iterations",
        FakeIterations,
    )
    monkeypatch.setattr(
        results.metrics,
        "_save_modal_share_delta_frame",
        lambda **kwargs: seen["frames"].append(kwargs),
    )
    monkeypatch.setattr(results.metrics, "_write_gif", lambda *args, **kwargs: None)

    results.metrics.modal_share_delta_gifs_by_iteration(
        SimpleNamespace(run=reference_run, demand_groups=pl.DataFrame().lazy()),
        modes=["car"],
        iterations=[1],
        inner_zones_only=True,
        output_folder=tmp_path,
    )

    assert [frame["global_delta"] for frame in seen["frames"]] == pytest.approx([0.25])


def test_modal_share_delta_gif_frame_adds_iteration_counter(monkeypatch, tmp_path):
    class FakeTransportZoneMaps:
        def __init__(self, *args, **kwargs):
            return None

        def metric(self, *args, **kwargs):
            seen["metric_kwargs"] = kwargs

    results = RunResults(
        inputs_hash="runhash",
        is_weekday=True,
        transport_zones=SimpleNamespace(get=lambda: pd.DataFrame()),
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=GroupDayTripsParameters(
            behavior_change=GroupDayTripsBehaviorChangeParameters(
                phases=[
                    BehaviorChangePhase(
                        start_iteration=5,
                        scope=BehaviorChangeScope.MODE_REPLANNING,
                    )
                ]
            )
        ),
        run=SimpleNamespace(population=None),
    )
    seen = {}
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.TransportZoneMaps",
        FakeTransportZoneMaps,
    )

    results.metrics._save_modal_share_delta_frame(
        modal_share_delta=pl.DataFrame(
            {
                "transport_zone_id": ["z1"],
                "modal_share": [0.5],
                "modal_share_reference": [0.4],
                "modal_share_delta": [0.1],
                "mode": ["car"],
            }
        ),
        mode="car",
        iteration=7,
        value_type="modal_share_delta",
        global_delta=0.12,
        output_path=tmp_path / "frame.png",
        inner_zones_only=True,
        color_range=0.25,
        labels=False,
        width=500,
        height=400,
        max_labels=30,
        simplify_tolerance=50.0,
    )

    assert seen["metric_kwargs"]["frame_title"] == "Iteration 7\nMode replanning\nGlobal delta: +12.00%"


def test_modal_share_delta_frame_can_plot_trip_count_delta(monkeypatch, tmp_path):
    class FakeTransportZoneMaps:
        def __init__(self, *args, **kwargs):
            return None

        def metric(self, *args, **kwargs):
            seen["metric_kwargs"] = kwargs

    results = RunResults(
        inputs_hash="runhash",
        is_weekday=True,
        transport_zones=SimpleNamespace(get=lambda: pd.DataFrame()),
        demand_groups=pl.DataFrame().lazy(),
        plan_steps=pl.DataFrame().lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=GroupDayTripsParameters(),
        run=SimpleNamespace(population=None),
    )
    seen = {}
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.metrics.TransportZoneMaps",
        FakeTransportZoneMaps,
    )

    results.metrics._save_modal_share_delta_frame(
        modal_share_delta=pl.DataFrame(
            {
                "transport_zone_id": ["z1"],
                "modal_share": [0.5],
                "modal_share_reference": [0.4],
                "modal_share_delta": [0.1],
                "n_trips": [12.0],
                "n_trips_reference": [9.0],
                "n_trips_delta": [3.0],
                "mode": ["car"],
            }
        ),
        mode="car",
        iteration=2,
        value_type="n_trips_delta",
        global_delta=3.0,
        output_path=tmp_path / "frame.png",
        inner_zones_only=True,
        color_range=5.0,
        labels=False,
        width=500,
        height=400,
        max_labels=30,
        simplify_tolerance=50.0,
    )

    assert seen["metric_kwargs"]["value_column"] == "n_trips_delta"
    assert seen["metric_kwargs"]["legend_label"] == "Car trip count difference"
    assert seen["metric_kwargs"]["colorbar_tickformat"] is None
    assert seen["metric_kwargs"]["frame_title"] == "Iteration 2\nFull replanning\nGlobal delta: +3 trips"


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

    demand_groups_path = tmp_path / "demand_groups.parquet"
    weekday_plan_steps = tmp_path / "weekday_plan_steps.parquet"
    weekday_costs = tmp_path / "weekday_costs.parquet"
    pl.DataFrame(
        {
            "home_zone_id": ["z1"],
            "csp": ["A"],
            "n_cars": ["1"],
            "n_persons": [4.0],
        }
    ).write_parquet(demand_groups_path)
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
            "demand_groups": demand_groups_path,
            "plan_steps": weekday_plan_steps,
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

    calibration_steps = to_calibration_plan_steps(raw_plan_steps.lazy()).collect()

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
        expected_plan_steps=_FrameAsset(expected),
        observed_plan_steps=_FrameAsset(observed),
        history=history_store,
    )

    summary = loss.summary()
    history = loss.history()
    history_row = loss.history_row(iteration=2, plan_steps=observed.lazy())

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
            ).lazy()
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

    loss = ModelLoss(expected_plan_steps=_FrameAsset(expected))
    scaled_loss = ModelLoss(expected_plan_steps=_FrameAsset(scaled_expected))

    assert loss.total_loss(plan_steps=observed.lazy()) == pytest.approx(
        scaled_loss.total_loss(plan_steps=scaled_observed.lazy())
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

    comparison = ModelLoss(expected_plan_steps=_FrameAsset(expected)).comparison(plan_steps=observed.lazy())

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
        expected_mobile_steps.lazy(),
        immobility_probabilities=pl.DataFrame({"country": ["fr"], "csp": ["A"], "p_immobility": [0.2]}),
    )
    loss = ModelTripCountLoss(
        expected_plan_steps=expected_mobile_steps.lazy(),
        surveys=[survey],
        is_weekday=True,
        observed_plan_steps=observed_steps.lazy(),
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
    distribution = build_trip_pattern_distribution(raw_plan_steps.lazy())
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
    summary = entropy.summary(plan_steps=raw_plan_steps.lazy())
    history = entropy.history()
    history_row = entropy.history_row(iteration=2, plan_steps=raw_plan_steps.lazy())

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
            population_weighted_plan_steps=pl.DataFrame(
                {"activity_seq_id": [0], "n_persons": [1.0]},
                schema={"activity_seq_id": pl.UInt32, "n_persons": pl.Float64},
            ).lazy(),
            surveys=[],
            is_weekday=True,
            plan_steps=pl.DataFrame(
                {"activity_seq_id": [0], "n_persons": [1.0]},
                schema={"activity_seq_id": pl.UInt32, "n_persons": pl.Float64},
            ).lazy(),
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
