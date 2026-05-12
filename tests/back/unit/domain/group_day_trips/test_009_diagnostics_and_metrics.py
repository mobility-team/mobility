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
from mobility.trips.group_day_trips.evaluation.model_entropy import ModelEntropy
from mobility.trips.group_day_trips.evaluation.model_loss import ModelLoss
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
    )

    assert occupation["opportunity_occupation"].to_list() == pytest.approx([0.02])
    assert seen["plot_variable"] == "opportunity_occupation"
    assert seen["plot_activity"] == "work"
    assert sorted(seen["masked_values"]) == pytest.approx([0.0, 0.02])
    assert sorted(seen["plotted_values"]) == pytest.approx([1.0, 1.02])


def test_model_loss_summary_history_and_validation():
    expected = pl.DataFrame(
        {
            "activity": ["work", "shop"],
            "distance_bin": ["(1, 5]", "(0, 1]"],
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
                "distance_loss": [0.1],
                "n_trips_loss": [0.05],
                "time_loss": [0.15],
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

    assert summary.height == 3
    assert summary["total_loss"].unique().item() > 0.0
    assert history.columns == ["iteration", "total_loss", "distance_loss", "n_trips_loss", "time_loss"]
    assert history_row["total_loss"] > 0.0
    assert history_row["distance_loss"] >= 0.0

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
                "distance_loss": [0.1],
                "n_trips_loss": [0.05],
                "time_loss": [0.15],
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
                "distance_loss": 0.1,
                "n_trips_loss": 0.2,
                "time_loss": 0.3,
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
    builder = IterationMetricsBuilder(model_loss=FakeLoss(), model_entropy=FakeEntropy())

    row = builder.history_row(
        iteration=2,
        current_plans=current_plans,
        current_plan_steps=current_plan_steps,
    )

    class FakeIteration:
        def __init__(self, state):
            self._state = state

        def load_state(self):
            return self._state

    fake_state = SimpleNamespace(current_plans=current_plans, current_plan_steps=current_plan_steps)
    fake_iterations = SimpleNamespace(iteration=lambda _: FakeIteration(fake_state))
    rebuilt = builder.rebuild_history(iterations=fake_iterations, resume_from_iteration=2)
    history_store = IterationMetricsHistory(IterationMetricsHistory.from_records([row]).lazy())
    diagnostics = RunDiagnostics(
        SimpleNamespace(
            expected_calibration_plan_steps=object(),
            observed_calibration_plan_steps=object(),
            expected_entropy_plan_steps=object(),
            observed_entropy_plan_steps=object(),
            iteration_metrics_store=history_store,
        )
    )

    assert row["mean_utility"] == pytest.approx(2.5)
    assert row["mean_trip_count"] == pytest.approx(0.5)
    assert row["mean_travel_time"] == pytest.approx(0.5)
    assert row["mean_travel_distance"] == pytest.approx(5.0)
    assert [record["iteration"] for record in rebuilt] == [1, 2]
    assert diagnostics.iteration_metrics()["iteration"].to_list() == [2]
    assert diagnostics.loss().history().columns == ["iteration", "total_loss", "distance_loss", "n_trips_loss", "time_loss"]
    assert diagnostics.entropy().history().columns == ["iteration", "observed_entropy"]
