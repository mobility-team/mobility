from types import SimpleNamespace

import pytest
import polars as pl

from mobility.trips.group_day_trips.core.results import RunResults


def _make_results(
    plan_steps: pl.DataFrame,
    demand_groups: pl.DataFrame | None = None,
    run: SimpleNamespace | None = None,
) -> RunResults:
    if demand_groups is None:
        demand_groups = pl.DataFrame({"n_persons": [float(plan_steps["n_persons"].sum())]})
    if run is None:
        run = SimpleNamespace()
    return RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=SimpleNamespace(get=lambda: None, study_area=SimpleNamespace(get=lambda: None)),
        demand_groups=demand_groups.lazy(),
        plan_steps=plan_steps.lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=pl.DataFrame().lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=run,
    )


def test_activity_time_series_uses_average_bin_occupancy_and_splits_transit_by_mode():
    """Check that 15-minute bins average occupancy and keep in-transit states by mode.

    In plain language: if one person spends half of a 15-minute bin travelling by
    car and the whole bin at work, the output should count 0.5 person in
    `in_transit:car` and 0.5 person in `work` for that bin. Any remaining
    people with no explicit out-of-home state in that bin should be counted at
    `home`, so the total always matches the modeled population. Different travel
    modes should appear as separate in-transit labels.
    """
    plan_steps = pl.DataFrame(
        {
            "activity": ["work", "shop"],
            "mode": ["car", "walk"],
            "n_persons": [1.0, 2.0],
            "departure_time": [8.0, 8.25],
            "arrival_time": [8.125, 8.5],
            "next_departure_time": [8.5, 9.0],
        }
    )
    results = _make_results(plan_steps)

    series = results.metrics.activity_time_series(interval_minutes=15)

    at_0800 = series.filter(pl.col("time_label") == "08:00").sort("label")
    assert at_0800["label"].to_list() == ["home", "in_transit:car", "work"]
    occupancy = dict(zip(at_0800["label"].to_list(), at_0800["n_persons"].to_list(), strict=True))
    assert occupancy["home"] == pytest.approx(2.0)
    assert occupancy["in_transit:car"] == pytest.approx(0.5)
    assert occupancy["work"] == pytest.approx(0.5)

    at_0815 = series.filter(pl.col("time_label") == "08:15").sort("label")
    assert at_0815["label"].to_list() == ["in_transit:walk", "work"]
    occupancy = dict(zip(at_0815["label"].to_list(), at_0815["n_persons"].to_list(), strict=True))
    assert occupancy["in_transit:walk"] == pytest.approx(2.0)
    assert occupancy["work"] == pytest.approx(1.0)

    at_0830 = series.filter(pl.col("time_label") == "08:30").sort("label")
    assert at_0830["label"].to_list() == ["home", "shop"]
    occupancy = dict(zip(at_0830["label"].to_list(), at_0830["n_persons"].to_list(), strict=True))
    assert occupancy["home"] == pytest.approx(1.0)
    assert occupancy["shop"] == pytest.approx(2.0)


def test_activity_time_series_completes_missing_home_time_so_totals_stay_constant():
    """Check that the plotted occupancy series adds implicit home time back in.

    In plain language: the plan-step table often stores explicit trips and
    out-of-home activities, but not every home period. The public time-series
    output should fill that residual as `home` so each 15-minute bin still sums
    to the full modeled population.
    """
    plan_steps = pl.DataFrame(
        {
            "activity": ["work", "shop"],
            "mode": ["car", "walk"],
            "n_persons": [1.0, 2.0],
            "departure_time": [8.0, 8.25],
            "arrival_time": [8.125, 8.5],
            "next_departure_time": [8.5, 9.0],
        }
    )
    results = _make_results(plan_steps, demand_groups=pl.DataFrame({"n_persons": [3.0]}))

    totals = (
        results.metrics.activity_time_series(interval_minutes=15)
        .group_by("time_label")
        .agg(total=pl.col("n_persons").sum())
    )

    assert totals["total"].to_list() == pytest.approx([3.0] * totals.height)


def test_activity_time_series_can_trigger_plotting_from_same_entrypoint(monkeypatch):
    """Check that callers can request the plot from the activity_time_series entrypoint.

    In plain language: the same method should still return the table, but when
    `plot=True` it should also call the plotting path so callers can use the
    grouped `results.metrics` API directly for both data and charts.
    """
    results = _make_results(
        pl.DataFrame(
            {
                "activity": ["home"],
                "mode": ["stay_home"],
                "n_persons": [1.0],
                "departure_time": [0.0],
                "arrival_time": [0.0],
                "next_departure_time": [24.0],
            }
        )
    )

    seen = {}

    def fake_plot(df, *, survey_time_series=None):
        seen["rows"] = df.height
        seen["survey_rows"] = None if survey_time_series is None else survey_time_series.height
        return "figure"

    monkeypatch.setattr(results.metrics, "_plot_activity_time_series", fake_plot)

    series = results.metrics.activity_time_series(interval_minutes=15, plot=True)

    assert seen["rows"] == series.height
    assert seen["survey_rows"] is None


