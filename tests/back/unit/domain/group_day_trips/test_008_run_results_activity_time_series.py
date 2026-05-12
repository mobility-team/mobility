from types import SimpleNamespace

from plotly.basedatatypes import BaseFigure
import pytest
import polars as pl

from mobility.trips.group_day_trips.core.results import RunResults


def _make_results(
    plan_steps: pl.DataFrame,
    demand_groups: pl.DataFrame | None = None,
    run: SimpleNamespace | None = None,
    population_weighted_plan_steps: pl.DataFrame | None = None,
) -> RunResults:
    if demand_groups is None:
        demand_groups = pl.DataFrame({"n_persons": [float(plan_steps["n_persons"].sum())]})
    if run is None:
        run = SimpleNamespace()
    if population_weighted_plan_steps is None:
        population_weighted_plan_steps = plan_steps
        if "home_zone_id" not in population_weighted_plan_steps.columns:
            population_weighted_plan_steps = population_weighted_plan_steps.with_columns(home_zone_id=pl.lit(0))
    return RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=SimpleNamespace(get=lambda: None, study_area=SimpleNamespace(get=lambda: None)),
        demand_groups=demand_groups.lazy(),
        plan_steps=plan_steps.lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=population_weighted_plan_steps.lazy(),
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
    series = series.filter(pl.col("source") == "observed").drop("source")

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
        .filter(pl.col("source") == "observed")
        .group_by("time_label")
        .agg(total=pl.col("n_persons").sum())
    )

    assert totals["total"].to_list() == pytest.approx([3.0] * totals.height)


def test_activity_time_series_plot_builds_survey_panel_when_survey_assets_are_available(monkeypatch):
    """Check that plotting can attach a survey-derived comparison panel.

    In plain language: when the results object carries the canonical
    population-weighted survey reference steps with timing fields, the plotting
    entrypoint should prepare both the modeled time series and a survey-derived
    counterpart so the chart can place survey on the left and model on the
    right.
    """
    plan_steps = pl.DataFrame(
        {
            "activity": ["work"],
            "mode": ["car"],
            "n_persons": [2.0],
            "departure_time": [8.0],
            "arrival_time": [8.25],
            "next_departure_time": [9.0],
        }
    )
    demand_groups = pl.DataFrame(
        {
            "country": ["fr"],
            "city_category": ["urban"],
            "csp": ["A"],
            "n_cars": ["1"],
            "n_persons": [2.0],
        }
    )
    population_weighted_plan_steps = pl.DataFrame(
        {
            "country": ["fr"],
            "home_zone_id": [1],
            "city_category": ["urban"],
            "csp": ["A"],
            "n_cars": ["1"],
            "activity_seq_id": [1],
            "time_seq_id": [1],
            "seq_step_index": [0],
            "activity": ["work"],
            "mode": ["car"],
            "is_anchor": [True],
            "departure_time": [8.0],
            "arrival_time": [8.25],
            "next_departure_time": [9.0],
            "duration_per_pers": [0.75],
            "travel_time": [0.25],
            "distance": [10.0],
            "n_persons": [2.0],
        }
    )
    results = _make_results(
        plan_steps,
        demand_groups=demand_groups,
        population_weighted_plan_steps=population_weighted_plan_steps,
    )

    seen = {}

    def fake_plot(df):
        seen["rows"] = df.height
        seen["sources"] = sorted(df["source"].unique().to_list())
        return "figure"

    monkeypatch.setattr(results.metrics, "_plot_activity_time_series", fake_plot)

    series = results.metrics.activity_time_series(interval_minutes=15, plot=True)

    assert seen["rows"] == series.height
    assert seen["sources"] == ["observed", "survey"]
    assert series.filter(pl.col("source") == "survey").height > 0


def test_activity_time_series_can_match_inner_zone_resident_scope():
    """Check that callers can restrict the series to inner-zone residents only.

    In plain language: this should use the same resident subset logic as
    opportunity-based metrics, so people living outside the inner study area do
    not contribute to the returned occupancy totals.
    """
    plan_steps = pl.DataFrame(
        {
            "activity": ["work", "work"],
            "mode": ["car", "car"],
            "n_persons": [2.0, 3.0],
            "departure_time": [8.0, 8.0],
            "arrival_time": [8.25, 8.25],
            "next_departure_time": [9.0, 9.0],
            "home_zone_id": [1, 2],
        }
    )
    demand_groups = pl.DataFrame(
        {
            "home_zone_id": [1, 2],
            "n_persons": [2.0, 3.0],
        }
    )
    transport_zones = SimpleNamespace(
        get=lambda: __import__("pandas").DataFrame(
            {
                "transport_zone_id": [1, 2],
                "is_inner_zone": [True, False],
            }
        ),
        study_area=SimpleNamespace(get=lambda: None),
    )
    results = RunResults(
        inputs_hash="run",
        is_weekday=True,
        transport_zones=transport_zones,
        demand_groups=demand_groups.lazy(),
        plan_steps=plan_steps.lazy(),
        opportunities=pl.DataFrame().lazy(),
        costs=pl.DataFrame().lazy(),
        population_weighted_plan_steps=plan_steps.lazy(),
        transitions=pl.DataFrame().lazy(),
        surveys=[],
        modes=[],
        parameters=SimpleNamespace(),
        run=SimpleNamespace(),
    )

    series = results.metrics.activity_time_series(interval_minutes=15, inner_zone_residents_only=True)
    observed = series.filter(pl.col("source") == "observed")
    totals = observed.group_by("time_label").agg(total=pl.col("n_persons").sum()).sort("time_label")

    assert totals["total"].to_list() == pytest.approx([2.0] * totals.height)


def test_activity_time_series_rejects_intervals_that_do_not_divide_the_day():
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

    with pytest.raises(ValueError, match="interval_minutes must be a positive divisor of 1440"):
        results.metrics.activity_time_series(interval_minutes=7)


def test_plot_activity_time_series_wrapper_passes_scope_and_returns_plot_result(monkeypatch):
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
    expected_series = pl.DataFrame(
        {
            "source": ["observed"],
            "time_bin_start": [0.0],
            "time_label": ["00:00"],
            "label": ["home"],
            "n_persons": [1.0],
        }
    )

    def fake_activity_time_series(*, interval_minutes, inner_zone_residents_only):
        seen["interval_minutes"] = interval_minutes
        seen["inner_zone_residents_only"] = inner_zone_residents_only
        return expected_series

    def fake_plot(df):
        seen["plotted_series"] = df
        return "figure"

    monkeypatch.setattr(results.metrics, "activity_time_series", fake_activity_time_series)
    monkeypatch.setattr(results.metrics, "_plot_activity_time_series", fake_plot)

    figure = results.metrics.plot_activity_time_series(
        interval_minutes=30,
        inner_zone_residents_only=True,
    )

    assert figure == "figure"
    assert seen["interval_minutes"] == 30
    assert seen["inner_zone_residents_only"] is True
    assert seen["plotted_series"].equals(expected_series)


def test_plot_activity_time_series_builds_one_panel_per_available_source(monkeypatch):
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
    time_series = pl.DataFrame(
        {
            "source": ["survey", "survey", "observed", "observed"],
            "time_bin_start": [8.0, 8.25, 8.0, 8.25],
            "time_label": ["08:00", "08:15", "08:00", "08:15"],
            "label": ["work", "mystery", "work", "mystery"],
            "n_persons": [1.0, 0.5, 2.0, 1.5],
        }
    )
    seen = {}

    def fake_show(self, *_args, **_kwargs):
        seen["show_called"] = True

    monkeypatch.setattr(BaseFigure, "show", fake_show, raising=False)

    fig = results.metrics._plot_activity_time_series(time_series)

    assert seen["show_called"] is True
    assert len(fig.data) == 4
    assert fig.layout.barmode == "stack"
    assert list(fig.layout.xaxis.categoryarray) == ["08:00", "08:15"]
    assert list(fig.layout.xaxis2.categoryarray) == ["08:00", "08:15"]


def test_activity_time_series_color_map_uses_fixed_and_fallback_colors():
    color_map = _make_results(
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
    ).metrics._activity_time_series_color_map(["work", "custom_a", "custom_b"])

    assert color_map["work"] == "#355CDE"
    assert color_map["custom_a"] != color_map["custom_b"]
