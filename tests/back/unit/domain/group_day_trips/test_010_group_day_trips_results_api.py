from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
import pandas as pd
from plotly.basedatatypes import BaseFigure
import plotly.graph_objects as go
import polars as pl
import pytest
from shapely.geometry import Polygon

from mobility.reports import TransportZoneMaps
from mobility.runtime import Scenario, Scenarios
from mobility.runtime.assets.file_asset import FileAsset
from mobility.trips.group_day_trips.core.group_day_trips import PopulationGroupDayTrips
from mobility.trips.group_day_trips.results import GroupDayTripsResults
from mobility.trips.group_day_trips.results.plots import (
    plot_metric_by_zone,
    plot_metric_grid_by_zone,
)


class _FakeRun(FileAsset):
    """Small run asset that writes the tables needed by result assets."""

    def __init__(
        self,
        *,
        base_folder: Path,
        name: str,
        plan_steps: pl.DataFrame,
        demand_groups: pl.DataFrame,
        costs: pl.DataFrame,
        iteration_metrics: pl.DataFrame,
        opportunities: pl.DataFrame | None = None,
        reference_plan_steps: FileAsset | None = None,
        transport_zones: FileAsset | None = None,
        surveys: list | None = None,
        iteration_plan_steps: dict[int, pl.DataFrame] | None = None,
    ) -> None:
        self.frames = {
            "plan_steps": plan_steps,
            "demand_groups": demand_groups,
            "costs": costs,
            "opportunities": opportunities if opportunities is not None else pl.DataFrame(),
            "transitions": pl.DataFrame(),
            "iteration_metrics": iteration_metrics,
        }
        self._reference_plan_steps = reference_plan_steps
        self._iteration_plan_steps = iteration_plan_steps or {}
        self.parameters = SimpleNamespace(
            run=SimpleNamespace(n_iterations=max(self._iteration_plan_steps, default=1))
        )
        self.population = SimpleNamespace(transport_zones=transport_zones)
        self.surveys = surveys or []
        cache_path = {
            table_name: base_folder / name / f"{table_name}.parquet"
            for table_name in self.frames
        }
        super().__init__({"version": 1, "name": name}, cache_path)

    def get_cached_asset(self):
        """Return lazy parquet readers for the fake run tables."""
        return {
            table_name: pl.scan_parquet(path)
            for table_name, path in self.cache_path.items()
        }

    def create_and_get_asset(self):
        """Write all fake run tables."""
        for table_name, frame in self.frames.items():
            path = self.cache_path[table_name]
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.write_parquet(path)
        return self.get_cached_asset()

    def _get_expected_diagnostics_inputs(self):
        """Return the fake survey reference asset expected by new diagnostics."""
        return SimpleNamespace(population_weighted_plan_steps=self._reference_plan_steps)

    def iteration_table(self, table_name: str, iteration: int):
        """Return one fake saved iteration table."""
        if table_name != "plan_steps":
            raise ValueError(f"Fake run has no saved iteration table `{table_name}`.")
        return self._iteration_plan_steps[iteration].lazy()


class _FakeLazyFrameAsset(FileAsset):
    """Small parquet-backed asset returning one lazy frame."""

    def __init__(self, *, base_folder: Path, name: str, frame: pl.DataFrame) -> None:
        self.frame = frame
        super().__init__({"version": 1, "name": name}, base_folder / f"{name}.parquet")

    def get_cached_asset(self):
        """Return a lazy parquet reader."""
        return pl.scan_parquet(self.cache_path)

    def create_and_get_asset(self):
        """Write the fake frame."""
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.frame.write_parquet(self.cache_path)
        return self.get_cached_asset()


class _FakeTransportZones(FileAsset):
    """Small transport-zone asset with a study-area table."""

    def __init__(self, *, base_folder: Path) -> None:
        self.frame = pd.DataFrame(
            {
                "transport_zone_id": ["z1", "z2"],
                "local_admin_unit_id": ["fr001", "fr001"],
                "is_inner_zone": [True, True],
                "geometry": [None, None],
            }
        )
        self.study_area = SimpleNamespace(
            get=lambda: pd.DataFrame(
                {
                    "local_admin_unit_id": ["fr001"],
                    "country": ["fr"],
                    "geometry": [None],
                }
            )
        )
        super().__init__({"version": 1}, base_folder / "transport_zones.parquet")

    def get_cached_asset(self):
        """Return the transport-zone table."""
        return self.frame

    def create_and_get_asset(self):
        """Write a marker parquet and return the in-memory table."""
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(self.frame.drop(columns="geometry")).write_parquet(self.cache_path)
        return self.frame


def _run_factory(base_folder: Path):
    transport_zones = _FakeTransportZones(base_folder=base_folder)
    reference_plan_steps = _FakeLazyFrameAsset(
        base_folder=base_folder,
        name="survey_reference_plan_steps",
        frame=pl.DataFrame(
            {
                "activity_seq_id": [1, 1],
                "home_zone_id": ["z1", "z2"],
                "country": ["fr", "fr"],
                "activity": ["work", "shop"],
                "mode": ["car", "walk"],
                "travel_time": [1.0, 1.0],
                "distance": [8.0, 4.0],
                "duration_per_pers": [8.0, 1.0],
                "departure_time": [8.0, 10.0],
                "arrival_time": [8.5, 10.25],
                "next_departure_time": [17.0, 11.0],
                "n_persons": [2.0, 1.0],
            }
        ),
    )
    survey = SimpleNamespace(
        inputs={"parameters": SimpleNamespace(country="fr")},
        get=lambda: {
            "p_immobility": pd.DataFrame(
                {"immobility_weekday": [0.2, 0.1], "immobility_weekend": [0.3, 0.2]},
                index=pd.Index(["worker", "retired"], name="csp"),
            )
        },
    )

    def run(day_type: str, *, scenario: str | None = None, replication: int = 0):
        scenario_distance_shift = 3.0 if scenario == "test" else 0.0
        demand_groups = pl.DataFrame(
            {
                "demand_group_id": [1, 2],
                "home_zone_id": ["z1", "z2"],
                "csp": ["worker", "retired"],
                "n_cars": ["1", "0"],
                "n_persons": [2.0, 1.0],
            }
        )
        plan_steps = pl.DataFrame(
            {
                "demand_group_id": [1, 2],
                "activity_seq_id": [1, 1],
                "home_zone_id": ["z1", "z2"],
                "csp": ["worker", "retired"],
                "n_cars": ["1", "0"],
                "from": ["z1", "z2"],
                "to": ["z3", "z4"],
                "mode": ["car", "walk"],
                "activity": ["work", "shop"],
                "distance": [
                    10.0 + 10.0 * replication + scenario_distance_shift,
                    5.0 + 2.0 * replication + scenario_distance_shift,
                ],
                "time": [1.0 + replication, 0.5 + 0.2 * replication],
                "duration_per_pers": [8.5, 0.75],
                "departure_time": [8.0, 10.0],
                "arrival_time": [8.5, 10.25],
                "next_departure_time": [17.0, 11.0],
                "n_persons": [2.0, 1.0],
            }
        )
        iteration_plan_steps = {
            1: plan_steps.with_columns(distance=pl.col("distance") * 0.5).drop(
                ["home_zone_id", "csp", "n_cars"]
            ),
            2: plan_steps.drop(["home_zone_id", "csp", "n_cars"]),
        }
        costs = pl.DataFrame(
            {
                "from": ["z1", "z2"],
                "to": ["z3", "z4"],
                "mode": ["car", "walk"],
                "cost": [2.0 + 2.0 * replication, 1.0 + 2.0 * replication],
                "ghg_emissions_per_trip": [1.0 + replication, 0.5 + 0.5 * replication],
            }
        )
        opportunities = pl.DataFrame(
            {
                "to": ["z3", "z4"],
                "activity": ["work", "shop"],
                "opportunity_capacity": [20.0, 10.0],
            }
        )
        iteration_metrics = pl.DataFrame(
            {
                "iteration": [1, 2],
                "total_loss": [float(replication), float(replication) + 10.0],
            }
        )
        return _FakeRun(
            base_folder=base_folder,
            name=f"{scenario}-{day_type}-{replication}",
            plan_steps=plan_steps,
            demand_groups=demand_groups,
            costs=costs,
            opportunities=opportunities,
            iteration_metrics=iteration_metrics,
            reference_plan_steps=reference_plan_steps,
            transport_zones=transport_zones,
            surveys=[survey],
            iteration_plan_steps=iteration_plan_steps,
        )

    return run


def _results(
    tmp_path: Path,
    *,
    scenarios="default",
    n_replications: int = 2,
    replication=None,
):
    return GroupDayTripsResults(
        run=_run_factory(tmp_path),
        day_type="weekday",
        scenarios=scenarios,
        n_replications=n_replications,
        replication=replication,
    )


def _titled_results(tmp_path: Path):
    return GroupDayTripsResults(
        run=_run_factory(tmp_path),
        day_type="weekday",
        scenarios=["default", "test"],
        n_replications=2,
        scenario_manifest=Scenarios(
            [
                Scenario(name="default", title="Reference"),
                Scenario(name="test", title="Project test"),
            ]
        ),
    )


@pytest.fixture(autouse=True)
def _project_data_folder(tmp_path, monkeypatch):
    """Point result assets at each test's temporary folder."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))


def _record_metric_plot(monkeypatch, name: str) -> list[dict]:
    """Patch one metric plot helper and record all calls."""
    calls = []

    def fake_plot(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return go.Figure()

    monkeypatch.setattr(f"mobility.trips.group_day_trips.results.metrics.{name}", fake_plot)
    return calls


def test_population_group_day_trips_results_selects_all_replications(tmp_path, monkeypatch):
    """Check that the top-level results method selects every configured seed."""
    wrapper = PopulationGroupDayTrips.__new__(PopulationGroupDayTrips)
    wrapper.parameters = SimpleNamespace(run=SimpleNamespace(n_replications=2))
    wrapper.scenarios = Scenarios()
    wrapper.run = _run_factory(tmp_path)

    results = wrapper.results("weekday")

    assert isinstance(results, GroupDayTripsResults)
    assert results.scenarios == ["default"]
    assert results.day_type == "weekday"
    assert results.replications == [0, 1]


def test_results_rejects_empty_and_duplicate_scenarios(tmp_path, monkeypatch):
    """Check that scenario selection fails before any run is built."""

    with pytest.raises(ValueError, match="at least one scenario"):
        GroupDayTripsResults(
            run=_run_factory(tmp_path),
            day_type="weekday",
            scenarios=[],
            n_replications=2,
        )

    with pytest.raises(ValueError, match="duplicate"):
        GroupDayTripsResults(
            run=_run_factory(tmp_path),
            day_type="weekday",
            scenarios=["default", "default"],
            n_replications=2,
        )


def test_results_rejects_scenarios_missing_from_manifest(tmp_path, monkeypatch):
    """Check that direct result objects still use the scenario manifest."""

    with pytest.raises(ValueError, match="Missing scenarios"):
        GroupDayTripsResults(
            run=_run_factory(tmp_path),
            day_type="weekday",
            scenarios=["unknown"],
            n_replications=2,
            scenario_manifest=Scenarios(),
        )


def test_results_object_no_longer_accepts_iterations(tmp_path, monkeypatch):
    """Check iteration selection belongs to tables and metrics."""

    with pytest.raises(TypeError, match="unexpected keyword"):
        GroupDayTripsResults(
            run=_run_factory(tmp_path),
            day_type="weekday",
            scenarios="default",
            iterations=[1, 2],
            n_replications=2,
        )


def test_result_tables_add_scope_columns(tmp_path, monkeypatch):
    """Check that raw tables keep run identity columns after concatenation."""
    results = _results(tmp_path)

    plan_steps = results.tables.plan_steps().sort("replication").collect()
    iteration_metrics = results.diagnostics.iteration_metrics().sort("replication").collect()

    assert plan_steps["scenario"].to_list() == ["default", "default", "default", "default"]
    assert plan_steps["day_type"].to_list() == ["weekday", "weekday", "weekday", "weekday"]
    assert plan_steps["iteration"].to_list() == [2, 2, 2, 2]
    assert plan_steps["replication"].to_list() == [0, 0, 1, 1]
    assert iteration_metrics["iteration"].to_list() == [2, 2]
    assert iteration_metrics["total_loss"].to_list() == [10.0, 11.0]


def test_result_tables_can_include_multiple_scenarios(tmp_path, monkeypatch):
    """Check that one result set can concatenate several scenarios."""
    results = _results(tmp_path, scenarios=["default", "test"])

    plan_steps = results.tables.plan_steps().collect()

    assert results.scenarios == ["default", "test"]
    assert plan_steps["scenario"].sort().to_list() == [
        "default",
        "default",
        "default",
        "default",
        "test",
        "test",
        "test",
        "test",
    ]


def test_result_tables_can_select_saved_iterations(tmp_path, monkeypatch):
    """Check that raw plan steps can be read from saved iteration artifacts."""
    results = _results(tmp_path, replication=0)

    plan_steps = (
        results.tables.plan_steps(iterations=[1, 2])
        .sort(["iteration", "mode"])
        .collect()
    )

    assert plan_steps["iteration"].to_list() == [1, 1, 2, 2]
    assert plan_steps["distance"].to_list() == pytest.approx([5.0, 2.5, 10.0, 5.0])


def test_results_last_iteration_uses_run_parameters(tmp_path, monkeypatch):
    """Check the last iteration comes from parameters.run."""
    results = _results(tmp_path, replication=0)

    assert results.last_iteration == 2


def test_trip_count_metrics_keep_selected_iterations(tmp_path, monkeypatch):
    """Check that metric tables keep iteration when several iterations are selected."""
    results = _results(tmp_path, replication=0)

    trip_count = results.metrics.trip_count(
        by_variable="mode",
        iterations=[1, 2],
    ).sort(["iteration", "mode"])

    assert trip_count["iteration"].to_list() == [1, 1, 2, 2]
    assert trip_count["trip_count"].to_list() == pytest.approx([2.0, 1.0, 2.0, 1.0])


def test_iteration_trip_count_can_group_by_home_zone_from_demand_groups(tmp_path, monkeypatch):
    """Check saved iteration plan steps can use resident zone columns from demand groups."""
    results = _results(tmp_path, replication=0)

    trip_count = results.metrics.trip_count(
        by_zone="home_zone",
        inner_zone_residents_only=True,
        iterations=[1, 2],
    ).sort(["iteration", "home_zone_id"])

    assert trip_count["iteration"].to_list() == [1, 1, 2, 2]
    assert trip_count["home_zone_id"].to_list() == ["z1", "z2", "z1", "z2"]
    assert trip_count["trip_count"].to_list() == pytest.approx([2.0, 1.0, 2.0, 1.0])


def test_iteration_travel_metrics_can_group_by_demand_group_columns(tmp_path, monkeypatch):
    """Check saved iteration travel metrics can use columns stored on demand groups."""
    results = _results(tmp_path, replication=0)

    distance = results.metrics.travel_distance(
        by_zone="home_zone",
        iterations=[1, 2],
    ).sort(["iteration", "home_zone_id"])
    time = results.metrics.travel_time(
        by_variable="csp",
        iterations=[1, 2],
    ).sort(["iteration", "csp"])

    assert distance["home_zone_id"].to_list() == ["z1", "z2", "z1", "z2"]
    assert distance["travel_distance"].to_list() == pytest.approx([10.0, 2.5, 20.0, 5.0])
    assert time["csp"].to_list() == ["retired", "worker", "retired", "worker"]
    assert time["travel_time"].to_list() == pytest.approx([0.5, 2.0, 0.5, 2.0])


def test_iteration_final_state_metrics_use_demand_group_columns(tmp_path, monkeypatch):
    """Check saved iteration final-state metrics can join demand-group dimensions."""
    results = _results(tmp_path, replication=0)

    immobility = results.metrics.immobility(
        iterations=[1, 2],
    ).sort(["iteration", "csp"])
    demand_group = results.metrics.trip_count_by_demand_group(
        iterations=[1, 2],
    ).sort(
        ["iteration", "home_zone_id"]
    )

    assert immobility["iteration"].to_list() == [1, 1, 2, 2]
    assert immobility["csp"].to_list() == ["retired", "worker", "retired", "worker"]
    assert immobility["p_immobility"].to_list() == pytest.approx([0.0, 0.0, 0.0, 0.0])
    assert demand_group["home_zone_id"].to_list() == ["z1", "z2", "z1", "z2"]
    assert demand_group["n_cars"].to_list() == ["1", "0", "1", "0"]
    assert demand_group["n_trips_per_person"].to_list() == pytest.approx([1.0, 1.0, 1.0, 1.0])


def test_compact_trip_count_api_handles_absolute_per_person_and_share(tmp_path, monkeypatch):
    """Check trip count normalization is controlled by normalize_by."""
    results = _results(tmp_path)

    absolute = results.metrics.trip_count(by_variable="mode").sort("mode")
    per_person = results.metrics.trip_count(by_variable="mode", normalize_by="person_count", normalize_scope="study_area").sort("mode")
    share = results.metrics.trip_count(by_variable="mode", normalize_by="metric_total", normalize_scope="study_area").sort("mode")

    assert absolute["trip_count"].to_list() == pytest.approx([2.0, 1.0])
    assert per_person["trip_count_per_person"].to_list() == pytest.approx([2.0 / 3.0, 1.0 / 3.0])
    assert share["trip_count_share"].to_list() == pytest.approx([2.0 / 3.0, 1.0 / 3.0])


def test_metrics_can_group_by_origin_and_destination_zone(tmp_path, monkeypatch):
    """Check explicit zone dimensions select the matching plan-step zone id."""
    results = _results(tmp_path)

    by_origin = results.metrics.trip_count(by_zone="origin_zone").sort("origin_zone_id")
    by_destination = results.metrics.trip_count(by_zone="destination_zone").sort("destination_zone_id")

    assert by_origin["origin_zone_id"].to_list() == ["z1", "z2"]
    assert by_origin["trip_count"].to_list() == pytest.approx([2.0, 1.0])
    assert by_destination["destination_zone_id"].to_list() == ["z3", "z4"]
    assert by_destination["trip_count"].to_list() == pytest.approx([2.0, 1.0])


def test_metrics_can_group_by_origin_destination_pairs(tmp_path, monkeypatch):
    """Check multiple zone dimensions return OD contribution tables."""
    results = _results(tmp_path)

    od_trips = results.metrics.trip_count(
        by_zone=["origin_zone", "destination_zone"],
    ).sort("origin_zone_id")
    od_distance = results.metrics.travel_distance(
        by_zone=["origin_zone", "destination_zone"],
    ).sort("origin_zone_id")

    assert od_trips["origin_zone_id"].to_list() == ["z1", "z2"]
    assert od_trips["destination_zone_id"].to_list() == ["z3", "z4"]
    assert od_trips["trip_count"].to_list() == pytest.approx([2.0, 1.0])
    assert od_distance["travel_distance"].to_list() == pytest.approx([30.0, 6.0])


def test_multi_zone_grouping_rejects_normalization(tmp_path, monkeypatch):
    """Check OD contributions stay absolute until normalization semantics are explicit."""
    results = _results(tmp_path)

    with pytest.raises(ValueError, match="Normalization is not supported"):
        results.metrics.trip_count(
            by_zone=["origin_zone", "destination_zone"],
            normalize_by="metric_total",
            normalize_scope="study_area",
        )

    with pytest.raises(ValueError, match="duplicate"):
        results.metrics.trip_count(by_zone=["origin_zone", "origin_zone"])


def test_zone_person_count_normalization_aligns_zone_id_types(tmp_path, monkeypatch):
    """Check zone normalization works when run tables store zone ids differently."""

    def run(day_type: str, *, scenario: str | None = None, replication: int = 0):
        demand_groups = pl.DataFrame(
            {
                "home_zone_id": ["1", "2"],
                "csp": ["worker", "retired"],
                "n_persons": [2.0, 1.0],
            }
        )
        plan_steps = pl.DataFrame(
            {
                "demand_group_id": [1, 2],
                "activity_seq_id": [1, 1],
                "home_zone_id": [1, 2],
                "csp": ["worker", "retired"],
                "from": [1, 2],
                "to": [3, 4],
                "mode": ["car", "walk"],
                "distance": [10.0, 5.0],
                "time": [1.0, 0.5],
                "n_persons": [2.0, 1.0],
            }
        )
        return _FakeRun(
            base_folder=tmp_path,
            name=f"{scenario}-{day_type}-{replication}",
            plan_steps=plan_steps,
            demand_groups=demand_groups,
            costs=pl.DataFrame({"from": [], "to": [], "mode": []}),
            iteration_metrics=pl.DataFrame({"iteration": [1], "total_loss": [0.0]}),
        )

    results = GroupDayTripsResults(
        run=run,
        day_type="weekday",
        scenarios="default",
        n_replications=1,
    )

    metric = results.metrics.trip_count(
        by_zone="home_zone",
        normalize_by="person_count",
        normalize_scope="zone",
    ).sort("home_zone_id")

    assert metric["home_zone_id"].to_list() == ["1", "2"]
    assert metric["trip_count_per_person"].to_list() == pytest.approx([1.0, 1.0])


def test_metrics_can_filter_to_inner_zone_residents(tmp_path, monkeypatch):
    """Check metrics can ignore residents whose home zone is outside the inner area."""

    class TransportZonesWithOuterZone(_FakeTransportZones):
        """Fake zones where one resident home zone is outside the inner area."""

        def __init__(self, *, base_folder: Path) -> None:
            super().__init__(base_folder=base_folder)
            self.frame["is_inner_zone"] = [True, False]

    def run(day_type: str, *, scenario: str | None = None, replication: int = 0):
        transport_zones = TransportZonesWithOuterZone(base_folder=tmp_path)
        demand_groups = pl.DataFrame(
            {
                "demand_group_id": [1, 2],
                "home_zone_id": ["z1", "z2"],
                "csp": ["worker", "retired"],
                "n_persons": [2.0, 1.0],
            }
        )
        plan_steps = pl.DataFrame(
            {
                "demand_group_id": [1, 2],
                "activity_seq_id": [1, 1],
                "home_zone_id": ["z1", "z2"],
                "csp": ["worker", "retired"],
                "from": ["z1", "z2"],
                "to": ["z3", "z4"],
                "mode": ["car", "walk"],
                "activity": ["work", "shop"],
                "distance": [10.0, 5.0],
                "time": [1.0, 0.5],
                "n_persons": [2.0, 1.0],
            }
        )
        return _FakeRun(
            base_folder=tmp_path,
            name=f"{scenario}-{day_type}-{replication}",
            plan_steps=plan_steps,
            demand_groups=demand_groups,
            costs=pl.DataFrame(
                {
                    "from": ["z1", "z2"],
                    "to": ["z3", "z4"],
                    "mode": ["car", "walk"],
                    "cost": [2.0, 1.0],
                    "ghg_emissions_per_trip": [1.0, 0.5],
                }
            ),
            iteration_metrics=pl.DataFrame({"iteration": [1], "total_loss": [0.0]}),
            transport_zones=transport_zones,
        )

    results = GroupDayTripsResults(
        run=run,
        day_type="weekday",
        scenarios="default",
        n_replications=1,
    )

    trips = results.metrics.trip_count(
        by_zone=["origin_zone", "destination_zone"],
        inner_zone_residents_only=True,
    )
    distance = results.metrics.travel_distance(
        normalize_by="person_count",
        normalize_scope="study_area",
        inner_zone_residents_only=True,
    )

    assert trips["origin_zone_id"].to_list() == ["z1"]
    assert trips["destination_zone_id"].to_list() == ["z3"]
    assert trips["trip_count"].to_list() == pytest.approx([2.0])
    assert distance["travel_distance_per_person"].to_list() == pytest.approx([10.0])


def test_compact_travel_distance_api_uses_public_travel_distance_name(tmp_path, monkeypatch):
    """Check travel distance replaces the old distance public name."""
    results = _results(tmp_path)

    by_mode = results.metrics.travel_distance(by_variable="mode", normalize_by="person_count", normalize_scope="study_area").sort("mode")
    by_mode_from_generic_metric = results.metrics.metric(
        "travel_distance",
        by_variable="mode",
        normalize_by="person_count",
        normalize_scope="study_area",
    ).sort("mode")
    by_csp = results.metrics.travel_distance(
        by_variable="csp",
        normalize_by="person_count",
        normalize_scope="study_area",
    ).sort("csp")
    total = results.metrics.travel_distance()

    assert "travel_distance_per_person" in by_mode.columns
    assert by_mode["travel_distance_per_person"].to_list() == pytest.approx([10.0, 2.0])
    assert by_mode_from_generic_metric.equals(by_mode)
    assert by_csp["travel_distance_per_person"].to_list() == pytest.approx([2.0, 10.0])
    assert total["travel_distance"].to_list() == pytest.approx([36.0])


def test_compact_time_cost_and_emissions_metrics(tmp_path, monkeypatch):
    """Check non-trip quantities use the same compact metric API."""
    results = _results(tmp_path)

    travel_time = results.metrics.travel_time(normalize_by="person_count", normalize_scope="study_area")
    cost = results.metrics.cost(normalize_by="person_count", normalize_scope="study_area")
    emissions = results.metrics.ghg_emissions(normalize_by="person_count", normalize_scope="study_area")

    assert travel_time["travel_time_per_person"].to_list() == pytest.approx([3.6 / 3.0])
    assert cost["cost_per_person"].to_list() == pytest.approx([(5.0 / 3.0 + 11.0 / 3.0) / 2.0])
    assert emissions["ghg_emissions_per_person"].to_list() == pytest.approx([(2.5 / 3.0 + 5.0 / 3.0) / 2.0])


def test_metrics_can_include_survey_reference(tmp_path, monkeypatch):
    """Check supported quantities can opt into external references."""
    results = _results(tmp_path)

    distance = results.metrics.travel_distance(
        by_variable="mode",
        normalize_by="person_count", normalize_scope="study_area",
        reference="external",
    ).sort("mode")
    share = results.metrics.trip_count(
        by_variable="mode",
        normalize_by="metric_total", normalize_scope="study_area",
        reference="external",
    ).sort("mode")
    overall = results.metrics.travel_time(
        normalize_by="person_count",
        normalize_scope="study_area",
        reference="external",
    )
    trip_count = results.metrics.trip_count(
        normalize_by="person_count",
        normalize_scope="study_area",
        reference="external",
    )
    trip_count_values = results.metrics.trip_count(
        by_variable="mode",
        reference="external",
        reference_view="values",
    ).sort(["series", "mode"])
    trip_count_without_reference = results.metrics.trip_count(
        normalize_by="person_count",
        normalize_scope="study_area",
    )
    inner_zone_reference_plot = results.metrics.trip_count(
        by_variable="mode",
        output="plot",
        inner_zone_residents_only=True,
        iterations="last",
        reference="external",
    )
    reference_values_plot = results.metrics.trip_count(
        by_variable="activity",
        output="plot",
        reference="external",
        reference_view="values",
    )

    assert "country" not in distance.columns
    assert distance["travel_distance_per_person"].to_list() == pytest.approx([10.0, 2.0])
    assert distance["travel_distance_per_person_reference"].to_list() == pytest.approx([16.0 / 3.0, 4.0 / 3.0])
    assert share["trip_count_share_reference"].to_list() == pytest.approx([2.0 / 3.0, 1.0 / 3.0])
    assert overall["travel_time_per_person_reference"].to_list() == pytest.approx([1.0])
    assert trip_count["trip_count_per_person"].to_list() == pytest.approx(
        trip_count_without_reference["trip_count_per_person"].to_list()
    )
    assert trip_count["trip_count_per_person_reference"].to_list() == pytest.approx([1.0])
    assert trip_count_values["series"].to_list() == ["model", "model", "reference", "reference"]
    assert trip_count_values["reference_source"].to_list() == [None, None, "survey", "survey"]
    assert trip_count_values["trip_count"].to_list() == pytest.approx([2.0, 1.0, 2.0, 1.0])
    assert isinstance(inner_zone_reference_plot, BaseFigure)
    reference_traces = [
        trace
        for trace in reference_values_plot.data
        if trace.name == "Reference"
    ]
    assert len(reference_traces) == 1
    assert reference_traces[0].marker.color == "#8A8F98"
    assert "pattern" not in reference_traces[0].marker.to_plotly_json()


def test_external_reference_person_count_uses_reference_population(tmp_path, monkeypatch):
    """Check external per-person metrics use the population represented by the reference."""

    class TransportZonesWithOuterZone(_FakeTransportZones):
        """Fake zones where one model home zone is outside the reference area."""

        def __init__(self, *, base_folder: Path) -> None:
            super().__init__(base_folder=base_folder)
            self.frame["is_inner_zone"] = [True, False]

    transport_zones = TransportZonesWithOuterZone(base_folder=tmp_path)
    reference_plan_steps = _FakeLazyFrameAsset(
        base_folder=tmp_path,
        name="outer_scope_reference_plan_steps",
        frame=pl.DataFrame(
            {
                "activity_seq_id": [1, 1],
                "home_zone_id": ["z1", "z2"],
                "country": ["fr", "fr"],
                "activity": ["work", "shop"],
                "mode": ["car", "walk"],
                "travel_time": [1.0, 1.0],
                "distance": [8.0, 4.0],
                "n_persons": [2.0, 8.0],
            }
        ),
    )

    def run(day_type: str, *, scenario: str | None = None, replication: int = 0):
        demand_groups = pl.DataFrame(
            {
                "demand_group_id": [1, 2],
                "home_zone_id": ["z1", "z2"],
                "csp": ["worker", "retired"],
                "n_persons": [2.0, 8.0],
            }
        )
        plan_steps = pl.DataFrame(
            {
                "demand_group_id": [1, 2],
                "activity_seq_id": [1, 1],
                "home_zone_id": ["z1", "z2"],
                "csp": ["worker", "retired"],
                "from": ["z1", "z2"],
                "to": ["z3", "z4"],
                "mode": ["car", "walk"],
                "activity": ["work", "shop"],
                "distance": [10.0, 5.0],
                "time": [1.0, 0.5],
                "n_persons": [2.0, 8.0],
            }
        )
        return _FakeRun(
            base_folder=tmp_path,
            name=f"{scenario}-{day_type}-{replication}",
            plan_steps=plan_steps,
            demand_groups=demand_groups,
            costs=pl.DataFrame({"from": [], "to": [], "mode": []}),
            iteration_metrics=pl.DataFrame({"iteration": [1], "total_loss": [0.0]}),
            reference_plan_steps=reference_plan_steps,
            transport_zones=transport_zones,
        )

    results = GroupDayTripsResults(
        run=run,
        day_type="weekday",
        scenarios="default",
        n_replications=1,
    )

    trip_count = results.metrics.trip_count(
        normalize_by="person_count",
        normalize_scope="study_area",
        reference="external",
    )

    assert trip_count["trip_count_per_person"].to_list() == pytest.approx([1.0])
    assert trip_count["trip_count_per_person_reference"].to_list() == pytest.approx([1.0])


def test_metrics_can_use_scenario_reference(tmp_path, monkeypatch):
    """Check metric tables can compare scenarios without keeping reference rows."""
    results = _results(tmp_path, scenarios=["default", "test"])

    distance = results.metrics.travel_distance(
        by_variable="mode",
        reference=("scenario", "default"),
    ).sort("mode")

    assert distance["scenario"].to_list() == ["test", "test"]
    assert distance["mode"].to_list() == ["car", "walk"]
    assert distance["reference_source"].to_list() == ["scenario:default", "scenario:default"]
    assert distance["travel_distance"].to_list() == pytest.approx([36.0, 9.0])
    assert distance["travel_distance_reference"].to_list() == pytest.approx([30.0, 6.0])
    assert distance["gap"].to_list() == pytest.approx([6.0, 3.0])
    assert distance["gap_std"].to_list() == pytest.approx([0.0, 0.0])
    assert distance["relative_gap"].to_list() == pytest.approx([0.2, 0.5])

    distance_values = results.metrics.travel_distance(
        by_variable="mode",
        reference=("scenario", "default"),
        reference_view="values",
    ).sort(["series", "scenario", "mode"])

    assert distance_values["scenario"].to_list() == ["test", "test", "default", "default"]
    assert distance_values["series"].to_list() == ["model", "model", "reference", "reference"]
    assert distance_values["reference_source"].to_list() == [
        None,
        None,
        "scenario:default",
        "scenario:default",
    ]
    assert distance_values["travel_distance"].to_list() == pytest.approx([36.0, 9.0, 30.0, 6.0])


def test_iteration_metrics_can_use_scenario_reference(tmp_path, monkeypatch):
    """Check scenario references match rows by iteration."""
    results = _results(tmp_path, scenarios=["default", "test"], replication=0)

    distance = results.metrics.travel_distance(
        by_variable="mode",
        iterations=[1, 2],
        reference=("scenario", "default"),
    ).sort(["iteration", "mode"])

    assert distance["scenario"].to_list() == ["test", "test", "test", "test"]
    assert distance["iteration"].to_list() == [1, 1, 2, 2]
    assert distance["mode"].to_list() == ["car", "walk", "car", "walk"]
    assert distance["travel_distance"].to_list() == pytest.approx([13.0, 4.0, 26.0, 8.0])
    assert distance["travel_distance_reference"].to_list() == pytest.approx([10.0, 2.5, 20.0, 5.0])
    assert distance["gap"].to_list() == pytest.approx([3.0, 1.5, 6.0, 3.0])

    distance_values_plot = results.metrics.travel_distance(
        by_variable="mode",
        iterations=[1, 2],
        reference=("scenario", "default"),
        reference_view="values",
        output="plot",
    )

    assert isinstance(distance_values_plot, BaseFigure)


def test_activity_and_bin_dimensions_are_available(tmp_path, monkeypatch):
    """Check activity, distance bins, and time bins are analysis dimensions."""
    results = _results(tmp_path)

    by_activity = results.metrics.travel_distance(
        by_variable="activity",
        normalize_by="person_count", normalize_scope="study_area",
        reference="external",
    ).sort("activity")
    by_distance_bin = results.metrics.trip_count(by_variable="distance_bin").sort("distance_bin")
    by_time_bin = results.metrics.travel_time(
        by_variable="time_bin",
        normalize_by="person_count", normalize_scope="study_area",
        reference="external",
    )

    assert by_activity["activity"].to_list() == ["shop", "work"]
    assert by_activity["travel_distance_per_person"].to_list() == pytest.approx([2.0, 10.0])
    assert by_distance_bin["trip_count"].sum() == pytest.approx(3.0)
    assert "travel_time_per_person_reference" in by_time_bin.columns


def test_immobility_and_demand_group_metrics_remain_special_methods(tmp_path, monkeypatch):
    """Check special result products keep their explicit names."""
    results = _results(tmp_path)

    immobility = results.metrics.immobility(reference="external").sort("csp")
    demand_group = results.metrics.trip_count_by_demand_group().sort("home_zone_id")

    assert immobility["p_immobility_reference"].to_list() == pytest.approx([0.1, 0.2])
    assert demand_group["n_trips_per_person"].to_list() == pytest.approx([1.0, 1.0])


def test_sparse_dimension_groups_count_missing_seed_as_zero(tmp_path, monkeypatch):
    """Check that a group missing in one seed still contributes a zero."""

    def run(day_type: str, *, scenario: str | None = None, replication: int = 0):
        demand_groups = pl.DataFrame(
            {
                "home_zone_id": ["z1", "z2"],
                "csp": ["worker", "retired"],
                "n_persons": [2.0, 1.0],
            }
        )
        mode = ["car", "walk"] if replication == 0 else ["car", "car"]
        plan_steps = pl.DataFrame(
            {
                "activity_seq_id": [1, 1],
                "home_zone_id": ["z1", "z2"],
                "csp": ["worker", "retired"],
                "from": ["z1", "z2"],
                "to": ["z3", "z4"],
                "mode": mode,
                "distance": [10.0, 5.0],
                "time": [1.0, 0.5],
                "n_persons": [2.0, 1.0],
            }
        )
        return _FakeRun(
            base_folder=tmp_path,
            name=f"{scenario}-{day_type}-{replication}",
            plan_steps=plan_steps,
            demand_groups=demand_groups,
            costs=pl.DataFrame({"from": [], "to": [], "mode": []}),
            iteration_metrics=pl.DataFrame({"iteration": [1], "total_loss": [0.0]}),
        )

    results = GroupDayTripsResults(
        run=run,
        day_type="weekday",
        scenarios="default",
        n_replications=2,
    )

    share = results.metrics.trip_count(by_variable="mode", normalize_by="metric_total", normalize_scope="study_area").sort("mode")

    assert share["mode"].to_list() == ["car", "walk"]
    assert share["trip_count_share"].to_list() == pytest.approx([5.0 / 6.0, 1.0 / 6.0])


def test_activity_duration_distribution_and_time_series_are_model_only_by_default(tmp_path, monkeypatch):
    """Check time-distribution products can opt into survey rows."""
    results = _results(tmp_path)

    duration = results.metrics.activity_duration_distribution(bin_width_minutes=60)
    duration_with_external = results.metrics.activity_duration_distribution(
        reference="external",
        bin_width_minutes=60,
    )
    time_series = results.metrics.activity_time_series(interval_minutes=60)
    time_series_with_external = results.metrics.activity_time_series(
        reference="external",
        interval_minutes=60,
    )

    assert {"model"} == set(duration["source"].to_list())
    assert {"model", "survey"} == set(duration_with_external["source"].to_list())
    assert {"model"} == set(time_series["source"].to_list())
    assert {"model", "survey"} == set(time_series_with_external["source"].to_list())


def test_activity_duration_distribution_and_time_series_can_plot(tmp_path, monkeypatch):
    """Check time-distribution products can return figures."""
    results = _results(tmp_path)

    duration_fig = results.metrics.activity_duration_distribution(
        bin_width_minutes=60,
        output="plot",
        width=640,
        height=360,
    )
    time_series_fig = results.metrics.activity_time_series(
        interval_minutes=60,
        output="plot",
        width=640,
        height=360,
    )

    assert isinstance(duration_fig, BaseFigure)
    assert duration_fig.layout.title.text == "Activity duration distribution"
    assert duration_fig.layout.width == 640
    assert isinstance(time_series_fig, BaseFigure)
    assert time_series_fig.layout.title.text == "Activity time series"
    assert time_series_fig.layout.width == 640


def test_opportunity_occupation_returns_table(tmp_path, monkeypatch):
    """Check opportunity occupation keeps capacities with modeled duration."""
    results = _results(tmp_path)

    occupation = results.metrics.opportunity_occupation().sort("activity")

    assert occupation["transport_zone_id"].to_list() == ["z4", "z3"]
    assert occupation["activity"].to_list() == ["shop", "work"]
    assert occupation["duration"].to_list() == pytest.approx([0.75, 17.0])
    assert occupation["opportunity_occupation"].to_list() == pytest.approx([0.075, 0.85])
    assert occupation["n_replications"].to_list() == [2, 2]


def test_opportunity_occupation_plot_uses_zone_helpers(tmp_path, monkeypatch):
    """Check opportunity occupation can be mapped by activity or as one activity."""
    results = _results(tmp_path)
    zone_calls = _record_metric_plot(monkeypatch, "plot_metric_by_zone")
    grid_calls = _record_metric_plot(monkeypatch, "plot_metric_grid_by_zone")

    grid_fig = results.metrics.opportunity_occupation(output="plot")
    zone_fig = results.metrics.opportunity_occupation(activity="work", output="plot")

    assert isinstance(grid_fig, BaseFigure)
    assert isinstance(zone_fig, BaseFigure)
    assert grid_calls[0]["kwargs"]["metric"] == "opportunity_occupation"
    assert grid_calls[0]["kwargs"]["variable_column"] == "activity"
    assert zone_calls[0]["args"][1]["activity"].unique().to_list() == ["work"]


def test_metric_output_plot_returns_report_figure(tmp_path, monkeypatch):
    """Check compact metric methods can return Plotly figures."""
    results = _results(tmp_path)

    fig = results.metrics.travel_distance(
        by_variable="mode",
        normalize_by="person_count", normalize_scope="study_area",
        reference="external",
        output="plot",
        width=640,
        height=360,
    )

    assert isinstance(fig, BaseFigure)
    assert fig.layout.width == 640
    assert fig.layout.height == 360
    assert fig.layout.title.text == "Gap to reference: Travel distance per person by mode"
    assert all(trace.type == "bar" for trace in fig.data)
    assert [trace.name for trace in fig.data] == ["default"]
    assert list(fig.data[0].x) == ["car", "walk"]
    assert list(fig.data[0].y) == pytest.approx([10.0 - 16.0 / 3.0, 2.0 - 4.0 / 3.0])


def test_metric_plots_include_all_scenarios_by_default(tmp_path, monkeypatch):
    """Check that plots use every scenario in the result set by default."""
    results = _results(tmp_path, scenarios=["default", "test"])

    fig = results.metrics.travel_distance(by_variable="mode", normalize_by="person_count", normalize_scope="study_area", output="plot")

    assert [trace.name for trace in fig.data] == ["default", "test"]
    assert all(trace.type == "bar" for trace in fig.data)
    assert list(fig.data[0].y) == pytest.approx([10.0, 2.0])
    assert list(fig.data[1].y) == pytest.approx([12.0, 3.0])


def test_metric_plot_uses_line_chart_for_multiple_iterations(tmp_path, monkeypatch):
    """Check metric plots use iteration lines when several iterations are selected."""
    results = _results(tmp_path)

    fig = results.metrics.travel_distance(
        by_variable="mode",
        iterations=[1, 2],
        output="plot",
        width=640,
        height=360,
    )

    assert isinstance(fig, BaseFigure)
    assert fig.layout.title.text == "Travel distance by mode"
    assert fig.layout.xaxis.title.text == "Iteration"
    assert all(trace.type == "scatter" for trace in fig.data)
    ribbon_traces = [trace for trace in fig.data if trace.fill == "toself"]
    line_traces = [trace for trace in fig.data if trace.mode == "lines+markers"]
    assert len(ribbon_traces) == 2
    assert all(not trace.showlegend for trace in ribbon_traces)
    assert [trace.name for trace in line_traces] == ["car", "walk"]
    assert all(trace.error_y.to_plotly_json() == {} for trace in line_traces)
    assert list(line_traces[0].x) == [1, 2]
    assert list(line_traces[0].y) == pytest.approx([15.0, 30.0])
    assert list(line_traces[1].x) == [1, 2]
    assert list(line_traces[1].y) == pytest.approx([3.0, 6.0])


def test_metric_plot_uses_gap_for_scenario_reference(tmp_path, monkeypatch):
    """Check scenario-reference plots show the gap to the reference scenario."""
    results = _results(tmp_path, scenarios=["default", "test"])

    fig = results.metrics.travel_distance(
        by_variable="mode",
        reference=("scenario", "default"),
        output="plot",
    )

    assert isinstance(fig, BaseFigure)
    assert fig.layout.title.text == "Gap to reference: Travel distance by mode"
    assert all(trace.type == "bar" for trace in fig.data)
    assert [trace.name for trace in fig.data] == ["test"]
    assert list(fig.data[0].x) == ["car", "walk"]
    assert list(fig.data[0].y) == pytest.approx([6.0, 3.0])


def test_metric_plots_use_scenario_titles_when_available(tmp_path, monkeypatch):
    """Check plot legends show scenario titles while tables keep scenario names."""
    results = _titled_results(tmp_path)

    table = results.metrics.travel_distance(by_variable="mode")
    fig = results.metrics.travel_distance(by_variable="mode", output="plot")

    assert table["scenario"].unique(maintain_order=True).to_list() == ["default", "test"]
    assert [trace.name for trace in fig.data] == ["Reference", "Project test"]


def test_metric_plots_can_filter_scenarios(tmp_path, monkeypatch):
    """Check that plot methods can select one scenario from the result set."""
    results = _results(tmp_path, scenarios=["default", "test"])

    fig = results.metrics.trip_count(
        by_variable="mode",
        normalize_by="metric_total", normalize_scope="study_area",
        output="plot",
        scenarios="test",
    )

    assert [trace.name for trace in fig.data] == ["test"]


def test_home_zone_metric_plot_uses_map_helper(tmp_path, monkeypatch):
    """Check home-zone metric plots are maps over loaded scenarios."""
    results = _results(tmp_path, scenarios=["default", "test"])
    calls = _record_metric_plot(monkeypatch, "plot_metric_by_zone")

    fig = results.metrics.travel_distance(
        by_zone="home_zone",
        normalize_by="person_count", normalize_scope="zone",
        output="plot",
        width=640,
        height=360,
    )
    results.metrics.travel_distance(
        by_zone="home_zone",
        normalize_by="person_count", normalize_scope="zone",
        output="plot",
    )

    assert isinstance(fig, BaseFigure)
    assert len(calls) == 2
    assert calls[0]["args"][0] is calls[1]["args"][0]
    assert calls[0]["kwargs"]["metric"] == "travel_distance_per_person"
    assert calls[0]["kwargs"]["zone_column"] == "home_zone_id"
    assert calls[0]["kwargs"]["title"] == "Travel distance per person by home zone"
    assert calls[0]["kwargs"]["width"] == 640
    assert calls[0]["kwargs"]["height"] == 360
    assert calls[0]["args"][1]["scenario"].unique(maintain_order=True).to_list() == ["default", "test"]
    assert "home_zone_id" in calls[0]["args"][1].columns


def test_zone_variable_metric_plot_uses_grid_map_helper(tmp_path, monkeypatch):
    """Check zone-variable metric plots are scenario x variable map grids."""
    results = _results(tmp_path, scenarios=["default", "test"])
    calls = _record_metric_plot(monkeypatch, "plot_metric_grid_by_zone")

    fig = results.metrics.trip_count(
        by_zone="home_zone",
        by_variable="mode",
        normalize_by="metric_total",
        normalize_scope="zone",
        output="plot",
        width=640,
        height=360,
    )

    assert isinstance(fig, BaseFigure)
    assert calls[0]["kwargs"]["metric"] == "trip_count_share"
    assert calls[0]["kwargs"]["zone_column"] == "home_zone_id"
    assert calls[0]["kwargs"]["variable_column"] == "mode"
    assert calls[0]["kwargs"]["title"] == "Trip count share by home zone and mode"
    assert calls[0]["kwargs"]["width"] == 640
    assert calls[0]["kwargs"]["height"] == 360


def test_reference_gap_map_uses_diverging_colors(tmp_path, monkeypatch):
    """Check map plots of reference gaps use a zero-centered diverging scale."""
    results = _results(tmp_path, scenarios=["default", "test"])
    calls = _record_metric_plot(monkeypatch, "plot_metric_grid_by_zone")

    fig = results.metrics.trip_count(
        by_variable="mode",
        by_zone="home_zone",
        normalize_by="metric_total",
        normalize_scope="zone",
        output="plot",
        inner_zone_residents_only=True,
        iterations="last",
        reference=("scenario", "default"),
    )

    assert isinstance(fig, BaseFigure)
    assert calls[0]["kwargs"]["metric"] == "gap"
    assert calls[0]["kwargs"]["title"] == "Gap to reference: Trip count share by home zone and mode"
    assert calls[0]["kwargs"]["diverging_center"] == 0.0
    assert calls[0]["args"][1]["scenario"].unique(maintain_order=True).to_list() == ["test"]


def test_origin_destination_metric_plot_uses_flow_map_helper(tmp_path, monkeypatch):
    """Check OD metric plots use the flow-map helper with default top-N filtering."""
    results = _results(tmp_path, scenarios=["default", "test"])
    calls = _record_metric_plot(monkeypatch, "plot_metric_flows_by_zone")

    fig = results.metrics.ghg_emissions(
        by_zone=["origin_zone", "destination_zone"],
        output="plot",
        width=640,
        height=360,
        n_largest=50,
        min_value=1.0,
    )

    assert isinstance(fig, BaseFigure)
    assert calls[0]["kwargs"]["metric"] == "ghg_emissions"
    assert calls[0]["kwargs"]["origin_column"] == "origin_zone_id"
    assert calls[0]["kwargs"]["destination_column"] == "destination_zone_id"
    assert calls[0]["kwargs"]["title"] == "Ghg emissions by origin zone and destination zone"
    assert calls[0]["kwargs"]["width"] == 640
    assert calls[0]["kwargs"]["height"] == 360
    assert calls[0]["kwargs"]["scenario_titles"] == {}
    assert calls[0]["kwargs"]["n_largest"] == 50
    assert calls[0]["kwargs"]["min_value"] == 1.0
    assert calls[0]["kwargs"]["min_share"] is None
    assert calls[0]["kwargs"]["max_line_width"] == 8.0
    assert calls[0]["kwargs"]["min_line_width"] == 0.1


def test_home_zone_metric_map_facets_scenarios():
    """Check the home-zone map helper creates one map facet per scenario."""
    zones = gpd.GeoDataFrame(
        {
            "transport_zone_id": ["z1", "z2"],
            "is_inner_zone": [True, True],
            "geometry": [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
            ],
        },
        crs=4326,
    )
    maps = TransportZoneMaps(zones)
    values = pl.DataFrame(
        {
            "scenario": ["default", "default", "test", "test"],
            "day_type": ["weekday", "weekday", "weekday", "weekday"],
            "home_zone_id": ["z1", "z2", "z1", "z2"],
            "travel_distance_per_person": [1.0, 2.0, 3.0, 4.0],
            "travel_distance_per_person_std": [0.1, 0.2, 0.3, 0.4],
            "n_replications": [2, 2, 2, 2],
        }
    )

    fig = plot_metric_by_zone(
        maps,
        values,
        metric="travel_distance_per_person",
        zone_column="home_zone_id",
        title="Travel distance per person by home zone",
        scenario_titles={"default": "Reference", "test": "Project test"},
        width=500,
        height=300,
    )

    assert isinstance(fig, BaseFigure)
    assert fig.layout.width == 1240
    assert fig.layout.height == 620
    assert len(fig.data) == 2
    assert [annotation.text for annotation in fig.layout.annotations] == ["Reference", "Project test"]


def test_zone_variable_metric_map_uses_variable_rows_and_scenario_columns():
    """Check the zone-variable map helper puts variables in rows."""
    seen = {}

    class FakeMaps:
        """Small map facade that records the requested grid layout."""

        def metric_grid(self, values, **kwargs):
            """Record the grid call and return a figure."""
            seen["values"] = values
            seen["kwargs"] = kwargs
            return go.Figure()

    values = pl.DataFrame(
        {
            "scenario": ["default", "test", "default", "test"],
            "home_zone_id": ["z1", "z1", "z2", "z2"],
            "mode": ["car", "car", "walk", "walk"],
            "trip_count_share": [0.7, 0.6, 0.3, 0.4],
            "trip_count_share_std": [0.01, 0.02, 0.03, 0.04],
        }
    )

    fig = plot_metric_grid_by_zone(
        FakeMaps(),
        values,
        metric="trip_count_share",
        zone_column="home_zone_id",
        variable_column="mode",
        title="Trip count share by home zone and mode",
        width=640,
        height=360,
    )

    assert isinstance(fig, BaseFigure)
    assert seen["kwargs"]["row_column"] == "mode"
    assert seen["kwargs"]["column_column"] == "scenario"
    assert seen["kwargs"]["value_column"] == "trip_count_share"
    assert seen["kwargs"]["hover_columns"] == ["trip_count_share_std"]
    assert seen["kwargs"]["range_color"] == (0.0, 1.0)
    assert seen["values"]["transport_zone_id"].to_list() == ["z1", "z1", "z2", "z2"]


def test_reference_gap_metric_map_uses_zero_centered_diverging_scale():
    """Check gap map helpers use blue-white-red colors centered on zero."""
    seen = {}

    class FakeMaps:
        """Small map facade that records the requested color settings."""

        def metric_grid(self, values, **kwargs):
            """Record the grid call and return a figure."""
            seen["kwargs"] = kwargs
            return go.Figure()

    values = pl.DataFrame(
        {
            "scenario": ["test", "test"],
            "home_zone_id": ["z1", "z2"],
            "mode": ["car", "walk"],
            "gap": [-0.25, 0.5],
            "gap_std": [0.01, 0.02],
        }
    )

    fig = plot_metric_grid_by_zone(
        FakeMaps(),
        values,
        metric="gap",
        zone_column="home_zone_id",
        variable_column="mode",
        title="Gap to reference: trip count share",
        diverging_center=0.0,
    )

    assert isinstance(fig, BaseFigure)
    assert seen["kwargs"]["color_continuous_midpoint"] == 0.0
    assert seen["kwargs"]["color_continuous_scale"] == [
        [0.0, "#2166AC"],
        [0.5, "#FFFFFF"],
        [1.0, "#B2182B"],
    ]
    assert seen["kwargs"]["range_color"] == (-0.5, 0.5)


def test_results_do_not_expose_parallel_analysis_namespaces(tmp_path, monkeypatch):
    """Check figures and references are requested from metric methods."""
    results = _results(tmp_path)

    assert not hasattr(results, "plots")
    assert not hasattr(results.diagnostics, "survey")


def test_metric_arguments_are_validated(tmp_path, monkeypatch):
    """Check bad query arguments fail with clear errors."""
    results = _results(tmp_path)

    with pytest.raises(ValueError, match="table"):
        results.metrics.trip_count(by_variable="mode", output="figure")

    with pytest.raises(ValueError, match="normalize_by"):
        results.metrics.trip_count(normalize_by="share")

    with pytest.raises(ValueError, match="by_variable"):
        results.metrics.trip_count(by_variable="bad")

    with pytest.raises(ValueError, match="metric_total"):
        results.metrics.trip_count(normalize_by="metric_total", normalize_scope="study_area")

    with pytest.raises(ValueError, match="normalize_scope"):
        results.metrics.trip_count(by_variable="mode", normalize_by="person_count")

    with pytest.raises(TypeError, match="unexpected keyword"):
        results.metrics.trip_count(by="mode")

    with pytest.raises(ValueError, match="No reference"):
        results.metrics.cost(by_variable="mode", reference="external")

    with pytest.raises(ValueError, match="reference should"):
        results.metrics.trip_count(by_variable="mode", reference="survey")

    with pytest.raises(ValueError, match="Missing scenarios"):
        results.metrics.trip_count(by_variable="mode", output="plot", scenarios="other")


def test_single_replication_keeps_multi_seed_schema(tmp_path, monkeypatch):
    """Check that one seed still returns the multi-seed schema."""
    results = _results(tmp_path, replication=0)

    metric = results.metrics.travel_distance(normalize_by="person_count", normalize_scope="study_area")

    assert metric["travel_distance_per_person"].to_list() == pytest.approx([25.0 / 3.0])
    assert metric["travel_distance_per_person_std"].to_list() == [None]
    assert metric["n_replications"].to_list() == [1]


def test_results_rejects_invalid_replication(tmp_path, monkeypatch):
    """Check that invalid seed indices fail before any run is built."""

    with pytest.raises(ValueError, match="n_replications=2"):
        GroupDayTripsResults(
            run=_run_factory(tmp_path),
            day_type="weekday",
            scenarios="default",
            n_replications=2,
            replications=[2],
        )
