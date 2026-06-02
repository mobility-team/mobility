from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from mobility.runtime import Scenarios
from mobility.runtime.assets.file_asset import FileAsset
from mobility.trips.group_day_trips.core.group_day_trips import PopulationGroupDayTrips
from mobility.trips.group_day_trips.results import GroupDayTripsResults


class _FakeRun(FileAsset):
    """Small run asset that writes the tables needed by result table tests."""

    def __init__(
        self,
        *,
        base_folder: Path,
        name: str,
        plan_steps: pl.DataFrame,
        demand_groups: pl.DataFrame,
        iteration_metrics: pl.DataFrame,
        iteration_plan_steps: dict[int, pl.DataFrame] | None = None,
    ) -> None:
        self.frames = {
            "plan_steps": plan_steps,
            "demand_groups": demand_groups,
            "costs": pl.DataFrame({"from": ["z1"], "to": ["z2"], "cost": [1.0]}),
            "opportunities": pl.DataFrame(),
            "transitions": pl.DataFrame(),
            "iteration_metrics": iteration_metrics,
        }
        self._iteration_plan_steps = iteration_plan_steps or {}
        self.n_iterations = max(self._iteration_plan_steps, default=1)
        self.population = SimpleNamespace(transport_zones=None)
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

    def iteration_table(self, table_name: str, iteration: int):
        """Return one fake saved iteration table."""
        if table_name != "plan_steps":
            raise ValueError(f"Fake run has no saved iteration table `{table_name}`.")
        return self._iteration_plan_steps[iteration].lazy()


def _run_factory(base_folder: Path):
    def run(day_type: str, *, scenario: str | None = None, replication: int = 0):
        scenario = "default" if scenario is None else scenario
        distance_shift = 3.0 if scenario == "test" else 0.0
        plan_steps = pl.DataFrame(
            {
                "demand_group_id": [1, 2],
                "activity_seq_id": [1, 1],
                "from": ["z1", "z2"],
                "to": ["z3", "z4"],
                "mode": ["car", "walk"],
                "distance": [
                    10.0 + 10.0 * replication + distance_shift,
                    5.0 + 2.0 * replication + distance_shift,
                ],
                "n_persons": [2.0, 1.0],
            }
        )
        demand_groups = pl.DataFrame(
            {
                "demand_group_id": [1, 2],
                "home_zone_id": ["z1", "z2"],
                "csp": ["worker", "retired"],
                "n_persons": [2.0, 1.0],
            }
        )
        iteration_metrics = pl.DataFrame(
            {
                "iteration": [1, 2],
                "model_loss": [0.5 + replication, 0.25 + replication],
            }
        )
        iteration_plan_steps = {
            1: plan_steps.with_columns(distance=pl.col("distance") * 0.5),
            2: plan_steps,
        }
        return _FakeRun(
            base_folder=base_folder,
            name=f"{day_type}-{scenario}-{replication}",
            plan_steps=plan_steps,
            demand_groups=demand_groups,
            iteration_metrics=iteration_metrics,
            iteration_plan_steps=iteration_plan_steps,
        )

    return run


def test_population_group_day_trips_results_selects_all_replications(tmp_path, monkeypatch):
    """Check that wrapper.results() selects all configured replications by default."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    wrapper = PopulationGroupDayTrips.__new__(PopulationGroupDayTrips)
    wrapper.parameters = SimpleNamespace(run=SimpleNamespace(n_replications=2))
    wrapper.scenarios = Scenarios()
    wrapper.run = _run_factory(tmp_path)

    results = wrapper.results("weekday")

    assert [(context.scenario, context.replication) for context in results.run_contexts] == [
        ("default", 0),
        ("default", 1),
    ]


def test_results_rejects_invalid_scope(tmp_path, monkeypatch):
    """Check scenario and replication validation at the public entry point."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    with pytest.raises(ValueError, match="at least one scenario"):
        GroupDayTripsResults(
            run=_run_factory(tmp_path),
            day_type="weekday",
            scenarios=[],
            n_replications=1,
        )

    with pytest.raises(ValueError, match="duplicate"):
        GroupDayTripsResults(
            run=_run_factory(tmp_path),
            day_type="weekday",
            scenarios=["a", "a"],
            n_replications=1,
        )

    with pytest.raises(ValueError, match="n_replications=1"):
        GroupDayTripsResults(
            run=_run_factory(tmp_path),
            day_type="weekday",
            scenarios=None,
            n_replications=1,
            replication=1,
        )


def test_results_rejects_scenarios_missing_from_manifest(tmp_path, monkeypatch):
    """Check that direct result objects still use the scenario manifest."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    with pytest.raises(ValueError, match="Missing scenarios"):
        GroupDayTripsResults(
            run=_run_factory(tmp_path),
            day_type="weekday",
            scenarios=["unknown"],
            n_replications=1,
            scenario_manifest=Scenarios(),
        )


def test_result_tables_add_scope_columns(tmp_path, monkeypatch):
    """Check raw tables include scenario, day type, iteration, and replication."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    results = GroupDayTripsResults(
        run=_run_factory(tmp_path),
        day_type="weekday",
        scenarios="default",
        n_replications=2,
    )

    table = results.tables.plan_steps().collect()

    assert {
        "scenario",
        "day_type",
        "iteration",
        "replication",
    }.issubset(set(table.columns))
    assert table["scenario"].unique().to_list() == ["default"]
    assert set(table["replication"].unique().to_list()) == {0, 1}
    assert table["iteration"].unique().to_list() == [2]


def test_result_tables_can_include_multiple_scenarios(tmp_path, monkeypatch):
    """Check scenario selection fans out to all requested concrete runs."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    results = GroupDayTripsResults(
        run=_run_factory(tmp_path),
        day_type="weekday",
        scenarios=["default", "test"],
        n_replications=1,
    )

    table = results.tables.plan_steps().collect()

    assert set(table["scenario"].unique().to_list()) == {"default", "test"}
    assert table.filter(pl.col("scenario") == "test")["distance"].sum() > table.filter(
        pl.col("scenario") == "default"
    )["distance"].sum()


def test_result_tables_can_select_saved_iterations(tmp_path, monkeypatch):
    """Check saved iteration selection for plan steps, demand groups, and diagnostics."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    results = GroupDayTripsResults(
        run=_run_factory(tmp_path),
        day_type="weekday",
        scenarios=None,
        n_replications=1,
    )

    plan_steps = results.tables.plan_steps(iterations=[1, 2]).collect()
    demand_groups = results.tables.demand_groups(iterations=[1, 2]).collect()
    diagnostics = results.diagnostics.iteration_metrics(iterations=[1, 2]).collect()

    assert set(plan_steps["iteration"].unique().to_list()) == {1, 2}
    assert set(demand_groups["iteration"].unique().to_list()) == {1, 2}
    assert diagnostics.select("iteration").to_series().to_list() == [1, 2]


def test_result_tables_reject_unavailable_saved_iteration_table(tmp_path, monkeypatch):
    """Check non-persisted tables fail clearly for saved iteration queries."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    results = GroupDayTripsResults(
        run=_run_factory(tmp_path),
        day_type="weekday",
        scenarios=None,
        n_replications=1,
    )

    with pytest.raises(ValueError, match="Saved iteration artifacts"):
        results.tables.costs(iterations=[1]).collect()
