import polars as pl
import pytest

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.trips.group_day_trips.core.run import Run
from mobility.trips.group_day_trips.evaluation.calibration_plan_steps import ObservedCalibrationPlanSteps
from mobility.trips.group_day_trips.evaluation.trip_pattern_distribution import ObservedTripPatternDistribution


def test_run_table_reads_a_named_output(monkeypatch):
    run = object.__new__(Run)
    monkeypatch.setattr(run, "get", lambda: {"plan_steps": pl.DataFrame({"plan_id": [3]}).lazy()})
    table = run.table("plan_steps")
    assert isinstance(table, pl.LazyFrame)
    assert table.collect().to_dict(as_series=False) == {"plan_id": [3]}
    with pytest.raises(ValueError, match="Unknown run output table"):
        run.table("unknown")


def test_run_table_reads_a_selected_iteration(monkeypatch):
    run = object.__new__(Run)
    monkeypatch.setattr(run, "iteration_table", lambda name, iteration: pl.DataFrame({"iteration": [iteration]}).lazy())
    table = run.table("plan_steps", iteration=2)
    assert isinstance(table, pl.LazyFrame)
    assert table.collect()["iteration"].to_list() == [2]


def test_observed_diagnostics_read_plan_steps_through_table_interface(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    class FakeRun(InMemoryAsset):
        def __init__(self):
            super().__init__({"name": "run"})
            self.requests = []

        def table(self, name):
            self.requests.append(name)
            return pl.DataFrame({
                "activity_seq_id": [1], "time_seq_id": [1], "seq_step_index": [0],
                "activity": ["work"], "mode": ["car"], "distance": [10.0],
                "time": [0.5], "n_persons": [2.0],
            }).lazy()

    run = FakeRun()
    calibration = ObservedCalibrationPlanSteps(run=run, is_weekday=True).get().collect()
    patterns = ObservedTripPatternDistribution(run=run, is_weekday=True).get().collect()

    assert calibration["n_persons"].to_list() == [2.0]
    assert patterns["n_persons"].to_list() == [2.0]
    assert run.requests == ["plan_steps", "plan_steps"]
