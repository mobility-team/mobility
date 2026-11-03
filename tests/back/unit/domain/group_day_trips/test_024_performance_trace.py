"""Performance tracing must preserve nested loop labels and model behavior."""

import json
from types import SimpleNamespace

import pytest

from mobility.trips.group_day_trips.core.performance_trace import (
    SimulationPerformanceTrace, simulation_step, trace_simulation_run, trace_simulation_step,
)


def test_nested_steps_keep_loop_labels_and_record_failures(tmp_path):
    path = tmp_path / "trace.jsonl"
    with SimulationPerformanceTrace(path) as trace:
        with trace.step("inner batch", None, {"outer_iteration": 2, "inner_batch": 3}):
            with pytest.raises(ValueError):
                with simulation_step("allocation", scenario="project"):
                    raise ValueError("expected test failure")
    child, parent = [json.loads(line) for line in path.read_text().splitlines()]
    assert child["parent_id"] == parent["step_id"]
    assert child["run_id"] == parent["run_id"]
    assert child["outer_iteration"] == 2
    assert child["inner_batch"] == 3
    assert child["scenario"] == "project"
    assert child["status"] == "error"
    assert child["error_type"] == "ValueError"
    assert parent["status"] == "ok"
    assert parent["wall_seconds"] >= child["wall_seconds"] >= 0
    assert parent["rss_peak_bytes"] >= child["rss_peak_bytes"] > 0
    assert child["rss_delta_bytes"] == child["rss_end_bytes"] - child["rss_start_bytes"]


def test_cached_reads_do_not_rewrite_trace_and_new_runs_append(tmp_path, monkeypatch):
    path = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MOBILITY_SIMULATION_TRACE", str(path))

    @trace_simulation_run
    def run(owner):
        owner._cached_result = 42
        with simulation_step("allocation"):
            return owner._cached_result

    owner = SimpleNamespace(_cached_result=None)
    assert run(owner) == 42
    original = path.read_bytes()
    assert run(owner) == 42
    assert path.read_bytes() == original
    assert run(SimpleNamespace(_cached_result=None)) == 42
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(rows) == 4
    assert len({row["run_id"] for row in rows}) == 2


def test_inner_loop_trace_uses_the_outer_iteration_number(tmp_path, monkeypatch):
    monkeypatch.delenv("MOBILITY_SIMULATION_TRACE", raising=False)

    @trace_simulation_step("inner loop")
    def inner(*, iteration, phase="inner"):
        return iteration

    assert inner(iteration=3) == 3
    path = tmp_path / "trace.jsonl"
    with SimulationPerformanceTrace(path) as trace:
        with trace.step("run", None, {}):
            assert inner(iteration=3, phase="inner") == 3
    row = json.loads(path.read_text().splitlines()[0])
    assert row["phase"] == "inner"
    assert row["outer_iteration"] == 3
    assert row["iteration"] == 3


def test_mode_search_gets_its_scenario_from_destination_asset(tmp_path):
    @trace_simulation_step("mode search")
    def search(self):
        return 42

    owner = SimpleNamespace(destination_sequences=SimpleNamespace(scenario="project"))
    path = tmp_path / "trace.jsonl"
    with SimulationPerformanceTrace(path) as trace:
        with trace.step("run", None, {}):
            assert search(owner) == 42
    row = json.loads(path.read_text().splitlines()[0])
    assert row["scenario"] == "project"
