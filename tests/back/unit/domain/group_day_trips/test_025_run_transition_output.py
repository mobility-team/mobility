from types import SimpleNamespace

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from mobility.trips.group_day_trips.core.run import Run
from mobility.trips.group_day_trips.transitions.transition_schema import TRANSITION_EVENT_SCHEMA


def test_final_transition_output_streams_all_events_in_iteration_order(tmp_path, monkeypatch):
    tables = []
    states = []
    for iteration in [1, 2, 3]:
        values = {
            name: ([False, True] if dtype == pl.Boolean else
                   [f'[{iteration}, 10, 20]', None] if dtype == pl.String else
                   [iteration, iteration + 1])
            for name, dtype in TRANSITION_EVENT_SCHEMA.items()
        }
        table = pl.DataFrame(values, schema=TRANSITION_EVENT_SCHEMA)
        table = table.with_columns(pl.lit(iteration, dtype=pl.UInt32).alias("iteration"))
        path = tmp_path / f"events_{iteration}.parquet"
        table.write_parquet(path)
        tables.append(table)
        states.append(SimpleNamespace(transition_events_asset=SimpleNamespace(cache_path=path)))
    run = SimpleNamespace(
        parameters=SimpleNamespace(outputs=SimpleNamespace(cache_iteration_events=True)),
        iteration_state_assets=states,
        cache_path={name: tmp_path / f"{name}.parquet" for name in [
            "plan_steps", "opportunities", "costs", "transitions", "demand_groups", "iteration_metrics"
        ]},
    )
    # The final table must not use eager parquet reads before writing.
    with monkeypatch.context() as context:
        def eager_read_forbidden(*args, **kwargs):
            raise AssertionError("Final transition assembly must stay lazy")
        context.setattr(pl, "read_parquet", eager_read_forbidden)
        transitions = Run._build_transitions(run)
        assert isinstance(transitions, pl.LazyFrame)
        small_table = pl.DataFrame({"value": [1]})
        Run._write_outputs(run, transitions=transitions, **{
            name: small_table for name in [
                "plan_steps", "opportunities", "costs", "demand_groups", "iteration_metrics"
            ]
        })
    assert_frame_equal(pl.read_parquet(run.cache_path["transitions"]), pl.concat(tables))


def test_final_transition_scan_checks_cached_schema(tmp_path):
    path = tmp_path / "old_events.parquet"
    pl.DataFrame({"iteration": [1]}).write_parquet(path)
    run = SimpleNamespace(
        parameters=SimpleNamespace(outputs=SimpleNamespace(cache_iteration_events=True)),
        iteration_state_assets=[SimpleNamespace(transition_events_asset=SimpleNamespace(cache_path=path))],
    )
    with pytest.raises(RuntimeError, match="Missing columns"):
        Run._build_transitions(run)


def test_disabled_transition_output_retains_empty_schema():
    run = SimpleNamespace(parameters=SimpleNamespace(outputs=SimpleNamespace(cache_iteration_events=False)))
    table = Run._build_transitions(run)
    assert table.schema == TRANSITION_EVENT_SCHEMA
    assert table.height == 0
