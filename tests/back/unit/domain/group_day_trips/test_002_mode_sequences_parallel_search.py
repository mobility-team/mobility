import json
import pickle
from types import SimpleNamespace

import polars as pl
import pytest
from mobility.trips.group_day_trips.plans.mode_sequence_search.prepare import build_location_chains
from mobility.trips.group_day_trips.plans.mode_sequence_search.search_python import (
    run_python_mode_sequence_search,
    run_python_mode_sequence_search_subprocess,
)
from mobility.trips.group_day_trips.plans.mode_sequence_search.search_rust import (
    run_rust_mode_sequence_search,
)


class _DummyLive:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyProcess:
    def __init__(self, command):
        self.command = command
        self.wait_called = False

    def wait(self):
        self.wait_called = True
        return 0


def _mode_sequence_parameters(
    *,
    k_mode_sequences,
    mode_sequence_search_parallel=True,
):
    return SimpleNamespace(
        mode_sequences=SimpleNamespace(
            k_mode_sequences=k_mode_sequences,
            mode_sequence_search_parallel=mode_sequence_search_parallel,
        )
    )


def test_build_location_chains_fails_on_one_location_chain():
    destination_steps = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "activity_seq_id": [10],
            "time_seq_id": [20],
            "dest_seq_id": [30],
            "seq_step_index": [1],
            "from": [100],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt8,
            "from": pl.UInt16,
        },
    )

    with pytest.raises(ValueError, match="fewer than two locations"):
        build_location_chains(destination_steps)


def test_build_location_chains_fails_when_destination_sequence_id_has_multiple_location_chains():
    destination_steps = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2, 2],
            "demand_subgroup_id": [0, 0, 0, 0],
            "activity_seq_id": [10, 10, 10, 10],
            "time_seq_id": [20, 20, 20, 20],
            "dest_seq_id": [30, 30, 30, 30],
            "seq_step_index": [1, 2, 1, 2],
            "from": [100, 200, 300, 400],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt8,
            "from": pl.UInt16,
        },
    )

    with pytest.raises(ValueError, match="multiple location chains"):
        build_location_chains(destination_steps)


def test_run_python_mode_sequence_search_subprocess_serializes_inputs_for_worker(monkeypatch, tmp_path):
    parameters = _mode_sequence_parameters(k_mode_sequences=7)
    unique_destination_chains = pl.DataFrame({"dest_seq_id": [1], "locations": [[101, 202, 303]]})
    cost_by_origin_destination_mode = {(101, 202, 0): 1200, (202, 303, 1): 900}
    mode_ids_by_leg = {(101, 202): [0, 1], (202, 303): [1]}
    modes_by_name = {"car": {"is_return_mode": False}, "walk": {"is_return_mode": False}}
    tmp_results_path = tmp_path / "tmp_results"
    tmp_results_path.mkdir()

    created_processes = []

    def fake_popen(command):
        process = _DummyProcess(command)
        created_processes.append(process)
        return process

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.plans.mode_sequence_search.search_python.Live",
        lambda *args, **kwargs: _DummyLive(),
    )
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.plans.mode_sequence_search.search_python.subprocess.Popen",
        fake_popen,
    )

    run_python_mode_sequence_search_subprocess(
        parameters=parameters,
        working_folder=tmp_path,
        unique_destination_chains=unique_destination_chains,
        cost_by_origin_destination_mode=cost_by_origin_destination_mode,
        mode_ids_by_leg=mode_ids_by_leg,
        modes_by_name=modes_by_name,
        tmp_folder=tmp_results_path,
    )

    costs_path = tmp_path / "tmp-costs.pkl"
    leg_modes_path = tmp_path / "tmp-leg-modes.pkl"
    modes_path = tmp_path / "modes-props.json"
    location_chains_path = tmp_path / "tmp-location-chains.parquet"

    with open(costs_path, "rb") as file:
        assert pickle.load(file) == cost_by_origin_destination_mode
    with open(leg_modes_path, "rb") as file:
        assert pickle.load(file) == mode_ids_by_leg
    with open(modes_path, encoding="utf-8") as file:
        assert json.load(file) == modes_by_name
    assert pl.read_parquet(location_chains_path).equals(unique_destination_chains)

    process = created_processes[0]
    assert process.wait_called is True
    assert len(created_processes) == 1


def test_run_python_mode_sequence_search_serial_backend_filters_return_modes(monkeypatch, tmp_path):
    captured = {}
    expected = pl.DataFrame(
        {
            "dest_seq_id": [1],
            "mode_seq_index": [0],
            "seq_step_index": [1],
            "location": [2],
            "mode_index": [0],
        }
    )

    def fake_serial_search(
        k_sequences,
        unique_destination_chains,
        cost_by_origin_destination_mode,
        mode_ids_by_leg,
        modes_by_name,
        tmp_folder,
    ):
        captured["k_sequences"] = k_sequences
        captured["unique_destination_chains"] = unique_destination_chains
        captured["cost_by_origin_destination_mode"] = cost_by_origin_destination_mode
        captured["mode_ids_by_leg"] = dict(mode_ids_by_leg)
        captured["modes_by_name"] = modes_by_name
        expected.write_parquet(tmp_folder / "part.parquet")

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.plans.mode_sequence_search.search_python.compute_subtour_mode_probabilities_serial",
        fake_serial_search,
    )

    result = run_python_mode_sequence_search(
        iteration=3,
        parameters=_mode_sequence_parameters(
            k_mode_sequences=4,
            mode_sequence_search_parallel=False,
        ),
        working_folder=tmp_path,
        unique_destination_chains=pl.DataFrame({"dest_seq_id": [1], "locations": [[1, 2]]}),
        leg_mode_costs=pl.DataFrame(
            {
                "from": [1, 1, 2],
                "to": [2, 2, 3],
                "mode_id": [0, 1, 2],
                "cost": [10.0, 12.0, 8.0],
            }
        ),
        modes_by_name={
            "walk": {"is_return_mode": False},
            "car_return": {"is_return_mode": True},
            "bike": {"is_return_mode": False},
        },
        is_return_mode_by_id={0: False, 1: True, 2: False},
    )

    assert result.equals(expected)
    assert captured["k_sequences"] == 4
    assert captured["cost_by_origin_destination_mode"] == {
        (1, 2, 0): 10.0,
        (1, 2, 1): 12.0,
        (2, 3, 2): 8.0,
    }
    assert captured["mode_ids_by_leg"] == {
        (1, 2): [0],
        (2, 3): [2],
    }

def test_run_rust_mode_sequence_search_transforms_inputs_for_package(monkeypatch):
    captured = {}
    expected = pl.DataFrame(
        {
            "dest_seq_id": [1],
            "mode_seq_index": [0],
            "seq_step_index": [1],
            "location": [2],
            "mode_index": [0],
        }
    )

    def fake_search_mode_sequences(**kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setitem(
        __import__("sys").modules,
        "mobility_mode_sequence_search",
        SimpleNamespace(search_mode_sequences=fake_search_mode_sequences),
    )

    result = run_rust_mode_sequence_search(
        unique_destination_chains=pl.DataFrame({"dest_seq_id": [1], "locations": [[1, 2]]}),
        leg_mode_costs=pl.DataFrame({"from": [1], "to": [2], "mode_id": [0], "cost": [1.0]}),
        needs_vehicle_by_id={0: False, 1: True, 2: True},
        return_mode_id_by_id={0: None, 1: 2, 2: None},
        is_return_mode_by_id={0: False, 1: False, 2: True},
        modes_by_name={
            "walk": {
                "vehicle": None,
                "multimodal": False,
                "is_return_mode": False,
                "return_mode": None,
            },
            "carpool": {
                "vehicle": "car",
                "multimodal": True,
                "is_return_mode": False,
                "return_mode": "carpool_return",
            },
            "carpool_return": {
                "vehicle": "car",
                "multimodal": True,
                "is_return_mode": True,
                "return_mode": None,
            },
        },
        mode_name_by_id={0: "walk", 1: "carpool", 2: "carpool_return"},
        k_mode_sequences=7,
    )

    assert result.equals(expected)
    assert captured["location_chain_steps"].equals(
        pl.DataFrame({"dest_seq_id": [1], "locations": [[1, 2]]})
    )
    assert captured["leg_mode_costs"].columns == ["origin", "destination", "mode_id", "cost"]
    assert captured["leg_mode_costs"].row(0) == (1, 2, 0, 1.0)
    mode_metadata = captured["mode_metadata"].sort("mode_id")
    assert mode_metadata.select(["mode_id", "needs_vehicle", "is_return_mode", "return_mode_id"]).to_dict(
        as_series=False
    ) == {
        "mode_id": [0, 1, 2],
        "needs_vehicle": [False, True, True],
        "is_return_mode": [False, False, True],
        "return_mode_id": [None, 2, None],
    }
    assert captured["k_sequences"] == 7


def test_python_and_rust_mode_sequence_backends_match_on_same_inputs(tmp_path):
    unique_destination_chains = pl.DataFrame({"dest_seq_id": [1], "locations": [[1, 2]]})
    leg_mode_costs = pl.DataFrame(
        {
            "from": [1, 2, 1, 2],
            "to": [2, 1, 2, 1],
            "mode_id": [0, 0, 1, 1],
            "cost": [10.0, 10.0, 15.0, 15.0],
        }
    )
    modes_by_name = {
        "walk": {
            "vehicle": None,
            "multimodal": False,
            "is_return_mode": False,
            "return_mode": None,
        },
        "bike": {
            "vehicle": None,
            "multimodal": False,
            "is_return_mode": False,
            "return_mode": None,
        },
    }

    python_rows = run_python_mode_sequence_search(
        iteration=1,
        parameters=_mode_sequence_parameters(
            k_mode_sequences=3,
            mode_sequence_search_parallel=False,
        ),
        working_folder=tmp_path,
        unique_destination_chains=unique_destination_chains,
        leg_mode_costs=leg_mode_costs,
        modes_by_name=modes_by_name,
        is_return_mode_by_id={0: False, 1: False},
    )
    rust_rows = run_rust_mode_sequence_search(
        unique_destination_chains=unique_destination_chains,
        leg_mode_costs=leg_mode_costs,
        needs_vehicle_by_id={0: False, 1: False},
        return_mode_id_by_id={0: None, 1: None},
        is_return_mode_by_id={0: False, 1: False},
        modes_by_name=modes_by_name,
        mode_name_by_id={0: "walk", 1: "bike"},
        k_mode_sequences=3,
    )

    sort_cols = ["dest_seq_id", "mode_seq_index", "seq_step_index", "location", "mode_index"]
    assert python_rows.sort(sort_cols).to_dict(as_series=False) == rust_rows.sort(sort_cols).to_dict(as_series=False)
