import json
import pathlib
import pickle

import polars as pl

from mobility.trips.group_day_trips.plans.mode_sequences import ModeSequences


class _DummyLive:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

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


def test_run_parallel_search_serializes_inputs_and_invokes_worker(monkeypatch, tmp_path):
    mode_sequences = object.__new__(ModeSequences)
    mode_sequences.parameters = type("Params", (), {"k_mode_sequences": 7})()

    unique_location_chains = pl.DataFrame(
        {
            "dest_seq_id": [1],
            "locations": [[101, 202, 303]],
        }
    )
    costs = {
        (101, 202, 0): 1200,
        (202, 303, 1): 900,
    }
    leg_modes = {
        (101, 202): [0, 1],
        (202, 303): [1],
    }
    modes = {
        "car": {"is_return_mode": False},
        "walk": {"is_return_mode": False},
    }
    tmp_results_path = tmp_path / "tmp_results"
    tmp_results_path.mkdir()

    popen_calls = []
    created_processes = []

    def fake_popen(command):
        process = _DummyProcess(command)
        popen_calls.append(command)
        created_processes.append(process)
        return process

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.plans.mode_sequences.Live",
        _DummyLive,
        raising=True,
    )
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.plans.mode_sequences.subprocess.Popen",
        fake_popen,
        raising=True,
    )

    mode_sequences._run_parallel_search(
        parent_folder_path=tmp_path,
        unique_location_chains=unique_location_chains,
        costs=costs,
        leg_modes=leg_modes,
        modes=modes,
        tmp_path=tmp_results_path,
    )

    costs_path = tmp_path / "tmp-costs.pkl"
    leg_modes_path = tmp_path / "tmp-leg-modes.pkl"
    modes_path = tmp_path / "modes-props.json"
    location_chains_path = tmp_path / "tmp-location-chains.parquet"

    assert costs_path.exists()
    assert leg_modes_path.exists()
    assert modes_path.exists()
    assert location_chains_path.exists()

    with open(costs_path, "rb") as file:
        assert pickle.load(file) == costs
    with open(leg_modes_path, "rb") as file:
        assert pickle.load(file) == leg_modes
    with open(modes_path, encoding="utf-8") as file:
        assert json.load(file) == modes
    assert pl.read_parquet(location_chains_path).equals(unique_location_chains)

    assert len(popen_calls) == 1
    command = popen_calls[0]
    assert command[0:2] == ["python", "-u"]
    assert pathlib.Path(command[2]).name == "compute_subtour_mode_probabilities.py"
    assert command[3:] == [
        "--k_sequences",
        "7",
        "--location_chains_path",
        str(location_chains_path),
        "--costs_path",
        str(costs_path),
        "--leg_modes_path",
        str(leg_modes_path),
        "--modes_path",
        str(modes_path),
        "--tmp_path",
        str(tmp_results_path),
    ]
    assert created_processes[0].wait_called is True
