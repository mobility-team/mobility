import json
import pathlib
import pickle
import tempfile

import polars as pl

from mobility.trips.group_day_trips.plans.mode_sequences import ModeSequences


def _make_temp_path() -> pathlib.Path:
    return pathlib.Path(tempfile.mkdtemp())


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


def test_run_parallel_search_serializes_inputs_and_invokes_worker(monkeypatch):
    tmp_path = _make_temp_path()
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


def test_compute_mode_sequence_search_results_dispatches_to_old_python_backend(monkeypatch):
    tmp_path = _make_temp_path()
    mode_sequences = object.__new__(ModeSequences)
    mode_sequences.parameters = type(
        "Params",
        (),
        {
            "mode_sequence_search_parallel": False,
            "k_mode_sequences": 7,
        },
    )()

    called = {}

    def fake_old_python_search(**kwargs):
        called["kwargs"] = kwargs

    monkeypatch.setattr(mode_sequences, "_run_old_python_search", fake_old_python_search)

    result = mode_sequences._compute_mode_sequence_search_results(
        use_rust_search=False,
        parent_folder_path=tmp_path,
        unique_location_chains=pl.DataFrame({"dest_seq_id": [1], "locations": [[1, 2]]}),
        leg_mode_costs=pl.DataFrame({"from": [1], "to": [2], "mode_id": [0], "cost": [1.0]}),
        modes={"walk": {"is_return_mode": False}},
        mode_id={"walk": 0},
        tmp_path=tmp_path / "tmp_results",
    )

    assert "kwargs" in called
    assert called["kwargs"]["parent_folder_path"] == tmp_path
    assert result is None


def test_compute_mode_sequence_search_results_dispatches_to_rust_backend(monkeypatch):
    tmp_path = _make_temp_path()
    mode_sequences = object.__new__(ModeSequences)
    mode_sequences.parameters = type(
        "Params",
        (),
        {
            "mode_sequence_search_parallel": True,
            "k_mode_sequences": 7,
        },
    )()

    called = {}

    def fake_rust_search(**kwargs):
        called["kwargs"] = kwargs

    monkeypatch.setattr(mode_sequences, "_run_rust_search", fake_rust_search)

    result = mode_sequences._compute_mode_sequence_search_results(
        use_rust_search=True,
        parent_folder_path=tmp_path,
        unique_location_chains=pl.DataFrame({"dest_seq_id": [1], "locations": [[1, 2]]}),
        leg_mode_costs=pl.DataFrame({"from": [1], "to": [2], "mode_id": [0], "cost": [1.0]}),
        modes={"walk": {"is_return_mode": False}},
        mode_id={"walk": 0},
        tmp_path=tmp_path / "tmp_results",
    )

    assert "kwargs" in called
    assert "parent_folder_path" not in called["kwargs"]
    assert result is None


def test_build_rust_mode_metadata():
    mode_sequences = object.__new__(ModeSequences)

    result = mode_sequences._build_rust_mode_metadata(
        modes={
            "walk": {
                "vehicle": None,
                "multimodal": False,
                "is_return_mode": False,
                "return_mode": None,
            },
            "carpool_return": {
                "vehicle": "car",
                "multimodal": True,
                "is_return_mode": True,
                "return_mode": None,
            },
            "carpool": {
                "vehicle": "car",
                "multimodal": True,
                "is_return_mode": False,
                "return_mode": "carpool_return",
            },
        },
        mode_id={"walk": 0, "carpool": 1, "carpool_return": 2},
    )

    assert result.sort("mode_id").to_dict(as_series=False) == {
        "mode_id": [0, 1, 2],
        "needs_vehicle": [False, True, True],
        "vehicle_id": [None, "car", "car"],
        "multimodal": [False, True, True],
        "is_return_mode": [False, False, True],
        "return_mode_id": [None, 2, None],
    }


def test_compute_mode_sequence_search_results_uses_explicit_backend_flag(monkeypatch):
    tmp_path = _make_temp_path()
    mode_sequences = object.__new__(ModeSequences)
    mode_sequences.parameters = type(
        "Params",
        (),
        {
            "mode_sequence_search_parallel": True,
            "k_mode_sequences": 7,
            "use_rust_mode_sequence_search": True,
        },
    )()

    called = {}

    def fake_rust_search(**kwargs):
        called["kwargs"] = kwargs

    monkeypatch.setattr(mode_sequences, "_run_rust_search", fake_rust_search)

    result = mode_sequences._compute_mode_sequence_search_results(
        use_rust_search=mode_sequences.parameters.use_rust_mode_sequence_search,
        parent_folder_path=tmp_path,
        unique_location_chains=pl.DataFrame({"dest_seq_id": [1], "locations": [[1, 2]]}),
        leg_mode_costs=pl.DataFrame({"from": [1], "to": [2], "mode_id": [0], "cost": [1.0]}),
        modes={"walk": {"is_return_mode": False}},
        mode_id={"walk": 0},
        tmp_path=tmp_path / "tmp_results",
    )

    assert "kwargs" in called
    assert result is None
