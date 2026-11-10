import json
import os
import pickle
import subprocess
import sys
import time
from types import SimpleNamespace

import polars as pl
import psutil
import pytest
from mobility.runtime.temporary import TemporaryDiskLimitExceeded
from mobility.transport.modes.choice.compute_subtour_mode_probs_parallel_utilities import process_batch_serial
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


def test_mode_search_output_respects_temporary_disk_limit(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", "1")
    folder = tmp_path / "temporary" / "mobility-mode-search-test" / "tmp_results"
    folder.mkdir(parents=True)
    monkeypatch.setattr(
        "mobility.transport.modes.choice.compute_subtour_mode_probs_parallel_utilities.run_top_k_search",
        lambda *args, **kwargs: pl.DataFrame({"value": [1]}),
    )
    with pytest.raises(TemporaryDiskLimitExceeded):
        process_batch_serial([(1, [1, 2])], 0, {}, {}, {}, {}, {}, {}, {}, 1, folder)
    assert not list(folder.iterdir())


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
            "home_zone_id": [100],
            "utility_profile_id": [0],
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "activity_seq_id": [10],
            "time_seq_id": [20],
            "dest_seq_id": [30],
            "seq_step_index": [1],
            "from": [100],
        },
        schema={
            "home_zone_id": pl.Int32,
            "utility_profile_id": pl.UInt32,
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
            "home_zone_id": [100, 100, 300, 300],
            "utility_profile_id": [0, 0, 0, 0],
            "demand_group_id": [1, 1, 2, 2],
            "demand_subgroup_id": [0, 0, 0, 0],
            "activity_seq_id": [10, 10, 10, 10],
            "time_seq_id": [20, 20, 20, 20],
            "dest_seq_id": [30, 30, 30, 30],
            "seq_step_index": [1, 2, 1, 2],
            "from": [100, 200, 300, 400],
        },
        schema={
            "home_zone_id": pl.Int32,
            "utility_profile_id": pl.UInt32,
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


def test_same_destination_sequence_can_have_one_chain_per_utility_profile():
    destination_steps = pl.DataFrame(
        {
            "home_zone_id": [100, 100, 100, 100],
            "demand_group_id": [1, 1, 2, 2],
            "demand_subgroup_id": [0, 0, 0, 0],
            "utility_profile_id": [0, 0, 1, 1],
            "activity_seq_id": [10, 10, 10, 10],
            "time_seq_id": [20, 20, 20, 20],
            "dest_seq_id": [30, 30, 30, 30],
            "seq_step_index": [1, 2, 1, 2],
            "from": [100, 200, 300, 400],
        }
    )

    _, unique_chains = build_location_chains(destination_steps)

    assert unique_chains.sort("utility_profile_id").to_dicts() == [
        {
            "utility_profile_id": 0,
            "dest_seq_id": 30,
            "locations": [100, 200],
        },
        {
            "utility_profile_id": 1,
            "dest_seq_id": 30,
            "locations": [300, 400],
        },
    ]


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


def test_interrupted_mode_search_stops_workers_before_removing_files(monkeypatch, tmp_path):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.plans.mode_sequence_search.search_python.Live",
        lambda *args, **kwargs: _DummyLive(),
    )
    marker = tmp_path / "worker.pid"
    original_popen = subprocess.Popen
    created = []
    parent_script = (
        "import pathlib, subprocess, sys, time; "
        "worker=subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
        "pathlib.Path(sys.argv[1]).write_text(str(worker.pid)); time.sleep(60)"
    )

    class InterruptibleProcess:
        def __init__(self):
            flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            self.process = original_popen([sys.executable, "-c", parent_script, str(marker)], creationflags=flags)
            self.pid = self.process.pid
            self.interrupted = False
            self.worker = None

        def wait(self, timeout=None):
            if not self.interrupted:
                self.interrupted = True
                deadline = time.monotonic() + 10
                worker_pid = ""
                while not worker_pid and time.monotonic() < deadline:
                    if marker.exists():
                        worker_pid = marker.read_text().strip()
                    time.sleep(0.05)
                if not worker_pid:
                    raise RuntimeError("The test worker did not start.")
                self.worker = psutil.Process(int(worker_pid))
                raise KeyboardInterrupt()
            return self.process.wait(timeout=timeout)

        def poll(self):
            return self.process.poll()

        def terminate(self):
            self.process.terminate()

        def kill(self):
            self.process.kill()

    def start_process(*args, **kwargs):
        process = InterruptibleProcess()
        created.append(process)
        return process

    monkeypatch.setattr(
        "mobility.trips.group_day_trips.plans.mode_sequence_search.search_python.subprocess.Popen",
        start_process,
    )
    try:
        with pytest.raises(KeyboardInterrupt):
            run_python_mode_sequence_search(
                iteration=1,
                parameters=_mode_sequence_parameters(k_mode_sequences=1),
                unique_destination_chains=pl.DataFrame({"dest_seq_id": [1], "locations": [[1, 2]]}),
                leg_mode_costs=pl.DataFrame({"from": [1], "to": [2], "mode_id": [0], "cost": [1.0]}),
                modes_by_name={"walk": {"vehicle": None, "multimodal": False, "return_mode": None, "is_return_mode": False}},
                is_return_mode_by_id={0: False},
            )
        assert created[0].poll() is not None
        worker = created[0].worker
        assert not worker.is_running() or worker.status() == psutil.STATUS_ZOMBIE
        assert not list((tmp_path / "temporary").glob("mobility-mode-search-*"))
    finally:
        for process in created:
            if process.worker is not None:
                try:
                    process.worker.kill()
                except (psutil.NoSuchProcess, psutil.ZombieProcess):
                    pass
            if process.poll() is None:
                process.kill()
                process.process.wait(timeout=5)


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


def test_run_rust_mode_sequence_search_keeps_profiles_in_one_package_call(monkeypatch):
    captured = {}
    expected = pl.DataFrame(
        {
            "utility_profile_id": [0, 1],
            "dest_seq_id": [1, 1],
            "mode_seq_index": [0, 0],
            "seq_step_index": [1, 1],
            "location": [2, 2],
            "mode_index": [0, 1],
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
        unique_destination_chains=pl.DataFrame(
            {
                "utility_profile_id": [0, 1],
                "dest_seq_id": [1, 1],
                "locations": [[1, 2], [1, 2]],
            }
        ),
        leg_mode_costs=pl.DataFrame(
            {
                "utility_profile_id": [0, 1],
                "from": [1, 1],
                "to": [2, 2],
                "mode_id": [0, 1],
                "cost": [1.0, 2.0],
            }
        ),
        needs_vehicle_by_id={0: False, 1: False},
        return_mode_id_by_id={0: None, 1: None},
        is_return_mode_by_id={0: False, 1: False},
        modes_by_name={
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
        },
        mode_name_by_id={0: "walk", 1: "bike"},
        k_mode_sequences=3,
    )

    assert result.equals(expected)
    assert captured["location_chain_steps"]["utility_profile_id"].to_list() == [0, 1]
    assert captured["leg_mode_costs"]["utility_profile_id"].to_list() == [0, 1]


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
