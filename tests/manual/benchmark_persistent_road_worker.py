"""Compare a persistent R road worker with ordinary one-shot R processes.

Requires pymizu and mizu in the active Python and R environments, and a
previously captured road-input directory from the Grand Geneve probe. This
is an experiment; it does not change the simulation's production path.
"""

import argparse
import json
import shutil
import tempfile
import threading
import time
from importlib import resources
from pathlib import Path

import polars as pl
import psutil
import pymizu

from mobility.runtime.r_integration.r_script_runner import RScriptRunner


def road_worker_program():
    """Load road inputs once and accept successive flow-file requests."""
    return """
library(mizu)
road_worker_channel <- ch
args <- unlist(mizu_recv(ch))
source(file.path(args[[1]], "transport", "costs", "path",
                 "prepare_shared_congested_costs.R"), local = TRUE)
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("road_input", type=Path)
    parser.add_argument("--alter-graph", action="store_true")
    args = parser.parse_args()
    work = Path(tempfile.mkdtemp(prefix="mobility-persistent-road-"))
    source = resources.files("mobility.transport.costs.path").joinpath(
        "prepare_shared_congested_costs.R"
    )
    captured = json.loads((args.road_input / "args.json").read_text(encoding="utf-8"))
    if args.alter_graph:
        # Keep topology and building mappings, but change the travel times on
        # the copied network. This leaves the cached source graph untouched.
        original_graph = Path(captured[0])
        graph_hash = original_graph.name.split("-")[0]
        graph_root = work / "changed-graph"
        graph_dir = graph_root / "modified"
        graph_dir.mkdir(parents=True)
        for source_file in original_graph.parent.glob(f"{graph_hash}*"):
            shutil.copyfile(source_file, graph_dir / source_file.name)
        for suffix in ("-vertices.parquet", "-od-vertex-map.parquet"):
            shutil.copyfile(original_graph.parent.parent / f"{graph_hash}{suffix}",
                            graph_root / f"{graph_hash}{suffix}")
        data_path = graph_dir / f"{graph_hash}data.parquet"
        data = pl.read_parquet(data_path).with_columns(dist=pl.col("dist") * 1.1)
        data.write_parquet(data_path)
        captured[0] = str(graph_dir / original_graph.name)
    captured[12] = str(args.road_input / "graphs.json")
    captured[13] = str(args.road_input / "route-requests.parquet")
    inputs = [args.road_input / f"flow-{i}.parquet" for i in range(2)]
    manifests = []
    for index, flow in enumerate(inputs):
        manifest = work / f"flows-{index}.json"
        manifest.write_text(json.dumps([str(flow)]), encoding="utf-8")
        manifests.append(manifest)

    stop = threading.Event()
    peak = [0]

    def sample_memory():
        process = psutil.Process()
        while not stop.wait(0.1):
            total = 0
            for child in [process, *process.children(recursive=True)]:
                try:
                    total += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            peak[0] = max(peak[0], total)

    def start_monitor():
        peak[0] = 0
        stop.clear()
        monitor = threading.Thread(target=sample_memory, daemon=True)
        monitor.start()
        return monitor

    runner = RScriptRunner(source)
    monitor = start_monitor()
    one_shot_times = []
    for index, manifest in enumerate(manifests):
        inputs_for_r = list(captured)
        inputs_for_r[3] = str(manifest)
        inputs_for_r[10] = str(work / f"one-shot-{index}.parquet")
        started = time.perf_counter()
        runner.run(inputs_for_r)
        one_shot_times.append(time.perf_counter() - started)
    stop.set()
    monitor.join()
    one_shot_peak = peak[0]

    monitor = start_monitor()
    started = time.perf_counter()
    channel = pymizu.Channel.create(road_worker_program(), launcher=pymizu.r_launcher(), startup_timeout=60)
    try:
        channel.send([str(resources.files("mobility")), *captured])
        response = channel.recv(timeout=120)
        if response != "ready":
            raise RuntimeError(f"R worker did not start: {response}")
        worker_startup = time.perf_counter() - started
        worker_times = []
        # Repeat the first flow after the second to check for retained state.
        for index, manifest in enumerate([*manifests, manifests[0]]):
            started = time.perf_counter()
            channel.send([str(manifest), str(work / f"worker-{index}.parquet"), captured[12]])
            response = channel.recv(timeout=180)
            if response != "done":
                raise RuntimeError(f"R worker failed: {response}")
            worker_times.append(time.perf_counter() - started)
        # A failed request should be reported and not poison the next one.
        channel.send([str(work / "missing-flows.json"), str(work / "invalid.parquet"), captured[12]])
        response = channel.recv(timeout=30)
        if not isinstance(response, str) or not response.startswith("ERROR:"):
            raise AssertionError(f"Expected a reported worker error, got {response!r}")
        channel.send([str(manifests[0]), str(work / "worker-3.parquet"), captured[12]])
        response = channel.recv(timeout=180)
        if response != "done":
            raise RuntimeError(f"R worker did not recover after an error: {response}")
    finally:
        channel.close()
        stop.set()
        monitor.join()
    worker_peak = peak[0]

    for index in range(len(manifests) + 2):
        baseline_index = index if index < len(manifests) else 0
        baseline = pl.read_parquet(work / f"one-shot-{baseline_index}.parquet").sort("from", "to")
        result = pl.read_parquet(work / f"worker-{index}.parquet").sort("from", "to")
        equal = baseline.equals(result)
        print("RESULT_EQUAL", index, equal, "rows", result.height, flush=True)
        if not equal:
            raise AssertionError(f"Worker result {index} differs from the one-shot result.")
    if args.alter_graph:
        original = pl.read_parquet(args.road_input / "reference.parquet").select("from", "to", "time_0")
        changed = pl.read_parquet(work / "worker-0.parquet").select("from", "to", "time_0")
        difference = original.join(changed, on=["from", "to"], suffix="_changed").select(
            (pl.col("time_0") - pl.col("time_0_changed")).abs().max()
        ).item()
        print("CHANGED_NETWORK_MAX_TIME_DIFFERENCE", difference, flush=True)
        if not difference:
            raise AssertionError("The modified network did not change any route times.")
    print("ONE_SHOT_SECONDS", one_shot_times, "PEAK_TREE_BYTES", one_shot_peak, flush=True)
    print("WORKER_STARTUP_SECONDS", worker_startup, "UPDATE_SECONDS", worker_times,
          "PEAK_TREE_BYTES", worker_peak, flush=True)
    print("PROTOTYPE_FILES", work, flush=True)


if __name__ == "__main__":
    main()
