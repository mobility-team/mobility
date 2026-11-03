"""Optional wall-time and process RAM measurements for simulation steps."""

import contextvars
import functools
import inspect
import json
import os
import pathlib
import threading
import uuid
from contextlib import contextmanager
from time import perf_counter

import psutil


# A context variable lets nested assets inherit the active run and step labels
# without adding profiler arguments to the asset graph or changing cache keys.
_ACTIVE_TRACE = contextvars.ContextVar("mobility_simulation_trace", default=None)


class SimulationPerformanceTrace:
    """Sample Python-process RSS every 100 ms and write completed spans as JSONL."""

    def __init__(self, path):
        self.path = pathlib.Path(path)
        self.process = psutil.Process()
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.active = {}
        self.next_id = 0
        self.started_at = perf_counter()
        self.run_id = uuid.uuid4().hex

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.output = self.path.open("a", encoding="utf-8")
        self.thread = threading.Thread(target=self._sample, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join()
        self.output.close()

    def _sample(self):
        while not self.stop.wait(0.1):
            rss = self.process.memory_info().rss
            with self.lock:
                for row in self.active.values():
                    row["rss_peak_bytes"] = max(row["rss_peak_bytes"], rss)

    @contextmanager
    def step(self, name, parent_id, labels):
        started_at = perf_counter()
        rss = self.process.memory_info().rss
        with self.lock:
            step_id = self.next_id
            self.next_id += 1
            row = {
                "run_id": self.run_id, "step_id": step_id, "parent_id": parent_id, "step": name,
                **labels, "start_seconds": started_at - self.started_at,
                "rss_start_bytes": rss, "rss_peak_bytes": rss, "status": "ok",
            }
            self.active[step_id] = row
        token = _ACTIVE_TRACE.set((self, step_id, labels))
        try:
            yield
        except BaseException as error:
            row["status"] = "error"
            row["error_type"] = type(error).__name__
            raise
        finally:
            _ACTIVE_TRACE.reset(token)
            ended_at = perf_counter()
            rss = self.process.memory_info().rss
            with self.lock:
                self.active.pop(step_id)
                row.update(
                    end_seconds=ended_at - self.started_at,
                    wall_seconds=ended_at - started_at, rss_end_bytes=rss,
                    rss_delta_bytes=rss - row["rss_start_bytes"],
                    rss_peak_bytes=max(row["rss_peak_bytes"], rss),
                )
                for parent in self.active.values():
                    parent["rss_peak_bytes"] = max(parent["rss_peak_bytes"], row["rss_peak_bytes"])
                self.output.write(json.dumps(row) + "\n")
                self.output.flush()


@contextmanager
def simulation_step(name, **labels):
    """Measure a nested block only when a run has enabled performance tracing."""
    active = _ACTIVE_TRACE.get()
    if active is None:
        yield
        return
    trace, parent_id, inherited = active
    with trace.step(name, parent_id, {**inherited, **labels}):
        yield


def trace_simulation_step(name):
    """Measure a function and inherit its scenario and loop position."""
    def decorate(function):
        signature = inspect.signature(function)

        @functools.wraps(function)
        def measured(*args, **kwargs):
            if _ACTIVE_TRACE.get() is None:
                return function(*args, **kwargs)
            bound = signature.bind_partial(*args, **kwargs)
            bound.apply_defaults()
            labels = {}
            owner = bound.arguments.get("self")
            if owner is not None:
                labels["asset_type"] = type(owner).__name__
            for key in ("iteration", "scenario", "phase"):
                value = bound.arguments.get(key)
                if value is None and owner is not None and key != "phase":
                    value = getattr(owner, key, None)
                if value is not None:
                    labels[key] = value
            source = bound.arguments.get("source")
            if source is not None:
                labels["scenario"] = source.scenario
            destinations = bound.arguments.get("destination_sequences")
            if destinations is None and owner is not None:
                destinations = getattr(owner, "destination_sequences", None)
            if destinations is not None and getattr(destinations, "scenario", None) is not None:
                labels["scenario"] = destinations.scenario
            if name == "inner loop":
                labels["outer_iteration"] = labels["iteration"]
            with simulation_step(name, **labels):
                return function(*args, **kwargs)
        return measured
    return decorate


def trace_simulation_run(function):
    """Enable tracing for one coordinated run when an output path is configured."""
    @functools.wraps(function)
    def measured(*args, **kwargs):
        path = os.environ.get("MOBILITY_SIMULATION_TRACE")
        cached = args and getattr(args[0], "_cached_result", None) is not None
        if not path or cached or _ACTIVE_TRACE.get() is not None:
            return function(*args, **kwargs)
        with SimulationPerformanceTrace(path) as trace:
            with trace.step("coordinated run", None, {}):
                return function(*args, **kwargs)
    return measured
