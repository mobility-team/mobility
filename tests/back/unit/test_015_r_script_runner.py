import io
import logging
import pathlib
import subprocess
import threading
from importlib import import_module

import pytest

from mobility.runtime.r_integration.r_script_runner import (
    RScriptRunner,
    RScriptRunnerError,
    RScriptRunState,
)


r_script_runner_module = import_module("mobility.runtime.r_integration.r_script_runner")


class _FakeThread:
    def __init__(self, *, target=None, args=(), daemon=False):
        self.target = target
        self.args = args
        self.daemon = daemon
        self.started = False
        self.joined = False

    def start(self):
        self.started = True

    def join(self):
        self.joined = True


class _TimeoutProcess:
    def __init__(self):
        self.pid = 1234
        self.stdout = io.BytesIO()
        self.stderr = io.BytesIO()
        self.returncode = None
        self.terminated = False
        self.killed = False
        self.wait_calls: list[int | None] = []

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        if timeout == 3:
            raise subprocess.TimeoutExpired(cmd=["Rscript"], timeout=timeout)
        self.returncode = -15
        return self.returncode

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True

    def poll(self):
        return self.returncode


class _KillAfterTerminateProcess:
    def __init__(self):
        self.pid = 5678
        self.terminate_calls = 0
        self.kill_calls = 0

    def wait(self, timeout=None):
        if timeout == 10 and self.terminate_calls == 1 and self.kill_calls == 0:
            raise subprocess.TimeoutExpired(cmd=["Rscript"], timeout=timeout)
        return 0

    def terminate(self):
        self.terminate_calls += 1

    def kill(self):
        self.kill_calls += 1


class _ChildProcess:
    def __init__(self, pid):
        self.pid = pid
        self.terminate_calls = 0
        self.kill_calls = 0

    def terminate(self):
        self.terminate_calls += 1

    def kill(self):
        self.kill_calls += 1


class _HeartbeatStopEvent:
    def __init__(self):
        self.calls = 0

    def wait(self, _interval):
        self.calls += 1
        return self.calls > 1


class _HeartbeatProcess:
    def __init__(self):
        self.pid = 999
        self.poll_calls = 0

    def poll(self):
        self.poll_calls += 1
        return None


class _CpuProcess:
    def __init__(self, cpu_seconds, memory_bytes=0, children=None, fails=False):
        self.cpu = cpu_seconds
        self.memory_bytes = memory_bytes
        self.child_processes = children or []
        self.fails = fails

    def children(self, recursive=True):
        return self.child_processes

    def cpu_times(self):
        if self.fails:
            raise r_script_runner_module.psutil.NoSuchProcess(pid=111)

        return type("CpuTimes", (), {"user": self.cpu, "system": 0.0})()

    def memory_info(self):
        if self.fails:
            raise r_script_runner_module.psutil.NoSuchProcess(pid=111)

        return type("MemoryInfo", (), {"rss": self.memory_bytes})()


class _IdleProcess:
    def __init__(self):
        self.pid = 4321
        self.returncode = None
        self.terminated = False
        self.killed = False

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -15

    def wait(self, timeout=None):
        return self.returncode

    def kill(self):
        self.killed = True
        self.returncode = -9


class _IdleStopEvent:
    def __init__(self):
        self.intervals = []

    def wait(self, interval):
        self.intervals.append(interval)
        return False


def _make_script(tmp_path: pathlib.Path) -> pathlib.Path:
    script_path = tmp_path / "script.R"
    script_path.write_text("cat('hello')\n", encoding="utf-8")
    return script_path


def test_runner_init_reads_settings_from_environment(monkeypatch, tmp_path):
    script_path = _make_script(tmp_path)
    monkeypatch.setenv("MOBILITY_R_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("MOBILITY_R_MAX_RETRIES", "2")
    monkeypatch.setenv("MOBILITY_R_RETRY_DELAY_SECONDS", "9")
    monkeypatch.setenv("MOBILITY_R_HEARTBEAT_INTERVAL_SECONDS", "12")
    monkeypatch.setenv("MOBILITY_R_IDLE_TIMEOUT_SECONDS", "60")
    monkeypatch.setenv("MOBILITY_R_IDLE_CPU_PERCENT", "0.5")
    monkeypatch.setenv("MOBILITY_R_IDLE_MEMORY_CHANGE_MB", "2")
    monkeypatch.setenv("MOBILITY_R_CPU_CHECK_INTERVAL_SECONDS", "3")
    monkeypatch.setattr(r_script_runner_module.shutil, "which", lambda _name: "Rscript.exe")

    runner = RScriptRunner(script_path)

    assert runner.script_path == str(script_path)
    assert runner.timeout_seconds == 45
    assert runner.max_retries == 2
    assert runner.retry_delay_seconds == 9
    assert runner.heartbeat_interval_seconds == 12
    assert runner.idle_timeout_seconds == 60
    assert runner.idle_cpu_percent == 0.5
    assert runner.idle_memory_change_bytes == 2 * 1024 * 1024
    assert runner.cpu_check_interval_seconds == 3
    assert runner.rscript_executable == "Rscript.exe"


def test_runner_init_keeps_existing_retry_default(monkeypatch, tmp_path):
    script_path = _make_script(tmp_path)
    monkeypatch.delenv("MOBILITY_R_IDLE_TIMEOUT_SECONDS", raising=False)

    runner = RScriptRunner(script_path)

    assert runner.max_retries == 0
    assert runner.idle_timeout_seconds is None


def test_runner_init_can_disable_idle_monitor(tmp_path):
    script_path = _make_script(tmp_path)

    runner = RScriptRunner(script_path, idle_timeout_seconds=0)

    assert runner.idle_timeout_seconds is None


def test_run_prepends_package_path_and_stringifies_arguments(monkeypatch, tmp_path):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path, max_retries=0)
    calls = []
    package_root = tmp_path / "mobility-package"
    nested_arg = pathlib.Path("nested/file.txt")

    monkeypatch.setattr(r_script_runner_module.resources, "files", lambda _package: package_root)

    def fake_run_once(cmd, args, attempt_number, total_attempts):
        calls.append((cmd, args, attempt_number, total_attempts))

    monkeypatch.setattr(runner, "_run_once", fake_run_once)

    runner.run([1, pathlib.Path("nested/file.txt")])

    assert calls == [
        (
            [
                runner.rscript_executable,
                str(script_path),
                str(package_root),
                "1",
                str(nested_arg),
            ],
            [str(package_root), "1", str(nested_arg)],
            1,
            1,
        )
    ]


def test_run_retries_after_first_failed_attempt(monkeypatch, tmp_path):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path, max_retries=1, retry_delay_seconds=7)
    package_root = tmp_path / "mobility-package"
    attempts = []
    sleeps = []

    monkeypatch.setattr(r_script_runner_module.resources, "files", lambda _package: package_root)
    monkeypatch.setattr(r_script_runner_module.time, "sleep", lambda seconds: sleeps.append(seconds))

    def fake_run_once(cmd, args, attempt_number, total_attempts):
        attempts.append((attempt_number, total_attempts, list(cmd), list(args)))
        if attempt_number == 1:
            raise RScriptRunnerError("boom")

    monkeypatch.setattr(runner, "_run_once", fake_run_once)

    runner.run(["arg"])

    assert [attempt[0] for attempt in attempts] == [1, 2]
    assert sleeps == [7]
    assert attempts[0][2] == [
        runner.rscript_executable,
        str(script_path),
        str(package_root),
        "arg",
    ]


def test_run_raises_after_idle_timeout_retries_are_exhausted(monkeypatch, tmp_path):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path, max_retries=2, retry_delay_seconds=0)
    package_root = tmp_path / "mobility-package"
    attempts = []

    monkeypatch.setattr(r_script_runner_module.resources, "files", lambda _package: package_root)

    def fail_with_idle_timeout(cmd, args, attempt_number, total_attempts):
        attempts.append((attempt_number, total_attempts))
        raise RScriptRunnerError("idle timeout")

    monkeypatch.setattr(runner, "_run_once", fail_with_idle_timeout)

    with pytest.raises(RScriptRunnerError, match="idle timeout"):
        runner.run(["arg"])

    assert attempts == [(1, 3), (2, 3), (3, 3)]


def test_run_once_terminates_timed_out_process_and_raises_timeout_error(monkeypatch, tmp_path):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path, timeout_seconds=3, max_retries=0)
    process = _TimeoutProcess()

    monkeypatch.setattr(r_script_runner_module.subprocess, "Popen", lambda *args, **kwargs: process)
    monkeypatch.setattr(r_script_runner_module.threading, "Thread", _FakeThread)
    monkeypatch.setattr(runner, "_build_timeout_message", lambda _elapsed: "timed out cleanly")

    with pytest.raises(RScriptRunnerError, match="timed out cleanly"):
        runner._run_once(["Rscript", str(script_path)], ["pkg-root"], 1, 1)

    assert process.terminated is True
    assert process.killed is False
    assert process.wait_calls == [3, 10]


def test_handle_timeout_kills_process_if_terminate_does_not_finish(monkeypatch, tmp_path, caplog):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path)
    process = _KillAfterTerminateProcess()
    monkeypatch.setattr(runner, "_get_last_output_summary", lambda _now: "last stdout 5s ago: still working")

    with caplog.at_level(logging.DEBUG):
        runner._handle_timeout(process, start_time=0.0)

    assert process.terminate_calls == 1
    assert process.kill_calls == 1
    assert "did not terminate after timeout, killing it" in caplog.text
    assert "was killed after timeout" in caplog.text


def test_stop_process_stops_child_processes_before_retrying(monkeypatch, tmp_path):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path)
    process = _TimeoutProcess()
    child_process = _ChildProcess(pid=9876)
    wait_calls = []

    def fake_wait_procs(processes, timeout):
        wait_calls.append((list(processes), timeout))
        if len(wait_calls) == 1:
            return [], list(processes)

        return list(processes), []

    monkeypatch.setattr(runner, "_get_child_processes", lambda _pid: [child_process])
    monkeypatch.setattr(r_script_runner_module.psutil, "wait_procs", fake_wait_procs)

    runner._stop_process(process, "idle timeout")

    assert child_process.terminate_calls == 1
    assert child_process.kill_calls == 1
    assert process.terminated is True
    assert wait_calls == [([child_process], 10), ([child_process], 10)]


def test_log_process_output_keeps_info_quiet_without_debug(monkeypatch, tmp_path, caplog):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path)
    monkeypatch.delenv("MOBILITY_DEBUG", raising=False)
    info_stream = io.BytesIO(b"[2026-05-12 10:00:00] INFO message from R\\n")
    error_stream = io.BytesIO(b"Error: crash happened\\n")

    with caplog.at_level(logging.INFO):
        runner.log_process_output(info_stream)
        runner.log_process_output(error_stream, is_error=True)

    assert "message from R" not in caplog.text
    assert "R script execution failed" in caplog.text
    assert runner._last_output_line == "Error: crash happened\\n".strip()
    assert runner._last_output_stream == "stderr"


def test_log_heartbeat_reports_cpu_and_memory(monkeypatch, tmp_path, caplog):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path, heartbeat_interval_seconds=4)
    process = _HeartbeatProcess()
    stop_event = _HeartbeatStopEvent()
    cpu_seconds_values = iter([10.0, 11.5])

    monotonic_values = iter([90.0, 100.0])
    monkeypatch.setattr(r_script_runner_module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(runner, "_get_process_tree_cpu_seconds", lambda _pid: next(cpu_seconds_values))
    monkeypatch.setattr(runner, "_get_process_tree_memory_bytes", lambda _pid: 2 * 1024 * 1024)

    with caplog.at_level(logging.DEBUG):
        runner.log_heartbeat(process, stop_event, start_time=90.0)

    assert "R is still running (PID 999, 10s): CPU 15.0%, RAM 2MiB." in caplog.text


def test_get_process_tree_cpu_seconds_sums_parent_and_children(monkeypatch, tmp_path):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path)
    child_process = _CpuProcess(0.75)
    missing_child_process = _CpuProcess(0.0, fails=True)
    root_process = _CpuProcess(0.5, children=[child_process, missing_child_process])

    monkeypatch.setattr(r_script_runner_module.psutil, "Process", lambda _pid: root_process)

    assert runner._get_process_tree_cpu_seconds(1234) == 1.25


def test_get_process_tree_memory_bytes_sums_parent_and_children(monkeypatch, tmp_path):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path)
    child_process = _CpuProcess(0.0, memory_bytes=750)
    missing_child_process = _CpuProcess(0.0, memory_bytes=0, fails=True)
    root_process = _CpuProcess(0.0, memory_bytes=500, children=[child_process, missing_child_process])

    monkeypatch.setattr(r_script_runner_module.psutil, "Process", lambda _pid: root_process)

    assert runner._get_process_tree_memory_bytes(1234) == 1250


def test_monitor_cpu_idle_stops_process_after_idle_timeout(monkeypatch, tmp_path, caplog):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(
        script_path,
        idle_timeout_seconds=10,
        idle_cpu_percent=1.0,
        idle_memory_change_mb=1.0,
        cpu_check_interval_seconds=2,
    )
    process = _IdleProcess()
    stop_event = _IdleStopEvent()
    run_state = RScriptRunState()
    monotonic_values = iter([100.0, 111.0, 122.0])
    cpu_seconds_values = iter([0.0, 0.0, 0.0])
    memory_bytes_values = iter([1000, 1000, 1000])

    monkeypatch.setattr(r_script_runner_module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(runner, "_get_process_tree_cpu_seconds", lambda _pid: next(cpu_seconds_values))
    monkeypatch.setattr(runner, "_get_process_tree_memory_bytes", lambda _pid: next(memory_bytes_values))

    with caplog.at_level(logging.INFO):
        runner.monitor_cpu_idle(process, stop_event, start_time=90.0, run_state=run_state)

    assert process.terminated is True
    assert process.killed is False
    assert run_state.failure_message is not None
    assert "stayed below 1.0% CPU and changed less than 1.00MiB RAM for 11s" in run_state.failure_message
    assert "retry loop can continue" in run_state.failure_message
    assert "idle timeout" in caplog.text
    assert stop_event.intervals == [2, 2]


def test_monitor_cpu_idle_keeps_running_when_memory_changes(monkeypatch, tmp_path):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(
        script_path,
        idle_timeout_seconds=10,
        idle_cpu_percent=1.0,
        idle_memory_change_mb=1.0,
        cpu_check_interval_seconds=2,
    )
    process = _IdleProcess()
    stop_event = _HeartbeatStopEvent()
    run_state = RScriptRunState()
    monotonic_values = iter([100.0, 105.0])
    cpu_seconds_values = iter([0.0, 0.0])
    memory_bytes_values = iter([0, 2 * 1024 * 1024])

    monkeypatch.setattr(r_script_runner_module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(runner, "_get_process_tree_cpu_seconds", lambda _pid: next(cpu_seconds_values))
    monkeypatch.setattr(runner, "_get_process_tree_memory_bytes", lambda _pid: next(memory_bytes_values))

    runner.monitor_cpu_idle(process, stop_event, start_time=90.0, run_state=run_state)

    assert process.terminated is False
    assert run_state.failure_message is None
