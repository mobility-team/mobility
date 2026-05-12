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
    monkeypatch.setattr(r_script_runner_module.shutil, "which", lambda _name: "Rscript.exe")

    runner = RScriptRunner(script_path)

    assert runner.script_path == str(script_path)
    assert runner.timeout_seconds == 45
    assert runner.max_retries == 2
    assert runner.retry_delay_seconds == 9
    assert runner.heartbeat_interval_seconds == 12
    assert runner.rscript_executable == "Rscript.exe"


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

    with caplog.at_level(logging.INFO):
        runner._handle_timeout(process, start_time=0.0)

    assert process.terminate_calls == 1
    assert process.kill_calls == 1
    assert "did not terminate after timeout, killing it" in caplog.text
    assert "was killed after timeout" in caplog.text


def test_print_output_logs_info_and_errors_without_debug(monkeypatch, tmp_path, caplog):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path)
    monkeypatch.delenv("MOBILITY_DEBUG", raising=False)
    info_stream = io.BytesIO(b"[2026-05-12 10:00:00] INFO message from R\\n")
    error_stream = io.BytesIO(b"Error: crash happened\\n")

    with caplog.at_level(logging.INFO):
        runner.print_output(info_stream)
        runner.print_output(error_stream, is_error=True)

    assert "message from R" in caplog.text
    assert "R script execution failed" in caplog.text
    assert runner._last_output_line == "Error: crash happened\\n".strip()
    assert runner._last_output_stream == "stderr"


def test_log_heartbeat_reports_last_known_output(monkeypatch, tmp_path, caplog):
    script_path = _make_script(tmp_path)
    runner = RScriptRunner(script_path, heartbeat_interval_seconds=4)
    process = _HeartbeatProcess()
    stop_event = _HeartbeatStopEvent()

    runner._last_output_line = "step complete"
    runner._last_output_time = 95.0
    runner._last_output_stream = "stdout"

    monotonic_values = iter([100.0])
    monkeypatch.setattr(r_script_runner_module.time, "monotonic", lambda: next(monotonic_values))

    with caplog.at_level(logging.INFO):
        runner.log_heartbeat(process, stop_event, start_time=90.0)

    assert "still running after 10s" in caplog.text
    assert "last stdout 5s ago: step complete" in caplog.text
