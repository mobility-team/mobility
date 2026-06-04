import contextlib
import logging
import os
import pathlib
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass
from importlib import resources
from typing import BinaryIO, Sequence

import psutil


@dataclass
class RScriptRunState:
    """Shared state for monitor threads during one R script attempt."""

    failure_message: str | None = None


class RScriptRunner:
    """
    Run an R script from Python and stream its logs to the Python logger.

    This helper is used at the Python/R boundary in Mobility. It starts an
    `Rscript` process for a given script file, forwards the package path as the
    first argument so the R code can source other project files reliably, and
    relays the R stdout/stderr messages to the Python logs.

    The runner also keeps track of the latest output line, logs a periodic
    heartbeat, and can optionally fail a stalled run after a timeout or after
    the R process tree stays idle for too long. A small retry loop can be
    enabled for intermittent problems such as transient file access issues or
    antivirus interference on Windows.

    When an attempt fails because of the CPU idle monitor, it raises the same
    Python error as the other R failures. The normal retry loop then retries it
    up to `max_retries` times, or raises the error after the last attempt.

    The CPU idle monitor checks the full R process tree, including child
    processes spawned by R. It is controlled by these environment variables:
    `MOBILITY_R_IDLE_TIMEOUT_SECONDS` defaults to 0, which disables this monitor.
    Set it to a positive number of seconds to enable idle protection.
    `MOBILITY_R_IDLE_CPU_PERCENT` defaults to 1.0 percent,
    `MOBILITY_R_IDLE_MEMORY_CHANGE_MB` defaults to 1 MiB, and
    `MOBILITY_R_CPU_CHECK_INTERVAL_SECONDS` defaults to 5 seconds.
    """

    def __init__(
        self,
        script_path: contextlib._GeneratorContextManager | pathlib.Path | str,
        timeout_seconds: int | None = None,
        max_retries: int | None = None,
        retry_delay_seconds: int | None = None,
        heartbeat_interval_seconds: int | None = None,
        idle_timeout_seconds: int | None = None,
        idle_cpu_percent: float | None = None,
        idle_memory_change_mb: float | None = None,
        cpu_check_interval_seconds: int | None = None,
    ):
        if isinstance(script_path, contextlib._GeneratorContextManager):
            with script_path as p:
                self.script_path = p
        elif isinstance(script_path, pathlib.Path):
            self.script_path = str(script_path)
        elif isinstance(script_path, str):
            self.script_path = script_path
        else:
            raise ValueError("R script path should be provided as str, pathlib.Path or contextlib._GeneratorContextManager")

        if pathlib.Path(self.script_path).exists() is False:
            raise ValueError("Rscript not found : " + self.script_path)

        timeout_seconds = self._get_int_setting(
            explicit_value=timeout_seconds,
            env_var_name="MOBILITY_R_TIMEOUT_SECONDS",
            allow_none=True,
        )
        max_retries = self._get_int_setting(
            explicit_value=max_retries,
            env_var_name="MOBILITY_R_MAX_RETRIES",
            default_value=0,
        )
        retry_delay_seconds = self._get_int_setting(
            explicit_value=retry_delay_seconds,
            env_var_name="MOBILITY_R_RETRY_DELAY_SECONDS",
            default_value=5,
        )
        heartbeat_interval_seconds = self._get_int_setting(
            explicit_value=heartbeat_interval_seconds,
            env_var_name="MOBILITY_R_HEARTBEAT_INTERVAL_SECONDS",
            default_value=30,
        )
        idle_timeout_seconds = self._get_int_setting(
            explicit_value=idle_timeout_seconds,
            env_var_name="MOBILITY_R_IDLE_TIMEOUT_SECONDS",
            default_value=0,
        )
        idle_cpu_percent = self._get_float_setting(
            explicit_value=idle_cpu_percent,
            env_var_name="MOBILITY_R_IDLE_CPU_PERCENT",
            default_value=1.0,
        )
        idle_memory_change_mb = self._get_float_setting(
            explicit_value=idle_memory_change_mb,
            env_var_name="MOBILITY_R_IDLE_MEMORY_CHANGE_MB",
            default_value=1.0,
        )
        cpu_check_interval_seconds = self._get_int_setting(
            explicit_value=cpu_check_interval_seconds,
            env_var_name="MOBILITY_R_CPU_CHECK_INTERVAL_SECONDS",
            default_value=5,
        )

        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds should be a positive integer or None")
        if max_retries < 0:
            raise ValueError("max_retries should be greater than or equal to 0")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds should be greater than or equal to 0")
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds should be a positive integer")
        if idle_timeout_seconds < 0:
            raise ValueError("idle_timeout_seconds should be greater than or equal to 0")
        if idle_cpu_percent < 0:
            raise ValueError("idle_cpu_percent should be greater than or equal to 0")
        if idle_memory_change_mb < 0:
            raise ValueError("idle_memory_change_mb should be greater than or equal to 0")
        if cpu_check_interval_seconds <= 0:
            raise ValueError("cpu_check_interval_seconds should be a positive integer")

        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.idle_timeout_seconds = None if idle_timeout_seconds == 0 else idle_timeout_seconds
        self.idle_cpu_percent = idle_cpu_percent
        self.idle_memory_change_bytes = idle_memory_change_mb * 1024 * 1024
        self.cpu_check_interval_seconds = cpu_check_interval_seconds
        self.rscript_executable = shutil.which("Rscript") or "Rscript"

        self._output_lock = threading.Lock()
        self._last_output_line: str | None = None
        self._last_output_time: float | None = None
        self._last_output_stream: str | None = None

    def _get_int_setting(
        self,
        explicit_value: int | None,
        env_var_name: str,
        default_value: int | None = None,
        allow_none: bool = False,
    ) -> int | None:
        """Resolve an integer setting from an explicit value or an environment variable."""
        if explicit_value is not None:
            return int(explicit_value)

        env_value = os.environ.get(env_var_name)
        if env_value in (None, ""):
            return None if allow_none else default_value

        return int(env_value)

    def _get_float_setting(
        self,
        explicit_value: float | None,
        env_var_name: str,
        default_value: float,
    ) -> float:
        """Resolve a float setting from an explicit value or an environment variable."""
        if explicit_value is not None:
            return float(explicit_value)

        env_value = os.environ.get(env_var_name)
        if env_value in (None, ""):
            return default_value

        return float(env_value)

    def run(self, args: Sequence[str]) -> None:
        """Run the R script with the given arguments."""
        args = [str(resources.files("mobility"))] + [str(arg) for arg in args]
        cmd = [self.rscript_executable, self.script_path] + args
        total_attempts = self.max_retries + 1

        for attempt_number in range(1, total_attempts + 1):
            try:
                self._run_once(cmd, args, attempt_number, total_attempts)
                return
            except RScriptRunnerError:
                if attempt_number == total_attempts:
                    raise

                logging.warning(
                    "Retrying R script %s after attempt %s/%s failed. Waiting %ss before retry.",
                    self.script_path,
                    attempt_number,
                    total_attempts,
                    self.retry_delay_seconds,
                )
                time.sleep(self.retry_delay_seconds)

    def _run_once(self, cmd: list[str], args: list[str], attempt_number: int, total_attempts: int) -> None:
        """Run one attempt of the R script."""
        start_time = time.monotonic()
        monitor_stop = threading.Event()
        run_state = RScriptRunState()
        self._reset_output_tracking()

        if os.environ.get("MOBILITY_DEBUG") == "1":
            logging.debug("Running R script " + self.script_path + " with the following arguments :")
            logging.debug(args)

        logging.debug(
            (
                "Starting R script attempt %s/%s: script=%s timeout=%s retries=%s heartbeat=%ss "
                "idle_timeout=%s idle_cpu=%.2f%% idle_memory_change=%.2fMiB cpu_check=%ss"
            ),
            attempt_number,
            total_attempts,
            self.script_path,
            f"{self.timeout_seconds}s" if self.timeout_seconds is not None else "none",
            self.max_retries,
            self.heartbeat_interval_seconds,
            f"{self.idle_timeout_seconds}s" if self.idle_timeout_seconds is not None else "disabled",
            self.idle_cpu_percent,
            self.idle_memory_change_bytes / 1024 / 1024,
            self.cpu_check_interval_seconds,
        )
        logging.debug("Rscript executable: %s", self.rscript_executable)
        logging.debug("Rscript command: %s", cmd)
        logging.debug("Rscript working directory: %s", os.getcwd())

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.debug("Started Rscript PID %s for %s", process.pid, self.script_path)

        stdout_thread = threading.Thread(target=self.log_process_output, args=(process.stdout,))
        stderr_thread = threading.Thread(target=self.log_process_output, args=(process.stderr, True))
        heartbeat_thread = threading.Thread(
            target=self.log_heartbeat,
            args=(process, monitor_stop, start_time),
            daemon=True,
        )
        idle_thread = threading.Thread(
            target=self.monitor_cpu_idle,
            args=(process, monitor_stop, start_time, run_state),
            daemon=True,
        )

        stdout_thread.start()
        stderr_thread.start()
        heartbeat_thread.start()
        idle_thread.start()

        timeout_expired = False
        try:
            process.wait(timeout=self.timeout_seconds)
        except subprocess.TimeoutExpired:
            timeout_expired = True
            self._handle_timeout(process, start_time)
        finally:
            monitor_stop.set()
            stdout_thread.join()
            stderr_thread.join()
            idle_thread.join()

        elapsed_seconds = int(time.monotonic() - start_time)
        logging.debug(
            "Rscript PID %s finished after %ss with return code %s",
            process.pid,
            elapsed_seconds,
            process.returncode,
        )

        if timeout_expired:
            raise RScriptRunnerError(self._build_timeout_message(elapsed_seconds))

        if run_state.failure_message is not None:
            raise RScriptRunnerError(run_state.failure_message)

        if process.returncode != 0:
            raise RScriptRunnerError(
                """
                    Rscript error (the error message is logged just before the error stack trace).
                    If you want more detail, you can print all R output by setting debug=True when calling set_params.
                """
            )

    def _handle_timeout(self, process: subprocess.Popen, start_time: float) -> None:
        """Stop a timed-out R subprocess and log what happened."""
        elapsed_seconds = int(time.monotonic() - start_time)
        logging.error(
            "Rscript PID %s timed out after %ss: %s (%s)",
            process.pid,
            elapsed_seconds,
            self.script_path,
            self._get_last_output_summary(time.monotonic()),
        )

        self._stop_process(process, "timeout")

    def _stop_process(self, process: subprocess.Popen, reason: str) -> None:
        """Stop an R subprocess tree, killing it if it does not stop cleanly."""
        child_processes = self._get_child_processes(process.pid)

        for child_process in child_processes:
            self._terminate_psutil_process(child_process)

        process.terminate()
        try:
            process.wait(timeout=10)
            logging.debug("Rscript PID %s terminated cleanly after %s.", process.pid, reason)
        except subprocess.TimeoutExpired:
            logging.warning("Rscript PID %s did not terminate after %s, killing it.", process.pid, reason)
            process.kill()
            process.wait(timeout=10)
            logging.debug("Rscript PID %s was killed after %s.", process.pid, reason)

        self._kill_surviving_child_processes(child_processes, reason)

    def _get_child_processes(self, pid: int) -> list[psutil.Process]:
        """Return live children of a process, or an empty list if they cannot be read."""
        try:
            return psutil.Process(pid).children(recursive=True)
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            return []

    def _terminate_psutil_process(self, process: psutil.Process) -> None:
        """Ask one psutil process to stop and ignore races with exiting processes."""
        try:
            process.terminate()
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            return

    def _kill_surviving_child_processes(self, child_processes: list[psutil.Process], reason: str) -> None:
        """Kill child processes that survived the normal stop request."""
        if not child_processes:
            return

        _gone, alive_processes = psutil.wait_procs(child_processes, timeout=10)
        for child_process in alive_processes:
            try:
                logging.warning(
                    "R child PID %s did not terminate after %s, killing it.",
                    child_process.pid,
                    reason,
                )
                child_process.kill()
            except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
                continue

        psutil.wait_procs(alive_processes, timeout=10)

    def _build_timeout_message(self, elapsed_seconds: int) -> str:
        """Build the timeout error message."""
        return (
            "Rscript timed out after "
            f"{elapsed_seconds}s while running {self.script_path}. "
            f"Last known activity: {self._get_last_output_summary(time.monotonic())}"
        )

    def _reset_output_tracking(self) -> None:
        """Clear the tracked R output state before starting a new run."""
        with self._output_lock:
            self._last_output_line = None
            self._last_output_time = None
            self._last_output_stream = None

    def _record_output(self, msg: str, is_error: bool) -> None:
        """Remember the latest line received from the R subprocess."""
        cleaned_msg = msg.strip()
        if cleaned_msg == "":
            return

        with self._output_lock:
            self._last_output_line = cleaned_msg
            self._last_output_time = time.monotonic()
            self._last_output_stream = "stderr" if is_error else "stdout"

    def _get_last_output_summary(self, now: float) -> str:
        """Summarize the most recent line received from the R subprocess."""
        with self._output_lock:
            last_output_line = self._last_output_line
            last_output_time = self._last_output_time
            last_output_stream = self._last_output_stream

        if last_output_line is None or last_output_time is None or last_output_stream is None:
            return "no output received yet"

        silent_seconds = int(now - last_output_time)
        return (
            f"last {last_output_stream} {silent_seconds}s ago: "
            f"{last_output_line[:200]}"
        )

    def log_heartbeat(
        self,
        process: subprocess.Popen,
        stop_event: threading.Event,
        start_time: float,
    ) -> None:
        """Log a periodic heartbeat while the R subprocess is running."""
        previous_cpu_seconds = self._get_process_tree_cpu_seconds(process.pid)
        previous_sample_time = time.monotonic()

        while not stop_event.wait(self.heartbeat_interval_seconds):
            if process.poll() is not None:
                return

            now = time.monotonic()
            elapsed_seconds = int(now - start_time)
            current_cpu_seconds = self._get_process_tree_cpu_seconds(process.pid)
            current_memory_bytes = self._get_process_tree_memory_bytes(process.pid)
            cpu_percent = self._get_cpu_percent_since_last_sample(
                previous_cpu_seconds,
                current_cpu_seconds,
                previous_sample_time,
                now,
            )

            if current_cpu_seconds is not None:
                previous_cpu_seconds = current_cpu_seconds
            previous_sample_time = now

            logging.debug(
                "R is still running (PID %s, %ss): CPU %s, RAM %s.",
                process.pid,
                elapsed_seconds,
                self._format_cpu_percent(cpu_percent),
                self._format_memory(current_memory_bytes),
            )

    def monitor_cpu_idle(
        self,
        process: subprocess.Popen,
        stop_event: threading.Event,
        start_time: float,
        run_state: RScriptRunState,
    ) -> None:
        """Stop the R subprocess if its process tree stays idle for too long."""
        if self.idle_timeout_seconds is None:
            return

        previous_cpu_seconds = self._get_process_tree_cpu_seconds(process.pid)
        previous_memory_bytes = self._get_process_tree_memory_bytes(process.pid)
        previous_sample_time = time.monotonic()
        idle_started_at: float | None = None
        last_seen_output_time: float | None = None

        while not stop_event.wait(self.cpu_check_interval_seconds):
            if process.poll() is not None:
                return

            now = time.monotonic()
            current_cpu_seconds = self._get_process_tree_cpu_seconds(process.pid)
            current_memory_bytes = self._get_process_tree_memory_bytes(process.pid)
            if (
                current_cpu_seconds is None
                or previous_cpu_seconds is None
                or current_memory_bytes is None
                or previous_memory_bytes is None
            ):
                return

            sample_seconds = now - previous_sample_time
            if sample_seconds <= 0:
                continue

            cpu_percent = max(0.0, current_cpu_seconds - previous_cpu_seconds) / sample_seconds * 100
            memory_change_bytes = abs(current_memory_bytes - previous_memory_bytes)
            previous_cpu_seconds = current_cpu_seconds
            previous_memory_bytes = current_memory_bytes
            previous_sample_time = now

            current_output_time = self._get_last_output_time()
            if current_output_time is not None and current_output_time != last_seen_output_time:
                last_seen_output_time = current_output_time
                idle_started_at = None

            if cpu_percent > self.idle_cpu_percent:
                idle_started_at = None
                continue

            if memory_change_bytes > self.idle_memory_change_bytes:
                idle_started_at = None
                continue

            if idle_started_at is None:
                idle_started_at = now

            idle_seconds = int(now - idle_started_at)
            logging.debug(
                "Rscript PID %s process tree CPU is %.2f%% and RAM changed by %.2fMiB after %ss, idle for %ss.",
                process.pid,
                cpu_percent,
                memory_change_bytes / 1024 / 1024,
                int(now - start_time),
                idle_seconds,
            )

            if idle_seconds >= self.idle_timeout_seconds:
                message = (
                    f"Rscript idle timeout: PID {process.pid} stayed below {self.idle_cpu_percent}% CPU "
                    f"and changed less than {self.idle_memory_change_bytes / 1024 / 1024:.2f}MiB RAM "
                    f"for {idle_seconds}s while running {self.script_path}. "
                    "Stopping this attempt so the retry loop can continue. "
                    f"Last known activity: {self._get_last_output_summary(now)}"
                )
                run_state.failure_message = message
                logging.warning(message)
                self._stop_process(process, "idle timeout")
                return

    def _get_process_tree_cpu_seconds(self, pid: int) -> float | None:
        """Return the total CPU time used by the R process and its children."""
        try:
            root_process = psutil.Process(pid)
            processes = [root_process] + root_process.children(recursive=True)
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            return None

        cpu_seconds = 0.0
        for child_process in processes:
            try:
                cpu_times = child_process.cpu_times()
                cpu_seconds += cpu_times.user + cpu_times.system
            except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
                continue

        return cpu_seconds

    def _get_process_tree_memory_bytes(self, pid: int) -> int | None:
        """Return the total resident memory used by the R process and its children."""
        try:
            root_process = psutil.Process(pid)
            processes = [root_process] + root_process.children(recursive=True)
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            return None

        memory_bytes = 0
        for child_process in processes:
            try:
                memory_bytes += child_process.memory_info().rss
            except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
                continue

        return memory_bytes

    def _get_cpu_percent_since_last_sample(
        self,
        previous_cpu_seconds: float | None,
        current_cpu_seconds: float | None,
        previous_sample_time: float,
        current_sample_time: float,
    ) -> float | None:
        """Return CPU use between two process-tree CPU samples."""
        if previous_cpu_seconds is None or current_cpu_seconds is None:
            return None

        sample_seconds = current_sample_time - previous_sample_time
        if sample_seconds <= 0:
            return None

        return max(0.0, current_cpu_seconds - previous_cpu_seconds) / sample_seconds * 100

    def _format_cpu_percent(self, cpu_percent: float | None) -> str:
        """Format process-tree CPU use for logs."""
        if cpu_percent is None:
            return "unknown"

        return f"{cpu_percent:.1f}%"

    def _format_memory(self, memory_bytes: int | None) -> str:
        """Format process-tree memory use for logs."""
        if memory_bytes is None:
            return "unknown"

        return f"{memory_bytes / 1024 / 1024:.0f}MiB"

    def _get_last_output_time(self) -> float | None:
        """Return when the R subprocess last wrote something."""
        with self._output_lock:
            return self._last_output_time

    def log_process_output(self, stream: BinaryIO, is_error: bool = False) -> None:
        """Log the R subprocess output."""
        for line in iter(stream.readline, b""):
            msg = line.decode("utf-8", errors="replace")
            self._record_output(msg, is_error)

            if os.environ.get("MOBILITY_DEBUG") == "1":
                logging.info(msg)
            else:
                if "INFO" in msg:
                    msg = msg.split("]")[1]
                    msg = msg.strip()
                    logging.debug(msg)
                elif (is_error and "Error" in msg) or "Erreur" in msg:
                    logging.error("R script execution failed, with the following message : " + msg)

    def print_output(self, stream: BinaryIO, is_error: bool = False) -> None:
        """Log the R subprocess output."""
        self.log_process_output(stream, is_error)


class RScriptRunnerError(Exception):
    """
    Error raised when an R script launched by ``RScriptRunner`` fails.
    """

    pass
