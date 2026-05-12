import contextlib
import logging
import os
import pathlib
import shutil
import subprocess
import threading
import time

from importlib import resources
from typing import BinaryIO, Sequence


class RScriptRunner:
    """
    Run an R script from Python and stream its logs to the Python logger.

    This helper is used at the Python/R boundary in Mobility. It starts an
    `Rscript` process for a given script file, forwards the package path as the
    first argument so the R code can source other project files reliably, and
    relays the R stdout/stderr messages to the Python logs.

    The runner also keeps track of the latest output line, logs a periodic
    heartbeat, and can optionally fail a stalled run after a timeout. A small
    retry loop can be enabled for intermittent problems such as transient file
    access issues or antivirus interference on Windows.
    """

    def __init__(
        self,
        script_path: contextlib._GeneratorContextManager | pathlib.Path | str,
        timeout_seconds: int | None = None,
        max_retries: int | None = None,
        retry_delay_seconds: int | None = None,
        heartbeat_interval_seconds: int | None = None,
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

        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds should be a positive integer or None")
        if max_retries < 0:
            raise ValueError("max_retries should be greater than or equal to 0")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds should be greater than or equal to 0")
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds should be a positive integer")

        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
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
        heartbeat_stop = threading.Event()
        self._reset_output_tracking()

        if os.environ.get("MOBILITY_DEBUG") == "1":
            logging.info("Running R script " + self.script_path + " with the following arguments :")
            logging.info(args)

        logging.info(
            "Starting R script attempt %s/%s: script=%s timeout=%s retries=%s heartbeat=%ss",
            attempt_number,
            total_attempts,
            self.script_path,
            f"{self.timeout_seconds}s" if self.timeout_seconds is not None else "none",
            self.max_retries,
            self.heartbeat_interval_seconds,
        )
        logging.info("Rscript executable: %s", self.rscript_executable)
        logging.info("Rscript command: %s", cmd)
        logging.info("Rscript working directory: %s", os.getcwd())

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info("Started Rscript PID %s for %s", process.pid, self.script_path)

        stdout_thread = threading.Thread(target=self.print_output, args=(process.stdout,))
        stderr_thread = threading.Thread(target=self.print_output, args=(process.stderr, True))
        heartbeat_thread = threading.Thread(
            target=self.log_heartbeat,
            args=(process, heartbeat_stop, start_time),
            daemon=True,
        )

        stdout_thread.start()
        stderr_thread.start()
        heartbeat_thread.start()

        timeout_expired = False
        try:
            process.wait(timeout=self.timeout_seconds)
        except subprocess.TimeoutExpired:
            timeout_expired = True
            self._handle_timeout(process, start_time)
        finally:
            heartbeat_stop.set()
            stdout_thread.join()
            stderr_thread.join()

        elapsed_seconds = int(time.monotonic() - start_time)
        logging.info(
            "Rscript PID %s finished after %ss with return code %s",
            process.pid,
            elapsed_seconds,
            process.returncode,
        )

        if timeout_expired:
            raise RScriptRunnerError(self._build_timeout_message(elapsed_seconds))

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

        process.terminate()
        try:
            process.wait(timeout=10)
            logging.info("Rscript PID %s terminated cleanly after timeout.", process.pid)
        except subprocess.TimeoutExpired:
            logging.warning("Rscript PID %s did not terminate after timeout, killing it.", process.pid)
            process.kill()
            process.wait(timeout=10)
            logging.info("Rscript PID %s was killed after timeout.", process.pid)

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
        while not stop_event.wait(self.heartbeat_interval_seconds):
            if process.poll() is not None:
                return

            now = time.monotonic()
            elapsed_seconds = int(now - start_time)
            last_output_summary = self._get_last_output_summary(now)
            logging.info(
                "Rscript PID %s still running after %ss: %s (%s)",
                process.pid,
                elapsed_seconds,
                self.script_path,
                last_output_summary,
            )

    def print_output(self, stream: BinaryIO, is_error: bool = False) -> None:
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
                    logging.info(msg)
                elif (is_error and "Error" in msg) or "Erreur" in msg:
                    logging.error("R script execution failed, with the following message : " + msg)


class RScriptRunnerError(Exception):
    """
    Error raised when an R script launched by ``RScriptRunner`` fails.
    """

    pass
