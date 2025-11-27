import logging
import subprocess
import threading
import contextlib
import pathlib
import os
import platform

from importlib import resources


class RScript:
    """
    Run R scripts from Python.

    Use run() to execute the script with arguments.

    Parameters
    ----------
    script_path : str | pathlib.Path | contextlib._GeneratorContextManager
        Path to the R script (mobility R scripts live in r_utils).
    """

    def __init__(self, script_path):
        if isinstance(script_path, contextlib._GeneratorContextManager):
            with script_path as p:
                self.script_path = p
        elif isinstance(script_path, pathlib.Path):
            self.script_path = str(script_path)
        elif isinstance(script_path, str):
            self.script_path = script_path
        else:
            raise ValueError("R script path should be str, pathlib.Path or a context manager")

        if not pathlib.Path(self.script_path).exists():
            raise ValueError("Rscript not found : " + self.script_path)

    def _normalized_args(self, args: list) -> list:
        """
        Ensure the download method is valid for the current OS.
        The R script expects:
          args[1] -> packages JSON (after we prepend package root)
          args[2] -> force_reinstall (as string "TRUE"/"FALSE")
          args[3] -> download_method
        """
        norm = list(args)
        if not norm:
            return norm

        # The last argument should be the download method; normalize it for Linux
        is_windows = (platform.system() == "Windows")
        dl_idx = len(norm) - 1
        method = str(norm[dl_idx]).strip().lower()

        if not is_windows:
            # Never use wininet/auto on Linux/WSL
            if method in ("", "auto", "wininet"):
                norm[dl_idx] = "libcurl"
        else:
            # On Windows, allow wininet; default to wininet if empty
            if method == "":
                norm[dl_idx] = "wininet"

        return norm

    def _build_env(self) -> dict:
        """
        Prepare environment variables for R in a robust, cross-platform way.
        """
        env = os.environ.copy()

        is_windows = (platform.system() == "Windows")
        # Default to disabling pak unless caller opts in
        env.setdefault("USE_PAK", "false")

        # Make R downloads sane by default
        if not is_windows:
            # Force libcurl on Linux/WSL
            env.setdefault("R_DOWNLOAD_FILE_METHOD", "libcurl")
            # Point to the system CA bundle if available (WSL/Ubuntu)
            cacert = "/etc/ssl/certs/ca-certificates.crt"
            if os.path.exists(cacert):
                env.setdefault("SSL_CERT_FILE", cacert)

        # Avoid tiny default timeouts in some R builds
        env.setdefault("R_DEFAULT_INTERNET_TIMEOUT", "600")

        return env

    def run(self, args: list) -> None:
        """
        Run the R script.

        Parameters
        ----------
        args : list
            Arguments to pass to the R script (without the package root; we prepend it).

        Raises
        ------
        RScriptError
            If the R script returns a non-zero exit code.
        """
        # Prepend the package path so the R script knows the mobility root
        args = [str(resources.files('mobility'))] + self._normalized_args(args)
        cmd = ["Rscript", self.script_path] + args

        if os.environ.get("MOBILITY_DEBUG") == "1":
            logging.info("Running R script %s with the following arguments :", self.script_path)
            logging.info(args)

        env = self._build_env()

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )
        stdout_thread = threading.Thread(target=self.print_output, args=(process.stdout,))
        stderr_thread = threading.Thread(target=self.print_output, args=(process.stderr, True))

        stdout_thread.start()
        stderr_thread.start()
        process.wait()
        stdout_thread.join()
        stderr_thread.join()

        if process.returncode != 0:
            raise RScriptError(
                """
Rscript error (the error message is logged just before the error stack trace).
If you want more detail, set MOBILITY_DEBUG=1 (or debug=True in set_params) to print all R output.
                """.rstrip()
            )

    def print_output(self, stream, is_error: bool = False):
        """
        Log all R messages if debug=True; otherwise show INFO lines + errors.

        Parameters
        ----------
        stream :
            R process stream.
        is_error : bool
            Whether this stream is stderr.
        """
        for line in iter(stream.readline, b""):
            msg = line.decode("utf-8", errors="replace")

            if os.environ.get("MOBILITY_DEBUG") == "1":
                logging.info(msg)
            else:
                if "INFO" in msg:
                    # keep the message payload after the log level tag if present
                    parts = msg.split("]")
                    if len(parts) > 1:
                        msg = parts[1].strip()
                    logging.info(msg)
                elif is_error and ("Error" in msg or "Erreur" in msg):
                    logging.error("RScript execution failed, with the following message : " + msg)


class RScriptError(Exception):
    """Exception for R errors."""
    pass
