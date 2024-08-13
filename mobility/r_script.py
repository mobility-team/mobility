import logging
import subprocess
import threading
import contextlib
import pathlib
import os

class RScript:
    def __init__(self, script_path: str | contextlib._GeneratorContextManager):
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

    def run(self, args: list) -> None:
        
        cmd = ["Rscript", self.script_path] + args
        
        if os.environ.get("MOBILITY_DEBUG") == "1":
            logging.info("Running R script " + self.script_path + " with the following arguments :")
            logging.info(args)
        
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
                    If you want more detail, you can print all R output by setting debug=True when calling set_params.
                """
            )

    def print_output(self, stream, is_error=False):
        for line in iter(stream.readline, b""):
            msg = line.decode("utf-8", errors="replace")

            if os.environ.get("MOBILITY_DEBUG") == "1":
                logging.info(msg)

            else:
                if "INFO" in msg:
                    msg = msg.split("]")[1]
                    msg = msg.strip()
                    logging.info(msg)
                elif is_error and "Error" in msg or "Erreur" in msg:
                    logging.error("RScript execution failed, with the following message : " + msg)


class RScriptError(Exception):
    pass

