import logging
import subprocess
import threading
import contextlib
import pathlib

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

    def run(self, args: list) -> None:
        cmd = ["Rscript", self.script_path] + args

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        output_thread = threading.Thread(target=self.print_output, args=(process.stdout,))
        output_thread.start()
        process.wait()
        output_thread.join()

    def print_output(self, stream):
        for line in iter(stream.readline, b""):
            msg = line.decode("utf-8", errors="replace")
            logging.info(msg)
            # if "INFO" in msg:
            #     msg = msg.split("]")[1]
            #     msg = msg.strip()
            #     logging.info(msg)
