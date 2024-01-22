import logging
import subprocess
import threading


class RScript:
    def __init__(self, script_path: str):
        self.script_path = script_path

    def run(self, args: list) -> None:
        cmd = ["Rscript", self.script_path] + args

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        output_thread = threading.Thread(target=self.print_output, args=(process.stdout,))
        output_thread.start()
        process.wait()
        output_thread.join()

    def print_output(self, stream):
        for line in iter(stream.readline, b""):
            msg = line.decode()
            print(msg)
            if "INFO" in msg:
                msg = msg.split("]")[1]
                msg = msg.strip()
                logging.info(msg)
