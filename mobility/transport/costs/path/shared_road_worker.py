"""Keep road routing inputs in one R process during a simulation."""

import logging
import os
import tempfile
from importlib import resources

import psutil
import pymizu


class SharedRoadWorker:
    """Run successive congestion updates on one loaded road network."""

    def __init__(self, args):
        self.channel = None
        self.process = None
        self.output = tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace")

        def launch(token, slot=None):
            self.process = launcher(token, slot)
            return self.process

        program = """
library(mizu)
road_worker_channel <- ch
args <- unlist(mizu_recv(ch))
source(file.path(args[[1]], "transport", "costs", "path",
                 "prepare_shared_congested_costs.R"), local = TRUE)
"""
        try:
            launcher = pymizu.r_launcher(stdout=self.output, stderr=self.output)
            self.channel = pymizu.Channel.create(program, launcher=launch, startup_timeout=60)
            self.channel.send([str(resources.files("mobility")), *map(str, args)])
            self._check_response(self.channel.recv(timeout=120), "ready")
        except BaseException:
            try:
                self.close()
            except Exception:
                logging.exception("Could not close the road worker after startup failed.")
            raise

    def _check_response(self, response, expected):
        if response != expected:
            self.output.flush()
            self.output.seek(0, 2)
            self.output.seek(max(0, self.output.tell() - 10000))
            details = self.output.read()
            raise RuntimeError(f"Road worker failed: {response}\n{details}")

    def run(self, args):
        """Compute costs for one set of scenario flow tables."""
        args = list(map(str, args))
        self.channel.send([args[3], args[10], args[12]])
        configured_timeout = os.environ.get("MOBILITY_R_TIMEOUT_SECONDS")
        timeout = int(configured_timeout) if configured_timeout else None
        self._check_response(self.channel.recv(timeout=timeout), "done")

    def close(self):
        """Stop the worker, including a worker that stopped responding."""
        try:
            if self.channel is not None:
                if not self.channel.close():
                    self.channel.destroy()
                self.channel = None
        finally:
            if self.process is not None and self.process.poll() is None:
                try:
                    process = psutil.Process(self.process.pid)
                    children = process.children(recursive=True)
                    for child in [*children, process]:
                        try:
                            child.terminate()
                        except psutil.NoSuchProcess:
                            pass
                    _, alive = psutil.wait_procs([process, *children], timeout=5)
                    for child in alive:
                        try:
                            child.kill()
                        except psutil.NoSuchProcess:
                            pass
                except psutil.NoSuchProcess:
                    pass
            self.process = None
            self.output.close()
