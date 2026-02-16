import os
import json
import pickle
import pathlib
import logging
import re

import polars as pl

from mobility.file_asset import FileAsset


class PopulationTripsCheckpointAsset(FileAsset):
    """Per-iteration checkpoint for PopulationTrips to enable crash-safe resume.

    The checkpoint is keyed by:
    - run_key: PopulationTrips.inputs_hash (includes the seed and all params)
    - is_weekday: True/False
    - iteration: last completed iteration k

    Payload:
    - current_states (pl.DataFrame)
    - remaining_sinks (pl.DataFrame)
    - rng_state (pickle of random.Random.getstate())

    Notes:
    - We write a JSON meta file last so incomplete checkpoints are ignored.
    - This asset is intentionally not part of the main model dependency graph;
      it is only used as an optional resume source.
    """

    SCHEMA_VERSION = 1

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        current_states: pl.DataFrame | None = None,
        remaining_sinks: pl.DataFrame | None = None,
        rng_state=None,
    ):
        self._payload_current_states = current_states
        self._payload_remaining_sinks = remaining_sinks
        self._payload_rng_state = rng_state

        inputs = {
            "run_key": str(run_key),
            "is_weekday": bool(is_weekday),
            "iteration": int(iteration),
            "schema_version": self.SCHEMA_VERSION,
        }

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        period = "weekday" if is_weekday else "weekend"
        base_dir = project_folder / "population_trips" / period / "checkpoints"

        stem = f"checkpoint_{run_key}_iter_{int(iteration)}"
        cache_path = {
            "current_states": base_dir / f"{stem}_current_states.parquet",
            "remaining_sinks": base_dir / f"{stem}_remaining_sinks.parquet",
            "rng_state": base_dir / f"{stem}_rng_state.pkl",
            "meta": base_dir / f"{stem}.json",
        }

        super().__init__(inputs, cache_path)

    def get_cached_asset(self):
        current_states = pl.read_parquet(self.cache_path["current_states"])
        remaining_sinks = pl.read_parquet(self.cache_path["remaining_sinks"])
        with open(self.cache_path["rng_state"], "rb") as f:
            rng_state = pickle.load(f)

        meta = {}
        try:
            with open(self.cache_path["meta"], "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            # Meta is only for convenience; payload files are the source of truth.
            pass

        return {
            "current_states": current_states,
            "remaining_sinks": remaining_sinks,
            "rng_state": rng_state,
            "meta": meta,
        }

    def create_and_get_asset(self):
        for p in self.cache_path.values():
            pathlib.Path(p).parent.mkdir(parents=True, exist_ok=True)

        if self._payload_current_states is None or self._payload_remaining_sinks is None or self._payload_rng_state is None:
            raise ValueError("Checkpoint payload is missing (current_states, remaining_sinks, rng_state).")

        def atomic_write_bytes(final_path: pathlib.Path, data: bytes):
            tmp = pathlib.Path(str(final_path) + ".tmp")
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, final_path)

        def atomic_write_text(final_path: pathlib.Path, text: str):
            tmp = pathlib.Path(str(final_path) + ".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(tmp, final_path)

        # Write payload first
        tmp_states = pathlib.Path(str(self.cache_path["current_states"]) + ".tmp")
        self._payload_current_states.write_parquet(tmp_states)
        os.replace(tmp_states, self.cache_path["current_states"])

        tmp_sinks = pathlib.Path(str(self.cache_path["remaining_sinks"]) + ".tmp")
        self._payload_remaining_sinks.write_parquet(tmp_sinks)
        os.replace(tmp_sinks, self.cache_path["remaining_sinks"])

        atomic_write_bytes(self.cache_path["rng_state"], pickle.dumps(self._payload_rng_state, protocol=pickle.HIGHEST_PROTOCOL))

        # Meta last, so readers only see complete checkpoints.
        meta = {
            "run_key": self.inputs["run_key"],
            "is_weekday": self.inputs["is_weekday"],
            "iteration": self.inputs["iteration"],
            "schema_version": self.SCHEMA_VERSION,
        }
        atomic_write_text(self.cache_path["meta"], json.dumps(meta, sort_keys=True))

        logging.info(
            "Checkpoint saved: run_key=%s is_weekday=%s iteration=%s",
            self.inputs["run_key"],
            str(self.inputs["is_weekday"]),
            str(self.inputs["iteration"]),
        )

        return self.get_cached_asset()

    @staticmethod
    def find_latest_checkpoint_iter(*, run_key: str, is_weekday: bool) -> int | None:
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        period = "weekday" if is_weekday else "weekend"
        base_dir = project_folder / "population_trips" / period / "checkpoints"
        if not base_dir.exists():
            return None

        # FileAsset prefixes filenames with its own inputs_hash, so we match on the suffix.
        pattern = f"*checkpoint_{run_key}_iter_*.json"
        candidates = list(base_dir.glob(pattern))
        if not candidates:
            return None

        rx = re.compile(rf"checkpoint_{re.escape(run_key)}_iter_(\d+)\.json$")
        best = None
        for p in candidates:
            m = rx.search(p.name)
            if not m:
                continue
            it = int(m.group(1))
            if best is None or it > best:
                best = it

        return best

    @staticmethod
    def remove_checkpoints_for_run(*, run_key: str, is_weekday: bool) -> int:
        """Remove all checkpoint files for a given run_key and period.

        Returns number of files removed.
        """
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        period = "weekday" if is_weekday else "weekend"
        base_dir = project_folder / "population_trips" / period / "checkpoints"
        if not base_dir.exists():
            return 0

        # FileAsset prefixes filenames with its own inputs_hash, so just match suffix fragments.
        pattern = f"*checkpoint_{run_key}_iter_*"
        removed = 0
        for p in base_dir.glob(pattern):
            try:
                p.unlink(missing_ok=True)
                removed += 1
            except Exception:
                logging.exception("Failed to remove checkpoint file: %s", str(p))

        # Also delete any stray tmp files.
        for p in base_dir.glob(pattern + ".tmp"):
            try:
                p.unlink(missing_ok=True)
                removed += 1
            except Exception:
                logging.exception("Failed to remove checkpoint tmp file: %s", str(p))

        return removed
