import json
import pathlib
import pickle

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from ..plans.candidate_plan_steps import CandidatePlanStepsAsset


class CurrentPlansAsset(FileAsset):
    """Persisted current plan distribution after one completed iteration."""

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        current_plans: pl.DataFrame | None = None,
    ) -> None:
        self.current_plans = current_plans
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"current_plans_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        if self.current_plans is None:
            raise ValueError("Cannot save current plans without a dataframe.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.current_plans.write_parquet(self.cache_path)
        return self.get_cached_asset()


class CurrentPlanStepsAsset(FileAsset):
    """Persisted step-level details for the current plans after one completed iteration."""

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        current_plan_steps: pl.DataFrame | None = None,
    ) -> None:
        self.current_plan_steps = current_plan_steps
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"current_plan_steps_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        if self.current_plan_steps is None:
            raise ValueError("Cannot save current plan steps without a dataframe.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.current_plan_steps.write_parquet(self.cache_path)
        return self.get_cached_asset()


class RemainingOpportunitiesAsset(FileAsset):
    """Persisted destination saturation state after one completed iteration.

    The class and filename keep the legacy "remaining opportunities" name so
    previously written iteration caches can still be resumed.
    """

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        destination_saturation: pl.DataFrame | None = None,
    ) -> None:
        self.destination_saturation = destination_saturation
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"remaining_opportunities_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        if self.destination_saturation is None:
            raise ValueError("Cannot save destination saturation without a dataframe.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.destination_saturation.write_parquet(self.cache_path)
        return self.get_cached_asset()


class RngStateAsset(FileAsset):
    """Persisted RNG state after one completed iteration."""

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        rng_state=None,
    ) -> None:
        self.rng_state = rng_state
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"rng_state_{iteration}.pkl"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self):
        with open(self.cache_path, "rb") as file:
            return pickle.load(file)

    def create_and_get_asset(self):
        if self.rng_state is None:
            raise ValueError("Cannot save RNG state without a value.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "wb") as file:
            pickle.dump(self.rng_state, file, protocol=pickle.HIGHEST_PROTOCOL)
        return self.get_cached_asset()


class IterationCompleteAsset(FileAsset):
    """Persisted marker saying that one iteration state was fully written."""

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
    ) -> None:
        self.run_key = run_key
        self.is_weekday = is_weekday
        self.iteration = iteration
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"iteration_complete_{iteration}.json"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> dict:
        with open(self.cache_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def create_and_get_asset(self) -> dict:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as file:
            json.dump(
                {
                    "run_key": self.run_key,
                    "is_weekday": self.is_weekday,
                    "iteration": self.iteration,
                },
                file,
                sort_keys=True,
            )
        return self.get_cached_asset()

    @classmethod
    def find_latest_completed_iteration(
        cls,
        *,
        base_folder: pathlib.Path,
        run_key: str,
        is_weekday: bool,
    ) -> int | None:
        """Return the latest iteration with a completion marker."""
        folder = pathlib.Path(base_folder)
        if not folder.exists():
            return None

        latest_iteration = None
        for path in folder.glob("*iteration_complete_*.json"):
            try:
                with open(path, "r", encoding="utf-8") as file:
                    marker = json.load(file)
            except Exception:
                continue

            if marker.get("run_key") != run_key or marker.get("is_weekday") != is_weekday:
                continue

            iteration = marker.get("iteration")
            if not isinstance(iteration, int):
                continue

            if latest_iteration is None or iteration > latest_iteration:
                latest_iteration = iteration

        return latest_iteration


class TransitionEventsAsset(FileAsset):
    """Persisted transition events produced during one iteration."""

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        transition_events: pl.DataFrame | None = None,
    ) -> None:
        self.transition_events = transition_events
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"transition_events_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        if self.transition_events is None:
            raise ValueError("Cannot save transition events without a dataframe.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.transition_events.write_parquet(self.cache_path)
        return self.get_cached_asset()
