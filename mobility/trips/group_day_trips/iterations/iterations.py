import pathlib
import re
import shutil
import logging
from dataclasses import dataclass
from typing import Any

import polars as pl

from ..core.run_state import RunState
from ..plans.destination_sequences import DestinationSequences
from ..plans.mode_sequences import ModeSequences
from .iteration_assets import (
    CurrentPlansAsset,
    IterationCompleteAsset,
    RemainingOpportunitiesAsset,
    RngStateAsset,
    TransitionEventsAsset,
)


@dataclass(frozen=True)
class IterationState:
    """Minimal persisted plan distribution required to resume a run from one iteration."""

    current_plans: pl.DataFrame
    remaining_opportunities: pl.DataFrame
    rng_state: object


class Iteration:
    """Persisted artifacts and saved state for one GroupDayTrips iteration."""

    def __init__(self, iterations: "Iterations", iteration: int) -> None:
        self.iterations = iterations
        self.iteration = iteration


    def destination_sequences(
        self,
        *,
        activities: list[Any] | None = None,
        resolved_activity_parameters: dict[str, Any] | None = None,
        transport_zones: Any = None,
        current_plans: pl.DataFrame | None = None,
        current_plan_steps: pl.DataFrame | None = None,
        remaining_opportunities: pl.DataFrame | None = None,
        chains: pl.DataFrame | None = None,
        demand_groups: pl.DataFrame | None = None,
        costs: pl.DataFrame | None = None,
        parameters: Any = None,
        seed: int | None = None,
    ) -> DestinationSequences:
        """Return the destination-sequences asset for this iteration."""
        return DestinationSequences(
            run_key=self.iterations.run_inputs_hash,
            is_weekday=self.iterations.is_weekday,
            iteration=self.iteration,
            base_folder=self.iterations.folder_paths["destination-sequences"],
            activities=activities,
            resolved_activity_parameters=resolved_activity_parameters,
            transport_zones=transport_zones,
            current_plans=current_plans,
            current_plan_steps=current_plan_steps,
            remaining_opportunities=remaining_opportunities,
            chains=chains,
            demand_groups=demand_groups,
            costs=costs,
            sequence_index_folder=self.iterations.folder_paths["sequences-index"],
            parameters=parameters,
            seed=seed,
        )


    def mode_sequences(
        self,
        *,
        destination_sequences: DestinationSequences,
        costs_aggregator: Any = None,
        parameters: Any = None,
        congestion_state: Any = None,
    ) -> ModeSequences:
        """Return the mode-sequences asset for this iteration."""
        return ModeSequences(
            run_key=self.iterations.run_inputs_hash,
            is_weekday=self.iterations.is_weekday,
            iteration=self.iteration,
            base_folder=self.iterations.folder_paths["modes"],
            destination_sequences=destination_sequences,
            costs_aggregator=costs_aggregator,
            working_folder=self.iterations.base_folder,
            sequence_index_folder=self.iterations.folder_paths["sequences-index"],
            parameters=parameters,
            congestion_state=congestion_state,
        )


    def load_state(self) -> IterationState:
        """Load the saved run state for this completed iteration."""
        iteration_state_folder = self.iterations.folder_paths["iteration-state"]
        return IterationState(
            current_plans=CurrentPlansAsset(
                run_key=self.iterations.run_inputs_hash,
                is_weekday=self.iterations.is_weekday,
                iteration=self.iteration,
                base_folder=iteration_state_folder,
            ).get(),
            remaining_opportunities=RemainingOpportunitiesAsset(
                run_key=self.iterations.run_inputs_hash,
                is_weekday=self.iterations.is_weekday,
                iteration=self.iteration,
                base_folder=iteration_state_folder,
            ).get(),
            rng_state=RngStateAsset(
                run_key=self.iterations.run_inputs_hash,
                is_weekday=self.iterations.is_weekday,
                iteration=self.iteration,
                base_folder=iteration_state_folder,
            ).get(),
        )


    def save_state(self, state: RunState, rng_state: object) -> None:
        """Persist the run state for this completed iteration."""
        iteration_state_folder = self.iterations.folder_paths["iteration-state"]
        try:
            CurrentPlansAsset(
                run_key=self.iterations.run_inputs_hash,
                is_weekday=self.iterations.is_weekday,
                iteration=self.iteration,
                base_folder=iteration_state_folder,
                current_plans=state.current_plans,
            ).create_and_get_asset()
            RemainingOpportunitiesAsset(
                run_key=self.iterations.run_inputs_hash,
                is_weekday=self.iterations.is_weekday,
                iteration=self.iteration,
                base_folder=iteration_state_folder,
                remaining_opportunities=state.remaining_opportunities,
            ).create_and_get_asset()
            RngStateAsset(
                run_key=self.iterations.run_inputs_hash,
                is_weekday=self.iterations.is_weekday,
                iteration=self.iteration,
                base_folder=iteration_state_folder,
                rng_state=rng_state,
            ).create_and_get_asset()
            IterationCompleteAsset(
                run_key=self.iterations.run_inputs_hash,
                is_weekday=self.iterations.is_weekday,
                iteration=self.iteration,
                base_folder=iteration_state_folder,
            ).create_and_get_asset()
        except Exception as exc:
            raise RuntimeError(
                "Failed to save GroupDayTrips iteration state for "
                f"run_inputs_hash={self.iterations.run_inputs_hash}, "
                f"is_weekday={self.iterations.is_weekday}, iteration={self.iteration}. "
                "Call `remove()` to clear cached iteration artifacts and rerun from scratch."
            ) from exc


    def save_transition_events(self, transition_events: pl.DataFrame) -> None:
        """Persist transition events produced during this iteration."""
        TransitionEventsAsset(
            run_key=self.iterations.run_inputs_hash,
            is_weekday=self.iterations.is_weekday,
            iteration=self.iteration,
            base_folder=self.iterations.folder_paths["transitions"],
            transition_events=transition_events,
        ).create_and_get_asset()


class Iterations:
    """Persisted iteration collection for one GroupDayTrips run."""

    def __init__(
        self,
        *,
        run_inputs_hash: str,
        is_weekday: bool,
        base_folder: pathlib.Path,
    ) -> None:
        self.run_inputs_hash = run_inputs_hash
        self.is_weekday = is_weekday
        self.base_folder = pathlib.Path(base_folder)
        self.folder_paths = self._build_folder_paths()


    def prepare(self, *, resume: bool = False) -> None:
        """Create run-scoped iteration folders, optionally clearing old contents first."""
        def ensure_dir(path: pathlib.Path) -> pathlib.Path:
            if resume is False:
                shutil.rmtree(path, ignore_errors=True)
            path.mkdir(parents=True, exist_ok=True)
            return path

        self.folder_paths = {name: ensure_dir(path) for name, path in self.folder_paths.items()}


    def get_resume_iteration(self, n_iterations: int) -> int | None:
        """Return and log the effective resume iteration for this run."""
        resume_iteration = self._find_latest_completed_iteration()
        if resume_iteration is not None:
            resume_iteration = min(int(resume_iteration), int(n_iterations))

        if resume_iteration is None:
            logging.info(
                "No saved iteration found for run_key=%s is_weekday=%s. Starting from scratch.",
                self.run_inputs_hash,
                str(self.is_weekday),
            )
            return None

        logging.info(
            "Latest saved iteration found for run_key=%s is_weekday=%s: iteration=%s",
            self.run_inputs_hash,
            str(self.is_weekday),
            str(resume_iteration),
        )
        return resume_iteration


    def iteration(self, iteration: int) -> Iteration:
        """Return the persisted object for one iteration."""
        return Iteration(self, iteration)


    def discard_future_iterations(self, *, iteration: int) -> None:
        """Delete per-iteration artifacts strictly after the given iteration."""
        artifact_patterns = {
            "destination-sequences": ["destination_sequences_*.parquet"],
            "modes": ["mode_sequences_*.parquet"],
            "transitions": ["*transition_events_*.parquet"],
            "iteration-state": [
                "*current_plans_*.parquet",
                "*remaining_opportunities_*.parquet",
                "*rng_state_*.pkl",
                "*iteration_complete_*.json",
            ],
        }
        for folder_name, patterns in artifact_patterns.items():
            for pattern in patterns:
                self._discard_files_after_iteration(
                    self.folder_paths[folder_name],
                    pattern,
                    iteration,
                )


    def list_transition_event_paths(self) -> list[pathlib.Path]:
        """Return persisted transition-event files in iteration order."""
        paths = list(self.folder_paths["transitions"].glob("*transition_events_*.parquet"))
        return sorted(paths, key=self._get_iteration_from_path)


    def remove_all(self) -> None:
        """Remove all run-scoped iteration folders."""
        for path in self.folder_paths.values():
            shutil.rmtree(path, ignore_errors=True)


    def _build_folder_paths(self) -> dict[str, pathlib.Path]:
        """Return the run-scoped folders used for iteration artifacts."""
        folder_names = [
            "destination-sequences",
            "modes",
            "sequences-index",
            "transitions",
            "iteration-state",
        ]
        return {name: self.base_folder / f"{self.run_inputs_hash}-{name}" for name in folder_names}


    def _find_latest_completed_iteration(self) -> int | None:
        """Return the latest completed iteration marker available for this run."""
        return IterationCompleteAsset.find_latest_completed_iteration(
            base_folder=self.folder_paths["iteration-state"],
            run_key=self.run_inputs_hash,
            is_weekday=self.is_weekday,
        )


    def _discard_files_after_iteration(
        self,
        folder: pathlib.Path,
        pattern: str,
        keep_up_to_iteration: int,
    ) -> None:
        """Delete files matching one iteration pattern beyond the keep boundary."""
        for path in folder.glob(pattern):
            file_iteration = self._get_iteration_from_path(path)
            if file_iteration > keep_up_to_iteration:
                path.unlink(missing_ok=True)


    def _get_iteration_from_path(self, path: pathlib.Path) -> int:
        """Extract the iteration number from one artifact filename."""
        match = re.search(r"(\d+)(?=\.[^.]+$)", path.name)
        if match is None:
            raise ValueError(f"Could not infer iteration from artifact path: {path}")
        return int(match.group(1))
