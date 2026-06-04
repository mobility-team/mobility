import json
import pathlib
import pickle
import random
from typing import Any

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from mobility.activities.activity import (
    resolve_activity_arrival_time_rigidity,
    resolve_activity_parameters,
)
from mobility.trips.group_day_trips.core.run_state import RunState
from mobility.trips.group_day_trips.evaluation.population_weighted_plan_steps import (
    PopulationWeightedPlanSteps,
)
from mobility.trips.group_day_trips.plans.activity_sequences import ActivitySequences
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
from mobility.trips.group_day_trips.plans.mode_sequence_search import ModeSequences
from mobility.trips.group_day_trips.plans.plan_initializer import PlanInitializer
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.transitions.transition_events import TransitionEventsAsset
from ..plans.candidate_plan_steps import CandidatePlanStepsAsset


STATE_TABLE_NAMES = [
    "survey_plans",
    "survey_plan_steps",
    "demand_groups",
    "activity_dur",
    "home_night_dur",
    "stay_home_plan",
    "opportunities",
    "current_plans",
    "current_plan_steps",
    "candidate_plan_steps",
    "destination_saturation",
    "costs",
]


def _state_cache_paths(base_folder: pathlib.Path, iteration: int) -> dict[str, pathlib.Path]:
    """Return the parquet and pickle paths used by one cached run state."""
    folder = pathlib.Path(base_folder) / "iteration-state-cache"
    paths = {
        table_name: folder / f"{table_name}_{iteration}.parquet"
        for table_name in STATE_TABLE_NAMES
    }
    paths["rng_state"] = folder / f"rng_state_{iteration}.pkl"
    paths["metadata"] = folder / f"metadata_{iteration}.json"
    return paths


def _write_run_state(cache_path: dict[str, pathlib.Path], state: RunState, rng_state: object) -> None:
    """Write a run state to parquet files and keep None table flags in metadata."""
    cache_path["metadata"].parent.mkdir(parents=True, exist_ok=True)
    metadata = {
        "current_plan_steps_is_none": state.current_plan_steps is None,
        "candidate_plan_steps_is_none": state.candidate_plan_steps is None,
    }
    for table_name in STATE_TABLE_NAMES:
        table = getattr(state, table_name)
        if table is None:
            table = pl.DataFrame()
        table.write_parquet(cache_path[table_name])

    with open(cache_path["rng_state"], "wb") as file:
        pickle.dump(rng_state, file, protocol=pickle.HIGHEST_PROTOCOL)
    with open(cache_path["metadata"], "w", encoding="utf-8") as file:
        json.dump(metadata, file, sort_keys=True)


def _read_run_state(cache_path: dict[str, pathlib.Path], *, start_iteration: int) -> RunState:
    """Read a cached run state from parquet files."""
    with open(cache_path["metadata"], "r", encoding="utf-8") as file:
        metadata = json.load(file)

    tables = {
        table_name: pl.read_parquet(cache_path[table_name])
        for table_name in STATE_TABLE_NAMES
    }
    if metadata.get("current_plan_steps_is_none", False):
        tables["current_plan_steps"] = None
    if metadata.get("candidate_plan_steps_is_none", False):
        tables["candidate_plan_steps"] = None

    return RunState(
        survey_plans=tables["survey_plans"],
        survey_plan_steps=tables["survey_plan_steps"],
        demand_groups=tables["demand_groups"],
        activity_dur=tables["activity_dur"],
        home_night_dur=tables["home_night_dur"],
        stay_home_plan=tables["stay_home_plan"],
        opportunities=tables["opportunities"],
        current_plans=tables["current_plans"],
        candidate_plan_steps=tables["candidate_plan_steps"],
        destination_saturation=tables["destination_saturation"],
        costs=tables["costs"],
        start_iteration=start_iteration,
        current_plan_steps=tables["current_plan_steps"],
    )


def _read_rng_state(cache_path: dict[str, pathlib.Path]) -> object:
    """Read the random generator state saved with one cached run state."""
    with open(cache_path["rng_state"], "rb") as file:
        return pickle.load(file)


class InitialIterationStateAsset(FileAsset):
    """Cached model state before the first behavior update iteration.

    This asset prepares the survey-derived inputs, the stay-home starting plan,
    the initial destination opportunities, the initial transport costs, and the
    random generator state. It is the root of the iteration-state cache chain.
    """

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        base_folder: pathlib.Path,
        population: Any,
        survey_plan_assets: Any,
        activities: list[Any],
        modes: list[Any],
        parameters: Any,
        scenario: str,
        initial_transport_costs: Any,
    ) -> None:
        self.population = population
        self.survey_plan_assets = survey_plan_assets
        self.activities = activities
        self.modes = modes
        self.parameters = parameters
        self.scenario = scenario
        self.initial_transport_costs = initial_transport_costs
        self.initializer = PlanInitializer()
        self.population_weighted_plan_steps = PopulationWeightedPlanSteps(
            population=population,
            survey_plan_assets=survey_plan_assets,
            is_weekday=is_weekday,
        )
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "population": population,
            "survey_plan_assets": survey_plan_assets,
            "population_weighted_plan_steps": self.population_weighted_plan_steps,
            "activities": activities,
            "modes": modes,
            "parameters": parameters,
            "scenario": scenario,
            "initial_transport_costs": initial_transport_costs,
        }
        super().__init__(inputs, _state_cache_paths(base_folder, 0))
        self.sequence_index_folder = pathlib.Path(base_folder) / f"{self.inputs_hash}-sequences-index"

    def get_cached_asset(self) -> RunState:
        """Return the cached state before iteration 1."""
        return _read_run_state(self.cache_path, start_iteration=1)

    def get_rng_state(self) -> object:
        """Return the cached random generator state."""
        return _read_rng_state(self.cache_path)

    def create_and_get_asset(self) -> RunState:
        """Build and cache the starting state for the iteration loop."""
        self.sequence_index_folder.mkdir(parents=True, exist_ok=True)
        survey_plans, survey_plan_steps, demand_groups = self.initializer.get_survey_plan_data(
            self.population,
            self.survey_plan_assets,
            self.inputs["is_weekday"],
        )
        activity_dur, home_night_dur, activity_demand_per_pers = self.initializer.get_survey_duration_summaries(
            self.population_weighted_plan_steps.get(),
            demand_groups,
        )
        resolved_activity_parameters = resolve_activity_parameters(
            self.activities,
            1,
            scenario=self.scenario,
        )
        stay_home_plan, current_plans = self.initializer.get_stay_home_state(
            demand_groups,
            home_night_dur,
            resolved_activity_parameters["home"],
            self.parameters.plan_update.min_activity_time_constant,
            self.sequence_index_folder,
            self.modes,
        )
        opportunities = self.initializer.get_opportunities(
            activity_demand_per_pers,
            demand_groups,
            self.activities,
            self.population.transport_zones,
        )
        rng = random.Random(self.parameters.run.seed)
        state = RunState(
            survey_plans=survey_plans,
            survey_plan_steps=survey_plan_steps,
            demand_groups=demand_groups,
            activity_dur=activity_dur,
            home_night_dur=home_night_dur,
            stay_home_plan=stay_home_plan,
            opportunities=opportunities,
            current_plans=current_plans,
            candidate_plan_steps=None,
            destination_saturation=opportunities.clone(),
            costs=self.initial_transport_costs.get_costs_by_od(["cost", "distance"]),
            start_iteration=1,
        )
        _write_run_state(self.cache_path, state, rng.getstate())
        return state


class IterationSeedsAsset(FileAsset):
    """Cached random seeds consumed by one model iteration.

    The old run loop drew two random seeds per iteration. This asset keeps the
    same rule while making the draws part of the cache DAG.
    """

    def __init__(
        self,
        *,
        previous_state: FileAsset,
        iteration: int,
        base_folder: pathlib.Path,
    ) -> None:
        self.previous_state = previous_state
        self.iteration = iteration
        inputs = {
            "version": 1,
            "previous_state": previous_state,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / "iteration-seeds" / f"iteration_seeds_{iteration}.pkl"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> dict[str, object]:
        """Return the cached activity and destination seeds for one iteration."""
        with open(self.cache_path, "rb") as file:
            return pickle.load(file)

    def create_and_get_asset(self) -> dict[str, object]:
        """Draw and cache the stochastic seeds used by one iteration."""
        self.previous_state.get()
        rng = random.Random()
        rng.setstate(self.previous_state.get_rng_state())
        seeds = {
            "activity_sequences": rng.getrandbits(64),
            "destination_sequences": rng.getrandbits(64),
            "rng_state_after_sampling": rng.getstate(),
        }
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "wb") as file:
            pickle.dump(seeds, file, protocol=pickle.HIGHEST_PROTOCOL)
        return seeds


class IterationStateAsset(FileAsset):
    """Cached model state after one completed behavior update iteration."""

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        previous_state: FileAsset,
        seeds: IterationSeedsAsset,
        activity_sequences: ActivitySequences,
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
        transport_costs: Any,
        next_transport_costs: Any,
        run_context: Any,
        population: Any,
        activities: list[Any],
        modes: list[Any],
        parameters: Any,
        scenario: str,
        sequence_index_folder: pathlib.Path,
        cache_iteration_events: bool,
    ) -> None:
        self.previous_state = previous_state
        self.seeds = seeds
        self.activity_sequences = activity_sequences
        self.destination_sequences = destination_sequences
        self.mode_sequences = mode_sequences
        self.transport_costs = transport_costs
        self.next_transport_costs = next_transport_costs
        self.run_context = run_context
        self.population = population
        self.activities = activities
        self.modes = modes
        self.parameters = parameters
        self.scenario = scenario
        self.sequence_index_folder = pathlib.Path(sequence_index_folder)
        self.cache_iteration_events = cache_iteration_events
        self.updater = PlanUpdater()
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "previous_state": previous_state,
            "seeds": seeds,
            "activity_sequences": activity_sequences,
            "destination_sequences": destination_sequences,
            "mode_sequences": mode_sequences,
            "transport_costs": transport_costs,
            "next_transport_costs": next_transport_costs,
            "parameters": parameters,
            "scenario": scenario,
            "cache_iteration_events": cache_iteration_events,
        }
        self.iteration = iteration
        super().__init__(inputs, _state_cache_paths(base_folder, iteration))
        self.transition_events_asset = TransitionEventsAsset(
            run_key=run_key,
            is_weekday=is_weekday,
            iteration=iteration,
            base_folder=pathlib.Path(base_folder) / f"{run_key}-transitions",
        )

    def get_cached_asset(self) -> RunState:
        """Return the cached state after this iteration."""
        return _read_run_state(self.cache_path, start_iteration=self.iteration + 1)

    def get_rng_state(self) -> object:
        """Return the random generator state after this iteration's sampling."""
        return _read_rng_state(self.cache_path)

    def create_and_get_asset(self) -> RunState:
        """Run one model iteration and cache the resulting state."""
        self.sequence_index_folder.mkdir(parents=True, exist_ok=True)
        previous = self.previous_state.get()
        seeds = self.seeds.get()
        self.activity_sequences.get()
        self.destination_sequences.get()
        self.mode_sequences.get()

        resolved_activity_parameters = resolve_activity_parameters(
            self.activities,
            self.iteration,
            scenario=self.scenario,
        )
        arrival_time_rigidity_by_activity = resolve_activity_arrival_time_rigidity(
            self.activities,
            self.iteration,
            scenario=self.scenario,
        )
        current_plans, current_plan_steps, candidate_plan_steps, transition_events = self.updater.get_new_plans(
            previous.current_plans,
            previous.current_plan_steps,
            previous.candidate_plan_steps,
            previous.demand_groups,
            previous.survey_plan_steps,
            self.transport_costs,
            previous.destination_saturation,
            previous.activity_dur,
            self.iteration,
            resolved_activity_parameters,
            arrival_time_rigidity_by_activity,
            self.destination_sequences,
            self.mode_sequences,
            previous.home_night_dur,
            previous.stay_home_plan,
            self.population.transport_zones,
            self.sequence_index_folder,
            self.parameters,
        )
        costs = self.updater.get_new_costs(
            previous.costs,
            self.iteration,
            self.parameters.run.n_iter_per_cost_update,
            current_plan_steps,
            self.next_transport_costs,
            run=self.run_context,
        )
        destination_saturation = self.updater.get_destination_saturation(
            current_plan_steps,
            previous.opportunities,
            resolved_activity_parameters,
        )
        state = RunState(
            survey_plans=previous.survey_plans,
            survey_plan_steps=previous.survey_plan_steps,
            demand_groups=previous.demand_groups,
            activity_dur=previous.activity_dur,
            home_night_dur=previous.home_night_dur,
            stay_home_plan=previous.stay_home_plan,
            opportunities=previous.opportunities,
            current_plans=current_plans,
            candidate_plan_steps=candidate_plan_steps,
            destination_saturation=destination_saturation,
            costs=costs,
            start_iteration=self.iteration + 1,
            current_plan_steps=current_plan_steps,
        )
        _write_run_state(self.cache_path, state, seeds["rng_state_after_sampling"])

        if transition_events is not None and self.cache_iteration_events:
            TransitionEventsAsset(
                run_key=self.inputs["run_key"],
                is_weekday=self.inputs["is_weekday"],
                iteration=self.iteration,
                base_folder=self.transition_events_asset.cache_path.parent,
                transition_events=transition_events,
            ).create_and_get_asset()

        return state


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
            except (OSError, json.JSONDecodeError):
                continue

            if marker.get("run_key") != run_key or marker.get("is_weekday") != is_weekday:
                continue

            iteration = marker.get("iteration")
            if not isinstance(iteration, int):
                continue

            if latest_iteration is None or iteration > latest_iteration:
                latest_iteration = iteration

        return latest_iteration


