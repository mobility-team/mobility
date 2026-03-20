import json
import logging
import os
import pathlib
import pickle
import random
import re
import shutil
from dataclasses import dataclass
from typing import Any, List

import pandas as pd
import polars as pl

from mobility.choice_models.congestion_state import CongestionState
from mobility.choice_models.destination_sequence_sampler import DestinationSequenceSampler
from mobility.choice_models.population_trips_parameters import PopulationTripsParameters
from mobility.choice_models.population_trips_run_results import PopulationTripsRunResults
from mobility.choice_models.state_initializer import StateInitializer
from mobility.choice_models.state_updater import StateUpdater
from mobility.choice_models.top_k_mode_sequence_search import ModeSequenceSearcher
from mobility.choice_models.transition_schema import TRANSITION_EVENT_SCHEMA
from mobility.choice_models.travel_costs_aggregator import TravelCostsAggregator
from mobility.file_asset import FileAsset
from mobility.motives import Motive
from mobility.parsers.mobility_survey import MobilitySurvey
from mobility.population import Population
from mobility.transport_costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport_modes.transport_mode import TransportMode


@dataclass(frozen=True)
class _ResumePlan:
    run_key: str
    is_weekday: bool
    resume_from_iteration: int | None
    start_iteration: int


@dataclass(frozen=True)
class _ResumeState:
    current_states: pl.DataFrame
    remaining_sinks: pl.DataFrame
    congestion_state: CongestionState | None
    start_iteration: int
    restored: bool


@dataclass(frozen=True)
class _RunContext:
    population: Population
    costs_aggregator: TravelCostsAggregator
    motives: List[Motive]
    modes: List[TransportMode]
    surveys: List[MobilitySurvey]
    parameters: PopulationTripsParameters
    is_weekday: bool
    run_key: str
    tmp_folders: dict[str, pathlib.Path]
    resume_plan: _ResumePlan


@dataclass
class _RunState:
    chains_by_motive: pl.DataFrame
    chains: pl.DataFrame
    demand_groups: pl.DataFrame
    motive_dur: pl.DataFrame
    home_night_dur: pl.DataFrame
    stay_home_state: pl.DataFrame
    sinks: pl.DataFrame
    current_states: pl.DataFrame
    remaining_sinks: pl.DataFrame
    costs: pl.DataFrame
    congestion_state: CongestionState | None
    start_iteration: int
    current_states_steps: pl.DataFrame | None = None


class PopulationTripsRun(FileAsset):
    """Single day-type PopulationTrips asset."""

    def __init__(
        self,
        *,
        population: Population,
        costs_aggregator: TravelCostsAggregator,
        motives: List[Motive],
        modes: List[TransportMode],
        surveys: List[MobilitySurvey],
        parameters: PopulationTripsParameters,
        is_weekday: bool,
        enabled: bool = True,
    ) -> None:
        """Initialize a single weekday or weekend PopulationTrips run."""
        inputs = {
            "version": 1,
            "population": population,
            "costs_aggregator": costs_aggregator,
            "motives": motives,
            "modes": modes,
            "surveys": surveys,
            "parameters": parameters,
            "is_weekday": is_weekday,
            "enabled": enabled,
        }

        self.rng = random.Random(parameters.seed) if parameters.seed is not None else random.Random()
        self.state_initializer = StateInitializer()
        self.destination_sequence_sampler = DestinationSequenceSampler()
        self.mode_sequence_searcher = ModeSequenceSearcher()
        self.state_updater = StateUpdater()

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        period = "weekday" if is_weekday else "weekend"

        cache_path = {
            "flows": project_folder / "population_trips" / period / "flows.parquet",
            "sinks": project_folder / "population_trips" / period / "sinks.parquet",
            "costs": project_folder / "population_trips" / period / "costs.parquet",
            "chains": project_folder / "population_trips" / period / "chains.parquet",
            "transitions": project_folder / "population_trips" / period / "transitions.parquet",
            "demand_groups": project_folder / "population_trips" / period / "demand_groups.parquet",
        }
        super().__init__(inputs, cache_path)


    def create_and_get_asset(self) -> dict[str, pl.LazyFrame]:
        """Run the simulation for this day type and materialize cached outputs."""
        if not self.inputs["enabled"]:
            self._write_empty_outputs()
            return self.get_cached_asset()

        ctx = self._build_run_context()
        state = self._build_run_state(ctx)
        for iteration in range(state.start_iteration, ctx.parameters.n_iterations + 1):
            self._run_iteration(ctx, state, iteration)
            self._save_iteration_state(ctx, state, iteration)

        self._materialize_current_states_steps_if_missing(ctx, state)

        final_costs = self._build_final_costs(ctx, state)
        final_flows = self._build_final_flows(ctx, state, final_costs)
        transitions = self._build_transitions(ctx)

        self._write_outputs(
            flows=final_flows,
            sinks=state.sinks,
            costs=final_costs,
            chains=state.chains,
            transitions=transitions,
            demand_groups=state.demand_groups,
        )

        return self.get_cached_asset()


    def _write_empty_outputs(self) -> None:
        """Write empty outputs for a disabled day type."""
        for path in self.cache_path.values():
            path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame().write_parquet(self.cache_path["flows"])
        pl.DataFrame().write_parquet(self.cache_path["sinks"])
        pl.DataFrame().write_parquet(self.cache_path["costs"])
        pl.DataFrame().write_parquet(self.cache_path["chains"])
        self._empty_transition_events().write_parquet(self.cache_path["transitions"])
        pl.DataFrame().write_parquet(self.cache_path["demand_groups"])


    def _build_run_context(self) -> _RunContext:
        """Assemble immutable run-level context for this execution."""
        parameters = self.inputs["parameters"]
        is_weekday = self.inputs["is_weekday"]
        run_key = self.inputs_hash
        resume_plan = self._build_resume_plan(
            run_key=run_key,
            is_weekday=is_weekday,
            n_iterations=parameters.n_iterations,
        )
        self._log_resume_plan(resume_plan)
        tmp_folders = self._prepare_tmp_folders(
            run_key=run_key,
            base_folder=self.cache_path["flows"].parent,
            resume=(resume_plan.resume_from_iteration is not None),
        )
        return _RunContext(
            population=self.inputs["population"],
            costs_aggregator=self.inputs["costs_aggregator"],
            motives=self.inputs["motives"],
            modes=self.inputs["modes"],
            surveys=self.inputs["surveys"],
            parameters=parameters,
            is_weekday=is_weekday,
            run_key=run_key,
            tmp_folders=tmp_folders,
            resume_plan=resume_plan,
        )


    def _build_resume_plan(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        n_iterations: int,
    ) -> _ResumePlan:
        """Decide whether this run resumes from a saved iteration or starts fresh."""
        latest_saved_iteration = self._find_latest_saved_iteration(
            run_key=run_key,
            is_weekday=is_weekday,
        )
        if latest_saved_iteration is None:
            return _ResumePlan(
                run_key=run_key,
                is_weekday=is_weekday,
                resume_from_iteration=None,
                start_iteration=1,
            )

        last_completed_iteration = min(int(latest_saved_iteration), int(n_iterations))
        return _ResumePlan(
            run_key=run_key,
            is_weekday=is_weekday,
            resume_from_iteration=last_completed_iteration,
            start_iteration=last_completed_iteration + 1,
        )


    def _find_latest_saved_iteration(self, *, run_key: str, is_weekday: bool) -> int | None:
        """Return the latest completed iteration whose saved state is available."""
        iteration_state_folder = self._get_tmp_folder_paths(
            run_key=run_key,
            base_folder=self.cache_path["flows"].parent,
        )["iteration-state"]
        if not iteration_state_folder.exists():
            return None

        completion_files = list(iteration_state_folder.glob("iteration_state_*.json"))
        if not completion_files:
            return None

        latest_iteration = None
        iteration_pattern = re.compile(r"iteration_state_(\d+)\.json$")
        for path in completion_files:
            match = iteration_pattern.search(path.name)
            if match is None:
                continue
            iteration = int(match.group(1))
            if latest_iteration is None or iteration > latest_iteration:
                latest_iteration = iteration

        return latest_iteration


    def _log_resume_plan(self, resume_plan: _ResumePlan) -> None:
        """Log whether this run starts fresh or resumes from a saved iteration."""
        if resume_plan.resume_from_iteration is None:
            logging.info(
                "No saved iteration found for run_key=%s is_weekday=%s. Starting from scratch.",
                resume_plan.run_key,
                str(resume_plan.is_weekday),
            )
            return

        logging.info(
            "Latest saved iteration found for run_key=%s is_weekday=%s: iteration=%s",
            resume_plan.run_key,
            str(resume_plan.is_weekday),
            str(resume_plan.resume_from_iteration),
        )


    def _prepare_tmp_folders(
        self,
        *,
        run_key: str,
        base_folder: pathlib.Path,
        resume: bool = False,
    ) -> dict[str, pathlib.Path]:
        """Create per-run temp folders next to the cache path."""
        def ensure_dir(path: pathlib.Path) -> pathlib.Path:
            if resume is False:
                shutil.rmtree(path, ignore_errors=True)
            os.makedirs(path, exist_ok=True)
            return path

        return {
            name: ensure_dir(path)
            for name, path in self._get_tmp_folder_paths(run_key=run_key, base_folder=base_folder).items()
        }


    def _get_tmp_folder_paths(
        self,
        *,
        run_key: str,
        base_folder: pathlib.Path,
    ) -> dict[str, pathlib.Path]:
        """Return the per-run temp folder paths without creating them."""
        folder_names = [
            "destination-sequences",
            "modes",
            "flows",
            "sequences-index",
            "transitions",
            "iteration-state",
        ]
        return {name: base_folder / f"{run_key}-{name}" for name in folder_names}


    def _build_run_state(self, ctx: _RunContext) -> _RunState:
        """Build the initial mutable state, then apply resume restoration."""
        chains_by_motive, chains, demand_groups = self.state_initializer.get_chains(
            ctx.population,
            ctx.surveys,
            ctx.motives,
            ctx.modes,
            ctx.is_weekday,
        )
        motive_dur, home_night_dur = self.state_initializer.get_mean_activity_durations(
            chains_by_motive,
            demand_groups,
        )
        stay_home_state, current_states = self.state_initializer.get_stay_home_state(
            demand_groups,
            home_night_dur,
            ctx.motives,
            ctx.parameters.min_activity_time_constant,
        )
        sinks = self.state_initializer.get_sinks(
            chains_by_motive,
            ctx.motives,
            ctx.population.transport_zones,
        )
        state = _RunState(
            chains_by_motive=chains_by_motive,
            chains=chains,
            demand_groups=demand_groups,
            motive_dur=motive_dur,
            home_night_dur=home_night_dur,
            stay_home_state=stay_home_state,
            sinks=sinks,
            current_states=current_states,
            remaining_sinks=sinks.clone(),
            costs=self.state_initializer.get_current_costs(
                ctx.costs_aggregator,
                congestion=False,
            ),
            congestion_state=None,
            start_iteration=1,
        )
        self._apply_resume_state(ctx, state)
        return state


    def _apply_resume_state(self, ctx: _RunContext, state: _RunState) -> None:
        """Apply either the restored iteration state or the fresh start state."""
        resume_state = self._restore_resume_state(ctx, state)
        state.current_states = resume_state.current_states
        state.remaining_sinks = resume_state.remaining_sinks
        state.congestion_state = resume_state.congestion_state
        state.start_iteration = resume_state.start_iteration
        if not resume_state.restored:
            return

        logging.info(
            "Resuming PopulationTrips from saved iteration: run_key=%s is_weekday=%s iteration=%s",
            ctx.run_key,
            str(ctx.is_weekday),
            str(ctx.resume_plan.resume_from_iteration),
        )
        state.costs = ctx.costs_aggregator.get(
            congestion=(state.congestion_state is not None),
            congestion_state=state.congestion_state,
        )


    def _restore_resume_state(self, ctx: _RunContext, state: _RunState) -> _ResumeState:
        """Restore the latest saved iteration state, or return a fresh start state."""
        fresh_state = self._build_fresh_resume_state(
            stay_home_state=state.stay_home_state,
            sinks=state.sinks,
        )
        if ctx.resume_plan.resume_from_iteration is None:
            return fresh_state

        saved_state = self._load_saved_iteration_state(
            ctx=ctx,
            iteration=ctx.resume_plan.resume_from_iteration,
        )
        if saved_state is None:
            return fresh_state

        try:
            self.rng.setstate(saved_state["rng_state"])
        except Exception:
            logging.exception("Failed to restore RNG state from saved iteration; restarting from scratch.")
            return fresh_state

        self._prune_iteration_artifacts(
            tmp_folders=ctx.tmp_folders,
            keep_up_to_iteration=ctx.resume_plan.resume_from_iteration,
        )
        congestion_state = self._load_congestion_state(
            ctx=ctx,
            last_completed_iteration=ctx.resume_plan.resume_from_iteration,
        )
        return _ResumeState(
            current_states=saved_state["current_states"],
            remaining_sinks=saved_state["remaining_sinks"],
            congestion_state=congestion_state,
            start_iteration=ctx.resume_plan.start_iteration,
            restored=True,
        )


    def _build_fresh_resume_state(
        self,
        *,
        stay_home_state: pl.DataFrame,
        sinks: pl.DataFrame,
    ) -> _ResumeState:
        """Build the clean initial state used when no saved iteration can be restored."""
        current_states = (
            stay_home_state
            .select(["demand_group_id", "iteration", "motive_seq_id", "mode_seq_id", "dest_seq_id", "utility", "n_persons"])
            .clone()
        )
        return _ResumeState(
            current_states=current_states,
            remaining_sinks=sinks.clone(),
            congestion_state=None,
            start_iteration=1,
            restored=False,
        )


    def _load_saved_iteration_state(
        self,
        *,
        ctx: _RunContext,
        iteration: int,
    ) -> dict[str, object] | None:
        """Load the saved run state for one completed iteration."""
        try:
            paths = self._get_iteration_state_paths(ctx=ctx, iteration=iteration)
            with open(paths["rng_state"], "rb") as file:
                rng_state = pickle.load(file)
            return {
                "current_states": pl.read_parquet(paths["current_states"]),
                "remaining_sinks": pl.read_parquet(paths["remaining_sinks"]),
                "rng_state": rng_state,
            }
        except Exception:
            logging.exception(
                "Failed to load saved iteration state (run_key=%s, is_weekday=%s, iteration=%s).",
                ctx.run_key,
                str(ctx.is_weekday),
                str(iteration),
            )
            return None


    def _load_congestion_state(
        self,
        *,
        ctx: _RunContext,
        last_completed_iteration: int,
    ) -> CongestionState | None:
        """Load the congestion state active after the last completed iteration."""
        if ctx.parameters.n_iter_per_cost_update <= 0 or last_completed_iteration < 1:
            return None

        last_update_iteration = (
            1 + ((last_completed_iteration - 1) // ctx.parameters.n_iter_per_cost_update) * ctx.parameters.n_iter_per_cost_update
        )
        if last_update_iteration < 1:
            return None

        try:
            flow_assets_by_mode = {}
            empty_flows = pd.DataFrame({"from": [], "to": [], "vehicle_volume": []})

            for mode in ctx.costs_aggregator.iter_congestion_enabled_modes():
                mode_name = mode.inputs["parameters"].name
                flow_asset = VehicleODFlowsAsset(
                    vehicle_od_flows=empty_flows,
                    run_key=ctx.run_key,
                    is_weekday=ctx.is_weekday,
                    iteration=last_update_iteration,
                    mode_name=mode_name,
                )
                flow_asset.get()
                flow_assets_by_mode[mode_name] = flow_asset

            if not flow_assets_by_mode:
                return None

            return CongestionState(
                run_key=ctx.run_key,
                is_weekday=ctx.is_weekday,
                iteration=last_update_iteration,
                flow_assets_by_mode=flow_assets_by_mode,
            )
        except Exception:
            logging.exception("Failed to load congestion state on resume; falling back to free-flow costs until next update.")
            return None


    def _run_iteration(self, ctx: _RunContext, state: _RunState, iteration: int) -> None:
        """Execute one simulation iteration and update the mutable run state."""
        logging.info("Iteration %s", str(iteration))
        seed = self.rng.getrandbits(64)

        self._sample_and_write_destination_sequences(ctx, state, iteration, seed)
        self._search_and_write_mode_sequences(ctx, state, iteration)
        transition_events = self._update_iteration_state(ctx, state, iteration)
        self._write_transition_events(ctx, iteration, transition_events)


    def _sample_and_write_destination_sequences(
        self,
        ctx: _RunContext,
        state: _RunState,
        iteration: int,
        seed: int,
    ) -> None:
        """Run destination sampling and persist destination sequences for one iteration."""
        (
            self.destination_sequence_sampler.sample(
                ctx.motives,
                ctx.population.transport_zones,
                state.remaining_sinks,
                iteration,
                state.chains_by_motive,
                state.demand_groups,
                state.costs,
                ctx.tmp_folders,
                ctx.parameters,
                seed,
            )
            .write_parquet(ctx.tmp_folders["destination-sequences"] / f"destination_sequences_{iteration}.parquet")
        )


    def _search_and_write_mode_sequences(self, ctx: _RunContext, state: _RunState, iteration: int) -> None:
        """Run mode-sequence search and persist the results for one iteration."""
        (
            self.mode_sequence_searcher.search(
                iteration,
                ctx.costs_aggregator,
                ctx.tmp_folders,
                ctx.parameters,
                congestion_state=state.congestion_state,
            )
            .write_parquet(ctx.tmp_folders["modes"] / f"mode_sequences_{iteration}.parquet")
        )


    def _update_iteration_state(self, ctx: _RunContext, state: _RunState, iteration: int) -> pl.DataFrame:
        """Advance the simulation state by one iteration and return transition events."""
        state.current_states, state.current_states_steps, transition_events = self.state_updater.get_new_states(
            state.current_states,
            state.demand_groups,
            state.chains_by_motive,
            ctx.costs_aggregator,
            state.congestion_state,
            state.remaining_sinks,
            state.motive_dur,
            iteration,
            ctx.tmp_folders,
            state.home_night_dur,
            state.stay_home_state,
            ctx.parameters,
            ctx.motives,
        )
        state.costs, state.congestion_state = self.state_updater.get_new_costs(
            state.costs,
            iteration,
            ctx.parameters.n_iter_per_cost_update,
            state.current_states_steps,
            ctx.costs_aggregator,
            congestion_state=state.congestion_state,
            run_key=ctx.run_key,
            is_weekday=ctx.is_weekday,
        )
        state.remaining_sinks = self.state_updater.get_new_sinks(
            state.current_states_steps,
            state.sinks,
            ctx.motives,
        )
        return transition_events


    def _write_transition_events(self, ctx: _RunContext, iteration: int, transition_events: pl.DataFrame) -> None:
        """Persist transition events produced for one iteration."""
        transition_events.write_parquet(
            ctx.tmp_folders["transitions"] / f"transition_events_{iteration}.parquet"
        )


    def _save_iteration_state(self, ctx: _RunContext, state: _RunState, iteration: int) -> None:
        """Save the run state after one completed iteration."""
        try:
            paths = self._get_iteration_state_paths(ctx=ctx, iteration=iteration)
            self._write_dataframe_file(paths["current_states"], state.current_states)
            self._write_dataframe_file(paths["remaining_sinks"], state.remaining_sinks)
            self._write_pickle_file(paths["rng_state"], self.rng.getstate())
            self._write_json_file(
                paths["completion"],
                {
                    "run_key": ctx.run_key,
                    "is_weekday": ctx.is_weekday,
                    "iteration": iteration,
                },
            )
        except Exception:
            logging.exception("Failed to save iteration state for iteration %s.", str(iteration))


    def _get_iteration_state_paths(self, *, ctx: _RunContext, iteration: int) -> dict[str, pathlib.Path]:
        """Return the file paths used to save the run state after one iteration."""
        folder = ctx.tmp_folders["iteration-state"]
        return {
            "current_states": folder / f"current_states_{iteration}.parquet",
            "remaining_sinks": folder / f"remaining_sinks_{iteration}.parquet",
            "rng_state": folder / f"rng_state_{iteration}.pkl",
            "completion": folder / f"iteration_state_{iteration}.json",
        }


    def _write_dataframe_file(self, final_path: pathlib.Path, dataframe: pl.DataFrame) -> None:
        """Write a dataframe through a temporary file, then replace the target."""
        temp_path = pathlib.Path(str(final_path) + ".tmp")
        dataframe.write_parquet(temp_path)
        os.replace(temp_path, final_path)


    def _write_pickle_file(self, final_path: pathlib.Path, value: Any) -> None:
        """Write a Python object through a temporary file, then replace the target."""
        temp_path = pathlib.Path(str(final_path) + ".tmp")
        with open(temp_path, "wb") as file:
            file.write(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))
        os.replace(temp_path, final_path)


    def _write_json_file(self, final_path: pathlib.Path, value: dict[str, Any]) -> None:
        """Write JSON through a temporary file, then replace the target."""
        temp_path = pathlib.Path(str(final_path) + ".tmp")
        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(value, file, sort_keys=True)
        os.replace(temp_path, final_path)


    def _prune_iteration_artifacts(
        self,
        *,
        tmp_folders: dict[str, pathlib.Path],
        keep_up_to_iteration: int,
    ) -> None:
        """Delete per-iteration artifacts beyond the last completed iteration."""
        try:
            self._prune_iteration_files(
                tmp_folders["destination-sequences"],
                "destination_sequences_*.parquet",
                keep_up_to_iteration,
            )
            self._prune_iteration_files(
                tmp_folders["modes"],
                "mode_sequences_*.parquet",
                keep_up_to_iteration,
            )
            self._prune_iteration_files(
                tmp_folders["transitions"],
                "transition_events_*.parquet",
                keep_up_to_iteration,
            )
            self._prune_iteration_files(
                tmp_folders["iteration-state"],
                "current_states_*.parquet",
                keep_up_to_iteration,
            )
            self._prune_iteration_files(
                tmp_folders["iteration-state"],
                "remaining_sinks_*.parquet",
                keep_up_to_iteration,
            )
            self._prune_iteration_files(
                tmp_folders["iteration-state"],
                "rng_state_*.pkl",
                keep_up_to_iteration,
            )
            self._prune_iteration_files(
                tmp_folders["iteration-state"],
                "iteration_state_*.json",
                keep_up_to_iteration,
            )
        except Exception:
            logging.exception("Failed to prune iteration artifacts on resume. Continuing anyway.")


    def _prune_iteration_files(
        self,
        folder: pathlib.Path,
        pattern: str,
        keep_up_to_iteration: int,
    ) -> None:
        """Delete files matching one iteration pattern beyond the keep boundary."""
        for path in folder.glob(pattern):
            match = re.search(r"(\d+)(?=\.[^.]+$)", path.name)
            if match is None:
                continue
            iteration = int(match.group(1))
            if iteration > keep_up_to_iteration:
                path.unlink(missing_ok=True)


    def _materialize_current_states_steps_if_missing(self, ctx: _RunContext, state: _RunState) -> None:
        """Materialize per-step state rows when no iteration produced them."""
        if state.current_states_steps is not None:
            return

        possible_states_steps = self.state_updater.get_possible_states_steps(
            state.current_states,
            state.demand_groups,
            state.chains_by_motive,
            ctx.costs_aggregator,
            state.congestion_state,
            state.remaining_sinks,
            state.motive_dur,
            ctx.parameters.n_iterations,
            ctx.motives,
            ctx.parameters.min_activity_time_constant,
            ctx.tmp_folders,
        )
        state.current_states_steps = self.state_updater.get_current_states_steps(
            state.current_states,
            possible_states_steps,
        )


    def _build_final_costs(self, ctx: _RunContext, state: _RunState) -> pl.DataFrame:
        """Compute the final OD costs to attach to the written outputs."""
        return ctx.costs_aggregator.get_costs_by_od_and_mode(
            ["cost", "distance", "time", "ghg_emissions"],
            congestion=(state.congestion_state is not None),
            congestion_state=state.congestion_state,
        )


    def _build_final_flows(self, ctx: _RunContext, state: _RunState, costs: pl.DataFrame) -> pl.DataFrame:
        """Join final per-step states with demand-group attributes and costs."""
        return (
            state.current_states_steps
            .join(
                state.demand_groups.select(["demand_group_id", "home_zone_id", "csp", "n_cars"]),
                on=["demand_group_id"],
            )
            .drop("demand_group_id")
            .join(
                costs,
                on=["from", "to", "mode"],
                how="left",
            )
            .with_columns(
                is_weekday=pl.lit(ctx.is_weekday),
            )
        )


    def _build_transitions(self, ctx: _RunContext) -> pl.DataFrame:
        """Combine persisted per-iteration transition events into the final table."""
        transition_paths = sorted(ctx.tmp_folders["transitions"].glob("transition_events_*.parquet"))
        if not transition_paths:
            return self._empty_transition_events()

        return pl.concat([pl.read_parquet(path) for path in transition_paths], how="vertical")


    def _empty_transition_events(self) -> pl.DataFrame:
        """Return an empty transition-events frame with the expected schema."""
        return pl.DataFrame(schema=TRANSITION_EVENT_SCHEMA)


    def _write_outputs(
        self,
        *,
        flows: pl.DataFrame,
        sinks: pl.DataFrame,
        costs: pl.DataFrame,
        chains: pl.DataFrame,
        transitions: pl.DataFrame,
        demand_groups: pl.DataFrame,
    ) -> None:
        """Write the final run artifacts to their parquet cache paths."""
        flows.write_parquet(self.cache_path["flows"])
        sinks.write_parquet(self.cache_path["sinks"])
        costs.write_parquet(self.cache_path["costs"])
        chains.write_parquet(self.cache_path["chains"])
        transitions.write_parquet(self.cache_path["transitions"])
        demand_groups.write_parquet(self.cache_path["demand_groups"])


    def get_cached_asset(self) -> dict[str, pl.LazyFrame]:
        """Return lazy readers for this run's cached parquet outputs."""
        return {key: pl.scan_parquet(path) for key, path in self.cache_path.items()}


    def results(self) -> PopulationTripsRunResults:
        """Return the analysis helper bound to this run's cached outputs."""
        self.get()
        cached = self.get_cached_asset()

        return PopulationTripsRunResults(
            inputs_hash=self.inputs_hash,
            is_weekday=self.inputs["is_weekday"],
            transport_zones=self.inputs["population"].inputs["transport_zones"],
            demand_groups=cached["demand_groups"],
            states_steps=cached["flows"],
            sinks=cached["sinks"],
            costs=cached["costs"],
            chains=cached["chains"],
            transitions=cached["transitions"],
            surveys=self.inputs["surveys"],
            modes=self.inputs["modes"],
        )


    def evaluate(self, metric, **kwargs) -> object:
        """Evaluate this run using a named run-level metric."""
        results = self.results()

        if metric not in results.metrics_methods:
            available = ", ".join(results.metrics_methods.keys())
            raise ValueError(f"Unknown evaluation metric: {metric}. Available metrics are: {available}")

        return results.metrics_methods[metric](**kwargs)


    def remove(self, remove_checkpoints: bool = True) -> None:
        """Remove cached outputs for this run and its saved iteration state."""
        super().remove()
        for path in self._get_tmp_folder_paths(
            run_key=self.inputs_hash,
            base_folder=self.cache_path["flows"].parent,
        ).values():
            shutil.rmtree(path, ignore_errors=True)
