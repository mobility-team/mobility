import json
import logging
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
from mobility.transport.costs.congestion_state import CongestionState
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
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
        initial_activity_parameters = resolve_activity_parameters(
            activities,
            1,
            scenario=scenario,
        )
        inputs = {
            "version": 2,
            "is_weekday": is_weekday,
            "population": population,
            "survey_plan_assets": survey_plan_assets,
            "population_weighted_plan_steps": self.population_weighted_plan_steps,
            # Initial opportunities depend on the opportunity source, not every
            # future activity parameter value. The resolved iteration-1
            # parameters below cover the coefficients used in the opportunity
            # capacity and saturation columns.
            "activity_opportunity_sources": [
                {
                    "name": activity.name,
                    "has_opportunities": activity.has_opportunities,
                    "opportunities": activity.opportunities,
                    "population": activity.inputs.get("population"),
                }
                for activity in activities
            ],
            "initial_activity_parameters": initial_activity_parameters,
            "modes": modes,
            "run_seed": parameters.run.seed,
            "min_activity_time_constant": parameters.plan_update.min_activity_time_constant,
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
        logging.debug("Building initial group-day-trips iteration state...")
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
        resolved_activity_parameters = self.inputs["initial_activity_parameters"]
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
            resolved_activity_parameters=resolved_activity_parameters,
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
        logging.debug("Initial group-day-trips iteration state is ready.")
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
        logging.debug("Drawing stochastic seeds for iteration %s...", str(self.iteration))
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
        logging.debug("Stochastic seeds for iteration %s are ready.", str(self.iteration))
        return seeds


class CongestionFlowsAsset(FileAsset):
    """Cached vehicle flows used to update congested costs after one iteration.

    The input is the previous iteration state, not the run folder. This means two
    scenario runs that produce the same current plan steps will reuse the same
    congestion-flow files before rebuilding the next transport costs.
    """

    def __init__(
        self,
        *,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        previous_state: FileAsset | None,
        transport_costs: Any,
        n_iter_per_cost_update: int,
    ) -> None:
        self.is_weekday = is_weekday
        self.iteration = iteration
        self.previous_state = previous_state
        self.transport_costs = transport_costs
        self.n_iter_per_cost_update = int(n_iter_per_cost_update)
        inputs = {
            "version": 1,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "previous_state": previous_state,
            "transport_costs": transport_costs,
            "n_iter_per_cost_update": int(n_iter_per_cost_update),
        }
        cache_path = pathlib.Path(base_folder) / "congestion-flows" / f"congestion_flows_{iteration}.json"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> CongestionState | None:
        """Return the cached congestion state, or None when no update is due."""
        with open(self.cache_path, "r", encoding="utf-8") as file:
            metadata = json.load(file)

        if metadata["has_congestion_state"] is False:
            return None

        flow_assets_by_mode = {
            mode_name: VehicleODFlowsAsset.from_inputs(
                run_key=self.inputs_hash,
                is_weekday=self.is_weekday,
                iteration=self.iteration - 1,
                mode_name=mode_name,
            )
            for mode_name in metadata["mode_names"]
        }
        return CongestionState(
            run_key=self.inputs_hash,
            is_weekday=self.is_weekday,
            iteration=self.iteration - 1,
            flow_assets_by_mode=flow_assets_by_mode,
        )

    def create_and_get_asset(self) -> CongestionState | None:
        """Aggregate plan-step flows and cache the matching vehicle-flow assets."""
        congestion_state = None

        # Iteration 1 uses the base transport costs. Later iterations only build
        # congestion flows when the configured cost-update schedule says so.
        should_update_costs = (
            self.previous_state is not None
            and self.iteration > 1
            and self.transport_costs.has_enabled_congestion()
            and self.n_iter_per_cost_update > 0
            and self.transport_costs.should_recompute_congested_costs(
                self.iteration - 1,
                self.n_iter_per_cost_update,
            )
        )

        if should_update_costs:
            logging.debug(
                "Building congestion flows after iteration %s...",
                str(self.iteration - 1),
            )
            previous = self.previous_state.get()
            od_flows_by_mode = (
                previous.current_plan_steps
                .filter(pl.col("activity_seq_id") != 0)
                .with_columns(mode=pl.col("mode").cast(pl.String))
                .group_by(["from", "to", "mode"])
                .agg(flow_volume=pl.col("n_persons").sum())
            )
            congestion_state = self.transport_costs.build_congestion_state(
                od_flows_by_mode,
                run_key=self.inputs_hash,
                is_weekday=self.is_weekday,
                iteration=self.iteration - 1,
            )
        else:
            logging.debug(
                "Skipping congestion-flow update before iteration %s.",
                str(self.iteration),
            )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as file:
            json.dump(
                {
                    "has_congestion_state": congestion_state is not None,
                    "mode_names": (
                        sorted(congestion_state.flow_assets_by_mode)
                        if congestion_state is not None
                        else []
                    ),
                },
                file,
                sort_keys=True,
            )
        logging.debug(
            "Congestion-flow asset for iteration %s is ready.",
            str(self.iteration),
        )
        return congestion_state


class IterationTransportCostsAsset(FileAsset):
    """Cached full transport costs used by one model iteration.

    This asset is the DAG edge between iteration states and congestion-sensitive
    transport costs. It resolves mode parameters for the current iteration, then
    applies the vehicle flows produced by the previous completed iteration when a
    congestion update is due.
    """

    def __init__(
        self,
        *,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        transport_costs: Any,
        scenario: str,
        previous_state: FileAsset | None,
        n_iter_per_cost_update: int,
    ) -> None:
        self.is_weekday = is_weekday
        self.iteration = iteration
        self.transport_costs = transport_costs.for_iteration(
            iteration,
            scenario=scenario,
        )
        self.congestion_flows = CongestionFlowsAsset(
            is_weekday=is_weekday,
            iteration=iteration,
            base_folder=base_folder,
            previous_state=previous_state,
            transport_costs=self.transport_costs,
            n_iter_per_cost_update=n_iter_per_cost_update,
        )
        self.modes = self.transport_costs.modes
        inputs = {
            "version": 1,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "transport_costs": self.transport_costs,
            "congestion_flows": self.congestion_flows,
        }
        cache_path = pathlib.Path(base_folder) / "iteration-transport-costs" / f"transport_costs_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        """Return the full OD-by-mode transport-cost table for this iteration."""
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build and cache transport costs for this iteration."""
        logging.debug(
            "Building transport costs for group-day-trips iteration %s...",
            str(self.iteration),
        )
        congestion_state = self.congestion_flows.get()
        effective_transport_costs = self.transport_costs.asset_for_congestion_state(
            congestion_state
        )
        costs = effective_transport_costs.get()
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        costs.write_parquet(self.cache_path)
        logging.debug(
            "Transport costs for group-day-trips iteration %s are ready.",
            str(self.iteration),
        )
        return costs

    def get_costs_by_od_and_mode(
        self,
        metrics: list,
        detail_distances: bool = False,
    ) -> pl.DataFrame:
        """Project cached costs to the OD-by-mode view used by plan scoring."""
        metrics = list(metrics)
        costs = self.get()

        dist_cols = [col for col in costs.columns if col.endswith("_distance")]
        selected_metrics = [metric for metric in metrics if metric != "ghg_emissions"]
        if (
            ("ghg_emissions" in metrics or "ghg_emissions_per_trip" in metrics)
            and "ghg_emissions_per_trip" not in selected_metrics
        ):
            selected_metrics.append("ghg_emissions_per_trip")
        if detail_distances:
            selected_metrics.extend(dist_cols)

        selected_metrics = list(dict.fromkeys(selected_metrics))
        columns = ["from", "to", "mode"] + selected_metrics
        available_columns = [column for column in columns if column in costs.columns]
        return costs.select(available_columns)

    def get_costs_by_od(self, metrics: list) -> pl.DataFrame:
        """Aggregate cached costs to the OD-only view used by destination choice."""
        costs = self.get_costs_by_od_and_mode(metrics, detail_distances=False)
        costs = costs.with_columns((pl.col("cost").neg().exp()).alias("prob"))
        costs = costs.with_columns(
            (pl.col("prob") / pl.col("prob").sum().over(["from", "to"])).alias("prob")
        )
        costs = costs.with_columns((pl.col("prob") * pl.col("cost")).alias("cost"))
        return costs.group_by(["from", "to"]).agg(pl.col("cost").sum())


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
        self.population = population
        self.activities = activities
        self.modes = modes
        self.parameters = parameters
        self.scenario = scenario
        self.sequence_index_folder = pathlib.Path(sequence_index_folder)
        self.cache_iteration_events = cache_iteration_events
        self.updater = PlanUpdater()
        inputs = {
            "version": 2,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "previous_state": previous_state,
            "seeds": seeds,
            "activity_sequences": activity_sequences,
            "destination_sequences": destination_sequences,
            "mode_sequences": mode_sequences,
            "transport_costs": transport_costs,
            # These resolved values are the activity/scenario inputs consumed by
            # the plan update. Keeping them here avoids a broad run-level hash.
            "resolved_activity_parameters": resolve_activity_parameters(
                activities,
                iteration,
                scenario=scenario,
            ),
            "arrival_time_rigidity_by_activity": resolve_activity_arrival_time_rigidity(
                activities,
                iteration,
                scenario=scenario,
            ),
            "plan_update_parameters": parameters.plan_update,
            "behavior_change_scope": parameters.behavior_change.scope_at(iteration),
            "cache_iteration_events": cache_iteration_events,
        }
        self.iteration = iteration
        super().__init__(inputs, _state_cache_paths(base_folder, iteration))
        self.transition_events_asset = TransitionEventsAsset(
            run_key=self.inputs_hash,
            is_weekday=is_weekday,
            iteration=iteration,
            base_folder=pathlib.Path(base_folder) / "transition-events",
        )

    def get_cached_asset(self) -> RunState:
        """Return the cached state after this iteration."""
        return _read_run_state(self.cache_path, start_iteration=self.iteration + 1)

    def get_rng_state(self) -> object:
        """Return the random generator state after this iteration's sampling."""
        return _read_rng_state(self.cache_path)

    def create_and_get_asset(self) -> RunState:
        """Run one model iteration and cache the resulting state."""
        logging.debug("Starting group-day-trips iteration %s...", str(self.iteration))
        self.sequence_index_folder.mkdir(parents=True, exist_ok=True)
        previous = self.previous_state.get()
        seeds = self.seeds.get()
        logging.debug("Preparing sampled alternatives for iteration %s...", str(self.iteration))
        self.activity_sequences.get()
        self.destination_sequences.get()
        self.mode_sequences.get()

        logging.debug("Updating plan choices for iteration %s...", str(self.iteration))
        resolved_activity_parameters = self.inputs["resolved_activity_parameters"]
        arrival_time_rigidity_by_activity = self.inputs["arrival_time_rigidity_by_activity"]
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
        costs = self.transport_costs.get_costs_by_od(["cost", "distance"])
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
                run_key=self.inputs_hash,
                is_weekday=self.inputs["is_weekday"],
                iteration=self.iteration,
                base_folder=self.transition_events_asset.cache_path.parent,
                transition_events=transition_events,
            ).create_and_get_asset()

        logging.debug("Group-day-trips iteration %s is ready.", str(self.iteration))
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


