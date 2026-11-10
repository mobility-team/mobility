import json
import logging
import pathlib
import pickle
import random
import shutil
import tempfile
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import trace_simulation_step

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.cache_schema import read_cached_parquet
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.temporary import write_temporary_bytes, write_temporary_parquet
from mobility.runtime.assets.resolver import (
    asset_resolution_context,
    get_current_asset_resolver,
)
from mobility.runtime.assets.graph import asset_graph_key
from mobility.activities.activity import (
    resolve_activity_arrival_time_rigidity,
    resolve_activity_parameters,
)
from mobility.runtime.parameter_values import SensitivityCase
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
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.modes.core.mode_values import get_mode_values
from mobility.trips.group_day_trips.core.progress import get_group_day_trips_progress
from ..plans.candidate_plan_steps import CandidatePlanStepsAsset
from ..plans.compact_candidate_steps import CompactCandidateSteps
from ..plans.plan_ids import PLAN_KEY_SCHEMA, PLAN_STEP_KEY_SCHEMA


CURRENT_PLANS_SCHEMA = {
    **PLAN_KEY_SCHEMA,
    "plan_id": pl.UInt32,
    "n_persons": pl.Float64,
}
CURRENT_PLAN_STEPS_SCHEMA = {
    **PLAN_STEP_KEY_SCHEMA,
}
PLAN_ID_INDEX_SCHEMA = {
    **PLAN_KEY_SCHEMA,
    "plan_id": pl.UInt32,
}


def _nested_lists_to_tuples(value: Any) -> Any:
    """Restore the tuple structure expected by ``random.Random.setstate``."""
    if isinstance(value, list):
        return tuple(_nested_lists_to_tuples(item) for item in value)
    return value


_INITIAL_STATE_TABLES = (
    "survey_plans",
    "survey_plan_steps",
    "demand_groups",
    "activity_dur",
    "home_night_dur",
    "stay_home_plan",
    "opportunities",
    "current_plans",
    "candidate_plan_steps",
    "plan_id_index",
    "destination_saturation",
    "costs",
    "current_plan_steps",
    "destination_sequence_index",
    "mode_sequence_index",
)


class InitialIterationStateAsset(InMemoryAsset):
    """Model state before the first behavior update iteration.

    This asset prepares the survey-derived inputs, the stay-home starting plan,
    the initial destination opportunities, the initial transport costs, and the
    random generator state. It is the root of the iteration-state cache chain.
    """

    is_execution_state = True

    def __init__(
        self,
        *,
        is_weekday: bool,
        population: Any,
        survey_plan_assets: Any,
        activities: list[Any],
        modes: list[Any],
        parameters: Any,
        scenario: str,
        initial_transport_costs: Any,
        sensitivity_case: SensitivityCase | None = None,
    ) -> None:
        self.population = population
        self.survey_plan_assets = survey_plan_assets
        self.activities = activities
        self.modes = modes
        self.parameters = parameters
        self.scenario = scenario
        self.sensitivity_case = sensitivity_case
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
            sensitivity_case=sensitivity_case,
        )
        inputs = {
            "version": 9,
            "is_weekday": is_weekday,
            "sensitivity_case": sensitivity_case,
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
            # The initial stay-home state only needs the canonical mode enum.
            # Keep the full mode objects as runtime attributes so scenario
            # routing parameters do not leak into this initial-state hash.
            "mode_values": get_mode_values(modes, "stay_home"),
            "run_seed": parameters.run.seed,
            "demand_group_parameters": parameters.demand_groups,
            "activity_support_threshold": parameters.activity_sequences.activity_support_threshold,
            "min_activity_time_constant": parameters.plan_update.min_activity_time_constant,
            "initial_transport_costs": initial_transport_costs,
        }
        super().__init__(inputs)

    def get(self) -> RunState:
        """Return the initial state, spilling it when later roots reuse it."""
        resolver = get_current_asset_resolver()
        if resolver is not None:
            with resolver.execution_cost_consumer(self):
                state = resolver.get_in_memory_asset(
                    self,
                    self.create_and_get_asset,
                    spill_to_disk=self._spill_state,
                    load_from_disk=self._load_spilled_state,
                )
            resolver.consume_execution_cost_state(self)
            return state
        with asset_resolution_context() as resolver:
            with resolver.execution_cost_consumer(self):
                state = resolver.get_in_memory_asset(
                    self,
                    self.create_and_get_asset,
                    spill_to_disk=self._spill_state,
                    load_from_disk=self._load_spilled_state,
                )
            resolver.consume_execution_cost_state(self)
            return state

    @staticmethod
    def _spill_state(state: RunState, path: pathlib.Path) -> None:
        """Write every initial table to temporary Parquet files."""
        if path.exists():
            raise FileExistsError(f"A state spill already exists: {path}")
        temporary_path = pathlib.Path(tempfile.mkdtemp(prefix=f".{path.name}-", dir=path.parent))
        try:
            none_tables = []
            for table_name in _INITIAL_STATE_TABLES:
                table = getattr(state, table_name)
                if table is None:
                    none_tables.append(table_name)
                else:
                    if isinstance(table, CompactCandidateSteps):
                        table = table.lazy()
                    write_temporary_parquet(table, temporary_path / f"{table_name}.parquet")
            metadata = {
                "none_tables": none_tables,
                "start_step": state.start_step,
                "rng_state": state.rng_state,
            }
            write_temporary_bytes(
                json.dumps(metadata, sort_keys=True).encode("utf-8"),
                temporary_path / "metadata.json",
            )
            temporary_path.rename(path)
        except BaseException:
            shutil.rmtree(temporary_path, ignore_errors=True)
            raise

    @staticmethod
    def _load_spilled_state(path: pathlib.Path) -> RunState:
        """Restore the initial tables from their temporary Parquet files."""
        with (path / "metadata.json").open("r", encoding="utf-8") as file:
            metadata = json.load(file)
        state = {
            table_name: (
                None
                if table_name in metadata["none_tables"]
                else pl.read_parquet(path / f"{table_name}.parquet")
            )
            for table_name in _INITIAL_STATE_TABLES
        }
        return RunState(
            **state,
            start_step=metadata["start_step"],
            rng_state=_nested_lists_to_tuples(metadata["rng_state"]),
        )

    def get_rng_state(self) -> object:
        """Return the random generator state held by the initial state."""
        return self.get().rng_state

    @trace_simulation_step("iteration asset build")
    def create_and_get_asset(self) -> RunState:
        """Build the starting state for the iteration loop."""
        logging.debug("Building initial group-day-trips iteration state...")
        get_group_day_trips_progress().step("Building initial population state")
        survey_plans, survey_plan_steps, demand_groups = self.initializer.get_survey_plan_data(
            self.population,
            self.survey_plan_assets,
            self.inputs["is_weekday"],
            self.parameters,
        )
        activity_dur, home_night_dur, activity_demand_per_pers = self.initializer.get_survey_duration_summaries(
            self.population_weighted_plan_steps.get(),
            demand_groups,
        )
        resolved_activity_parameters = self.inputs["initial_activity_parameters"]
        stay_home_plan, current_plans, plan_id_index = self.initializer.get_stay_home_state(
            demand_groups,
            home_night_dur,
            resolved_activity_parameters["home"],
            self.parameters.plan_update.min_activity_time_constant,
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
            plan_id_index=plan_id_index,
            destination_saturation=opportunities.clone(),
            costs=self.initial_transport_costs.get_costs_by_od(["cost", "distance"]),
            start_step=1,
            rng_state=rng.getstate(),
        )
        logging.debug("Initial group-day-trips iteration state is ready.")
        return state


class IterationSeedsAsset(InMemoryAsset):
    """Random seeds consumed by one model iteration.

    The old run loop drew two random seeds per iteration. This asset keeps the
    same rule while making the draws part of the cache DAG.
    """

    def __init__(
        self,
        *,
        previous_state: Asset,
        iteration: int,
        batch_index: int = 0,
    ) -> None:
        self.previous_state = previous_state
        self.iteration = iteration
        self.batch_index = batch_index
        inputs = {
            "version": 2,
            "previous_state": previous_state,
            "iteration": iteration,
            "batch_index": batch_index,
        }
        super().__init__(inputs)

    @trace_simulation_step("iteration asset build")
    def create_and_get_asset(self) -> dict[str, object]:
        """Draw the stochastic seeds used by one iteration."""
        logging.debug("Drawing stochastic seeds for iteration %s...", str(self.iteration))
        get_group_day_trips_progress().iteration_step(self.iteration, "drawing random seeds")
        self.previous_state.get()
        rng = random.Random()
        rng.setstate(self.previous_state.get_rng_state())
        activity_seed = destination_seed = None
        for _ in range(self.batch_index + 1):
            activity_seed = rng.getrandbits(64)
            destination_seed = rng.getrandbits(64)
        seeds = {
            "activity_sequences": activity_seed,
            "destination_sequences": destination_seed,
            "rng_state_after_sampling": rng.getstate(),
        }
        logging.debug("Stochastic seeds for iteration %s are ready.", str(self.iteration))
        return seeds


class PersonODFlowsByModeAsset(InMemoryAsset):
    """Person OD flows by mode after one completed iteration."""

    def __init__(
        self,
        *,
        source_iteration: int,
        previous_state: Asset,
    ) -> None:
        self.source_iteration = int(source_iteration)
        self.iteration = self.source_iteration
        self.previous_state = previous_state
        inputs = {
            "version": 3,
            "source_iteration": int(source_iteration),
            "previous_state": previous_state,
        }
        super().__init__(inputs)

    @trace_simulation_step("iteration asset build")
    def create_and_get_asset(self) -> pl.DataFrame:
        """Aggregate current plan steps into person OD flows by mode."""
        previous = self.previous_state.get()
        if previous.current_plan_steps is None:
            flows = pl.DataFrame(
                schema={
                    "from": pl.Int32,
                    "to": pl.Int32,
                    "mode": pl.String,
                    "flow_volume": pl.Float64,
                }
            )
        else:
            flows = (
                previous.current_plan_steps
                .filter(pl.col("activity_seq_id") != 0)
                .with_columns(
                    pl.col("from").cast(pl.Int32),
                    pl.col("to").cast(pl.Int32),
                    mode=pl.col("mode").cast(pl.String),
                )
                .group_by(["from", "to", "mode"])
                # A different plan-row order must not change the sum sent to
                # traffic assignment through floating-point rounding.
                .agg(flow_volume=pl.col("n_persons").cast(pl.Float64).sort().implode().list.sum())
                .sort(["from", "to", "mode"])
            )

        logging.debug(
            "Person OD flows by mode after iteration %s are ready.",
            str(self.source_iteration),
        )
        return flows


class CongestionFlowsAsset(InMemoryAsset):
    """Vehicle flows used to update congested costs after one iteration.

    The inputs are the previous iteration state and the road flow conversion
    settings. This means two scenario runs that share the same upstream state
    and road settings also share the road traffic assignment.
    """

    def __init__(
        self,
        *,
        is_weekday: bool,
        iteration: int,
        previous_state: Asset | None,
        transport_costs: Any,
        n_steps_per_cost_update: int,
    ) -> None:
        self.is_weekday = is_weekday
        self.iteration = iteration
        self.previous_state = previous_state
        self.transport_costs = transport_costs
        self.n_steps_per_cost_update = int(n_steps_per_cost_update)
        self.road_flow_parameters = (
            transport_costs.road_flows.road_flow_parameters()
            if transport_costs.has_enabled_congestion()
            else []
        )
        self.should_update_costs = (
            previous_state is not None
            and iteration > 1
            and bool(self.road_flow_parameters)
            and self.n_steps_per_cost_update > 0
            and transport_costs.should_recompute_congested_costs(
                iteration - 1,
                self.n_steps_per_cost_update,
            )
        )
        self.previous_congestion_flows = (
            self._previous_congestion_flows(previous_state)
            if (
                previous_state is not None
                and iteration > 1
                and bool(self.road_flow_parameters)
                and self.n_steps_per_cost_update > 0
                and not self.should_update_costs
            )
            else None
        )
        self.person_od_flows_by_mode = (
            PersonODFlowsByModeAsset(
                source_iteration=iteration - 1,
                previous_state=previous_state,
            )
            if self.should_update_costs
            else None
        )
        inputs = {
            "version": 5,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "previous_congestion_flows": self.previous_congestion_flows,
            "person_od_flows_by_mode": self.person_od_flows_by_mode,
            "road_flow_parameters": self.road_flow_parameters,
            "n_steps_per_cost_update": int(n_steps_per_cost_update),
        }
        super().__init__(inputs)

    def _previous_congestion_flows(self, previous_state: Asset | None) -> Asset | None:
        """Return the previous iteration congestion-flow asset when it exists."""
        if previous_state is None:
            return None

        previous_transport_costs = previous_state.inputs.get("transport_costs")
        return getattr(previous_transport_costs, "congestion_flows", None)

    @trace_simulation_step("iteration asset build")
    def create_and_get_asset(self) -> VehicleODFlowsAsset | None:
        """Build road vehicle flows from the matching iteration state."""
        road_flow_asset = None

        if self.should_update_costs:
            logging.debug(
                "Building congestion flows after iteration %s...",
                str(self.iteration - 1),
            )
            get_group_day_trips_progress().iteration_step(
                self.iteration,
                "updating congestion flows",
            )
            road_flow_asset = self.transport_costs.build_road_flow_asset(
                self.person_od_flows_by_mode,
            )
        elif self.previous_congestion_flows is not None:
            road_flow_asset = self.previous_congestion_flows.get()
            logging.debug(
                "Reusing congestion flows before iteration %s.",
                str(self.iteration),
            )
        else:
            logging.debug(
                "Skipping congestion-flow update before iteration %s.",
                str(self.iteration),
            )

        logging.debug(
            "Congestion-flow asset for iteration %s is ready.",
            str(self.iteration),
        )
        return road_flow_asset


class IterationTransportCostsAsset(InMemoryAsset):
    """Full transport costs used by one model iteration.

    This asset is the DAG edge between iteration states and congestion-sensitive
    transport costs. It resolves mode parameters for the current iteration, then
    applies the vehicle flows produced by the previous completed iteration when a
    congestion update is due.
    """

    is_execution_scoped_cost_asset = True

    def __init__(
        self,
        *,
        is_weekday: bool,
        iteration: int,
        transport_costs: Any,
        scenario: str,
        previous_state: Asset | None,
        n_steps_per_cost_update: int,
        sensitivity_case: SensitivityCase | None = None,
    ) -> None:
        self.is_weekday = is_weekday
        self.iteration = iteration
        self.transport_costs = transport_costs.for_iteration(
            iteration,
            scenario=scenario,
            sensitivity_case=sensitivity_case,
        )
        self.congestion_flows = CongestionFlowsAsset(
            is_weekday=is_weekday,
            iteration=iteration,
            previous_state=previous_state,
            transport_costs=self.transport_costs,
            n_steps_per_cost_update=n_steps_per_cost_update,
        )
        self.modes = self.transport_costs.modes
        self._profile_costs_cache = None
        self._profile_costs_cache_key = None
        inputs = {
            "version": 2,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "sensitivity_case": sensitivity_case,
            "transport_costs": self.transport_costs,
            "congestion_flows": self.congestion_flows,
        }
        super().__init__(inputs)

    @trace_simulation_step("iteration asset build")
    def create_and_get_asset(self) -> pl.DataFrame:
        """Build the full OD-by-mode transport-cost table for this iteration."""
        logging.debug(
            "Building transport costs for group-day-trips iteration %s...",
            str(self.iteration),
        )
        get_group_day_trips_progress().iteration_step(self.iteration, "preparing transport costs")
        road_flow_asset = self.congestion_flows.get()
        if road_flow_asset is None:
            costs = self.transport_costs.get()
        else:
            effective_transport_costs = self.transport_costs.asset_for_road_flows(
                road_flow_asset
            )
            costs = effective_transport_costs._build_full_detail_costs()
        resolver = get_current_asset_resolver()
        if resolver is not None:
            resolver.release_in_memory_asset(self.congestion_flows)
            if self.congestion_flows.person_od_flows_by_mode is not None:
                resolver.release_in_memory_asset(
                    self.congestion_flows.person_od_flows_by_mode
                )
            if road_flow_asset is not None:
                resolver.release_in_memory_asset(road_flow_asset)
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

    @trace_simulation_step("expected OD transport costs")
    def get_costs_by_od(self, metrics: list) -> pl.DataFrame:
        """Aggregate cached costs to the OD-only view used by destination choice."""
        costs = self.get_costs_by_od_and_mode(metrics, detail_distances=False)
        costs = costs.with_columns((pl.col("cost").neg().exp()).alias("prob"))
        costs = costs.with_columns(
            (pl.col("prob") / pl.col("prob").sum().over(["from", "to"])).alias("prob")
        )
        costs = costs.with_columns((pl.col("prob") * pl.col("cost")).alias("cost"))
        return costs.group_by(["from", "to"]).agg(pl.col("cost").sum())

    def get_utility_profiles(self, demand_groups, population_segments):
        """Delegate compact utility-profile construction to transport costs."""
        return self.transport_costs.get_utility_profiles(
            demand_groups,
            population_segments,
        )

    @trace_simulation_step("profile transport costs")
    def get_profile_costs_by_od_and_mode(self, profiles, metrics):
        """Return profile-specific costs with this iteration's congestion state."""
        cache_key = tuple(
            (profile_id, tuple(asset.inputs_hash for asset in assets))
            for profile_id, assets in sorted(profiles.items())
        )
        if self._profile_costs_cache_key != cache_key:
            effective_transport_costs = self.transport_costs.asset_for_road_flows(
                self.congestion_flows.get()
            )
            self._profile_costs_cache = (
                effective_transport_costs.get_profile_costs_by_od_and_mode(
                    profiles,
                    ["cost", "distance", "time"],
                )
            )
            self._profile_costs_cache_key = cache_key
        return self._profile_costs_cache.select(
            ["from", "to", "mode", "utility_profile_id"] + list(metrics)
        )


class IterationStateAsset(Asset):
    """Transient model state after one completed behavior update iteration."""

    is_execution_state = True

    def __init__(
        self,
        *,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        previous_state: Asset,
        initial_state: InitialIterationStateAsset,
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
        cache_step_events: bool,
        sensitivity_case: SensitivityCase | None = None,
    ) -> None:
        self.previous_state = previous_state
        self.initial_state = initial_state
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
        self.sensitivity_case = sensitivity_case
        self.cache_step_events = cache_step_events
        self.updater = PlanUpdater()
        self.checkpoint_callback = None
        self.resume_checkpoint_loader = None
        self._rng_state = None
        self._loaded_checkpoint = False
        self._checkpoint_saved = False
        self.coordinated_candidate_plan_steps: pl.DataFrame | pl.LazyFrame | None = None
        self.coordinated_evaluation = None
        self.coordinated_current_plans: pl.DataFrame | None = None
        self.coordinated_current_plan_steps: pl.DataFrame | None = None
        self.coordinated_plan_id_index: pl.DataFrame | None = None
        self.coordinated_destination_sequence_index: pl.DataFrame | None = None
        self.coordinated_mode_sequence_index: pl.DataFrame | None = None
        self.coordinated_rng_state: object | None = None
        self.coordinated_time_sequence_steps: pl.DataFrame | None = None
        inputs = {
            "version": 22,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "sensitivity_case": sensitivity_case,
            "previous_state": previous_state,
            "initial_state": initial_state,
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
                sensitivity_case=sensitivity_case,
            ),
            "arrival_time_rigidity_by_activity": resolve_activity_arrival_time_rigidity(
                activities,
                iteration,
                scenario=scenario,
                sensitivity_case=sensitivity_case,
            ),
            "plan_update_parameters": parameters.plan_update,
            "behavior_change_scope": parameters.behavior_change.scope_at(iteration),
            "cache_step_events": cache_step_events,
        }
        self.iteration = iteration
        super().__init__(inputs)
        self.transition_events_asset = (
            TransitionEventsAsset(
                run_key=self.inputs_hash,
                is_weekday=is_weekday,
                iteration=iteration,
                base_folder=pathlib.Path(base_folder) / "transition-events",
            )
            if cache_step_events
            else None
        )

    def get(self) -> RunState:
        """Return this state, spilling shared intermediates to temporary disk."""
        resolver = get_current_asset_resolver()
        if resolver is not None:
            with resolver.execution_cost_consumer(self):
                state = resolver.get_in_memory_asset(
                    self,
                    self.create_and_get_asset,
                    spill_to_disk=self._spill_state,
                    load_from_disk=self._load_spilled_state,
                )
        else:
            with asset_resolution_context() as resolver:
                with resolver.execution_cost_consumer(self):
                    state = resolver.get_in_memory_asset(
                        self,
                        self.create_and_get_asset,
                        spill_to_disk=self._spill_state,
                        load_from_disk=self._load_spilled_state,
                    )

        self._rng_state = state.rng_state
        if self.checkpoint_callback is not None and not self._checkpoint_saved:
            if self._loaded_checkpoint:
                self._checkpoint_saved = True
            else:
                self.checkpoint_callback(self.iteration, state, self._rng_state)
                self._checkpoint_saved = True

        # Release the prior state after this state has consumed it. This also
        # runs when the current state was restored from a spill or checkpoint.
        active_resolver = get_current_asset_resolver()
        if active_resolver is not None:
            if self.coordinated_candidate_plan_steps is None:
                active_resolver.release_in_memory_asset(self.previous_state)
            for asset in (
                self.seeds,
                self.activity_sequences,
                self.destination_sequences,
                self.mode_sequences,
            ):
                active_resolver.release_in_memory_asset(asset)
            if self.iteration < self.parameters.run.n_steps:
                active_resolver.release_in_memory_asset(self.transport_costs)
            active_resolver.consume_execution_cost_state(self)
        self.destination_sequences.sequence_index = None
        self.destination_sequences.previous_sequence_index = None
        self.mode_sequences.sequence_index = None
        self.mode_sequences.previous_sequence_index = None
        return state

    def _spill_state(self, state: RunState, path: pathlib.Path) -> None:
        """Write shared plan tables to parquet and the RNG tuple to JSON."""
        if path.exists():
            raise FileExistsError(f"A state spill already exists: {path}")
        temporary_path = pathlib.Path(tempfile.mkdtemp(prefix=f".{path.name}-", dir=path.parent))
        table_names = [
            "current_plans",
            "current_plan_steps",
            "candidate_plan_steps",
            "plan_id_index",
            "destination_saturation",
            "destination_sequence_index",
            "mode_sequence_index",
            "time_sequence_steps",
        ]
        try:
            metadata = {"none_tables": [], "rng_state": state.rng_state}
            for table_name in table_names:
                table = getattr(state, table_name)
                if table is None:
                    metadata["none_tables"].append(table_name)
                else:
                    if isinstance(table, CompactCandidateSteps):
                        write_temporary_parquet(table.steps, temporary_path / "candidate_steps.parquet")
                        write_temporary_parquet(table.membership, temporary_path / "candidate_membership.parquet")
                        metadata["compact_candidate_columns"] = table.columns
                    else:
                        write_temporary_parquet(table, temporary_path / f"{table_name}.parquet")
            write_temporary_bytes(
                json.dumps(metadata, sort_keys=True).encode("utf-8"),
                temporary_path / "metadata.json",
            )
            temporary_path.rename(path)
        except BaseException:
            shutil.rmtree(temporary_path, ignore_errors=True)
            raise

    def _load_spilled_state(self, path: pathlib.Path) -> RunState:
        """Restore a spilled state using invariant tables from iteration zero."""
        with (path / "metadata.json").open("r", encoding="utf-8") as file:
            metadata = json.load(file)
        table_names = [
            "current_plans",
            "current_plan_steps",
            "candidate_plan_steps",
            "plan_id_index",
            "destination_saturation",
            "destination_sequence_index",
            "mode_sequence_index",
            "time_sequence_steps",
        ]
        spilled_state = {}
        for table_name in table_names:
            if table_name in metadata["none_tables"]:
                spilled_state[table_name] = None
            elif table_name == "candidate_plan_steps" and "compact_candidate_columns" in metadata:
                spilled_state[table_name] = CompactCandidateSteps(
                    steps=pl.read_parquet(path / "candidate_steps.parquet"),
                    membership=pl.read_parquet(path / "candidate_membership.parquet"),
                    columns=tuple(metadata["compact_candidate_columns"]),
                )
            else:
                spilled_state[table_name] = pl.read_parquet(path / f"{table_name}.parquet")
        spilled_state["rng_state"] = _nested_lists_to_tuples(metadata["rng_state"])
        return self._restore_compact_state(spilled_state)

    def _restore_compact_state(self, compact_state: Any) -> RunState:
        """Combine changing iteration tables with invariant tables from the start."""
        initial = self.initial_state.get()

        def value(name: str) -> Any:
            if isinstance(compact_state, dict):
                return compact_state[name]
            return getattr(compact_state, name)

        self._rng_state = value("rng_state")
        candidates = value("candidate_plan_steps")
        if candidates is not None and not isinstance(candidates, CompactCandidateSteps):
            candidates = CompactCandidateSteps.from_frame(candidates)
        return RunState(
            survey_plans=initial.survey_plans,
            survey_plan_steps=initial.survey_plan_steps,
            demand_groups=initial.demand_groups,
            activity_dur=initial.activity_dur,
            home_night_dur=initial.home_night_dur,
            stay_home_plan=initial.stay_home_plan,
            opportunities=initial.opportunities,
            current_plans=value("current_plans"),
            candidate_plan_steps=candidates,
            plan_id_index=value("plan_id_index"),
            destination_saturation=value("destination_saturation"),
            costs=self.transport_costs.get_costs_by_od(["cost", "distance"]),
            start_step=self.iteration + 1,
            current_plan_steps=value("current_plan_steps"),
            rng_state=self._rng_state,
            destination_sequence_index=value("destination_sequence_index"),
            mode_sequence_index=value("mode_sequence_index"),
            time_sequence_steps=value("time_sequence_steps"),
        )

    def get_rng_state(self) -> object:
        """Return the random generator state after this iteration's sampling."""
        if self._rng_state is None:
            state = self.get()
            self._rng_state = state.rng_state
        if self._rng_state is None:
            raise RuntimeError(
                f"Iteration {self.iteration} has not been computed in this execution."
            )
        return self._rng_state

    @trace_simulation_step("iteration asset build")
    def create_and_get_asset(self) -> RunState:
        """Load a saved checkpoint or run one iteration in memory."""
        transition_events = None
        checkpoint = None
        if self.resume_checkpoint_loader is not None:
            try:
                checkpoint = self.resume_checkpoint_loader()
            except (FileNotFoundError, RuntimeError) as exc:
                # Once a cohort has started from checkpoints, silently
                # recomputing only one member could change its shared search.
                raise RuntimeError(
                    f"Could not restore the checkpoint for step {self.iteration}; "
                    "the cohort cannot safely continue from this saved state."
                ) from exc

        # Checkpoints contain only the changing plan state. Read invariant
        # tables from the initial state so a restart can skip earlier updates.
        if checkpoint is not None:
            logging.debug(
                "Restoring group-day-trips iteration %s from checkpoint...",
                self.iteration,
            )
            get_group_day_trips_progress().iteration_step(
                self.iteration,
                "restoring saved state",
            )
            state = self._restore_compact_state(checkpoint)
            self._loaded_checkpoint = True
            logging.debug("Restored iteration %s from its checkpoint.", self.iteration)
        else:
            previous = self.previous_state.get()
            logging.debug("Starting group-day-trips iteration %s...", str(self.iteration))
            seeds = self.seeds.get()
            logging.debug("Preparing sampled alternatives for iteration %s...", str(self.iteration))
            if self.coordinated_candidate_plan_steps is None:
                self.activity_sequences.get()
                self.destination_sequences.get()
                self.mode_sequences.get()

            logging.debug("Updating plan choices for iteration %s...", str(self.iteration))
            get_group_day_trips_progress().iteration_step(
                self.iteration,
                "updating plan choices",
            )
            resolved_activity_parameters = self.inputs["resolved_activity_parameters"]
            arrival_time_rigidity_by_activity = self.inputs["arrival_time_rigidity_by_activity"]
            current_plans_before_transition = (
                previous.current_plans
                if self.coordinated_current_plans is None
                else self.coordinated_current_plans
            )
            current_plan_steps_before_transition = (
                previous.current_plan_steps
                if self.coordinated_current_plan_steps is None
                else self.coordinated_current_plan_steps
            )
            previous_plan_id_index = (
                previous.plan_id_index if self.coordinated_plan_id_index is None
                else self.coordinated_plan_id_index
            )
            precomputed_evaluation = (
                None if self.coordinated_evaluation is None else
                self.updater.expand_context_evaluation(
                    self.coordinated_evaluation, current_plans_before_transition, previous_plan_id_index,
                )
            )
            # The expanded evaluation owns the inputs needed by this update;
            # the compact search evaluation has no further consumer here.
            self.coordinated_evaluation = None
            current_plans, current_plan_steps, candidate_plan_steps, transition_events, plan_id_index, time_sequence_steps = self.updater.get_new_plans(
                current_plans_before_transition,
                current_plan_steps_before_transition,
                previous.candidate_plan_steps,
                previous.demand_groups,
                previous.schedule_steps if self.coordinated_time_sequence_steps is None else self.coordinated_time_sequence_steps,
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
                previous_plan_id_index,
                self.parameters,
                shared_candidate_plan_steps=self.coordinated_candidate_plan_steps,
                precomputed_evaluation=precomputed_evaluation,
            )
            # Do not retain scoring and timing tables while compacting the
            # returned candidates for the next simulation step.
            del precomputed_evaluation
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
                candidate_plan_steps=(
                    None if candidate_plan_steps is None
                    else CompactCandidateSteps.from_frame(candidate_plan_steps)
                ),
                plan_id_index=plan_id_index,
                time_sequence_steps=time_sequence_steps,
                destination_saturation=destination_saturation,
                costs=costs,
                start_step=self.iteration + 1,
                current_plan_steps=current_plan_steps,
                rng_state=(
                    seeds["rng_state_after_sampling"]
                    if self.coordinated_rng_state is None
                    else self.coordinated_rng_state
                ),
                destination_sequence_index=(
                    self.destination_sequences.sequence_index
                    if self.coordinated_destination_sequence_index is None
                    else self.coordinated_destination_sequence_index
                ),
                mode_sequence_index=(
                    self.mode_sequences.sequence_index
                    if self.coordinated_mode_sequence_index is None
                    else self.coordinated_mode_sequence_index
                ),
            )
            self._rng_state = state.rng_state

        if transition_events is not None and self.cache_step_events:
            if self.transition_events_asset is None:
                raise RuntimeError("Transition event persistence was not planned.")
            self.transition_events_asset.transition_events = transition_events
            # A recomputed step must replace an old or rejected event file.
            self.transition_events_asset.hash_path.unlink(missing_ok=True)
            resolver = get_current_asset_resolver()
            if resolver is not None:
                resolver.prepared_asset_keys.discard(asset_graph_key(self.transition_events_asset))
            self.transition_events_asset.get()

        logging.debug("Group-day-trips iteration %s is ready.", str(self.iteration))
        get_group_day_trips_progress().finish_iteration(self.iteration)

        return state


class CurrentPlansAsset(FileAsset):
    """Persisted current plan distribution after one completed iteration."""

    REQUIRED_SCHEMA = CURRENT_PLANS_SCHEMA

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
        return read_cached_parquet(
            self.cache_path,
            table_name="current_plans",
            required_schema=self.REQUIRED_SCHEMA,
        )

    @trace_simulation_step("iteration asset build")
    def create_and_get_asset(self) -> pl.DataFrame:
        if self.current_plans is None:
            raise ValueError("Cannot save current plans without a dataframe.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.current_plans.write_parquet(self.cache_path)
        return self.get_cached_asset()


class CurrentPlanStepsAsset(FileAsset):
    """Persisted step-level details for the current plans after one completed iteration."""

    REQUIRED_SCHEMA = CURRENT_PLAN_STEPS_SCHEMA

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
        return read_cached_parquet(
            self.cache_path,
            table_name="current_plan_steps",
            required_schema=self.REQUIRED_SCHEMA,
        )

    @trace_simulation_step("iteration asset build")
    def create_and_get_asset(self) -> pl.DataFrame:
        if self.current_plan_steps is None:
            raise ValueError("Cannot save current plan steps without a dataframe.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.current_plan_steps.write_parquet(self.cache_path)
        return self.get_cached_asset()


class PlanIdIndexAsset(FileAsset):
    """Saved-state table wrapper for the plan-id index.

    Main iteration execution stores the same table inside `RunState`. This asset
    supports the older `Iterations.load_state()` and `Iterations.save_state()`
    table API, where each saved state table has its own small FileAsset wrapper.
    """

    REQUIRED_SCHEMA = PLAN_ID_INDEX_SCHEMA

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        plan_id_index: pl.DataFrame | None = None,
    ) -> None:
        self.plan_id_index = plan_id_index
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"plan_id_index_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        return read_cached_parquet(
            self.cache_path,
            table_name="plan_id_index",
            required_schema=self.REQUIRED_SCHEMA,
        )

    @trace_simulation_step("iteration asset build")
    def create_and_get_asset(self) -> pl.DataFrame:
        if self.plan_id_index is None:
            raise ValueError("Cannot save the plan-id index without a dataframe.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.plan_id_index.write_parquet(self.cache_path)
        return self.get_cached_asset()


class SequenceIndexAsset(FileAsset):
    """Persist one compact stable sequence index for restart checkpoints."""

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        sequence_name: str,
        base_folder: pathlib.Path,
        sequence_index: pl.DataFrame | None = None,
    ) -> None:
        self.sequence_name = sequence_name
        self.sequence_index = sequence_index
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "sequence_name": sequence_name,
        }
        cache_path = pathlib.Path(base_folder) / f"{sequence_name}_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        return pl.read_parquet(self.cache_path)

    @trace_simulation_step("iteration asset build")
    def create_and_get_asset(self) -> pl.DataFrame:
        if self.sequence_index is None:
            raise ValueError(
                f"Cannot save {self.sequence_name} without a dataframe."
            )
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.sequence_index.write_parquet(self.cache_path)
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

    @trace_simulation_step("iteration asset build")
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

    @trace_simulation_step("iteration asset build")
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

    @trace_simulation_step("iteration asset build")
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


