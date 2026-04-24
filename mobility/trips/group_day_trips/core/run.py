import logging
import os
import pathlib
import random
from typing import List

import polars as pl

from ..iterations import Iteration, Iterations
from ..plans import ActivitySequences, DestinationSequences, ModeSequences, PlanInitializer, PlanUpdater
from .parameters import Parameters
from .results import RunResults
from .run_state import RunState
from mobility.transport.costs.transport_costs import TransportCosts
from mobility.transport.costs.congestion_state import CongestionState
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.runtime.assets.file_asset import FileAsset
from mobility.activities import Activity
from mobility.activities.activity import resolve_activity_parameters
from mobility.surveys import MobilitySurvey
from mobility.population import Population
from mobility.transport.modes.core.transport_mode import TransportMode


class Run(FileAsset):
    """Single day-type PopulationGroupDayTrips asset."""

    def __init__(
        self,
        *,
        population: Population,
        transport_costs: TransportCosts,
        activities: List[Activity],
        modes: List[TransportMode],
        surveys: List[MobilitySurvey],
        parameters: Parameters,
        is_weekday: bool,
        enabled: bool = True,
    ) -> None:
        """Initialize a single weekday or weekend PopulationGroupDayTrips run."""
        inputs = {
            "version": 2,
            "population": population,
            "activities": activities,
            "modes": modes,
            "surveys": surveys,
            "parameters": parameters,
            "is_weekday": is_weekday,
            "enabled": enabled,
        }
        
        self.transport_costs = transport_costs
        self.rng = random.Random(parameters.seed) if parameters.seed is not None else random.Random()
        self.initializer = PlanInitializer()
        self.updater = PlanUpdater()

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])

        cache_path = {
            "plan_steps": project_folder / "group_day_trips" / "plan_steps.parquet",
            "opportunities": project_folder / "group_day_trips" / "opportunities.parquet",
            "costs": project_folder / "group_day_trips" / "costs.parquet",
            "chains": project_folder / "group_day_trips" / "chains.parquet",
            "transitions": project_folder / "group_day_trips" / "transitions.parquet",
            "demand_groups": project_folder / "group_day_trips" / "demand_groups.parquet",
        }
        super().__init__(inputs, cache_path)


    def create_and_get_asset(self) -> dict[str, pl.LazyFrame]:
        """Run the simulation for this day type and materialize cached outputs."""
        self._raise_if_disabled()

        iterations, resume_from_iteration = self._prepare_iterations(self.inputs_hash)

        state = self._build_state(
            iterations=iterations,
            resume_from_iteration=resume_from_iteration,
        )
        for iteration_index in range(state.start_iteration, self.parameters.n_iterations + 1):
            iteration = iterations.iteration(iteration_index)
            self._run_model_iteration(
                state=state,
                iteration=iteration,
            )
            iteration.save_state(state, self.rng.getstate())

        self._assert_current_plan_steps_are_available(state)

        final_costs = self._build_final_costs(state)
        final_plan_steps = self._build_final_plan_steps(state, final_costs)
        transitions = self._build_transitions(iterations)

        self._write_outputs(
            plan_steps=final_plan_steps,
            opportunities=state.opportunities,
            costs=final_costs,
            chains=state.chains,
            transitions=transitions,
            demand_groups=state.demand_groups,
        )

        return self.get_cached_asset()


    def _raise_if_disabled(self) -> None:
        """Fail fast when callers try to access a disabled run."""
        if self.enabled:
            return

        day_type = "weekday" if self.is_weekday else "weekend"
        raise RuntimeError(
            f"Run for {day_type} is disabled. "
            "Enable this day type or avoid accessing its outputs."
        )


    def _prepare_iterations(
        self,
        run_inputs_hash: str,
    ) -> tuple[Iterations, int | None]:
        """Prepare persisted iterations and determine the resume iteration."""
        iterations = Iterations(
            run_inputs_hash=run_inputs_hash,
            is_weekday=self.is_weekday,
            base_folder=self.cache_path["plan_steps"].parent,
        )
        resume_from_iteration = iterations.get_resume_iteration(self.parameters.n_iterations)
        iterations.prepare(resume=(resume_from_iteration is not None))
        return iterations, resume_from_iteration


    def _build_state(
        self,
        *,
        iterations: Iterations,
        resume_from_iteration: int | None,
    ) -> RunState:
        """Build the initial mutable state and restore it when resuming."""
        chains_by_activity, chains, demand_groups = self.initializer.get_chains(
            self.population,
            self.surveys,
            self.activities,
            self.modes,
            self.is_weekday,
        )
        activity_dur, home_night_dur = self.initializer.get_mean_activity_durations(
            chains_by_activity,
            demand_groups,
        )
        resolved_activity_parameters = resolve_activity_parameters(self.activities, 1)
        stay_home_plan, current_plans = self.initializer.get_stay_home_state(
            demand_groups,
            home_night_dur,
            resolved_activity_parameters["home"],
            self.parameters.min_activity_time_constant,
            iterations.folder_paths["sequences-index"],
        )
        opportunities = self.initializer.get_opportunities(
            chains_by_activity,
            self.activities,
            self.population.transport_zones,
        )
        state = RunState(
            chains_by_activity=chains_by_activity,
            chains=chains,
            demand_groups=demand_groups,
            activity_dur=activity_dur,
            home_night_dur=home_night_dur,
            stay_home_plan=stay_home_plan,
            opportunities=opportunities,
            current_plans=current_plans,
            candidate_plan_steps=None,
            destination_saturation=opportunities.clone(),
            costs=self.transport_costs.asset_for_iteration(self, 1).get_costs_by_od(["cost", "distance"]),
            start_iteration=1,
        )
        self._restore_saved_state(
            iterations=iterations,
            state=state,
            resume_from_iteration=resume_from_iteration,
        )
        return state


    def _restore_saved_state(
        self,
        *,
        iterations: Iterations,
        state: RunState,
        resume_from_iteration: int | None,
    ) -> None:
        """Restore the saved iteration state into the mutable run state."""
        if resume_from_iteration is None:
            return
        try:
            saved_state = iterations.iteration(resume_from_iteration).load_state()
        except Exception as exc:
            raise RuntimeError(
                "Failed to load saved PopulationGroupDayTrips iteration state for "
                f"run_inputs_hash={self.inputs_hash}, is_weekday={self.is_weekday}, "
                f"iteration={resume_from_iteration}. "
                "Call `remove()` to clear cached iteration artifacts and rerun from scratch."
            ) from exc

        try:
            self.rng.setstate(saved_state.rng_state)
        except Exception as exc:
            raise RuntimeError(
                "Failed to restore RNG state from saved PopulationGroupDayTrips iteration state for "
                f"run_inputs_hash={self.inputs_hash}, is_weekday={self.is_weekday}, "
                f"iteration={resume_from_iteration}. "
                "Call `remove()` to clear cached iteration artifacts and rerun from scratch."
            ) from exc

        iterations.discard_future_iterations(iteration=resume_from_iteration)
        state.current_plans = saved_state.current_plans
        state.current_plan_steps = saved_state.current_plan_steps
        state.candidate_plan_steps = saved_state.candidate_plan_steps
        state.destination_saturation = saved_state.destination_saturation
        state.start_iteration = resume_from_iteration + 1

        logging.info(
            "Resuming from saved iteration: run_key=%s is_weekday=%s iteration=%s",
            self.inputs_hash,
            str(self.is_weekday),
            str(resume_from_iteration),
        )
        state.costs = self.transport_costs.asset_for_iteration(
            self,
            state.start_iteration,
        ).get_costs_by_od(
            ["cost", "distance"],
        )


    def _run_model_iteration(
        self,
        *,
        state: RunState,
        iteration: Iteration,
    ) -> None:
        """Execute one simulation iteration and update the mutable run state."""
        logging.info("Iteration %s", str(iteration.iteration))
        seed = self.rng.getrandbits(64)

        resolved_transport_costs = self.transport_costs.asset_for_iteration(self, iteration.iteration)

        destination_sequences = self._sample_and_write_destination_sequences(
            state,
            iteration,
            seed,
        )
        mode_sequences = self._search_and_write_mode_sequences(
            state,
            iteration,
            destination_sequences,
            resolved_transport_costs,
        )
        transition_events = self._update_iteration_state(
            state,
            iteration,
            destination_sequences,
            mode_sequences,
            resolved_transport_costs,
        )
        iteration.save_transition_events(transition_events)


    def _sample_and_write_activity_sequences(
        self,
        state: RunState,
        iteration: Iteration,
        seed: int,
    ) -> ActivitySequences:
        """Admit timed survey activity-sequence seeds for one iteration."""
        activity_sequences = iteration.activity_sequences(
            current_plans=state.current_plans,
            chains=state.chains_by_activity,
            parameters=self.parameters,
            seed=seed,
        )
        activity_sequences.get()
        return activity_sequences

    def _sample_and_write_destination_sequences(
        self,
        state: RunState,
        iteration: Iteration,
        seed: int,
    ) -> DestinationSequences:
        """Run destination sampling and persist destination sequences for one iteration."""
        resolved_activity_parameters = resolve_activity_parameters(self.activities, iteration.iteration)
        activity_sequences = self._sample_and_write_activity_sequences(
            state,
            iteration,
            seed,
        )
        destination_sequences = iteration.destination_sequences(
            activity_sequences=activity_sequences,
            activities=self.activities,
            resolved_activity_parameters=resolved_activity_parameters,
            transport_zones=self.population.transport_zones,
            current_plans=state.current_plans,
            current_plan_steps=state.current_plan_steps,
            destination_saturation=state.destination_saturation,
            chains=state.chains_by_activity,
            demand_groups=state.demand_groups,
            costs=state.costs,
            parameters=self.parameters,
            seed=seed,
        )
        destination_sequences.get()
        return destination_sequences


    def _search_and_write_mode_sequences(
        self,
        state: RunState,
        iteration: Iteration,
        destination_sequences: DestinationSequences,
        resolved_transport_costs: TransportCosts,
    ) -> ModeSequences:
        """Run mode-sequence search and persist the results for one iteration."""
        mode_sequences = iteration.mode_sequences(
            destination_sequences=destination_sequences,
            transport_costs=resolved_transport_costs,
            parameters=self.parameters,
        )
        mode_sequences.get()
        return mode_sequences


    def _update_iteration_state(
        self,
        state: RunState,
        iteration: Iteration,
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
        resolved_transport_costs: TransportCosts,
    ) -> pl.DataFrame:
        """Advance the simulation state by one iteration and return transition events."""
        resolved_activity_parameters = resolve_activity_parameters(self.activities, iteration.iteration)
        state.current_plans, state.current_plan_steps, state.candidate_plan_steps, transition_events = self.updater.get_new_plans(
            state.current_plans,
            state.current_plan_steps,
            state.candidate_plan_steps,
            state.demand_groups,
            state.chains_by_activity,
            resolved_transport_costs,
            state.destination_saturation,
            state.activity_dur,
            iteration.iteration,
            resolved_activity_parameters,
            destination_sequences,
            mode_sequences,
            state.home_night_dur,
            state.stay_home_plan,
            self.inputs["population"].transport_zones,
            iteration.iterations.folder_paths["sequences-index"],
            self.parameters,
        )
        state.costs = self.updater.get_new_costs(
            state.costs,
            iteration.iteration,
            self.parameters.n_iter_per_cost_update,
            state.current_plan_steps,
            self.transport_costs.for_iteration(iteration.iteration + 1),
            run=self,
        )
        state.destination_saturation = self.updater.get_destination_saturation(
            state.current_plan_steps,
            state.opportunities,
            resolved_activity_parameters,
        )
        return transition_events


    def _assert_current_plan_steps_are_available(self, state: RunState) -> None:
        """Fail fast if final step-level plans were not produced by the iteration loop."""
        if state.current_plan_steps is not None:
            return

        raise RuntimeError(
            "Run reached finalization without `current_plan_steps`. "
            "This usually means the run resumed after the final saved iteration "
            "but before final outputs were materialized."
        )


    def _build_final_costs(self, state: RunState) -> pl.DataFrame:
        """Compute the final OD costs to attach to the written outputs."""
        return self.transport_costs.asset_for_iteration(
            self,
            self.parameters.n_iterations,
        ).get_costs_by_od_and_mode(
            ["cost", "distance", "time", "ghg_emissions_per_trip"]
        )


    def _build_final_plan_steps(self, state: RunState, costs: pl.DataFrame) -> pl.DataFrame:
        """Join final per-step states with demand-group attributes and costs."""
        return (
            state.current_plan_steps
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
                is_weekday=pl.lit(self.is_weekday),
            )
        )


    def _build_transitions(self, iterations: Iterations) -> pl.DataFrame:
        """Combine persisted per-iteration transition events into the final table."""
        transition_paths = iterations.list_transition_event_paths()
        if not transition_paths:
            raise RuntimeError(
                "Run finished without persisted transition events. "
                "Call `remove()` to clear cached artifacts and rerun from scratch."
            )

        return pl.concat([pl.read_parquet(path) for path in transition_paths], how="vertical")


    def _write_outputs(
        self,
        *,
        plan_steps: pl.DataFrame,
        opportunities: pl.DataFrame,
        costs: pl.DataFrame,
        chains: pl.DataFrame,
        transitions: pl.DataFrame,
        demand_groups: pl.DataFrame,
    ) -> None:
        """Write the final run artifacts to their parquet cache paths."""
        plan_steps.write_parquet(self.cache_path["plan_steps"])
        opportunities.write_parquet(self.cache_path["opportunities"])
        costs.write_parquet(self.cache_path["costs"])
        chains.write_parquet(self.cache_path["chains"])
        transitions.write_parquet(self.cache_path["transitions"])
        demand_groups.write_parquet(self.cache_path["demand_groups"])


    def get_cached_asset(self) -> dict[str, pl.LazyFrame]:
        """Return lazy readers for this run's cached parquet outputs."""
        self._raise_if_disabled()
        return {key: pl.scan_parquet(path) for key, path in self.cache_path.items()}


    def results(self) -> RunResults:
        """Return the analysis helper bound to this run's cached outputs."""
        self.get()
        cached = self.get_cached_asset()

        return RunResults(
            inputs_hash=self.inputs_hash,
            is_weekday=self.is_weekday,
            transport_zones=self.population.inputs["transport_zones"],
            demand_groups=cached["demand_groups"],
            plan_steps=cached["plan_steps"],
            opportunities=cached["opportunities"],
            costs=cached["costs"],
            chains=cached["chains"],
            transitions=cached["transitions"],
            surveys=self.surveys,
            modes=self.modes,
            parameters=self.parameters,
            run=self,
        )


    def evaluate(self, metric, **kwargs) -> object:
        """Evaluate this run using a named run-level metric."""
        results = self.results()

        if metric not in results.metrics_methods:
            available = ", ".join(results.metrics_methods.keys())
            raise ValueError(f"Unknown evaluation metric: {metric}. Available metrics are: {available}")

        return results.metrics_methods[metric](**kwargs)


    def remove(self) -> None:
        """Remove cached outputs and saved iteration artifacts for this run."""
        super().remove()
        Iterations(
            run_inputs_hash=self.inputs_hash,
            is_weekday=self.is_weekday,
            base_folder=self.cache_path["plan_steps"].parent,
        ).remove_all()
        self._remove_run_owned_congestion_artifacts()

    def _remove_run_owned_congestion_artifacts(self) -> None:
        """Remove congestion snapshots and congestion-derived cost caches for this run."""
        if self.transport_costs.has_enabled_congestion() is False:
            return

        update_interval = int(self.parameters.n_iter_per_cost_update)
        if update_interval <= 0:
            return

        for completed_iteration in range(1, int(self.parameters.n_iterations) + 1):
            if self.transport_costs.should_recompute_congested_costs(completed_iteration, update_interval) is False:
                continue

            next_transport_costs = self.transport_costs.for_iteration(completed_iteration + 1)
            flow_assets_by_mode = {}
            for mode in next_transport_costs.congestion_states._iter_congestion_enabled_modes():
                mode_name = mode.inputs["parameters"].name
                flow_asset = VehicleODFlowsAsset.from_inputs(
                    run_key=self.inputs_hash,
                    is_weekday=self.is_weekday,
                    iteration=completed_iteration,
                    mode_name=mode_name,
                )
                if flow_asset.cache_path.exists():
                    flow_assets_by_mode[mode_name] = flow_asset

            if not flow_assets_by_mode:
                continue

            congestion_state = CongestionState(
                run_key=str(self.inputs_hash),
                is_weekday=bool(self.is_weekday),
                iteration=int(completed_iteration),
                flow_assets_by_mode=flow_assets_by_mode,
            )

            self._remove_asset_if_possible(
                next_transport_costs.asset_for_congestion_state(congestion_state)
            )
            for mode in next_transport_costs.modes:
                self._remove_mode_congestion_variant_caches(
                    mode.inputs.get("travel_costs"),
                    congestion_state,
                )

            for flow_asset in flow_assets_by_mode.values():
                flow_asset.remove()

    def _remove_mode_congestion_variant_caches(
        self,
        travel_costs,
        congestion_state: CongestionState,
    ) -> None:
        """Remove one mode's congestion-derived travel-cost and graph caches."""
        if travel_costs is None or not hasattr(travel_costs, "asset_for_congestion_state"):
            return

        try:
            variant = travel_costs.asset_for_congestion_state(congestion_state)
        except Exception:
            return

        self._remove_congestion_variant_artifact_tree(variant)

    def _remove_congestion_variant_artifact_tree(self, variant) -> None:
        """Remove a congestion-specific asset variant and its derived graph caches."""
        if variant is None:
            return

        self._remove_asset_if_possible(variant)

        inputs = getattr(variant, "inputs", {})

        contracted_graph = inputs.get("contracted_path_graph")
        if contracted_graph is not None:
            self._remove_asset_if_possible(contracted_graph)
            congested_graph = getattr(contracted_graph, "inputs", {}).get("congested_graph")
            self._remove_asset_if_possible(congested_graph)

        nested_travel_costs = inputs.get("car_travel_costs")
        nested_flow_asset = inputs.get("road_flow_asset")
        if (
            nested_travel_costs is not None
            and nested_flow_asset is not None
            and hasattr(nested_travel_costs, "asset_for_flow_asset")
        ):
            try:
                nested_variant = nested_travel_costs.asset_for_flow_asset(nested_flow_asset)
            except Exception:
                return
            self._remove_congestion_variant_artifact_tree(nested_variant)

    @staticmethod
    def _remove_asset_if_possible(asset) -> None:
        """Remove one cache-bearing asset when the object supports it."""
        if asset is not None and hasattr(asset, "remove"):
            asset.remove()
