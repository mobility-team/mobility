import logging
import os
import pathlib
import random
from typing import List

import polars as pl

from ..iterations import Iteration, Iterations
from ..evaluation.population_weighted_plan_steps import PopulationWeightedPlanSteps
from ..plans import ActivitySequences, DestinationSequences, ModeSequences, PlanInitializer, PlanUpdater
from ..transitions.transition_schema import TRANSITION_EVENT_SCHEMA
from .memory_logging import log_memory_checkpoint
from .parameters import Parameters
from .results import RunResults
from .run_state import RunState
from mobility.transport.costs.transport_costs import TransportCosts
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
            "version": 7,
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
            "transitions": project_folder / "group_day_trips" / "transitions.parquet",
            "demand_groups": project_folder / "group_day_trips" / "demand_groups.parquet",
        }
        super().__init__(inputs, cache_path)


    def create_and_get_asset(self) -> dict[str, pl.LazyFrame]:
        """Run the simulation for this day type and materialize cached outputs."""
        self._raise_if_disabled()
        log_memory_checkpoint("run:start")

        iterations, resume_from_iteration = self._prepare_iterations(self.inputs_hash)

        state = self._build_state(
            iterations=iterations,
            resume_from_iteration=resume_from_iteration,
        )
        self._log_state_memory_checkpoint("state:initialized", state)
        for iteration_index in range(state.start_iteration, self.parameters.n_iterations + 1):
            iteration = iterations.iteration(iteration_index)
            self._run_model_iteration(
                state=state,
                iteration=iteration,
            )
            if self.parameters.persist_iteration_artifacts:
                self._log_state_memory_checkpoint(f"iteration:{iteration_index}:before_save_state", state)
                iteration.save_state(state, self.rng.getstate())
                log_memory_checkpoint(f"iteration:{iteration_index}:after_save_state")

        self._assert_current_plan_steps_are_available(state)
        self._log_state_memory_checkpoint("state:before_finalization", state)

        final_costs = self._build_final_costs(state)
        log_memory_checkpoint("finalization:after_build_final_costs", costs=final_costs)
        final_plan_steps = self._build_final_plan_steps(state, final_costs)
        log_memory_checkpoint(
            "finalization:after_build_final_plan_steps",
            plan_steps=final_plan_steps,
        )
        transitions = self._build_transitions(iterations)
        log_memory_checkpoint("finalization:after_build_transitions", transitions=transitions)

        self._write_outputs(
            plan_steps=final_plan_steps,
            opportunities=state.opportunities,
            costs=final_costs,
            transitions=transitions,
            demand_groups=state.demand_groups,
        )
        log_memory_checkpoint("run:after_write_outputs")

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
        if self.parameters.persist_iteration_artifacts:
            resume_from_iteration = iterations.get_resume_iteration(self.parameters.n_iterations)
            iterations.prepare(resume=(resume_from_iteration is not None))
        else:
            resume_from_iteration = None
            iterations.prepare(resume=False)
            logging.info(
                "Iteration artifact persistence disabled for run_key=%s is_weekday=%s. "
                "Resume and iteration inspection are unavailable for this run.",
                self.inputs_hash,
                str(self.is_weekday),
            )
        return iterations, resume_from_iteration


    def _build_state(
        self,
        *,
        iterations: Iterations,
        resume_from_iteration: int | None,
    ) -> RunState:
        """Build the initial mutable state and restore it when resuming."""
        survey_plans, survey_plan_steps, demand_groups = self.initializer.get_survey_plan_data(
            self.population,
            self.surveys,
            self.activities,
            self.modes,
            self.is_weekday,
        )
        activity_dur, home_night_dur, activity_demand_per_pers = self.initializer.get_survey_duration_summaries(
            self.surveys,
            self.activities,
            self.modes,
            self.is_weekday,
            demand_groups,
            survey_plan_steps,
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
            activity_demand_per_pers,
            demand_groups,
            self.activities,
            self.population.transport_zones,
        )
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
            costs=self.transport_costs.asset_for_iteration(self, 1).get_costs_by_od(["cost", "distance"]),
            start_iteration=1,
        )
        self._restore_saved_state(
            iterations=iterations,
            state=state,
            resume_from_iteration=resume_from_iteration,
        )
        self._log_state_memory_checkpoint("state:after_restore", state)
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
        self._log_state_memory_checkpoint(f"iteration:{iteration.iteration}:start", state)
        seed = self.rng.getrandbits(64)

        resolved_transport_costs = self.transport_costs.asset_for_iteration(self, iteration.iteration)
        log_memory_checkpoint(f"iteration:{iteration.iteration}:after_resolve_transport_costs")

        activity_sequences = self._sample_and_write_activity_sequences(
            state,
            iteration,
            seed,
        )
        log_memory_checkpoint(
            f"iteration:{iteration.iteration}:activity_sequences",
            asset=activity_sequences.get_cached_asset(),
        )
        destination_sequences = self._sample_and_write_destination_sequences(
            state,
            iteration,
            activity_sequences,
        )
        log_memory_checkpoint(
            f"iteration:{iteration.iteration}:destination_sequences",
            asset=destination_sequences.get_cached_asset(),
        )
        mode_sequences = self._search_and_write_mode_sequences(
            state,
            iteration,
            destination_sequences,
            resolved_transport_costs,
        )
        log_memory_checkpoint(
            f"iteration:{iteration.iteration}:mode_sequences",
            asset=mode_sequences.get_cached_asset(),
        )
        transition_events = self._update_iteration_state(
            state,
            iteration,
            destination_sequences,
            mode_sequences,
            resolved_transport_costs,
        )
        log_memory_checkpoint(
            f"iteration:{iteration.iteration}:after_update_state",
            transition_events=transition_events,
        )
        self._log_state_memory_checkpoint(f"iteration:{iteration.iteration}:after_update_state", state)
        if transition_events is not None and self.parameters.persist_iteration_artifacts:
            iteration.save_transition_events(transition_events)
            log_memory_checkpoint(f"iteration:{iteration.iteration}:after_save_transition_events")


    def _sample_and_write_activity_sequences(
        self,
        state: RunState,
        iteration: Iteration,
        seed: int,
    ) -> ActivitySequences:
        """Admit timed survey activity-sequence seeds for one iteration."""
        activity_sequences = iteration.activity_sequences(
            current_plans=state.current_plans,
            survey_plans=state.survey_plans,
            survey_plan_steps=state.survey_plan_steps,
            demand_groups=state.demand_groups,
            parameters=self.parameters,
            seed=seed,
        )
        activity_sequences.get()
        return activity_sequences

    def _sample_and_write_destination_sequences(
        self,
        state: RunState,
        iteration: Iteration,
        activity_sequences: ActivitySequences,
    ) -> DestinationSequences:
        """Run destination sampling and persist destination sequences for one iteration."""
        resolved_activity_parameters = resolve_activity_parameters(self.activities, iteration.iteration)
        destination_sequences = iteration.destination_sequences(
            activity_sequences=activity_sequences,
            activities=self.activities,
            resolved_activity_parameters=resolved_activity_parameters,
            transport_zones=self.population.transport_zones,
            current_plans=state.current_plans,
            current_plan_steps=state.current_plan_steps,
            destination_saturation=state.destination_saturation,
            demand_groups=state.demand_groups,
            costs=state.costs,
            parameters=self.parameters,
            seed=self.rng.getrandbits(64),
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
    ) -> pl.LazyFrame | None:
        """Advance the simulation state by one iteration and return transition events."""
        resolved_activity_parameters = resolve_activity_parameters(self.activities, iteration.iteration)
        state.current_plans, state.current_plan_steps, state.candidate_plan_steps, transition_events = self.updater.get_new_plans(
            state.current_plans,
            state.current_plan_steps,
            state.candidate_plan_steps,
            state.demand_groups,
            state.survey_plan_steps,
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
        plan_steps = state.current_plan_steps
        if "mode" in plan_steps.columns:
            plan_steps = plan_steps.with_columns(mode=pl.col("mode").cast(pl.String))

        return (
            plan_steps
            .join(
                state.current_plans.select(
                    ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id", "utility"]
                ),
                on=["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"],
                how="left",
            )
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


    def _build_transitions(self, iterations: Iterations) -> pl.DataFrame | pl.LazyFrame:
        """Combine persisted per-iteration transition events into the final table."""
        if self.parameters.persist_iteration_artifacts is False or self.parameters.save_transition_events is False:
            return pl.DataFrame(schema=TRANSITION_EVENT_SCHEMA)

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
        transitions: pl.DataFrame,
        demand_groups: pl.DataFrame,
    ) -> None:
        """Write the final run artifacts to their parquet cache paths."""
        plan_steps.write_parquet(self.cache_path["plan_steps"])
        opportunities.write_parquet(self.cache_path["opportunities"])
        costs.write_parquet(self.cache_path["costs"])
        transitions.write_parquet(self.cache_path["transitions"])
        demand_groups.write_parquet(self.cache_path["demand_groups"])

    def _log_state_memory_checkpoint(self, label: str, state: RunState) -> None:
        """Log process memory together with the main mutable state tables."""
        log_memory_checkpoint(
            label,
            survey_plans=state.survey_plans,
            survey_plan_steps=state.survey_plan_steps,
            demand_groups=state.demand_groups,
            activity_dur=state.activity_dur,
            home_night_dur=state.home_night_dur,
            stay_home_plan=state.stay_home_plan,
            opportunities=state.opportunities,
            current_plans=state.current_plans,
            current_plan_steps=state.current_plan_steps,
            candidate_plan_steps=state.candidate_plan_steps,
            destination_saturation=state.destination_saturation,
            costs=state.costs,
        )

    def get_cached_asset(self) -> dict[str, pl.LazyFrame]:
        """Return lazy readers for this run's cached parquet outputs."""
        self._raise_if_disabled()
        return {key: pl.scan_parquet(path) for key, path in self.cache_path.items()}


    def results(self) -> RunResults:
        """Return the analysis helper bound to this run's cached outputs."""
        self.get()
        cached = self.get_cached_asset()

        population_weighted_plan_steps = PopulationWeightedPlanSteps(
            population=self.population,
            surveys=self.surveys,
            activities=self.activities,
            modes=self.modes,
            is_weekday=self.is_weekday,
        ).get()

        return RunResults(
            inputs_hash=self.inputs_hash,
            is_weekday=self.is_weekday,
            transport_zones=self.population.inputs["transport_zones"],
            demand_groups=cached["demand_groups"],
            plan_steps=cached["plan_steps"],
            opportunities=cached["opportunities"],
            costs=cached["costs"],
            population_weighted_plan_steps=population_weighted_plan_steps,
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
        self._remove_run_iteration_artifacts()

    def _remove_run_iteration_artifacts(self) -> None:
        """Remove all iteration-derived artifacts for this run."""
        self._remove_run_iteration_state_artifacts()
        self._remove_run_iteration_congestion_artifacts()

    def _remove_run_iteration_state_artifacts(self) -> None:
        """Remove saved run iteration state folders."""
        Iterations(
            run_inputs_hash=self.inputs_hash,
            is_weekday=self.is_weekday,
            base_folder=self.cache_path["plan_steps"].parent,
        ).remove_all()

    def _remove_run_iteration_congestion_artifacts(self) -> None:
        """Remove congestion snapshots and congestion-derived caches for this run."""
        for next_transport_costs, congestion_state, flow_assets_by_mode in (
            self.transport_costs.iter_run_congestion_artifacts(self)
        ):
            next_transport_costs.remove_congestion_artifacts(congestion_state)

            for flow_asset in flow_assets_by_mode.values():
                flow_asset.remove()
