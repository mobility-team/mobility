import os
import pathlib
import logging
from dataclasses import dataclass
from types import SimpleNamespace
from typing import List

import polars as pl

from mobility.runtime.assets.cache_schema import read_cached_parquet
from ..iterations import (
    InitialIterationStateAsset,
    IterationSeedsAsset,
    Iterations,
    IterationTransportCostsAsset,
    IterationStateAsset,
)
from ..evaluation.population_weighted_plan_steps import (
    PopulationWeightedPlanSteps,
    PopulationWeightedSurveyReferenceSteps,
)
from ..evaluation.calibration_plan_steps import (
    ObservedCalibrationPlanSteps,
    PopulationWeightedCalibrationPlanSteps,
)
from ..evaluation.iteration_metrics import IterationMetricsBuilder, IterationMetricsHistory
from ..evaluation.model_entropy import ModelEntropy
from ..evaluation.model_loss import ModelLoss
from ..evaluation.model_trip_count_loss import ModelTripCountLoss
from ..evaluation.trip_pattern_distribution import (
    ObservedTripPatternDistribution,
    PopulationWeightedTripPatternDistribution,
)
from ..plans import ActivitySequences, DestinationSequences, ModeSequences
from ..plans.activity_schedule_pool import ActivitySchedulePoolAsset
from ..plans.demand_subgroups import DEMAND_UNIT_COLS
from ..plans.plan_ids import PLAN_KEY_COLS
from ..transitions.transition_schema import TRANSITION_EVENT_SCHEMA
from .memory_logging import log_memory_checkpoint
from .parameters import BehaviorChangeScope, GroupDayTripsParameters
from .progress import (
    GroupDayTripsProgressReporter,
    get_group_day_trips_progress,
    is_group_day_trips_progress_active,
)
from .results import RunResults
from .run_state import RunState
from mobility.transport.costs.transport_costs import TransportCosts
from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import (
    asset_resolution_context,
    get_current_asset_resolver,
)
from mobility.runtime.assets.input_hashing import hash_inputs
from mobility.runtime.parameter_values import SensitivityCase
from mobility.activities import Activity
from mobility.surveys import SurveyPlanAssets
from mobility.surveys.mobility_survey import MobilitySurvey
from mobility.population import Population
from mobility.transport.modes.core.transport_mode import TransportMode


@dataclass(frozen=True)
class ExpectedDiagnosticsInputs:
    """Shared survey-derived reference inputs used by run diagnostics."""

    population_weighted_plan_steps: PopulationWeightedPlanSteps
    survey_reference_plan_steps: PopulationWeightedSurveyReferenceSteps
    calibration_plan_steps: PopulationWeightedCalibrationPlanSteps
    trip_pattern_distribution: PopulationWeightedTripPatternDistribution


class Run(InMemoryAsset):
    """Single day-type PopulationGroupDayTrips asset."""

    def __init__(
        self,
        *,
        population: Population,
        transport_costs: TransportCosts,
        activities: List[Activity],
        modes: List[TransportMode],
        surveys: List[MobilitySurvey],
        survey_plan_assets: SurveyPlanAssets,
        parameters: GroupDayTripsParameters,
        is_weekday: bool,
        enabled: bool = True,
        scenario: str,
        sensitivity_case: SensitivityCase | None = None,
        replication: int = 0,
    ) -> None:
        """Initialize a single weekday or weekend PopulationGroupDayTrips run."""
        sensitivity_case_id = (
            "base"
            if sensitivity_case is None
            else sensitivity_case.case_id
        )
        parameters_for_cache = parameters.model_dump(mode="python")
        parameters_for_cache["outputs"].pop("checkpoint_each_iteration", None)
        # Polars 2 changes seeded hashes, so older runs and saved states must
        # not be reused with the new candidate identities.
        run_context_inputs = {
            "version": 29,
            "population": population,
            "activities": activities,
            "modes": modes,
            "surveys": surveys,
            "survey_plan_assets": survey_plan_assets,
            "parameters": parameters_for_cache,
            "is_weekday": is_weekday,
            "enabled": enabled,
            "scenario": scenario,
            "sensitivity_case": sensitivity_case,
        }
        run_context_hash = hash_inputs(run_context_inputs)
        checkpoint_parameters = {
            **parameters_for_cache,
            "run": {**parameters_for_cache["run"]},
        }
        # The run length selects how many states to build. It does not change
        # the checkpoints for iterations that have already completed.
        checkpoint_parameters["run"].pop("n_iterations", None)
        checkpoint_run_context = {
            **run_context_inputs,
            "parameters": checkpoint_parameters,
            "checkpoint_version": 6,
        }
        checkpoint_run_hash = hash_inputs(checkpoint_run_context)
        self.run_context = SimpleNamespace(
            inputs_hash=run_context_hash,
            parameters=parameters,
            is_weekday=is_weekday,
            scenario=scenario,
            sensitivity_case=sensitivity_case,
            replication=replication,
        )
        self.population = population
        self.activities = activities
        self.modes = modes
        self.surveys = surveys
        self.survey_plan_assets = survey_plan_assets
        self.parameters = parameters
        self.is_weekday = is_weekday
        self.enabled = enabled
        self.scenario = scenario
        self.sensitivity_case = sensitivity_case
        self.sensitivity_case_id = sensitivity_case_id
        self.replication = int(replication)
        self.coordinated_run_key: int | None = None

        self.transport_costs = transport_costs
        self._expected_diagnostics_inputs: ExpectedDiagnosticsInputs | None = None
        self.coordinated_run_asset = None
        self.coordinated_final_transport_cost_asset = None

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        group_day_trips_folder = project_folder / "group_day_trips"
        self.iterations = Iterations(
            run_inputs_hash=checkpoint_run_hash,
            is_weekday=is_weekday,
            base_folder=group_day_trips_folder,
        )
        self.initial_iteration_state = InitialIterationStateAsset(
            is_weekday=is_weekday,
            population=population,
            survey_plan_assets=survey_plan_assets,
            activities=activities,
            modes=modes,
            parameters=parameters,
            scenario=scenario,
            sensitivity_case=sensitivity_case,
            initial_transport_costs=IterationTransportCostsAsset(
                is_weekday=is_weekday,
                iteration=1,
                transport_costs=transport_costs,
                scenario=scenario,
                sensitivity_case=sensitivity_case,
                previous_state=None,
                n_iter_per_cost_update=parameters.run.n_iter_per_cost_update,
            ),
        )
        self.iteration_transport_cost_assets: list[IterationTransportCostsAsset] = []
        self.iteration_state_assets = self._build_iteration_state_assets(
            is_weekday=is_weekday,
            base_folder=group_day_trips_folder,
            population=population,
            transport_costs=transport_costs,
            activities=activities,
            modes=modes,
            parameters=parameters,
            scenario=scenario,
            sensitivity_case=sensitivity_case,
            initial_state=self.initial_iteration_state,
        )
        if parameters.outputs.checkpoint_each_iteration:
            resume_iteration = self.iterations.get_resume_iteration(
                parameters.run.n_iterations
            )

            # Transition events are a separate requested output. A checkpoint
            # cannot recreate them, so restart from the beginning if any saved
            # iteration event file is missing.
            if (
                resume_iteration is not None
                and parameters.outputs.cache_iteration_events
                and any(
                    not state_asset.transition_events_asset.cache_path.exists()
                    for state_asset in self.iteration_state_assets
                    if state_asset.iteration <= resume_iteration
                )
            ):
                logging.info(
                    "Some saved transition events are missing; recomputing the run from the start."
                )
                resume_iteration = None

            for state_asset in self.iteration_state_assets:
                checkpoint = self.iterations.iteration(state_asset.iteration)
                state_asset.checkpoint_callback = (
                    lambda iteration, state, rng_state, checkpoint=checkpoint: (
                        checkpoint.save_state(state, rng_state)
                    )
                )
                if resume_iteration is not None and state_asset.iteration <= resume_iteration:
                    state_asset.resume_checkpoint_loader = checkpoint.load_state

        self.final_iteration_state = (
            self.iteration_state_assets[-1]
            if self.iteration_state_assets
            else self.initial_iteration_state
        )

        inputs = {
            "version": 23,
            "run_context_hash": run_context_hash,
            "final_iteration_state": self.final_iteration_state,
        }
        super().__init__(inputs)

    def get(self, *args, **kwargs) -> RunState:
        """Resolve the final iteration state while reporting run progress."""
        if self.coordinated_run_asset is not None:
            self._raise_if_disabled()
            if is_group_day_trips_progress_active():
                coordinated = self.coordinated_run_asset.get()
            else:
                day_type = "weekday" if self.is_weekday else "weekend"
                label = (
                    f"GroupDayTrips {day_type} coordinated runs="
                    f"{len(self.coordinated_run_asset.run_keys)}"
                )
                with GroupDayTripsProgressReporter(
                    label=label,
                    total_iterations=self.parameters.run.n_iterations,
                ):
                    coordinated = self.coordinated_run_asset.get()
            return coordinated.final_states_by_run_key[self.coordinated_run_key]

        if is_group_day_trips_progress_active():
            return super().get(*args, **kwargs)

        day_type = "weekday" if self.is_weekday else "weekend"
        label = (
            f"GroupDayTrips {day_type} "
            f"scenario={self.scenario} sensitivity={self.sensitivity_case_id} "
            f"repl={self.replication}"
        )
        with GroupDayTripsProgressReporter(
            label=label,
            total_iterations=self.parameters.run.n_iterations,
        ):
            return super().get(*args, **kwargs)

    def _build_iteration_state_assets(
        self,
        *,
        is_weekday: bool,
        base_folder: pathlib.Path,
        population: Population,
        transport_costs: TransportCosts,
        activities: List[Activity],
        modes: List[TransportMode],
        parameters: GroupDayTripsParameters,
        scenario: str,
        sensitivity_case: SensitivityCase | None,
        initial_state: Asset,
    ) -> list[IterationStateAsset]:
        """Build the content-addressed state asset chain for the run iterations."""
        state_assets: list[IterationStateAsset] = []
        previous_state: Asset = initial_state
        activity_schedule_pool = ActivitySchedulePoolAsset(
            initial_state=initial_state,
            is_weekday=is_weekday,
        )

        for iteration_index in range(1, parameters.run.n_iterations + 1):
            seeds = IterationSeedsAsset(
                previous_state=previous_state,
                iteration=iteration_index,
            )
            resolved_transport_costs = IterationTransportCostsAsset(
                is_weekday=is_weekday,
                iteration=iteration_index,
                transport_costs=transport_costs,
                scenario=scenario,
                sensitivity_case=sensitivity_case,
                previous_state=(None if iteration_index == 1 else previous_state),
                n_iter_per_cost_update=parameters.run.n_iter_per_cost_update,
            )
            self.iteration_transport_cost_assets.append(resolved_transport_costs)
            prepared_schedule_pool = (
                activity_schedule_pool
                if parameters.behavior_change.scope_at(iteration_index)
                == BehaviorChangeScope.FULL_REPLANNING
                else None
            )
            activity_sequences = ActivitySequences(
                is_weekday=is_weekday,
                iteration=iteration_index,
                previous_state=previous_state,
                seed_asset=seeds,
                activity_schedule_pool=prepared_schedule_pool,
                parameters=parameters,
            )
            destination_sequences = DestinationSequences(
                is_weekday=is_weekday,
                iteration=iteration_index,
                previous_state=previous_state,
                seed_asset=seeds,
                activity_sequences=activity_sequences,
                activities=activities,
                scenario=scenario,
                sensitivity_case=sensitivity_case,
                transport_zones=population.transport_zones,
                transport_costs=resolved_transport_costs,
                population_segments=population.population_segments,
                parameters=parameters,
            )
            mode_sequences = ModeSequences(
                is_weekday=is_weekday,
                iteration=iteration_index,
                previous_state=previous_state,
                destination_sequences=destination_sequences,
                transport_costs=resolved_transport_costs,
                population_segments=population.population_segments,
                parameters=parameters,
            )
            state_asset = IterationStateAsset(
                is_weekday=is_weekday,
                iteration=iteration_index,
                base_folder=base_folder,
                previous_state=previous_state,
                initial_state=self.initial_iteration_state,
                seeds=seeds,
                activity_sequences=activity_sequences,
                destination_sequences=destination_sequences,
                mode_sequences=mode_sequences,
                transport_costs=resolved_transport_costs,
                population=population,
                activities=activities,
                modes=modes,
                parameters=parameters,
                scenario=scenario,
                sensitivity_case=sensitivity_case,
                cache_iteration_events=parameters.outputs.cache_iteration_events,
            )
            state_assets.append(state_asset)
            previous_state = state_asset

        return state_assets

    def create_and_get_asset(self) -> RunState:
        """Run the requested iteration chain and return its final state."""
        self._raise_if_disabled()
        resolver = get_current_asset_resolver()
        if resolver is not None:
            resolver.retain_assets_shared_by_roots(self.iteration_state_assets)
        logging.debug(
            "Starting PopulationGroupDayTrips run: run_hash=%s is_weekday=%s iterations=%s",
            self.inputs_hash,
            str(self.is_weekday),
            str(self.parameters.run.n_iterations),
        )
        log_memory_checkpoint("run:start")
        get_group_day_trips_progress().step("Running model iterations")
        state = self.final_iteration_state.get()
        self._assert_current_plan_steps_are_available(state)
        logging.debug("PopulationGroupDayTrips run finished: run_hash=%s", self.inputs_hash)
        return state

    def _raise_if_disabled(self) -> None:
        """Fail fast when callers try to access a disabled run."""
        if self.enabled:
            return

        day_type = "weekday" if self.is_weekday else "weekend"
        raise RuntimeError(
            f"Run for {day_type} is disabled. "
            "Enable this day type or avoid accessing its outputs."
        )

    def _get_expected_diagnostics_inputs(self) -> ExpectedDiagnosticsInputs:
        """Return cached survey-derived reference inputs shared by run diagnostics."""
        if self._expected_diagnostics_inputs is not None:
            return self._expected_diagnostics_inputs

        population_weighted_plan_steps = PopulationWeightedPlanSteps(
            population=self.population,
            survey_plan_assets=self.survey_plan_assets,
            is_weekday=self.is_weekday,
        )
        survey_reference_plan_steps = PopulationWeightedSurveyReferenceSteps(
            population=self.population,
            survey_plan_assets=self.survey_plan_assets,
            is_weekday=self.is_weekday,
        )
        expected_calibration_plan_steps = PopulationWeightedCalibrationPlanSteps(
            population_weighted_plan_steps=population_weighted_plan_steps,
            is_weekday=self.is_weekday,
        )
        expected_trip_pattern_distribution = PopulationWeightedTripPatternDistribution(
            survey_reference_plan_steps=survey_reference_plan_steps,
            surveys=self.surveys,
            is_weekday=self.is_weekday,
        )
        self._expected_diagnostics_inputs = ExpectedDiagnosticsInputs(
            population_weighted_plan_steps=population_weighted_plan_steps,
            survey_reference_plan_steps=survey_reference_plan_steps,
            calibration_plan_steps=expected_calibration_plan_steps,
            trip_pattern_distribution=expected_trip_pattern_distribution,
        )
        return self._expected_diagnostics_inputs

    def _build_iteration_metrics_builder(self) -> IterationMetricsBuilder:
        """Build the helper that computes one compact diagnostics row per iteration."""
        expected_inputs = self._get_expected_diagnostics_inputs()
        return IterationMetricsBuilder(
            model_loss=ModelLoss(expected_plan_steps=expected_inputs.calibration_plan_steps),
            model_trip_count_loss=ModelTripCountLoss(
                expected_plan_steps=expected_inputs.population_weighted_plan_steps.get(),
                surveys=self.surveys,
                is_weekday=self.is_weekday,
            ),
            model_entropy=ModelEntropy(expected_plan_steps=expected_inputs.trip_pattern_distribution),
        )

    def _assert_current_plan_steps_are_available(self, state: RunState) -> None:
        """Fail fast if final step-level plans were not produced by the iteration loop."""
        if state.current_plan_steps is not None:
            return

        raise RuntimeError(
            "Run reached finalization without `current_plan_steps`. "
            "This usually means the run resumed after the final saved iteration "
            "but before final outputs were materialized."
        )


    def _build_final_costs(self) -> pl.DataFrame:
        """Build final OD costs for a requested plan-step table."""
        get_group_day_trips_progress().step("Building final cost table")
        final_transport_costs = (
            self.coordinated_final_transport_cost_asset
            or (
                self.iteration_transport_cost_assets[-1]
                if self.iteration_transport_cost_assets
                else self.initial_iteration_state.initial_transport_costs
            )
        )
        return final_transport_costs.get_costs_by_od_and_mode(
            ["cost", "distance", "time", "ghg_emissions_per_trip"]
        )


    def _build_final_plan_steps(self, state: RunState, costs: pl.DataFrame) -> pl.DataFrame:
        """Join final per-step states with demand-group attributes and costs."""
        get_group_day_trips_progress().step("Building final plan-step table")
        plan_steps = state.current_plan_steps
        duplicate_cost_columns = {"cost", "distance", "time", "ghg_emissions_per_trip"}
        existing_duplicate_columns = [col for col in plan_steps.columns if col in duplicate_cost_columns]
        if existing_duplicate_columns:
            plan_steps = plan_steps.drop(existing_duplicate_columns)
        if "mode" in plan_steps.columns:
            plan_steps = plan_steps.with_columns(mode=pl.col("mode").cast(pl.String))

        return (
            plan_steps
            .join(
                state.current_plans.select(
                    PLAN_KEY_COLS + ["utility"]
                ),
                on=PLAN_KEY_COLS,
                how="left",
            )
            .join(
                state.demand_groups.select(DEMAND_UNIT_COLS + ["home_zone_id", "csp", "n_cars"]),
                on=DEMAND_UNIT_COLS,
            )
            .drop(DEMAND_UNIT_COLS)
            .join(
                costs,
                on=["from", "to", "mode"],
                how="left",
            )
            .with_columns(
                is_weekday=pl.lit(self.is_weekday),
            )
        )


    def _build_transitions(self) -> pl.DataFrame | pl.LazyFrame:
        """Combine persisted per-iteration transition events into the final table."""
        get_group_day_trips_progress().step("Building final transition table")
        if self.parameters.outputs.cache_iteration_events is False:
            return pl.DataFrame(schema=TRANSITION_EVENT_SCHEMA)

        transition_paths = [
            state_asset.transition_events_asset.cache_path
            for state_asset in self.iteration_state_assets
            if state_asset.transition_events_asset.cache_path.exists()
        ]
        if not transition_paths:
            raise RuntimeError(
                "Run finished without persisted transition events. "
                "Call `remove()` to clear cached artifacts and rerun from scratch."
            )

        transitions = [
            read_cached_parquet(
                path,
                table_name="transition_events",
                required_schema=TRANSITION_EVENT_SCHEMA,
            )
            for path in transition_paths
        ]
        return pl.concat(transitions, how="vertical")


    def table(
        self,
        table_name: str,
        *,
        iteration: int | None = None,
    ) -> pl.DataFrame:
        """Return one requested run table as a Polars DataFrame."""
        return self._table_asset(table_name, iteration=iteration).get()

    def _table_asset(
        self,
        table_name: str,
        *,
        iteration: int | None = None,
    ) -> "RunTableAsset":
        """Build the transient output used by the result planner."""
        supported_tables = {
            "plan_steps",
            "opportunities",
            "costs",
            "transitions",
            "demand_groups",
            "iteration_metrics",
            "adaptive_search_diagnostics",
            "mnl_od_mode_flows",
        }
        if table_name not in supported_tables:
            raise ValueError(
                f"Unknown run output table `{table_name}`. "
                f"Expected one of {sorted(supported_tables)}."
            )
        if iteration is not None:
            if table_name in {"adaptive_search_diagnostics", "mnl_od_mode_flows"}:
                raise ValueError(
                    f"{table_name} contains search diagnostics and cannot "
                    "be selected as one outer iteration."
                )
            maximum_iteration = self.parameters.run.n_iterations
            if iteration < 1 or iteration > maximum_iteration:
                raise ValueError(
                    "iteration should be between 1 and the maximum number of outer "
                    f"iterations ({maximum_iteration}). Received {iteration}."
                )
        return RunTableAsset(self, table_name=table_name, iteration=iteration)

    def _all_tables(self) -> dict[str, pl.LazyFrame]:
        """Build every run table for the legacy RunResults helper."""
        self._raise_if_disabled()
        table_names = (
            "iteration_metrics",
            "plan_steps",
            "opportunities",
            "costs",
            "transitions",
            "demand_groups",
        )
        return {
            name: self._table_asset(name).get().lazy()
            for name in table_names
        }

    def iteration_table(self, table_name: str, iteration: int) -> pl.LazyFrame:
        """Return one table for a completed model iteration."""
        if table_name not in {"plan_steps", "costs"}:
            raise ValueError(
                "Saved iteration tables currently support only `plan_steps` and `costs`. "
                f"Received `{table_name}`."
            )
        coordinated_states = None
        if getattr(self, "coordinated_run_asset", None) is not None:
            coordinated_states = self.coordinated_run_asset.get().states_by_run_key[
                self.coordinated_run_key
            ]
        available_iterations = (
            len(coordinated_states)
            if coordinated_states is not None
            else len(self.iteration_state_assets)
        )
        if iteration < 1 or iteration > available_iterations:
            raise ValueError(
                "iteration should be between 1 and the run's number of iterations. "
                f"Received {iteration}."
            )

        if table_name == "costs":
            costs_asset = self.iteration_transport_cost_assets[iteration - 1]
            return costs_asset.get_costs_by_od_and_mode(
                ["cost", "distance", "time", "ghg_emissions_per_trip"]
            ).lazy()

        state = (
            coordinated_states[iteration - 1].get()
            if coordinated_states is not None
            else self.iteration_state_assets[iteration - 1].get()
        )
        if state.current_plan_steps is None:
            raise RuntimeError(
                "PopulationGroupDayTrips iteration state has no current plan steps "
                f"for iteration={iteration}."
            )
        return state.current_plan_steps.lazy()


    def results(self) -> RunResults:
        """Return the analysis helper bound to transient run output tables."""
        with asset_resolution_context():
            cached = self._all_tables()

        expected_inputs = self._get_expected_diagnostics_inputs()
        observed_calibration_plan_steps = ObservedCalibrationPlanSteps(
            run=self,
            is_weekday=self.is_weekday,
        )
        iteration_metrics = IterationMetricsHistory(cached["iteration_metrics"])
        observed_trip_pattern_distribution = ObservedTripPatternDistribution(
            run=self,
            is_weekday=self.is_weekday,
        )

        return RunResults(
            inputs_hash=self.inputs_hash,
            is_weekday=self.is_weekday,
            transport_zones=self.population.inputs["transport_zones"],
            demand_groups=cached["demand_groups"],
            plan_steps=cached["plan_steps"],
            opportunities=cached["opportunities"],
            costs=cached["costs"],
            population_weighted_plan_steps=expected_inputs.population_weighted_plan_steps.get(),
            expected_calibration_plan_steps=expected_inputs.calibration_plan_steps,
            observed_calibration_plan_steps=observed_calibration_plan_steps,
            iteration_metrics=iteration_metrics,
            expected_entropy_plan_steps=expected_inputs.trip_pattern_distribution,
            observed_entropy_plan_steps=observed_trip_pattern_distribution,
            transitions=cached["transitions"],
            surveys=self.surveys,
            modes=self.modes,
            parameters=self.parameters,
            run=self,
        )

    def remove(self) -> None:
        """Remove this run's restart checkpoints without clearing shared assets."""
        coordinated_run_asset = getattr(self, "coordinated_run_asset", None)
        if coordinated_run_asset is not None:
            coordinated_run_asset.remove_checkpoint_manifests()
        for state_asset in self.iteration_state_assets:
            iteration = self.iterations.iteration(state_asset.iteration)
            for checkpoint_asset in iteration.checkpoint_assets().values():
                checkpoint_asset.remove()


class RunTableAsset(InMemoryAsset):
    """Build one requested output table from its exact run state dependencies."""

    is_streaming_output = True

    def __init__(
        self,
        run: Run,
        *,
        table_name: str,
        iteration: int | None = None,
    ) -> None:
        self.run = run
        self.table_name = table_name
        self.iteration = iteration
        final_cost_asset = (
            run.iteration_transport_cost_assets[-1]
            if run.iteration_transport_cost_assets
            else run.initial_iteration_state.initial_transport_costs
        )
        if table_name == "costs":
            self.execution_cost_assets = (
                run.iteration_transport_cost_assets[iteration - 1]
                if iteration is not None
                else final_cost_asset,
            )
        elif table_name == "plan_steps" and iteration is None:
            self.execution_cost_assets = (final_cost_asset,)
        else:
            self.execution_cost_assets = ()
        if iteration is not None:
            if run.coordinated_run_asset is not None:
                dependencies = (run.coordinated_run_asset,)
            elif table_name == "plan_steps":
                dependencies = (run.iteration_state_assets[iteration - 1],)
            elif table_name == "costs":
                dependencies = (run.iteration_transport_cost_assets[iteration - 1],)
            elif table_name == "iteration_metrics":
                dependencies = (run.iteration_state_assets[iteration - 1],)
            else:
                raise ValueError(
                    f"Iteration-scoped output `{table_name}` is not supported."
                )
        elif run.coordinated_run_asset is not None and table_name in {
            "plan_steps",
            "costs",
            "transitions",
            "iteration_metrics",
            "adaptive_search_diagnostics",
            "mnl_od_mode_flows",
        }:
            dependencies = (run.coordinated_run_asset,)
        elif table_name == "iteration_metrics":
            dependencies = tuple(run.iteration_state_assets)
        elif table_name == "transitions":
            dependencies = (
                tuple(run.iteration_state_assets)
                if run.parameters.outputs.cache_iteration_events
                else ()
            )
        elif table_name in {"opportunities", "demand_groups"}:
            dependencies = (run.initial_iteration_state,)
        elif table_name == "costs":
            final_cost_asset = (
                run.iteration_transport_cost_assets[-1]
                if run.iteration_transport_cost_assets
                else run.initial_iteration_state.initial_transport_costs
            )
            dependencies = (final_cost_asset,)
        elif table_name == "plan_steps":
            final_cost_asset = (
                run.iteration_transport_cost_assets[-1]
                if run.iteration_transport_cost_assets
                else run.initial_iteration_state.initial_transport_costs
            )
            dependencies = (run.final_iteration_state, final_cost_asset)
        else:
            dependencies = (run.final_iteration_state,)
        super().__init__(
            {
                "version": 4,
                # Scenario views share a coordinator but return different rows.
                "scenario": run.scenario,
                "dependencies": dependencies,
                "table_name": table_name,
                "iteration": iteration,
            }
        )

    def create_and_get_asset(self) -> pl.DataFrame:
        """Compute only the table selected by this output request."""
        self.run._raise_if_disabled()
        resolver = get_current_asset_resolver()
        if resolver is not None:
            resolver.retain_assets_shared_by_roots([self])
        if self.iteration is not None:
            if self.table_name == "iteration_metrics":
                builder = self.run._build_iteration_metrics_builder()
                if self.run.coordinated_run_asset is not None:
                    state_asset = self.run.coordinated_run_asset.get().states_by_run_key[
                        self.run.coordinated_run_key
                    ][self.iteration - 1]
                    state = state_asset.get()
                    iteration_number = self.iteration
                else:
                    state_asset = self.run.iteration_state_assets[self.iteration - 1]
                    state = state_asset.get()
                    iteration_number = state_asset.iteration
                row = builder.history_row(
                    iteration=iteration_number,
                    current_plans=state.current_plans,
                    current_plan_steps=state.current_plan_steps,
                    destination_saturation=state.destination_saturation,
                )
                return IterationMetricsHistory.from_records([row])
            if self.table_name not in {"plan_steps", "costs"}:
                raise ValueError(
                    "Iteration-scoped run tables currently support "
                    "`plan_steps` and `costs`."
                )
            try:
                return self.run.iteration_table(
                    self.table_name,
                    self.iteration,
                ).collect(engine="streaming")
            finally:
                resolver = get_current_asset_resolver()
                if resolver is not None:
                    if self.table_name == "costs":
                        resolver.release_in_memory_asset(
                            self.run.iteration_transport_cost_assets[self.iteration - 1]
                        )
                        resolver.consume_execution_cost_root(self)

        if self.table_name in {"opportunities", "demand_groups"}:
            initial_asset = self.run.initial_iteration_state
            try:
                initial_state = initial_asset.get()
                if self.table_name == "opportunities":
                    return initial_state.opportunities
                return initial_state.demand_groups
            finally:
                resolver = get_current_asset_resolver()
                if resolver is not None:
                    resolver.release_in_memory_asset(initial_asset)

        if self.table_name == "iteration_metrics":
            get_group_day_trips_progress().step("Preparing iteration diagnostics")
            builder = self.run._build_iteration_metrics_builder()
            records = []
            if self.run.coordinated_run_asset is not None:
                iteration_state_assets = (
                    self.run.coordinated_run_asset.get().states_by_run_key[
                        self.run.coordinated_run_key
                    ]
                )
                iteration_states = [state_asset.get() for state_asset in iteration_state_assets]
            else:
                iteration_states = [
                    state_asset.get()
                    for state_asset in self.run.iteration_state_assets
                ]
            for iteration, iteration_state in enumerate(iteration_states, start=1):
                records.append(
                    builder.history_row(
                        iteration=iteration,
                        current_plans=iteration_state.current_plans,
                        current_plan_steps=iteration_state.current_plan_steps,
                        destination_saturation=iteration_state.destination_saturation,
                    )
                )
            return IterationMetricsHistory.from_records(records)
        if self.table_name == "adaptive_search_diagnostics":
            state = self.run.get()
            if state.adaptive_search_diagnostics is None:
                return pl.DataFrame()
            return state.adaptive_search_diagnostics
        if self.table_name == "plan_steps":
            configured_final_cost_asset = (
                self.run.iteration_transport_cost_assets[-1]
                if self.run.iteration_transport_cost_assets
                else self.run.initial_iteration_state.initial_transport_costs
            )
            try:
                state = self.run.get()
                return self.run._build_final_plan_steps(
                    state,
                    self.run._build_final_costs(),
                )
            finally:
                resolver = get_current_asset_resolver()
                if resolver is not None:
                    resolver.release_in_memory_asset(self.run)
                    resolver.release_in_memory_asset(self.run.final_iteration_state)
                    resolver.release_in_memory_asset(
                        self.run.coordinated_final_transport_cost_asset
                        or configured_final_cost_asset
                    )
                    resolver.consume_execution_cost_root(self)
        if self.table_name == "costs":
            configured_final_cost_asset = (
                self.run.iteration_transport_cost_assets[-1]
                if self.run.iteration_transport_cost_assets
                else self.run.initial_iteration_state.initial_transport_costs
            )
            try:
                if self.run.coordinated_run_asset is not None:
                    self.run.get()
                return self.run._build_final_costs()
            finally:
                resolver = get_current_asset_resolver()
                if resolver is not None:
                    resolver.release_in_memory_asset(
                        self.run.coordinated_final_transport_cost_asset
                        or configured_final_cost_asset
                    )
                    resolver.consume_execution_cost_root(self)
        if self.table_name == "transitions":
            if self.run.coordinated_run_asset is not None or self.run.parameters.outputs.cache_iteration_events:
                try:
                    self.run.get()
                finally:
                    resolver = get_current_asset_resolver()
                    if resolver is not None:
                        resolver.release_in_memory_asset(self.run)
                        resolver.release_in_memory_asset(self.run.final_iteration_state)
            transitions = self.run._build_transitions()
            return transitions.collect(engine="streaming") if isinstance(
                transitions, pl.LazyFrame
            ) else transitions
        if self.table_name == "mnl_od_mode_flows":
            if self.run.coordinated_run_asset is None:
                raise ValueError("MNL flow diagnostics require a coordinated simulation.")
            return self.run.coordinated_run_asset.get().mnl_od_mode_flows_by_run_key[
                self.run.coordinated_run_key
            ]
        raise ValueError(f"Unknown run output table `{self.table_name}`.")
