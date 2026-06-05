import os
import pathlib
import logging
from dataclasses import dataclass
from types import SimpleNamespace
from typing import List

import polars as pl

from ..iterations import (
    InitialIterationStateAsset,
    IterationSeedsAsset,
    Iterations,
    IterationTransportCostsAsset,
    IterationStateAsset,
)
from ..evaluation.population_weighted_plan_steps import PopulationWeightedPlanSteps
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
from ..transitions.transition_schema import TRANSITION_EVENT_SCHEMA
from .memory_logging import log_memory_checkpoint
from .parameters import GroupDayTripsParameters
from .progress import (
    GroupDayTripsProgressReporter,
    get_group_day_trips_progress,
    is_group_day_trips_progress_active,
)
from .results import RunResults
from .run_state import RunState
from mobility.transport.costs.transport_costs import TransportCosts
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.input_hashing import hash_inputs
from mobility.activities import Activity
from mobility.surveys import SurveyPlanAssets
from mobility.surveys.mobility_survey import MobilitySurvey
from mobility.population import Population
from mobility.transport.modes.core.transport_mode import TransportMode


@dataclass(frozen=True)
class ExpectedDiagnosticsInputs:
    """Shared survey-derived reference inputs used by run diagnostics."""

    population_weighted_plan_steps: PopulationWeightedPlanSteps
    calibration_plan_steps: PopulationWeightedCalibrationPlanSteps
    trip_pattern_distribution: PopulationWeightedTripPatternDistribution


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
        survey_plan_assets: SurveyPlanAssets,
        parameters: GroupDayTripsParameters,
        is_weekday: bool,
        enabled: bool = True,
        scenario: str,
        replication: int = 0,
    ) -> None:
        """Initialize a single weekday or weekend PopulationGroupDayTrips run."""
        run_context_inputs = {
            "version": 17,
            "population": population,
            "activities": activities,
            "modes": modes,
            "surveys": surveys,
            "survey_plan_assets": survey_plan_assets,
            "parameters": parameters,
            "is_weekday": is_weekday,
            "enabled": enabled,
            "scenario": scenario,
        }
        run_context_hash = hash_inputs(run_context_inputs)
        self.run_context = SimpleNamespace(
            inputs_hash=run_context_hash,
            parameters=parameters,
            is_weekday=is_weekday,
            scenario=scenario,
            replication=replication,
        )
        self.scenario = scenario
        self.replication = int(replication)

        self.transport_costs = transport_costs
        self._expected_diagnostics_inputs: ExpectedDiagnosticsInputs | None = None

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        group_day_trips_folder = project_folder / "group_day_trips"
        self.iterations = Iterations(
            run_inputs_hash=run_context_hash,
            is_weekday=is_weekday,
            base_folder=group_day_trips_folder,
        )
        self.initial_iteration_state = InitialIterationStateAsset(
            is_weekday=is_weekday,
            base_folder=group_day_trips_folder,
            population=population,
            survey_plan_assets=survey_plan_assets,
            activities=activities,
            modes=modes,
            parameters=parameters,
            scenario=scenario,
            initial_transport_costs=IterationTransportCostsAsset(
                is_weekday=is_weekday,
                iteration=1,
                base_folder=group_day_trips_folder,
                transport_costs=transport_costs,
                scenario=scenario,
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
            initial_state=self.initial_iteration_state,
        )
        self.final_iteration_state = (
            self.iteration_state_assets[-1]
            if self.iteration_state_assets
            else self.initial_iteration_state
        )

        inputs = {
            **run_context_inputs,
            "version": 18,
            "final_iteration_state": self.final_iteration_state,
        }

        cache_path = {
            "plan_steps": group_day_trips_folder / "plan_steps.parquet",
            "opportunities": group_day_trips_folder / "opportunities.parquet",
            "costs": group_day_trips_folder / "costs.parquet",
            "transitions": group_day_trips_folder / "transitions.parquet",
            "demand_groups": group_day_trips_folder / "demand_groups.parquet",
            "iteration_metrics": group_day_trips_folder / "iteration_metrics.parquet",
        }
        super().__init__(inputs, cache_path)

    def get(self, *args, **kwargs) -> dict[str, pl.LazyFrame]:
        """Materialize the run while reporting high-level progress."""
        if is_group_day_trips_progress_active():
            return super().get(*args, **kwargs)

        day_type = "weekday" if self.is_weekday else "weekend"
        label = (
            f"GroupDayTrips {day_type} "
            f"scenario={self.scenario} repl={self.replication}"
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
        initial_state: FileAsset,
    ) -> list[IterationStateAsset]:
        """Build the content-addressed state asset chain for the run iterations."""
        state_assets: list[IterationStateAsset] = []
        previous_state: FileAsset = initial_state
        previous_destination_sequences: DestinationSequences | None = None
        previous_mode_sequences: ModeSequences | None = None

        for iteration_index in range(1, parameters.run.n_iterations + 1):
            seeds = IterationSeedsAsset(
                previous_state=previous_state,
                iteration=iteration_index,
                base_folder=base_folder,
            )
            resolved_transport_costs = IterationTransportCostsAsset(
                is_weekday=is_weekday,
                iteration=iteration_index,
                base_folder=base_folder,
                transport_costs=transport_costs,
                scenario=scenario,
                previous_state=(None if iteration_index == 1 else previous_state),
                n_iter_per_cost_update=parameters.run.n_iter_per_cost_update,
            )
            self.iteration_transport_cost_assets.append(resolved_transport_costs)
            activity_sequences = ActivitySequences(
                is_weekday=is_weekday,
                iteration=iteration_index,
                base_folder=self.iterations.folder_paths["activity-sequences"],
                previous_state=previous_state,
                seed_asset=seeds,
                parameters=parameters,
            )
            destination_sequences = DestinationSequences(
                is_weekday=is_weekday,
                iteration=iteration_index,
                base_folder=self.iterations.folder_paths["destination-sequences"],
                previous_state=previous_state,
                previous_destination_sequences=previous_destination_sequences,
                seed_asset=seeds,
                activity_sequences=activity_sequences,
                activities=activities,
                scenario=scenario,
                transport_zones=population.transport_zones,
                transport_costs=resolved_transport_costs,
                parameters=parameters,
            )
            mode_sequences = ModeSequences(
                is_weekday=is_weekday,
                iteration=iteration_index,
                base_folder=self.iterations.folder_paths["modes"],
                previous_mode_sequences=previous_mode_sequences,
                destination_sequences=destination_sequences,
                transport_costs=resolved_transport_costs,
                working_folder=base_folder,
                parameters=parameters,
            )
            state_asset = IterationStateAsset(
                is_weekday=is_weekday,
                iteration=iteration_index,
                base_folder=base_folder,
                previous_state=previous_state,
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
                cache_iteration_events=parameters.outputs.cache_iteration_events,
            )
            state_assets.append(state_asset)
            previous_state = state_asset
            previous_destination_sequences = destination_sequences
            previous_mode_sequences = mode_sequences

        return state_assets

    def create_and_get_asset(self) -> dict[str, pl.LazyFrame]:
        """Run the simulation for this day type and materialize cached outputs."""
        self._raise_if_disabled()
        logging.debug(
            "Starting PopulationGroupDayTrips run: run_hash=%s is_weekday=%s iterations=%s",
            self.inputs_hash,
            str(self.is_weekday),
            str(self.parameters.run.n_iterations),
        )
        get_group_day_trips_progress().step("Finalizing group-day-trips run")
        log_memory_checkpoint("run:start")

        state = self.final_iteration_state.get()
        iteration_metrics_records = self._build_iteration_metrics_records()

        self._assert_current_plan_steps_are_available(state)
        self._log_state_memory_checkpoint("state:before_finalization", state)

        final_costs = self._build_final_costs(state)
        log_memory_checkpoint("finalization:after_build_final_costs", costs=final_costs)

        final_plan_steps = self._build_final_plan_steps(state, final_costs)
        log_memory_checkpoint(
            "finalization:after_build_final_plan_steps",
            plan_steps=final_plan_steps,
        )

        transitions = self._build_transitions()
        log_memory_checkpoint("finalization:after_build_transitions", transitions=transitions)

        self._write_outputs(
            plan_steps=final_plan_steps,
            opportunities=state.opportunities,
            costs=final_costs,
            transitions=transitions,
            demand_groups=state.demand_groups,
            iteration_metrics=IterationMetricsHistory.from_records(iteration_metrics_records),
        )
        log_memory_checkpoint("run:after_write_outputs")
        logging.debug("PopulationGroupDayTrips run finished: run_hash=%s", self.inputs_hash)

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

    def _get_expected_diagnostics_inputs(self) -> ExpectedDiagnosticsInputs:
        """Return cached survey-derived reference inputs shared by run diagnostics."""
        if self._expected_diagnostics_inputs is not None:
            return self._expected_diagnostics_inputs

        population_weighted_plan_steps = PopulationWeightedPlanSteps(
            population=self.population,
            survey_plan_assets=self.survey_plan_assets,
            is_weekday=self.is_weekday,
        )
        expected_calibration_plan_steps = PopulationWeightedCalibrationPlanSteps(
            population_weighted_plan_steps=population_weighted_plan_steps,
            is_weekday=self.is_weekday,
        )
        expected_trip_pattern_distribution = PopulationWeightedTripPatternDistribution(
            population_weighted_plan_steps=population_weighted_plan_steps,
            surveys=self.surveys,
            is_weekday=self.is_weekday,
        )
        self._expected_diagnostics_inputs = ExpectedDiagnosticsInputs(
            population_weighted_plan_steps=population_weighted_plan_steps,
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
                expected_plan_steps=expected_inputs.population_weighted_plan_steps,
                surveys=self.surveys,
                is_weekday=self.is_weekday,
            ),
            model_entropy=ModelEntropy(expected_plan_steps=expected_inputs.trip_pattern_distribution),
        )

    def _build_iteration_metrics_records(self) -> list[dict]:
        """Build one diagnostics row from each cached iteration state."""
        get_group_day_trips_progress().step("Building iteration diagnostics")
        iteration_metrics_builder = self._build_iteration_metrics_builder()
        records = []
        for state_asset in self.iteration_state_assets:
            state = state_asset.get()
            records.append(
                iteration_metrics_builder.history_row(
                    iteration=state_asset.iteration,
                    current_plans=state.current_plans,
                    current_plan_steps=state.current_plan_steps,
                    destination_saturation=state.destination_saturation,
                )
            )
        return records


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
        get_group_day_trips_progress().step("Building final cost table")
        final_transport_costs = (
            self.iteration_transport_cost_assets[-1]
            if self.iteration_transport_cost_assets
            else self.initial_iteration_state.initial_transport_costs
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

        return pl.concat([pl.read_parquet(path) for path in transition_paths], how="vertical")


    def _write_outputs(
        self,
        *,
        plan_steps: pl.DataFrame,
        opportunities: pl.DataFrame,
        costs: pl.DataFrame,
        transitions: pl.DataFrame,
        demand_groups: pl.DataFrame,
        iteration_metrics: pl.DataFrame,
    ) -> None:
        """Write the final run artifacts to their parquet cache paths."""
        get_group_day_trips_progress().step("Writing final run outputs")
        plan_steps.write_parquet(self.cache_path["plan_steps"])
        opportunities.write_parquet(self.cache_path["opportunities"])
        costs.write_parquet(self.cache_path["costs"])
        transitions.write_parquet(self.cache_path["transitions"])
        demand_groups.write_parquet(self.cache_path["demand_groups"])
        iteration_metrics.write_parquet(self.cache_path["iteration_metrics"])

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
            plan_id_index=state.plan_id_index,
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
        """Remove final cached outputs for this run.

        Iteration states, sequence samples, and congestion costs are shared DAG
        cache assets. They are intentionally kept so other scenario runs can
        reuse unchanged iterations.
        """
        super().remove()
