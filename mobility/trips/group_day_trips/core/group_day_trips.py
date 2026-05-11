import warnings

import polars as pl
from typing import Dict, List

from mobility.runtime.assets.asset import Asset
from mobility.transport.costs.transport_costs import TransportCosts
from .parameters import BehaviorChangePhase, Parameters
from .run import Run
from mobility.activities import Activity, HomeActivity, OtherActivity
from mobility.surveys import SurveyPlanAssets
from mobility.surveys.mobility_survey import MobilitySurvey
from mobility.population import Population
from mobility.transport.modes.core.transport_mode import TransportMode


class PopulationGroupDayTrips:
    """Top-level asset exposing weekday and weekend grouped day trips."""

    def __init__(
        self,
        population: Population,
        modes: List[TransportMode] = None,
        activities: List[Activity] = None,
        surveys: List[MobilitySurvey] = None,
        parameters: Parameters = None,
        n_iterations: int = None,
        alpha: float = None,
        k_activity_sequences: int | None = None,
        k_destination_sequences: int = None,
        k_mode_sequences: int = None,
        n_warmup_iterations: int = None,
        max_inactive_age: int = None,
        dest_prob_cutoff: float = None,
        n_iter_per_cost_update: int = None,
        cost_uncertainty_sd: float = None,
        seed: int = None,
        mode_sequence_search_parallel: bool = None,
        use_rust_mode_sequence_search: bool = None,
        save_transition_events: bool = None,
        persist_iteration_artifacts: bool = None,
        min_activity_time_constant: float = None,
        update_plan_timings_from_modeled_travel_times: bool = None,
        transition_distance_threshold: float = None,
        enable_transition_distance_model: bool = None,
        transition_revision_probability: float = None,
        transition_logit_scale: float = None,
        transition_utility_pruning_delta: float = None,
        transition_distance_friction: float = None,
        plan_embedding_dimension_weights: list[float] | None = None,
        behavior_change_phases: list[BehaviorChangePhase] | None = None,
        simulate_weekend: bool = None,
    ):
        """Initialize the grouped day-trips asset.

        The wrapper validates its high-level inputs, normalizes constructor
        parameters into a `Parameters` instance, builds a shared
        `TransportCosts`, and creates one underlying `Run`
        asset for weekdays plus one for weekends. The weekend run is enabled or
        disabled according to `simulate_weekend`.

        Args:
            population: Population object containing demand groups and transport
                zones.
            modes: Available transport modes. Must contain at least one
                `TransportMode`.
            activities: Activities used to build and spatialize schedules.
                Must contain at least one `Activity` and include `HomeActivity`
                and `OtherActivity`.
            surveys: Mobility surveys providing empirical activity programs. Must
                contain at least one `MobilitySurvey`.
            parameters: Parameter container. When provided, explicit keyword
                arguments are merged into this model and revalidated.
            n_iterations: Optional override for
                `Parameters.n_iterations`.
            alpha: Optional override for `Parameters.alpha`.
            k_activity_sequences: Optional override for
                `Parameters.k_activity_sequences`.
            k_destination_sequences: Optional override for
                `Parameters.k_destination_sequences`.
            k_mode_sequences: Optional override for
                `Parameters.k_mode_sequences`.
            n_warmup_iterations: Optional override for
                `Parameters.n_warmup_iterations`.
            max_inactive_age: Optional override for
                `Parameters.max_inactive_age`.
            dest_prob_cutoff: Optional override for
                `Parameters.dest_prob_cutoff`.
            n_iter_per_cost_update: Optional override for
                `Parameters.n_iter_per_cost_update`.
            cost_uncertainty_sd: Optional override for
                `Parameters.cost_uncertainty_sd`.
            seed: Optional override for `Parameters.seed`.
            mode_sequence_search_parallel: Optional override for
                `Parameters.mode_sequence_search_parallel`.
            use_rust_mode_sequence_search: Optional override for
                `Parameters.use_rust_mode_sequence_search`.
            save_transition_events: Optional override for
                `Parameters.save_transition_events`.
            persist_iteration_artifacts: Optional override for
                `Parameters.persist_iteration_artifacts`.
            min_activity_time_constant: Optional override for
                `Parameters.min_activity_time_constant`.
            update_plan_timings_from_modeled_travel_times: Optional override for
                `Parameters.update_plan_timings_from_modeled_travel_times`.
            transition_distance_threshold: Optional override for
                `Parameters.transition_distance_threshold`.
            enable_transition_distance_model: Optional override for
                `Parameters.enable_transition_distance_model`.
            transition_revision_probability: Optional override for
                `Parameters.transition_revision_probability`.
            transition_logit_scale: Optional override for
                `Parameters.transition_logit_scale`.
            transition_utility_pruning_delta: Optional override for
                `Parameters.transition_utility_pruning_delta`.
            transition_distance_friction: Optional override for
                `Parameters.transition_distance_friction`.
            plan_embedding_dimension_weights: Optional override for
                `Parameters.plan_embedding_dimension_weights`.
            behavior_change_phases: Optional override for
                `Parameters.behavior_change_phases`.
            simulate_weekend: Optional override for
                `Parameters.simulate_weekend`.

        Raises:
            ValueError: If modes, activities, or surveys are empty, or if required
                activities are missing.
            TypeError: If any element of modes, activities, or surveys has an
                incorrect type.
        """
        self._validate_modes(modes)
        self._validate_activities(activities)
        self._validate_surveys(surveys)

        parameters = Asset.prepare_parameters(
            parameters=parameters,
            parameters_cls=Parameters,
            explicit_args={
                "n_iterations": n_iterations,
                "alpha": alpha,
                "k_activity_sequences": k_activity_sequences,
                "k_destination_sequences": k_destination_sequences,
                "k_mode_sequences": k_mode_sequences,
                "n_warmup_iterations": n_warmup_iterations,
                "max_inactive_age": max_inactive_age,
                "dest_prob_cutoff": dest_prob_cutoff,
                "n_iter_per_cost_update": n_iter_per_cost_update,
                "cost_uncertainty_sd": cost_uncertainty_sd,
                "seed": seed,
                "mode_sequence_search_parallel": mode_sequence_search_parallel,
                "use_rust_mode_sequence_search": use_rust_mode_sequence_search,
                "save_transition_events": save_transition_events,
                "persist_iteration_artifacts": persist_iteration_artifacts,
                "min_activity_time_constant": min_activity_time_constant,
                "update_plan_timings_from_modeled_travel_times": update_plan_timings_from_modeled_travel_times,
                "transition_distance_threshold": transition_distance_threshold,
                "enable_transition_distance_model": enable_transition_distance_model,
                "transition_revision_probability": transition_revision_probability,
                "transition_logit_scale": transition_logit_scale,
                "transition_utility_pruning_delta": transition_utility_pruning_delta,
                "transition_distance_friction": transition_distance_friction,
                "plan_embedding_dimension_weights": plan_embedding_dimension_weights,
                "behavior_change_phases": behavior_change_phases,
                "simulate_weekend": simulate_weekend,
            },
            owner_name="PopulationGroupDayTrips",
        )

        transport_costs = TransportCosts(modes)
        selected_surveys = self._select_surveys_for_population(population, surveys)
        survey_plan_assets = SurveyPlanAssets(
            surveys=selected_surveys,
            activities=activities,
            modes=modes,
        )

        weekday_run = Run(
            population=population,
            transport_costs=transport_costs,
            activities=activities,
            modes=modes,
            surveys=selected_surveys,
            survey_plan_assets=survey_plan_assets,
            parameters=parameters,
            is_weekday=True,
            enabled=True,
        )

        weekend_run = Run(
            population=population,
            transport_costs=transport_costs,
            activities=activities,
            modes=modes,
            surveys=selected_surveys,
            survey_plan_assets=survey_plan_assets,
            parameters=parameters,
            is_weekday=False,
            enabled=parameters.simulate_weekend,
        )

        self.survey_plan_assets = survey_plan_assets
        self.weekday_run = weekday_run
        self.weekend_run = weekend_run

        self.cache_path = self._build_cache_path()

    
    def get(self) -> Dict[str, pl.LazyFrame]:
        """Return the combined weekday/weekend outputs for this wrapper.

        This method delegates execution to the underlying
        `Run` assets. Weekend outputs are included
        only when the weekend run is enabled.

        Returns:
            dict[str, pl.LazyFrame]: Lazy readers for weekday outputs, shared
            `demand_groups`, and weekend outputs when weekend simulation is
            enabled.
        """
        self.weekday_run.get()
        if self.weekend_run.enabled:
            self.weekend_run.get()
        return self.get_cached_asset()

    
    def create_and_get_asset(self) -> Dict[str, pl.LazyFrame]:
        """Return the combined outputs through the legacy asset-style API.

        Returns:
            dict[str, pl.LazyFrame]: Mapping of cache names to parquet scans.
        """
        return self.get()
    

    def get_cached_asset(self) -> Dict[str, pl.LazyFrame]:
        """Compatibility alias returning lazy readers for cached outputs.

        Returns:
            dict[str, pl.LazyFrame]: Mapping of cache names to parquet scans.
        """
        return {key: pl.scan_parquet(path) for key, path in self.cache_path.items()}


    def remove(self):
        """Remove cached outputs and saved iteration artifacts for both run assets."""
        self.weekday_run.remove()
        self.weekend_run.remove()


    def _validate_activities(self, activities: List[Activity]) -> None:
        """Validate the activities passed to the wrapper constructor.

        Args:
            activities: Activity objects that define the activity types available to
                the simulation.

        Raises:
            ValueError: If no activities are provided, or if `HomeActivity` or
                `OtherActivity` is missing.
            TypeError: If any element is not an `Activity` instance.
        """
        if not activities:
            raise ValueError("PopulationGroupDayTrips needs at least one activity in `activities`.")

        for activity in activities:
            if not isinstance(activity, Activity):
                raise TypeError(
                    "PopulationGroupDayTrips activities argument should be a list of `Activity` "
                    f"instances, but received one object of class {type(activity)}."
                )

        if not any(isinstance(a, OtherActivity) for a in activities):
            raise ValueError("PopulationGroupDayTrips `activities` argument should contain an `OtherActivity`.")

        if not any(isinstance(a, HomeActivity) for a in activities):
            raise ValueError("PopulationGroupDayTrips `activities` argument should contain a `HomeActivity`.")

    def _validate_modes(self, modes: List[TransportMode]) -> None:
        """Validate the transport modes passed to the wrapper constructor.

        Args:
            modes: Transport modes available to the simulation.

        Raises:
            ValueError: If no modes are provided.
            TypeError: If any element is not a `TransportMode` instance.
        """
        if not modes:
            raise ValueError("PopulationGroupDayTrips needs at least one mode in `modes`.")

        for mode in modes:
            if not isinstance(mode, TransportMode):
                raise TypeError(
                    "PopulationGroupDayTrips modes argument should be a list of `TransportMode` "
                    f"instances, but received one object of class {type(mode)}."
                )

    def _validate_surveys(self, surveys: List[MobilitySurvey]) -> None:
        """Validate the mobility surveys passed to the wrapper constructor.

        Args:
            surveys: Survey assets used to derive survey-based schedules.

        Raises:
            ValueError: If no surveys are provided.
            TypeError: If any element is not a `MobilitySurvey` instance.
        """
        if not surveys:
            raise ValueError("PopulationGroupDayTrips needs at least one survey in `surveys`.")

        for survey in surveys:
            if not isinstance(survey, MobilitySurvey):
                raise TypeError(
                    "PopulationGroupDayTrips surveys argument should be a list of `MobilitySurvey` "
                    f"instances, but received one object of class {type(survey)}."
                )

    def _get_population_countries(self, population: Population) -> list[str]:
        """Infer population countries from the study-area admin prefixes."""
        study_area = population.transport_zones.study_area.get()
        if "geometry" in study_area.columns:
            study_area = study_area.drop("geometry", axis=1)
        return (
            pl.from_pandas(study_area[["local_admin_unit_id"]])
            .with_columns(country=pl.col("local_admin_unit_id").str.slice(0, 2))
            .get_column("country")
            .unique()
            .sort()
            .to_list()
        )

    def _select_surveys_for_population(
        self,
        population: Population,
        surveys: list[MobilitySurvey],
    ) -> list[MobilitySurvey]:
        """Validate survey coverage against the population and keep relevant surveys."""
        population_countries = set(self._get_population_countries(population))
        surveys_by_country: dict[str, list[MobilitySurvey]] = {}
        for survey in surveys:
            surveys_by_country.setdefault(survey.inputs["parameters"].country, []).append(survey)

        survey_countries = set(surveys_by_country)
        missing_countries = sorted(population_countries - survey_countries)
        if missing_countries:
            raise ValueError(
                "PopulationGroupDayTrips requires at least one survey for each population country. "
                f"Missing survey coverage for: {', '.join(missing_countries)}. "
                f"Provided survey countries: {', '.join(sorted(survey_countries))}."
            )

        unused_countries = sorted(survey_countries - population_countries)
        if unused_countries:
            warnings.warn(
                "Some provided surveys will not be used because their countries are absent from the population: "
                + ", ".join(unused_countries)
                + ".",
                stacklevel=2,
            )

        selected_surveys: list[MobilitySurvey] = []
        for country in sorted(population_countries):
            selected_surveys.extend(surveys_by_country[country])
        return selected_surveys

    def _build_cache_path(self) -> Dict[str, str]:
        """Expose child run cache paths through the wrapper keys.

        Returns:
            dict[str, pathlib.Path]: Mapping from wrapper output names to the
            underlying run asset files. Weekend keys are included only when
            the weekend run is enabled.
        """
        weekday_paths = self.weekday_run.cache_path
        keys = (
            "plan_steps",
            "opportunities",
            "costs",
            "transitions",
            "iteration_metrics",
        )
        cache_paths = {f"weekday_{key}": weekday_paths[key] for key in keys}
        cache_paths["demand_groups"] = weekday_paths["demand_groups"]
        if self.weekend_run.enabled:
            weekend_paths = self.weekend_run.cache_path
            cache_paths.update({f"weekend_{key}": weekend_paths[key] for key in keys})
        return cache_paths
