import polars as pl

from typing import Dict, List

from mobility.asset import Asset
from mobility.choice_models.population_trips_parameters import PopulationTripsParameters
from mobility.choice_models.population_trips_run import PopulationTripsRun
from mobility.choice_models.travel_costs_aggregator import TravelCostsAggregator
from mobility.motives import HomeMotive, Motive, OtherMotive
from mobility.parsers.mobility_survey import MobilitySurvey
from mobility.population import Population
from mobility.transport_modes.transport_mode import TransportMode


class PopulationTrips:
    """Compatibility wrapper around weekday and weekend `PopulationTripsRun` assets."""

    def __init__(
        self,
        population: Population,
        modes: List[TransportMode] = None,
        motives: List[Motive] = None,
        surveys: List[MobilitySurvey] = None,
        parameters: PopulationTripsParameters = None,
        n_iterations: int = None,
        alpha: float = None,
        k_mode_sequences: int = None,
        dest_prob_cutoff: float = None,
        n_iter_per_cost_update: int = None,
        cost_uncertainty_sd: float = None,
        seed: int = None,
        mode_sequence_search_parallel: bool = None,
        min_activity_time_constant: float = None,
        simulate_weekend: bool = None,
    ):
        """Initialize the multi-day PopulationTrips compatibility wrapper.

        The wrapper validates its high-level inputs, normalizes constructor
        parameters into a `PopulationTripsParameters` instance, builds a shared
        `TravelCostsAggregator`, and creates one underlying `PopulationTripsRun`
        asset for weekdays plus one for weekends. The weekend run is enabled or
        disabled according to `simulate_weekend`.

        Args:
            population: Population object containing demand groups and transport
                zones.
            modes: Available transport modes. Must contain at least one
                `TransportMode`.
            motives: Activity motives used to build and spatialize schedules.
                Must contain at least one `Motive` and include `HomeMotive` and
                `OtherMotive`.
            surveys: Mobility surveys providing empirical activity chains. Must
                contain at least one `MobilitySurvey`.
            parameters: Parameter container. When provided, explicit keyword
                arguments are merged into this model and revalidated.
            n_iterations: Optional override for
                `PopulationTripsParameters.n_iterations`.
            alpha: Optional override for `PopulationTripsParameters.alpha`.
            k_mode_sequences: Optional override for
                `PopulationTripsParameters.k_mode_sequences`.
            dest_prob_cutoff: Optional override for
                `PopulationTripsParameters.dest_prob_cutoff`.
            n_iter_per_cost_update: Optional override for
                `PopulationTripsParameters.n_iter_per_cost_update`.
            cost_uncertainty_sd: Optional override for
                `PopulationTripsParameters.cost_uncertainty_sd`.
            seed: Optional override for `PopulationTripsParameters.seed`.
            mode_sequence_search_parallel: Optional override for
                `PopulationTripsParameters.mode_sequence_search_parallel`.
            min_activity_time_constant: Optional override for
                `PopulationTripsParameters.min_activity_time_constant`.
            simulate_weekend: Optional override for
                `PopulationTripsParameters.simulate_weekend`.

        Raises:
            ValueError: If modes, motives, or surveys are empty, or if required
                motives are missing.
            TypeError: If any element of modes, motives, or surveys has an
                incorrect type.
        """
        self.validate_modes(modes)
        self.validate_motives(motives)
        self.validate_surveys(surveys)

        parameters = Asset.prepare_parameters(
            parameters=parameters,
            parameters_cls=PopulationTripsParameters,
            explicit_args={
                "n_iterations": n_iterations,
                "alpha": alpha,
                "k_mode_sequences": k_mode_sequences,
                "dest_prob_cutoff": dest_prob_cutoff,
                "n_iter_per_cost_update": n_iter_per_cost_update,
                "cost_uncertainty_sd": cost_uncertainty_sd,
                "seed": seed,
                "mode_sequence_search_parallel": mode_sequence_search_parallel,
                "min_activity_time_constant": min_activity_time_constant,
                "simulate_weekend": simulate_weekend,
            },
            owner_name="PopulationTrips",
        )

        costs_aggregator = TravelCostsAggregator(modes)

        weekday_run = PopulationTripsRun(
            population=population,
            costs_aggregator=costs_aggregator,
            motives=motives,
            modes=modes,
            surveys=surveys,
            parameters=parameters,
            is_weekday=True,
            enabled=True,
        )

        weekend_run = PopulationTripsRun(
            population=population,
            costs_aggregator=costs_aggregator,
            motives=motives,
            modes=modes,
            surveys=surveys,
            parameters=parameters,
            is_weekday=False,
            enabled=parameters.simulate_weekend,
        )

        self.weekday_run = weekday_run
        self.weekend_run = weekend_run

        self.cache_path = self._build_cache_path()

    
    def get(self) -> Dict[str, pl.LazyFrame]:
        """Return the combined weekday/weekend outputs for this wrapper.

        This method delegates execution to the underlying `PopulationTripsRun`
        assets, then exposes their cached parquet outputs through the legacy
        `PopulationTrips` mapping.

        Returns:
            dict[str, pl.LazyFrame]: Lazy readers for weekday outputs, weekend
            outputs, and shared `demand_groups`.
        """
        self.weekday_run.get()
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


    def remove(self, remove_checkpoints: bool = True):
        """Remove cached outputs for the underlying day-type run assets.

        Args:
            remove_checkpoints: When `True`, also remove saved checkpoints for
                the underlying weekday and weekend runs.
        """
        self.weekday_run.remove(remove_checkpoints=remove_checkpoints)
        self.weekend_run.remove(remove_checkpoints=remove_checkpoints)


    def validate_motives(self, motives: List[Motive]) -> None:
        """Validate the motives passed to the wrapper constructor.

        Args:
            motives: Motive objects that define the activity types available to
                the simulation.

        Raises:
            ValueError: If no motives are provided, or if `HomeMotive` or
                `OtherMotive` is missing.
            TypeError: If any element is not a `Motive` instance.
        """
        if not motives:
            raise ValueError("PopulationTrips needs at least one motive in `motives`.")

        for motive in motives:
            if not isinstance(motive, Motive):
                raise TypeError(
                    "PopulationTrips motives argument should be a list of `Motive` "
                    f"instances, but received one object of class {type(motive)}."
                )

        if not any(isinstance(m, OtherMotive) for m in motives):
            raise ValueError("PopulationTrips `motives` argument should contain a `OtherMotive`.")

        if not any(isinstance(m, HomeMotive) for m in motives):
            raise ValueError("PopulationTrips `motives` argument should contain a `HomeMotive`.")

    def validate_modes(self, modes: List[TransportMode]) -> None:
        """Validate the transport modes passed to the wrapper constructor.

        Args:
            modes: Transport modes available to the simulation.

        Raises:
            ValueError: If no modes are provided.
            TypeError: If any element is not a `TransportMode` instance.
        """
        if not modes:
            raise ValueError("PopulationTrips needs at least one mode in `modes`.")

        for mode in modes:
            if not isinstance(mode, TransportMode):
                raise TypeError(
                    "PopulationTrips modes argument should be a list of `TransportMode` "
                    f"instances, but received one object of class {type(mode)}."
                )

    def validate_surveys(self, surveys: List[MobilitySurvey]) -> None:
        """Validate the mobility surveys passed to the wrapper constructor.

        Args:
            surveys: Survey assets used to derive reference activity chains.

        Raises:
            ValueError: If no surveys are provided.
            TypeError: If any element is not a `MobilitySurvey` instance.
        """
        if not surveys:
            raise ValueError("PopulationTrips needs at least one survey in `surveys`.")

        for survey in surveys:
            if not isinstance(survey, MobilitySurvey):
                raise TypeError(
                    "PopulationTrips surveys argument should be a list of `MobilitySurvey` "
                    f"instances, but received one object of class {type(survey)}."
                )

    def _build_cache_path(self) -> Dict[str, str]:
        """Expose child run cache paths through the legacy wrapper keys.

        Returns:
            dict[str, pathlib.Path]: Mapping from legacy `PopulationTrips`
            output names to the underlying weekday and weekend run asset files.
        """
        weekday_paths = self.weekday_run.cache_path
        weekend_paths = self.weekend_run.cache_path
        keys = ("flows", "sinks", "costs", "chains", "transitions")
        cache_paths = {f"weekday_{key}": weekday_paths[key] for key in keys}
        cache_paths.update({f"weekend_{key}": weekend_paths[key] for key in keys})
        cache_paths["demand_groups"] = weekday_paths["demand_groups"]
        return cache_paths
