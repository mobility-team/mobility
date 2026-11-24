import pathlib
import shutil
import tempfile
import logging
import math
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import trace_simulation_step
from scipy.stats import norm

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.graph import asset_graph_key
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import asset_resolution_context, get_current_asset_resolver
from mobility.runtime.temporary import write_temporary_parquet
from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from .debug_logs import (
    log_destination_sequence_diagnostics,
    log_destination_spatialization_step,
    log_incomplete_destination_draws,
    log_missing_anchor_destination_samples,
    log_step_dropout_diagnostics,
)
from .stable_key_index import StableKeyIndex, add_ordered_key_hashes
from mobility.activities.activity import resolve_activity_parameters
from mobility.runtime.parameter_values import SensitivityCase
from mobility.trips.group_day_trips.core.progress import get_group_day_trips_progress
from .demand_subgroups import DEMAND_UNIT_COLS


class TravelOptionsCache:
    """Retain a limited amount of travel options during one inner search."""

    def __init__(self, max_bytes: int):
        self.max_bytes = max_bytes
        self.used_bytes = 0
        self.tables: dict[tuple, pl.DataFrame] = {}

    def get(self, key: tuple) -> pl.DataFrame | None:
        return self.tables.get(key)

    def retain(self, key: tuple, table: pl.DataFrame) -> None:
        if key in self.tables:
            return
        size = table.estimated_size()
        if self.used_bytes + size <= self.max_bytes:
            self.tables[key] = table
            self.used_bytes += size

    def clear(self) -> None:
        self.tables.clear()
        self.used_bytes = 0


class DestinationSequences(InMemoryAsset):
    """Build sequential destination alternatives shared by home-zone schedule."""

    OUTPUT_SCHEMA = {
        "home_zone_id": pl.Int32,
        "activity_seq_id": pl.UInt32,
        "time_seq_id": pl.UInt32,
        "dest_seq_id": pl.UInt32,
        "seq_step_index": pl.UInt8,
        "from": pl.UInt16,
        "to": pl.UInt16,
        "departure_time": pl.Float32,
        "arrival_time": pl.Float32,
        "next_departure_time": pl.Float32,
        "iteration": pl.UInt16,
    }
    OUTPUT_COLUMNS = list(OUTPUT_SCHEMA)
    REQUIRED_SCHEMA = {
        "home_zone_id": pl.Int32,
        "activity_seq_id": pl.UInt32,
        "time_seq_id": pl.UInt32,
        "dest_seq_id": pl.UInt32,
        "seq_step_index": pl.UInt8,
    }

    def __init__(
        self,
        *,
        is_weekday: bool,
        iteration: int,
        previous_state: Asset | None = None,
        seed_asset: Asset | None = None,
        activity_sequences: Asset | None = None,
        generation_requests: pl.DataFrame | None = None,
        activities: list[Any] | None = None,
        resolved_activity_parameters: dict[str, Any] | None = None,
        scenario: str | None = None,
        sensitivity_case: SensitivityCase | None = None,
        transport_zones: Any = None,
        transport_costs: Any = None,
        population_segments: list[Any] | None = None,
        current_plans: pl.DataFrame | None = None,
        current_plan_steps: pl.DataFrame | None = None,
        destination_saturation: pl.DataFrame | None = None,
        demand_groups: pl.DataFrame | None = None,
        costs: pl.DataFrame | None = None,
        parameters: Any = None,
        seed: int | None = None,
        sampling_probabilities: dict[int, pl.DataFrame] | None = None,
        travel_options_cache: TravelOptionsCache | None = None,
        travel_options_run_key: int | None = None,
    ) -> None:
        self.previous_state = previous_state
        self.previous_sequence_index: pl.DataFrame | None = None
        self.seed_asset = seed_asset
        self.activity_sequences = activity_sequences
        self.generation_requests = generation_requests
        self.activities = activities
        self.resolved_activity_parameters = (
            resolved_activity_parameters
            if resolved_activity_parameters is not None
            else (
                resolve_activity_parameters(
                    activities,
                    iteration,
                    scenario=scenario,
                    sensitivity_case=sensitivity_case,
                )
                if activities is not None
                else None
            )
        )
        self.scenario = scenario
        self.sensitivity_case = sensitivity_case
        self.transport_zones = transport_zones
        self.transport_costs = transport_costs
        self.population_segments = population_segments or []
        self.current_plans = current_plans
        self.current_plan_steps = current_plan_steps
        self.destination_saturation = destination_saturation
        self.demand_groups = demand_groups
        self.activity_dur: pl.DataFrame | None = None
        self.home_night_dur: pl.DataFrame | None = None
        self.costs = costs
        self.parameters = parameters
        self.seed = seed
        # Costs and opportunities stay fixed during one inner search.
        self.sampling_probabilities = sampling_probabilities
        self.travel_options_cache = travel_options_cache
        self.travel_options_run_key = travel_options_run_key
        self.spatialization_context_columns = ["home_zone_id"]
        generation_request_profile_pairs = (
            tuple(
                tuple(row)
                for row in generation_requests
                .select("home_zone_id", "utility_profile_id")
                .unique()
                .sort("home_zone_id", "utility_profile_id")
                .iter_rows()
            )
            if generation_requests is not None
            else None
        )
        inputs = {
            "version": (27 if parameters is not None and parameters.destination_sequences.sampling_method == "utility"
                        else 24),
            "is_weekday": is_weekday,
            "iteration": iteration,
            "sensitivity_case": sensitivity_case,
            "activity_sequences_asset": activity_sequences,
            "generation_request_profile_pairs": generation_request_profile_pairs,
            "previous_state": previous_state,
            "seed_asset": seed_asset,
            # Destination choice only needs the resolved activity/scenario values
            # and the current cost table. Run does not need to know these details.
            "resolved_activity_parameters": self.resolved_activity_parameters,
            "transport_zones": transport_zones,
            "transport_costs": transport_costs,
            "population_segments": self.population_segments,
            "destination_sequence_parameters": (
                parameters.destination_sequences if parameters is not None else None
            ),
            "plan_update_shadow_price_flag": (
                parameters.plan_update.use_destination_shadow_prices
                if parameters is not None
                else None
            ),
            "utility_sampling_parameters": (
                (parameters.plan_update.min_activity_time_constant,
                 parameters.plan_update.transition_logit_scale,
                 parameters.plan_update.update_plan_timings_from_modeled_travel_times)
                if parameters is not None and parameters.destination_sequences.sampling_method == "utility"
                else None
            ),
            "behavior_change_scope": (
                parameters.behavior_change.scope_at(iteration)
                if parameters is not None
                else None
            ),
            "seed": seed,
        }
        self.sequence_index: pl.DataFrame | None = None
        super().__init__(inputs)


    def get_index(self) -> pl.DataFrame:
        """Return the destination-sequence index built with this iteration."""
        self.get()
        if self.sequence_index is None:
            raise RuntimeError("Destination sequences have no sequence index.")
        return self.sequence_index


    def get(self, *args, **kwargs) -> pl.DataFrame:
        """Return destination sequences and spill their stable index together."""
        resolver = get_current_asset_resolver()
        if resolver is None:
            with asset_resolution_context() as resolver:
                return self._get_with_resolver(resolver, *args, **kwargs)
        return self._get_with_resolver(resolver, *args, **kwargs)


    def _get_with_resolver(self, resolver, *args, **kwargs) -> pl.DataFrame:
        sequences = resolver.get_in_memory_asset(
            self,
            lambda: self.create_and_get_asset(*args, **kwargs),
            spill_to_disk=self._spill_sequences,
            load_from_disk=self._load_spilled_sequences,
        )
        # Equivalent scenario assets can share one cached table in the resolver.
        # Their sequence index lives on the canonical asset instance, so copy it
        # onto this instance before callers ask for the matching index.
        canonical_asset = resolver.in_memory_assets.get(asset_graph_key(self))
        if canonical_asset is not None and canonical_asset is not self:
            self.sequence_index = canonical_asset.sequence_index
        return sequences


    def _spill_sequences(self, sequences: pl.DataFrame, path: pathlib.Path) -> None:
        """Spill sampled rows and their stable index together."""
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = pathlib.Path(tempfile.mkdtemp(prefix=f".{path.name}-", dir=path.parent))
        try:
            write_temporary_parquet(sequences, temporary_path / "sequences.parquet")
            if self.sequence_index is not None:
                write_temporary_parquet(self.sequence_index, temporary_path / "sequence_index.parquet")
            if path.exists():
                raise FileExistsError(f"A sequence spill already exists at {path}.")
            temporary_path.rename(path)
        finally:
            shutil.rmtree(temporary_path, ignore_errors=True)


    def _load_spilled_sequences(self, path: pathlib.Path) -> pl.DataFrame:
        """Restore sampled rows and their matching stable index."""
        index_path = path / "sequence_index.parquet"
        self.sequence_index = pl.read_parquet(index_path) if index_path.exists() else None
        return pl.read_parquet(path / "sequences.parquet")


    @trace_simulation_step("destination sampling")
    def create_and_get_asset(self) -> pl.DataFrame:
        """Compute destination sequences for one iteration."""
        try:
            get_group_day_trips_progress().iteration_step(self.iteration, "destination sequences")
            self._load_missing_runtime_inputs_from_previous_state()
            if self.seed is None and self.seed_asset is not None:
                self.seed = self.seed_asset.get()["destination_sequences"]

            scope = self.parameters.behavior_change.scope_at(self.iteration)
            return self._build_destination_sequences_for_scope(scope)
        finally:
            if self.previous_state is not None:
                self.current_plans = None
                self.current_plan_steps = None
                self.destination_saturation = None
                self.demand_groups = None
                self.activity_dur = None
                self.home_night_dur = None
                self.costs = None
                self.previous_sequence_index = None

    def _load_missing_runtime_inputs_from_previous_state(self) -> None:
        """Fill runtime dataframes from the cached state produced by the previous iteration."""
        if self.previous_state is None:
            return

        state = self.previous_state.get()
        self.previous_sequence_index = state.destination_sequence_index
        if self.current_plans is None:
            self.current_plans = state.current_plans
        if self.current_plan_steps is None:
            self.current_plan_steps = state.current_plan_steps
        if self.destination_saturation is None:
            self.destination_saturation = state.destination_saturation
        if self.demand_groups is None:
            self.demand_groups = state.demand_groups
        if self.parameters.destination_sequences.sampling_method == "utility":
            self.activity_dur = state.activity_dur
            self.home_night_dur = state.home_night_dur
        if self.costs is None and self.transport_costs is not None and self.demand_groups is None:
            # Profile-based sampling builds its own OD costs and never uses
            # this aggregate. Avoid rebuilding it in every inner batch.
            self.costs = self.transport_costs.get_costs_by_od(["cost", "distance"])
        elif self.costs is None and self.transport_costs is None:
            self.costs = state.costs

    def _build_destination_sequences_for_scope(self, scope: BehaviorChangeScope) -> pl.DataFrame:
        """Return the destination sequences to use for this iteration."""
        if scope == BehaviorChangeScope.FULL_REPLANNING:
            sampled_sequences = self._sample_all_destination_sequences()
            return self._with_current_active_destination_sequences(sampled_sequences)

        if scope == BehaviorChangeScope.DESTINATION_REPLANNING:
            sampled_sequences = self._sample_active_destination_sequences()
            return self._with_current_active_destination_sequences(sampled_sequences)

        if scope in (BehaviorChangeScope.MODE_REPLANNING, BehaviorChangeScope.NO_TRANSITIONS):
            reused_sequences = self._reuse_current_destination_sequences()
            self._cache_empty_destination_sequence_index()
            return reused_sequences

        raise ValueError(f"Unsupported behavior change scope: {scope}")

    def _sample_all_destination_sequences(self) -> pl.DataFrame:
        """Sample destination sequences from all non-stay-home activity sequences."""
        return self.run(
            self.activities,
            self.transport_zones,
            self.destination_saturation,
            self.activity_sequences.get(),
            self.costs,
            self.parameters,
            self.seed,
        )

    def _sample_active_destination_sequences(self) -> pl.DataFrame:
        """Sample destination sequences for currently active activity sequences only."""
        filtered_sequences = self.activity_sequences.get()
        if filtered_sequences.height == 0:
            self._cache_empty_destination_sequence_index()
            return self._empty_destination_sequences()

        return self.run(
            self.activities,
            self.transport_zones,
            self.destination_saturation,
            filtered_sequences,
            self.costs,
            self.parameters,
            self.seed,
        )

    def _reuse_current_destination_sequences(self) -> pl.DataFrame:
        """Reuse the current iteration's destination sequences without resampling."""
        active_dest_sequences = self._get_active_non_stay_home_plans().select(
            DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id"]
        ).unique()

        if active_dest_sequences.height == 0:
            return self._empty_destination_sequences()

        unit_home_zones = self.demand_groups.select(DEMAND_UNIT_COLS + ["home_zone_id"])
        active_dest_sequences = active_dest_sequences.join(
            unit_home_zones,
            on=DEMAND_UNIT_COLS,
        )
        current_plan_steps = self.current_plan_steps.lazy().join(
            unit_home_zones.lazy(),
            on=DEMAND_UNIT_COLS,
            how="inner",
        )
        active_dest_sequences = (
            active_dest_sequences
            .select(["home_zone_id", "activity_seq_id", "time_seq_id", "dest_seq_id"])
            .unique()
        )
        reuse_keys = ["home_zone_id", "activity_seq_id", "time_seq_id", "dest_seq_id"]

        reused = (
            current_plan_steps
            .join(
                active_dest_sequences.lazy(),
                on=reuse_keys,
                how="inner",
            )
            .with_columns(iteration=pl.lit(self.iteration).cast(pl.UInt16()))
            .select(
                reuse_keys
                + [
                    "seq_step_index",
                    "from",
                    "to",
                    "departure_time",
                    "arrival_time",
                    "next_departure_time",
                    "iteration",
                ]
            )
            .unique(
                subset=reuse_keys + ["seq_step_index"],
                keep="first",
            )
            .collect(engine="streaming")
        )

        if reused.height == 0:
            raise ValueError(
                "Active non-stay-home plans could not be matched to reusable "
                f"destination sequences at iteration={self.iteration}."
            )

        if self.generation_requests is not None:
            reused = reused.join(
                self.generation_requests.select("home_zone_id").unique(),
                on="home_zone_id",
                how="semi",
            )
        return reused

    def _with_current_active_destination_sequences(self, sampled_sequences: pl.DataFrame) -> pl.DataFrame:
        """Append current active destination sequences when the option is enabled."""
        if not self.parameters.destination_sequences.refresh_active_mode_alternatives:
            return sampled_sequences

        active_sequences = self._reuse_current_destination_sequences()
        if active_sequences.height == 0:
            return sampled_sequences.select(self.OUTPUT_COLUMNS)
        if sampled_sequences.height == 0:
            return active_sequences.select(self.OUTPUT_COLUMNS)

        return (
            pl.concat(
                [
                    active_sequences.select(self.OUTPUT_COLUMNS),
                    sampled_sequences.select(self.OUTPUT_COLUMNS),
                ],
                how="vertical_relaxed",
            )
            .unique(
                subset=[
                    "home_zone_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "dest_seq_id",
                    "seq_step_index",
                ],
                keep="first",
                maintain_order=True,
            )
        )

    def _get_active_non_stay_home_plans(self) -> pl.DataFrame:
        """Return the distinct currently active non-stay-home plans."""
        return (
            self.current_plans
            .filter(pl.col("time_seq_id") != 0)
            .select(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"])
            .unique()
        )

    def _empty_destination_sequences(self) -> pl.DataFrame:
        """Return an empty destination-sequences dataframe with the expected schema."""
        return pl.DataFrame(schema=self.OUTPUT_SCHEMA)

    def _cache_empty_destination_sequence_index(self) -> None:
        """Carry forward the destination index when no sequence is sampled."""
        empty_keys = pl.DataFrame(
            schema={
                "destination_sequence_key_1": pl.UInt64,
                "destination_sequence_key_2": pl.UInt64,
            }
        )
        _, self.sequence_index = StableKeyIndex(
            key_cols=["destination_sequence_key_1", "destination_sequence_key_2"],
            index_col="dest_seq_id",
            first_new_id=1,
        ).extend(
            empty_keys,
            previous_index=self.previous_sequence_index,
        )
    def run(
        self,
        activities: list[Any],
        transport_zones: Any,
        destination_saturation: pl.DataFrame,
        activity_sequences: pl.DataFrame,
        costs: pl.DataFrame | None,
        parameters: Any,
        seed: int,
    ) -> pl.DataFrame:
        """Draw destinations using the costs of each profile present in a zone."""
        activity_sequences = activity_sequences.filter(pl.col("activity_seq_id") != 0)
        if self.generation_requests is not None:
            activity_sequences = activity_sequences.join(
                self.generation_requests.select("home_zone_id").unique(),
                on="home_zone_id",
                how="semi",
            )
        if activity_sequences.is_empty():
            self._cache_empty_destination_sequence_index()
            return self._empty_destination_sequences()
        if self.transport_costs is None or self.demand_groups is None:
            if parameters.destination_sequences.sampling_method == "utility" and self.transport_costs is not None:
                mode_costs = self.transport_costs.get_costs_by_od_and_mode(
                    ["cost", "time"], detail_distances=False,
                )
                costs = costs.join(self._travel_options(mode_costs), on=["from", "to"], how="inner")
            return self._run_for_utility_profile(
                activities, transport_zones, destination_saturation,
                activity_sequences, costs, parameters, seed,
            )

        assignments, profiles = self.transport_costs.get_utility_profiles(
            self.demand_groups, self.population_segments,
        )
        profile_activity_durations = {}
        profile_home_durations = {}
        if parameters.destination_sequences.sampling_method == "utility" and self.activity_dur is not None:
            profile_population = (
                assignments.join(self.demand_groups.select(DEMAND_UNIT_COLS + ["country", "csp", "n_persons"]),
                                 on=DEMAND_UNIT_COLS)
                .group_by("utility_profile_id", "country", "csp")
                .agg(pl.col("n_persons").sort().implode().list.sum())
            )
            profile_means = (
                profile_population
                .join(self.activity_dur, on=["country", "csp"])
                .group_by("utility_profile_id", "activity")
                .agg(((pl.col("mean_duration_per_pers") * pl.col("n_persons")).sort().implode().list.sum()
                      / pl.col("n_persons").sort().implode().list.sum()).alias("mean_duration_per_pers"))
            )
            for profile, activity, duration in profile_means.iter_rows():
                profile_activity_durations.setdefault(profile, {})[str(activity)] = duration
            if self.home_night_dur is not None:
                home_means = (
                    profile_population.join(self.home_night_dur, on=["country", "csp"])
                    .group_by("utility_profile_id")
                    .agg(((pl.col("mean_home_night_per_pers") * pl.col("n_persons"))
                          .sort().implode().list.sum() / pl.col("n_persons").sort().implode().list.sum())
                         .alias("mean_home_night_per_pers"))
                )
                profile_home_durations = dict(home_means.iter_rows())
        zone_profiles = assignments.join(
            self.demand_groups.select(DEMAND_UNIT_COLS + ["home_zone_id"]),
            on=DEMAND_UNIT_COLS,
        ).select("home_zone_id", "utility_profile_id").unique()
        if self.generation_requests is not None:
            requested_profiles = self.generation_requests.select(
                "home_zone_id", "utility_profile_id"
            ).unique()
            zone_profiles = zone_profiles.join(
                requested_profiles,
                on=["home_zone_id", "utility_profile_id"],
                how="semi",
            )
        previous_index = self.previous_sequence_index
        sampled_profiles = []
        try:
            # Reuse the growing index between profiles so identical destination
            # chains share ids. Only one profile's cost table is held at a time.
            for profile_id in sorted(profiles):
                profile_sequences = activity_sequences.join(
                    zone_profiles.filter(pl.col("utility_profile_id") == profile_id)
                    .select("home_zone_id"),
                    on="home_zone_id", how="semi",
                )
                if profile_sequences.is_empty():
                    continue
                utility_sampling = parameters.destination_sequences.sampling_method == "utility"
                cache_key = None
                options = None
                if (utility_sampling and self.travel_options_cache is not None
                        and self.travel_options_run_key is not None
                        and isinstance(self.transport_costs, Asset)
                        and all(isinstance(asset, Asset) for asset in profiles[profile_id])):
                    # Profile IDs alone are not stable identities: include the
                    # scenario, its fixed cost asset and resolved mode costs.
                    cache_key = (
                        self.travel_options_run_key, asset_graph_key(self.transport_costs),
                        tuple(asset_graph_key(asset) for asset in profiles[profile_id]),
                    )
                    options = self.travel_options_cache.get(cache_key)
                mode_costs = None
                if options is None:
                    mode_costs = self.transport_costs.get_profile_costs_by_od_and_mode(
                        {profile_id: profiles[profile_id]}, ["cost", "time"] if utility_sampling else ["cost"],
                    )
                if utility_sampling:
                    # Keep each option's cost and time together, with mode as a
                    # stable tie-breaker. Both legs may use different modes.
                    if options is None:
                        options = self._travel_options(mode_costs)
                        if cache_key is not None:
                            self.travel_options_cache.retain(cache_key, options)
                    profile_costs = options.with_columns(cost=pl.col("cheapest_cost"))
                else:
                    # Match TransportCosts.get_costs_by_od's expected cost.
                    profile_costs = (
                        mode_costs.with_columns(
                            probability=(pl.col("cost").min().over("from", "to") - pl.col("cost")).exp()
                        )
                        .with_columns(
                            weighted_cost=pl.col("cost") * pl.col("probability")
                            / pl.col("probability").sum().over("from", "to")
                        )
                        .group_by("from", "to").agg(cost=pl.col("weighted_cost").sum())
                    )
                profile_seed = int(pl.Series([profile_id], dtype=pl.UInt32).hash(seed=seed)[0])
                sampled_profiles.append(self._run_for_utility_profile(
                    activities, transport_zones, destination_saturation,
                    profile_sequences, profile_costs, parameters, profile_seed,
                    profile_id=profile_id, mean_durations=profile_activity_durations.get(profile_id),
                    mean_home_duration=profile_home_durations.get(profile_id),
                ))
                self.previous_sequence_index = self.sequence_index
                del mode_costs, profile_costs
            if not sampled_profiles:
                self._cache_empty_destination_sequence_index()
                return self._empty_destination_sequences()
            # Candidate generation shares complete destinations across compatible
            # demand units. Profiles control the draws, not subsequent eligibility.
            return pl.concat(sampled_profiles, how="vertical_relaxed").unique().sort(
                ["home_zone_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "seq_step_index"]
            )
        finally:
            self.previous_sequence_index = previous_index

    @staticmethod
    def _travel_options(mode_costs: pl.DataFrame) -> pl.DataFrame:
        """Keep matching cost and time for the cheapest and fastest modes."""
        valid_time = pl.col("time").is_finite().fill_null(False)
        unknown_times = (
            mode_costs.filter(pl.col("cost").is_finite() & ~valid_time)
            .select("from", "to").unique()
        )
        options = (
            mode_costs.filter(pl.col("cost").is_finite() & valid_time)
            .group_by("from", "to")
            .agg(
                cheapest_cost=pl.col("cost").sort_by(["cost", "time", "mode"]).first(),
                cheapest_time=pl.col("time").sort_by(["cost", "time", "mode"]).first(),
                fastest_cost=pl.col("cost").sort_by(["time", "cost", "mode"]).first(),
                fastest_time=pl.col("time").sort_by(["time", "cost", "mode"]).first(),
            )
        )
        if unknown_times.is_empty():
            return options.with_columns(has_unknown_time=pl.lit(False))
        return options.join(
            unknown_times.with_columns(has_unknown_time=pl.lit(True)), on=["from", "to"], how="left"
        ).with_columns(has_unknown_time=pl.col("has_unknown_time").fill_null(False))

    def _sampling_rigidity(self) -> dict[str, float]:
        anchors = {activity.name: getattr(activity, "is_anchor", False) for activity in self.activities or []}
        rigidity = {}
        for name, parameters in self.resolved_activity_parameters.items():
            value = getattr(parameters, "activity_time_rigidity", None)
            rigidity[name] = float(value if value is not None else anchors.get(name, False))
        return rigidity

    @trace_simulation_step("destination sampling per profile")
    def _run_for_utility_profile(
        self,
        activities: list[Any],
        transport_zones: Any,
        destination_saturation: pl.DataFrame,
        activity_sequences: pl.DataFrame,
        costs: pl.DataFrame,
        parameters: Any,
        seed: int,
        *, profile_id: int | None = None, mean_durations: dict[str, float] | None = None,
        mean_home_duration: float | None = None,
    ) -> pl.DataFrame:
        """Compute destination sequences for one iteration."""
        activity_sequences = activity_sequences.filter(pl.col("activity_seq_id") != 0)
        if activity_sequences.height == 0:
            self._cache_empty_destination_sequence_index()
            return self._empty_destination_sequences()
        activity_sequences = activity_sequences.with_columns(
            pl.col("activity_draw_id")
            if "activity_draw_id" in activity_sequences.columns
            else pl.lit(0, dtype=pl.UInt32).alias("activity_draw_id")
        ).select(
            [
                "home_zone_id",
                "activity_draw_id",
                "activity_seq_id",
                "time_seq_id",
                "activity",
                "is_anchor",
                "seq_step_index",
                "step_count",
                "departure_time",
                "arrival_time",
                "next_departure_time",
            ]
        )
        source_activity_sequences = activity_sequences
        destination_probability = (
            self.sampling_probabilities.get(profile_id)
            if self.sampling_probabilities is not None and profile_id is not None else None
        )
        if destination_probability is None:
            if parameters.destination_sequences.sampling_method == "utility":
                destination_probability = self._get_utility_destination_opportunities(
                    destination_saturation, costs, parameters.destination_sequences.dest_prob_cutoff,
                )
            else:
                utility_inputs = self._get_destination_probability_inputs(
                    destination_saturation, costs, parameters.destination_sequences.cost_uncertainty_sd,
                )
                destination_probability = self._get_destination_probability(
                    utility_inputs, activities, self.resolved_activity_parameters,
                    parameters.destination_sequences.dest_prob_cutoff,
                )
            if self.sampling_probabilities is not None and profile_id is not None:
                self.sampling_probabilities[profile_id] = destination_probability
        cost_views = self._spatialization_cost_views(costs)
        anchor_spatialized_sequences = self._spatialize_anchor_activities(
            source_activity_sequences,
            destination_probability,
            parameters.destination_sequences.alpha,
            seed,
            cost_views,
            mean_durations=mean_durations,
            mean_home_duration=mean_home_duration,
        )
        spatialized_activity_sequences = self._spatialize_other_activities(
            anchor_spatialized_sequences,
            destination_probability,
            costs,
            parameters.destination_sequences.alpha,
            seed,
            cost_views,
            mean_durations=mean_durations,
            mean_home_duration=mean_home_duration,
        )
        complete_activity_sequences = self._drop_incomplete_destination_draws(
            activity_sequences=spatialized_activity_sequences,
            iteration=self.iteration,
            sequence_context_columns=self.spatialization_context_columns,
        )

        destination_sequences = add_ordered_key_hashes(
            complete_activity_sequences,
            group_cols=self.spatialization_context_columns
            + ["activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id"],
            order_col="seq_step_index",
            step_content=pl.struct(pl.col("to")),
            key_prefix="destination_sequence_key",
        )
        destination_sequences = (
            destination_sequences
            .group_by(
                self.spatialization_context_columns
                + [
                    "activity_seq_id",
                    "time_seq_id",
                    "destination_sequence_key_1",
                    "destination_sequence_key_2",
                ]
            )
            .agg(
                activity_draw_id=pl.col("activity_draw_id").first(),
                dest_draw_id=pl.col("dest_draw_id").first(),
            )
            .sort(
                self.spatialization_context_columns
                + ["activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id"]
            )
        )
        destination_sequences, self.sequence_index = StableKeyIndex(
            key_cols=["destination_sequence_key_1", "destination_sequence_key_2"],
            index_col="dest_seq_id",
            first_new_id=1,
        ).extend(
            destination_sequences,
            previous_index=self.previous_sequence_index,
        )
        destination_sequences = (
            complete_activity_sequences
            .join(
                destination_sequences.select(
                    self.spatialization_context_columns
                    + ["activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "dest_seq_id"]
                ),
                on=self.spatialization_context_columns
                + ["activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id"],
            )
            .drop(["activity", "activity_draw_id", "dest_draw_id"])
            .with_columns(
                pl.col("seq_step_index").cast(pl.UInt8),
                pl.col("from").cast(pl.UInt16),
                pl.col("to").cast(pl.UInt16),
                pl.col("departure_time").cast(pl.Float32),
                pl.col("arrival_time").cast(pl.Float32),
                pl.col("next_departure_time").cast(pl.Float32),
                pl.lit(self.iteration).cast(pl.UInt16).alias("iteration"),
            )
        )
        log_destination_sequence_diagnostics(
            iteration=self.iteration,
            source_activity_sequences=source_activity_sequences,
            destination_sequences=destination_sequences,
        )
        return destination_sequences.select(self.OUTPUT_COLUMNS)

    def _get_utility_destination_opportunities(
        self, opportunities: pl.DataFrame, costs: pl.DataFrame, cutoff: float,
    ) -> pl.DataFrame:
        """Keep promising OD destinations using a cheap first-leg approximation.

        The final sampling weights also depend on the schedule and onward leg.
        This preliminary cutoff bounds the size of those two-leg joins.
        """
        if "cheapest_cost" not in costs.columns:
            raise ValueError("Utility destination sampling requires per-mode travel cost and time.")
        use_shadow_prices = self.parameters.plan_update.use_destination_shadow_prices
        if use_shadow_prices and "destination_sampling_attraction_factor" in opportunities.columns:
            factor = pl.col("destination_sampling_attraction_factor").fill_null(1.0)
        elif "k_saturation_utility" in opportunities.columns:
            factor = pl.col("k_saturation_utility").fill_null(1.0)
        else:
            factor = pl.lit(1.0)
        saturation = (
            pl.col("k_saturation_utility").fill_null(1.0)
            if not use_shadow_prices and "k_saturation_utility" in opportunities.columns else pl.lit(1.0)
        )
        shadow_price = (
            pl.col("destination_shadow_price").fill_null(0.0)
            if use_shadow_prices and "destination_shadow_price" in opportunities.columns else pl.lit(0.0)
        )
        country_values = [
            {"activity": name, "destination_country": country, "country_value_coefficient": value}
            for name, parameters in self.resolved_activity_parameters.items()
            for country, value in (getattr(parameters, "country_value_coefficients", None) or {}).items()
        ]
        if country_values and self.transport_zones is not None:
            zones = pl.from_pandas(
                self.transport_zones.get().drop("geometry", axis=1, errors="ignore")[["transport_zone_id", "country"]]
            ).select(pl.col("transport_zone_id").cast(opportunities.schema["to"]).alias("to"),
                     pl.col("country").cast(pl.String).alias("destination_country"))
            coefficients = pl.from_dicts(country_values).with_columns(
                pl.col("activity").cast(opportunities.schema["activity"]),
            )
            opportunities = (opportunities.join(zones, on="to", how="left")
                             .join(coefficients, on=["activity", "destination_country"], how="left"))
        else:
            opportunities = opportunities.with_columns(country_value_coefficient=pl.lit(1.0))
        return (
            costs.lazy().select("from", "to", "cheapest_cost")
            .join(opportunities.lazy().with_columns(
                attraction=(pl.col("opportunity_capacity") * factor).clip(0.0),
                activity_saturation=saturation,
                activity_shadow_price=shadow_price,
                country_value_coefficient=pl.col("country_value_coefficient").fill_null(1.0),
            ).select("to", "activity", "attraction", "activity_saturation", "activity_shadow_price",
                     "country_value_coefficient"), on="to")
            .filter((pl.col("attraction") > 0) & pl.col("cheapest_cost").is_finite())
            .with_columns(_score=pl.col("attraction").log() - pl.col("cheapest_cost"))
            .with_columns(_weight=(pl.col("_score") - pl.col("_score").max().over("from", "activity")).exp())
            .with_columns(_weight=pl.col("_weight") / pl.col("_weight").sum().over("from", "activity"))
            .sort(["from", "activity", "_weight", "to"], descending=[False, False, True, False])
            .with_columns(_before=pl.col("_weight").cum_sum().over("from", "activity") - pl.col("_weight"))
            .filter(pl.col("_before") < cutoff)
            .select("from", "activity", "to", pl.col("attraction").alias("p_ij"),
                    "activity_saturation", "activity_shadow_price", "country_value_coefficient")
            .collect(engine="streaming")
        )

    def _get_destination_probability_inputs(
        self,
        opportunities: pl.DataFrame,
        costs: pl.DataFrame,
        cost_uncertainty_sd: float,
    ) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """Assemble radiation-model inputs from travel costs and destination sinks."""
        x = [-2.0, -1.0, 0.0, 1.0, 2.0]
        probabilities = norm.pdf(x, loc=0.0, scale=cost_uncertainty_sd)
        probabilities /= probabilities.sum()
        plan_update_parameters = getattr(self.parameters, "plan_update", None)
        use_shadow_prices = bool(
            getattr(plan_update_parameters, "use_destination_shadow_prices", False)
        )
        opportunities_columns = set(opportunities.columns)
        if (
            use_shadow_prices
            and "destination_sampling_attraction_factor" in opportunities_columns
        ):
            sink_factor = pl.col("destination_sampling_attraction_factor").fill_null(1.0)
        elif "k_saturation_utility" in opportunities_columns:
            sink_factor = pl.col("k_saturation_utility").fill_null(1.0)
        else:
            sink_factor = pl.lit(1.0)

        uncertainty_offsets = pl.DataFrame(
            {
                "cost_delta": x,
                "prob": probabilities.tolist(),
            },
            schema={
                "cost_delta": pl.Float64,
                "prob": pl.Float64,
            },
        )
        costs = (
            costs.lazy()
            .join(uncertainty_offsets.lazy(), how="cross")
            .with_columns(cost=pl.col("cost") + pl.col("cost_delta"))
            .drop("cost_delta")
            .join(opportunities.lazy(), on="to")
            .with_columns(
                effective_sink=(
                    pl.col("opportunity_capacity")
                    * sink_factor
                    * pl.col("prob")
                ).clip(0.0),
            )
            .drop("prob")
            .filter(pl.col("effective_sink") > 0.0)
            .with_columns(cost_bin=pl.col("cost").floor())
        )
        cost_bin_to_destination = (
            costs
            .with_columns(
                p_to=pl.col("effective_sink") / pl.col("effective_sink").sum().over(["from", "activity", "cost_bin"])
            )
            .select(["activity", "from", "cost_bin", "to", "p_to"])
        )
        costs_by_bin = (
            costs
            .group_by(["from", "activity", "cost_bin"])
            .agg(pl.col("effective_sink").sum())
            .sort(["from", "activity", "cost_bin"])
        )
        return costs_by_bin, cost_bin_to_destination


    def _get_destination_probability(
        self,
        destination_probability_inputs: tuple[pl.LazyFrame, pl.LazyFrame],
        activities: list[Any],
        resolved_activity_parameters: dict[str, Any] | None,
        destination_probability_cutoff: float,
    ) -> pl.DataFrame:
        """Compute destination probabilities from utilities."""
        logging.debug(
            "Computing the probability of choosing a destination based on current location, potential destinations, and activity (with radiation models)..."
        )
        costs_by_bin = destination_probability_inputs[0]
        cost_bin_to_destination = destination_probability_inputs[1]
        activities_lambda = {
            activity.name: resolved_activity_parameters[activity.name].radiation_lambda
            for activity in activities
        }
        return (
            costs_by_bin
            .with_columns(
                s_ij=pl.col("effective_sink").cum_sum().over(["from", "activity"]),
                selection_lambda=pl.col("activity").cast(pl.Utf8).replace_strict(activities_lambda),
            )
            .with_columns(
                p_a=(1 - pl.col("selection_lambda") ** (1 + pl.col("s_ij")))
                / (1 + pl.col("s_ij"))
                / (1 - pl.col("selection_lambda"))
            )
            .with_columns(
                p_a_lag=pl.col("p_a").shift(fill_value=1.0).over(["from", "activity"]).alias("p_a_lag")
            )
            .with_columns(p_ij=pl.col("p_a_lag") - pl.col("p_a"))
            .with_columns(p_ij=pl.col("p_ij") / pl.col("p_ij").sum().over(["from", "activity"]))
            .filter(pl.col("p_ij") > 0.0)
            .with_columns(p_ij=pl.col("p_ij").round(9))
            .sort(["from", "activity", "p_ij", "cost_bin"], descending=[False, False, True, False])
            .with_columns(
                p_ij_cum=pl.col("p_ij").cum_sum().over(["from", "activity"]),
                p_count=pl.col("p_ij").cum_count().over(["from", "activity"]),
            )
            .filter((pl.col("p_ij_cum") < destination_probability_cutoff) | (pl.col("p_count") == 1))
            .with_columns(p_ij=pl.col("p_ij") / pl.col("p_ij").sum().over(["from", "activity"]))
            .join(cost_bin_to_destination, on=["activity", "from", "cost_bin"])
            .with_columns(p_ij=pl.col("p_ij") * pl.col("p_to"))
            .group_by(["activity", "from", "to"])
            .agg(pl.col("p_ij").sum())
            .with_columns(p_ij=pl.col("p_ij").round(9))
            .sort(["from", "activity", "p_ij", "to"], descending=[False, False, True, False])
            .with_columns(
                p_ij_cum=pl.col("p_ij").cum_sum().over(["from", "activity"]),
                p_count=pl.col("p_ij").cum_count().over(["from", "activity"]),
            )
            .filter((pl.col("p_ij_cum") < destination_probability_cutoff) | (pl.col("p_count") == 1))
            .with_columns(p_ij=pl.col("p_ij") / pl.col("p_ij").sum().over(["from", "activity"]))
            .select(["activity", "from", "to", "p_ij"])
            .collect(engine="streaming")
        )


    @staticmethod
    def _spatialization_cost_views(costs: pl.DataFrame) -> dict[str, pl.LazyFrame]:
        """Build the cost projections reused by each destination spatialization step."""
        if "cheapest_cost" in costs.columns and "has_unknown_time" not in costs.columns:
            # A caller supplying its own options has not established whether
            # other modes have unknown times, so do not use them to exclude stops.
            costs = costs.with_columns(has_unknown_time=pl.lit(True))
        costs_lf = costs.lazy()
        options = (["cheapest_cost", "cheapest_time", "fastest_cost", "fastest_time", "has_unknown_time"]
                   if "cheapest_cost" in costs.columns else [])
        views = {
            "to_candidate": costs_lf.select(
                [
                    pl.col("from").alias("candidate_from"),
                    pl.col("to").alias("candidate_to"),
                    pl.col("cost").alias("cost_to_candidate"),
                    *[pl.col(column).alias(f"candidate_{column}") for column in options],
                ]
            ),
            "to_anchor": costs_lf.select(
                [
                    pl.col("from").alias("anchor_from"),
                    pl.col("to").alias("anchor_to_cost"),
                    pl.col("cost").alias("cost_to_anchor"),
                    *[pl.col(column).alias(f"onward_{column}") for column in options],
                ]
            ),
            "to_home": costs_lf.select(
                [
                    pl.col("from").alias("home_return_from"),
                    pl.col("to").alias("home_return_to"),
                    pl.col("cost").alias("cost_to_home"),
                    *[pl.col(column).alias(f"onward_{column}") for column in options],
                ]
            ),
            "anchor_leg_pairs": costs_lf.select(
                [
                    pl.col("from").alias("anchor_leg_from"),
                    pl.col("to").alias("anchor_leg_to"),
                ]
            ).unique(),
        }
        return views


    @staticmethod
    def _filter_impossible_timing(
        choices: pl.LazyFrame, activity_rigidity: dict[str, float], update_timings: bool,
    ) -> pl.LazyFrame:
        """Drop a stop only when even its fastest two legs cannot fit."""
        rigidity = pl.col("activity").cast(pl.String).replace_strict(activity_rigidity, default=0.0)
        previous_rigidity = pl.col("_previous_activity").cast(pl.String).replace_strict(activity_rigidity, default=0.0)
        next_rigidity = pl.col("_next_activity").cast(pl.String).replace_strict(activity_rigidity, default=0.0)
        incoming_share = (1.0 + rigidity - previous_rigidity) / 2.0
        onward_share = (1.0 + next_rigidity - rigidity) / 2.0
        incoming_reference = (pl.col("arrival_time") - pl.col("departure_time")).clip(0.0, 24.0)
        onward_reference = (pl.col("_next_arrival") - pl.col("_next_departure")).clip(0.0, 24.0)
        earliest_arrival = pl.col("arrival_time") + float(update_timings) * (1.0 - incoming_share) * (
            pl.col("candidate_fastest_time") - incoming_reference
        )
        latest_departure = pl.col("_next_departure") - float(update_timings) * onward_share * (
            pl.col("onward_fastest_time") - onward_reference
        )
        # Final modeled schedules are rounded to seconds. Keep a generous
        # two-second margin for that rounding and Float32 time calculations.
        return choices.filter(
            ~pl.col("_next_is_direct")
            | pl.col("candidate_has_unknown_time") | pl.col("onward_has_unknown_time")
            | (latest_departure - earliest_arrival >= -2.0 / 3600.0)
        )

    @staticmethod
    def _activity_utility_delta(
        reference_duration: pl.Expr, modeled_duration: pl.Expr, typical_duration: pl.Expr,
        value: pl.Expr, min_activity_time_constant: float,
    ) -> pl.Expr:
        """Estimate the change in the same duration utility used for plans."""
        minimum = typical_duration * math.exp(-min_activity_time_constant)
        return value * typical_duration * (
            (modeled_duration / minimum).log().clip(0.0)
            - (reference_duration / minimum).log().clip(0.0)
        )

    @staticmethod
    def _sample_sequence_aware_destinations(
        *,
        steps_lf: pl.LazyFrame,
        destination_probability_lf: pl.LazyFrame,
        origin_cost_lf: pl.LazyFrame,
        onward_cost_lf: pl.LazyFrame,
        onward_left_on: list[str],
        onward_right_on: list[str],
        onward_cost_col: str,
        alpha: float,
        sequence_context_columns: list[str],
        seed: int,
        output_cols: list[str],
        utility_values: dict[str, float] | None = None,
        min_activity_time_constant: float = 0.0,
        logit_scale: float = 1.0,
        mean_durations: dict[str, float] | None = None,
        activity_rigidity: dict[str, float] | None = None,
        use_next_timing: bool = False,
        update_timings: bool = True,
        filter_infeasible: bool = False,
        use_previous_timing: bool = False,
        anchor_sampling: bool = False,
        mean_home_duration: float | None = None,
        home_value: float = 0.0,
        model_previous_arrival: bool = False,
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """Sample destinations while checking both legs around the candidate zone."""
        if utility_values is not None:
            schema = steps_lf.collect_schema()
            missing = [name for name in ("_previous_activity", "_previous_previous_activity") if name not in schema]
            if missing:
                steps_lf = steps_lf.with_columns(*[pl.lit("home").alias(name) for name in missing])
        active_probability = destination_probability_lf.join(
            steps_lf.select(["from", "activity"]).unique(),
            on=["from", "activity"],
            how="semi",
        )
        candidates = steps_lf.join(active_probability, on=["from", "activity"])
        candidates_with_origin_costs = candidates.join(
            origin_cost_lf,
            left_on=["from", "to"],
            right_on=["candidate_from", "candidate_to"],
        )
        candidates_with_costs = candidates_with_origin_costs.join(
            onward_cost_lf,
            left_on=onward_left_on,
            right_on=onward_right_on,
        )
        # Many schedule draws have the same origin, activity and onward zone.
        # Compute their choice weights once, then run a few draws as columns so
        # candidate rows do not have to be repeated for every draw.
        context_columns = list(dict.fromkeys(
            ["from", "activity"] + [column for column in onward_left_on if column != "to"]
        ))
        if utility_values is not None:
            context_columns += ["departure_time", "arrival_time", "next_departure_time", "_previous_activity"]
            if use_next_timing:
                context_columns += ["_next_activity", "_next_departure", "_next_arrival",
                                    "_next_next_departure", "_next_is_direct"]
            if use_previous_timing:
                context_columns += ["_previous_activity", "_previous_arrival", "_previous_is_anchor",
                                    "_previous_is_first", "_home_night_reference"]
                if model_previous_arrival:
                    context_columns += ["_previous_departure", "_previous_origin", "_previous_origin_known",
                                        "_previous_previous_activity"]
        context_columns = list(dict.fromkeys(context_columns))
        choice_contexts = steps_lf.select(context_columns).unique()
        if utility_values is not None:
            # These values depend on the schedule, not the destination. Build
            # them before each context is joined to its destination options.
            reference_duration = (pl.col("next_departure_time") - pl.col("arrival_time")).clip(0.001)
            choice_contexts = choice_contexts.with_columns(
                _sampling_typical_duration=(
                    pl.col("activity").cast(pl.String).replace_strict(mean_durations, default=None)
                    .fill_null(reference_duration) if mean_durations else reference_duration
                ).clip(0.001),
                _sampling_activity_value=pl.col("activity").cast(pl.String)
                .replace_strict(utility_values, default=0.0),
                _sampling_rigidity=(
                    pl.col("activity").cast(pl.String).replace_strict(activity_rigidity, default=0.0)
                    if activity_rigidity else pl.lit(0.0)
                ),
                _sampling_incoming_reference=(pl.col("arrival_time") - pl.col("departure_time")).clip(0.0, 24.0),
            )
        weighted_choices = (
            choice_contexts.join(destination_probability_lf, on=["from", "activity"])
            .join(
                origin_cost_lf,
                left_on=["from", "to"],
                right_on=["candidate_from", "candidate_to"],
            )
            .join(
                onward_cost_lf,
                left_on=onward_left_on,
                right_on=onward_right_on,
            )
        )
        if filter_infeasible and utility_values is not None and use_next_timing:
            # The previous location and the next anchor are actual neighbouring
            # stops only for non-anchor activities immediately before an anchor.
            # The fastest mode on each leg gives the widest possible activity
            # window, even if those two modes cannot form a usable chain.
            weighted_choices = DestinationSequences._filter_impossible_timing(
                weighted_choices, activity_rigidity or {}, update_timings,
            )
        if utility_values is None:
            weighted_choices = weighted_choices.with_columns(
                p_ij=(pl.col("p_ij").clip(1e-18).log() - alpha * (
                    pl.col("cost_to_candidate") + pl.col(onward_cost_col)
                )).exp(),
            )
        else:
            if use_previous_timing:
                if model_previous_arrival:
                    preceding_costs = origin_cost_lf.select(
                        pl.col("candidate_from").alias("_previous_origin"),
                        pl.col("candidate_to").alias("from"),
                        *[pl.col(f"candidate_{name}").alias(f"_preceding_{name}") for name in
                          ("fastest_time", "fastest_cost", "cheapest_time", "cheapest_cost", "has_unknown_time")],
                    )
                    weighted_choices = weighted_choices.join(
                        preceding_costs, on=["_previous_origin", "from"], how="left",
                    )
                previous_values = destination_probability_lf.select(
                    pl.col("to").alias("from"), pl.col("activity").alias("_previous_activity"),
                    pl.col("activity_saturation").alias("_previous_saturation"),
                    pl.col("activity_shadow_price").alias("_previous_shadow_price"),
                    pl.col("country_value_coefficient").alias("_previous_country_value"),
                )
                previous_values = previous_values.unique(subset=["from", "_previous_activity"])
                weighted_choices = weighted_choices.join(
                    previous_values, on=["from", "_previous_activity"], how="left",
                ).with_columns(
                    pl.col("_previous_saturation", "_previous_country_value").fill_null(1.0),
                    pl.col("_previous_shadow_price").fill_null(0.0),
                )
            if use_next_timing and "anchor_to" in context_columns:
                anchor_values = destination_probability_lf.select(
                    pl.col("to").alias("anchor_to"), pl.col("activity").alias("_next_activity"),
                    pl.col("activity_saturation").alias("_next_saturation"),
                    pl.col("activity_shadow_price").alias("_next_shadow_price"),
                    pl.col("country_value_coefficient").alias("_next_country_value"),
                )
                anchor_values = anchor_values.unique(subset=["anchor_to", "_next_activity"])
                weighted_choices = weighted_choices.join(
                    anchor_values, on=["anchor_to", "_next_activity"], how="left",
                ).with_columns(
                    pl.col("_next_saturation", "_next_country_value").fill_null(1.0),
                    pl.col("_next_shadow_price").fill_null(0.0),
                )
            else:
                weighted_choices = weighted_choices.with_columns(
                    _next_saturation=pl.lit(1.0), _next_country_value=pl.lit(1.0), _next_shadow_price=pl.lit(0.0),
                )
            # Use the reference activity duration as a proxy for its typical
            # duration; exact population-specific utility is scored later.
            typical_duration = pl.col("_sampling_typical_duration")
            minimum_duration = typical_duration * math.exp(-min_activity_time_constant)
            activity_value = pl.col("_sampling_activity_value")
            rigidity = pl.col("_sampling_rigidity")
            previous_rigidity = pl.col("_previous_activity").cast(pl.String).replace_strict(activity_rigidity or {}, default=0.0)
            incoming_share = (1.0 + rigidity - previous_rigidity) / 2.0
            incoming_reference = pl.col("_sampling_incoming_reference")
            alternatives = []
            for incoming in ("fastest", "cheapest"):
                incoming_delta = pl.col(f"candidate_{incoming}_time") - incoming_reference
                arrival = (pl.col("arrival_time") + (1.0 - incoming_share) * (
                    incoming_delta
                )) if update_timings else pl.col("arrival_time")
                previous_delta = pl.lit(0.0)
                if use_previous_timing:
                    previous_reference = (pl.col("departure_time") - pl.col("_previous_arrival")).clip(0.001)
                    previous_typical = (
                        pl.col("_previous_activity").cast(pl.String)
                        .replace_strict(mean_durations, default=None).fill_null(previous_reference)
                        if mean_durations else previous_reference
                    ).clip(0.001)
                    previous_duration = (previous_reference - float(update_timings) * incoming_share * incoming_delta).clip(0.001)
                    previous_value = (pl.col("_previous_activity").cast(pl.String)
                                      .replace_strict(utility_values, default=0.0))
                    previous_location_known = (
                        pl.col("_previous_is_anchor") if anchor_sampling and not model_previous_arrival
                        else pl.lit(True)
                    )
                    previous_value = (
                        previous_value * pl.when(previous_location_known)
                        .then(pl.col("_previous_country_value") * pl.col("_previous_saturation")).otherwise(1.0)
                        + pl.when(previous_location_known).then(pl.col("_previous_shadow_price")).otherwise(0.0)
                    )
                    previous_delta = DestinationSequences._activity_utility_delta(
                        previous_reference, previous_duration, previous_typical, previous_value,
                        min_activity_time_constant,
                    )
                    if model_previous_arrival:
                        before_previous_rigidity = pl.col("_previous_previous_activity").cast(pl.String).replace_strict(
                            activity_rigidity or {}, default=0.0,
                        )
                        preceding_share = (1.0 + previous_rigidity - before_previous_rigidity) / 2.0
                        previous_travel_reference = (
                            pl.col("_previous_arrival") - pl.col("_previous_departure")
                        ).clip(0.0, 24.0)
                        modeled_previous = []
                        for preceding in ("fastest", "cheapest"):
                            preceding_time = pl.col(f"_preceding_{preceding}_time")
                            preceding_arrival = pl.col("_previous_arrival") + (1.0 - preceding_share) * (
                                preceding_time - previous_travel_reference
                            )
                            modeled_duration = (
                                pl.col("departure_time") - incoming_share * incoming_delta - preceding_arrival
                            ).clip(0.001)
                            modeled_previous.append(
                                DestinationSequences._activity_utility_delta(
                                    previous_reference, modeled_duration, previous_typical, previous_value,
                                    min_activity_time_constant,
                                ) - pl.col(f"_preceding_{preceding}_cost")
                            )
                        previous_delta = pl.when(
                            pl.col("_previous_origin_known") & pl.lit(update_timings)
                            & ~pl.col("_preceding_has_unknown_time").fill_null(True)
                            & pl.col("_preceding_fastest_time").is_not_null()
                        ).then(pl.max_horizontal(modeled_previous)).otherwise(previous_delta)
                    home_reference = pl.col("_home_night_reference").clip(0.001)
                    home_typical = pl.lit(mean_home_duration) if mean_home_duration is not None else home_reference
                    home_typical = home_typical.clip(0.001)
                    home_duration = (home_reference - float(update_timings) * incoming_share * incoming_delta).clip(0.001)
                    home_delta = DestinationSequences._activity_utility_delta(
                        home_reference, home_duration, home_typical, pl.lit(home_value),
                        min_activity_time_constant,
                    )
                    previous_delta = pl.when(pl.col("_previous_is_first")).then(home_delta).otherwise(previous_delta)
                for onward in (("fastest", "cheapest") if use_next_timing else ("cheapest",)):
                    if use_next_timing:
                        next_rigidity = (pl.col("_next_activity").cast(pl.String)
                                         .replace_strict(activity_rigidity or {}, default=0.0))
                        onward_share = (1.0 + next_rigidity - rigidity) / 2.0
                        next_reference = (pl.col("_next_arrival") - pl.col("_next_departure")).clip(0.0, 24.0)
                        departure = pl.when(pl.col("_next_is_direct") & pl.lit(update_timings)).then(
                            pl.col("_next_departure") - onward_share * (
                                pl.col(f"onward_{onward}_time") - next_reference
                            )
                        ).otherwise(pl.col("next_departure_time"))
                        next_arrival = pl.col("_next_arrival") + float(update_timings) * (1.0 - onward_share) * (
                            pl.col(f"onward_{onward}_time") - next_reference
                        )
                        next_reference_duration = (pl.col("_next_next_departure") - pl.col("_next_arrival")).clip(0.001)
                        next_typical = (
                            pl.col("_next_activity").cast(pl.String)
                            .replace_strict(mean_durations, default=None).fill_null(next_reference_duration)
                            if mean_durations else next_reference_duration
                        ).clip(0.001)
                        next_value = pl.col("_next_activity").cast(pl.String).replace_strict(utility_values, default=0.0)
                        next_duration = (pl.col("_next_next_departure") - next_arrival).clip(0.001)
                        next_utility = pl.when(pl.col("_next_is_direct") & (pl.col("_next_activity") != "home"))
                        next_utility = next_utility.then(
                            (next_value * pl.col("_next_country_value") * pl.col("_next_saturation")
                             + pl.col("_next_shadow_price")) * next_typical
                            * (next_duration / (next_typical * math.exp(-min_activity_time_constant))).log().clip(0.0)
                        ).otherwise(0.0)
                    else:
                        departure = pl.col("next_departure_time")
                        next_utility = pl.lit(0.0)
                    duration = (departure - arrival).clip(0.001)
                    alternatives.append(
                        (activity_value * pl.col("country_value_coefficient") * pl.col("activity_saturation")
                         + pl.col("activity_shadow_price"))
                        * typical_duration * (duration / minimum_duration).log().clip(0.0)
                        - pl.col(f"candidate_{incoming}_cost") - pl.col(f"onward_{onward}_cost")
                        + next_utility + previous_delta
                    )
            weighted_choices = (
                weighted_choices.with_columns(
                    _utility=pl.max_horizontal(alternatives) * logit_scale,
                )
                .with_columns(p_ij=(pl.col("_utility") - pl.col("_utility").max().over(context_columns)).exp())
            )
        weighted_choices = weighted_choices.select(context_columns + ["to", "p_ij"])
        draw_columns = sequence_context_columns + [
            "activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id"
        ]
        requests = steps_lf.collect(engine="streaming").with_columns(
            _race_lane=pl.int_range(pl.len(), dtype=pl.UInt32).over(context_columns)
        )
        choices = weighted_choices.collect(engine="streaming")
        winner_batches = []
        batch_size = 8
        max_lanes = int(requests["_race_lane"].max()) + 1 if requests.height else 0
        for start in range(0, max_lanes, batch_size):
            lanes = range(start, min(start + batch_size, max_lanes))
            batch = requests.filter(
                pl.col("_race_lane").is_between(start, start + batch_size - 1)
            )
            per_context = batch.group_by(context_columns).agg([
                pl.col(column).filter(pl.col("_race_lane") == lane).first().alias(f"_race_{column}_{lane}")
                for lane in lanes for column in draw_columns
            ])
            scores = []
            for lane in lanes:
                # Keep the original field names and types inside the struct:
                # Polars hashes both, so this preserves every seeded draw.
                draw_hash = pl.struct(
                    [pl.col(f"_race_{column}_{lane}").alias(column) for column in draw_columns]
                    + [pl.col("to")]
                ).hash(seed=seed)
                scores.append(
                    pl.when(pl.col(f"_race_{draw_columns[0]}_{lane}").is_not_null())
                    .then(
                        draw_hash.cast(pl.Float64).truediv(18446744073709551616.0).log().neg()
                        / pl.col("p_ij").clip(1e-18)
                        + pl.col("to").cast(pl.Float64) * 1e-18
                    )
                    .otherwise(None)
                    .alias(f"_race_score_{lane}")
                )
            winners = (
                choices.lazy().join(per_context.lazy(), on=context_columns)
                .with_columns(scores)
                .group_by(context_columns)
                .agg([
                    pl.struct("to", pl.col(f"_race_score_{lane}").alias("score"))
                    .filter(pl.col(f"_race_score_{lane}") == pl.col(f"_race_score_{lane}").min())
                    .alias(f"_race_winner_{lane}")
                    for lane in lanes
                ])
                .collect(engine="streaming")
                .unpivot(index=context_columns, variable_name="_race_lane", value_name="_race_winner")
                .with_columns(pl.col("_race_lane").str.strip_prefix("_race_winner_").cast(pl.UInt32))
                .explode("_race_winner")
                .filter(pl.col("_race_winner").is_not_null())
                .unnest("_race_winner")
            )
            winner_batches.append(batch.join(winners, on=context_columns + ["_race_lane"]))
        if winner_batches:
            # A draw key may occur in more than one context. Match the old
            # grouped race by selecting its minimum across all those contexts.
            sampled = (
                pl.concat(winner_batches, how="vertical_relaxed")
                .lazy()
                .filter(pl.col("score") == pl.col("score").min().over(draw_columns))
                .select(output_cols)
            )
        else:
            sampled = choices.head(0).join(
                requests.head(0), on=context_columns, how="inner"
            ).lazy().select(output_cols)
        return candidates, candidates_with_origin_costs, candidates_with_costs, sampled


    @trace_simulation_step("anchor destination sampling")
    def _spatialize_anchor_activities(
        self,
        sequences: pl.DataFrame,
        destination_probability: pl.DataFrame,
        alpha: float,
        seed: int,
        cost_views: dict[str, pl.LazyFrame],
        *, mean_durations: dict[str, float] | None = None,
        mean_home_duration: float | None = None,
    ) -> pl.DataFrame:
        """Choose the anchor destinations of each daily tour.

        Anchor activities structure the day, such as home, work, and studies.
        They are sampled before non-anchor activities because a later shopping
        or leisure stop needs to know the next anchor destination the person is
        travelling toward.

        Anchors are sampled in travel order. For example, if a person goes from
        home to studies and then to work, the work destination is sampled from
        the sampled studies zone, not independently from home. We also use a
        simple two-leg penalty:

        `current location -> candidate anchor -> home`

        This keeps sampled anchor sequences reachable and avoids impossible legs
        such as sampling a work zone that cannot be reached from the previous
        anchor zone.
        """
        logging.debug("Spatializing anchor activities...")
        if "activity_draw_id" not in sequences.columns:
            sequences = sequences.with_columns(
                pl.lit(0, dtype=pl.UInt32).alias("activity_draw_id")
            )
        sequence_key_cols = self.spatialization_context_columns + [
            "activity_draw_id",
            "activity_seq_id",
            "time_seq_id",
            "dest_draw_id",
        ]
        anchor_output_cols = sequence_key_cols + [
            "activity",
            "is_anchor",
            "seq_step_index",
            "step_count",
            "from",
            "to",
            "departure_time",
            "arrival_time",
            "next_departure_time",
        ]
        destination_probability_lf = destination_probability.lazy()
        destination_draw_ids = pl.DataFrame(
            {
                "dest_draw_id": range(
                    self.parameters.destination_sequences.k_destination_sequences
                )
            },
            schema={"dest_draw_id": pl.UInt32},
        )
        sequences = (
            sequences
            .sort(
                self.spatialization_context_columns
                + ["activity_draw_id", "activity_seq_id", "time_seq_id", "seq_step_index"]
            )
            .with_columns(
                next_anchor_seq_step_index=(
                    pl.when(pl.col("is_anchor"))
                    .then(pl.col("seq_step_index"))
                    .otherwise(pl.lit(None, dtype=pl.UInt8))
                    .backward_fill()
                    .over(
                        self.spatialization_context_columns
                        + ["activity_draw_id", "activity_seq_id", "time_seq_id"]
                    )
                )
            )
        )
        utility_sampling = self.parameters.destination_sequences.sampling_method == "utility"
        if utility_sampling:
            schedule_keys = self.spatialization_context_columns + ["activity_draw_id", "activity_seq_id", "time_seq_id"]
            sequences = sequences.with_columns(
                _next_activity=pl.col("activity").shift(-1).over(schedule_keys),
                _next_departure=pl.col("departure_time").shift(-1).over(schedule_keys),
                _next_arrival=pl.col("arrival_time").shift(-1).over(schedule_keys),
                _next_next_departure=pl.col("next_departure_time").shift(-1).over(schedule_keys),
                _next_anchor=pl.col("is_anchor").shift(-1).over(schedule_keys).fill_null(False),
                _previous_activity=pl.col("activity").shift(1).over(schedule_keys).fill_null("home"),
                _previous_previous_activity=pl.col("activity").shift(2).over(schedule_keys).fill_null("home"),
                _previous_departure=pl.col("departure_time").shift(1).over(schedule_keys).fill_null(0.0),
                _previous_arrival=pl.col("arrival_time").shift(1).over(schedule_keys).fill_null(0.0),
                _previous_is_anchor=pl.col("is_anchor").shift(1).over(schedule_keys).fill_null(False),
                _previous_is_first=(pl.col("seq_step_index") == pl.col("seq_step_index").min().over(schedule_keys)),
                _home_night_reference=(24.0 - (pl.col("next_departure_time") - pl.col("departure_time"))
                                      .sum().over(schedule_keys)).clip(0.0),
            ).with_columns(
                # During anchor sampling the proxy onward location is home;
                # during other sampling it is the known next anchor.
                _next_is_direct=(pl.col("_next_anchor") &
                                 (~pl.col("is_anchor") | (pl.col("_next_activity") == "home"))).fill_null(False),
            )

        expanded_anchor_steps = (
            sequences
            .filter(pl.col("is_anchor"))
            .join(destination_draw_ids, how="cross")
            .sort(
                sequence_key_cols + ["seq_step_index"]
            )
        )
        expanded_non_anchor_steps = (
            sequences
            .filter(pl.col("is_anchor").not_())
            .join(destination_draw_ids, how="cross")
            .with_columns(
                pl.lit(None, dtype=pl.UInt16).alias("from"),
                pl.lit(None, dtype=pl.UInt16).alias("to"),
            )
            .select(anchor_output_cols + ["next_anchor_seq_step_index"])
        )

        # Track the previous anchor destination for each draw. Non-anchor
        # activities are sampled later, so they do not move this location.
        current_locations = (
            expanded_anchor_steps
            .select(sequence_key_cols)
            .unique()
            .with_columns(pl.col("home_zone_id").alias("from"))
        )
        if utility_sampling:
            current_locations = current_locations.with_columns(
                pl.col("home_zone_id").alias("_previous_origin"),
            )

        sampled_anchor_steps_by_position: list[pl.DataFrame] = []

        anchor_step_indexes = expanded_anchor_steps["seq_step_index"].unique().sort().to_list()
        for seq_step_index in anchor_step_indexes:
            anchor_steps_at_position = (
                expanded_anchor_steps
                .filter(pl.col("seq_step_index") == seq_step_index)
                .join(current_locations, on=sequence_key_cols)
            )
            if utility_sampling:
                # If an intervening stop is not known yet, use the last known
                # anchor as its provisional location and preceding origin.
                anchor_steps_at_position = anchor_steps_at_position.with_columns(
                    _previous_origin=pl.when(pl.col("_previous_is_anchor"))
                    .then(pl.col("_previous_origin")).otherwise(pl.col("from")),
                    _previous_origin_known=~pl.col("_previous_is_first"),
                )
            if anchor_steps_at_position.height == 0:
                continue

            anchor_steps_at_position_lf = anchor_steps_at_position.lazy()

            # Home is an anchor, but it is not sampled: its destination is
            # always the household home zone.
            home_anchor_steps = (
                anchor_steps_at_position_lf
                .filter(pl.col("activity") == "home")
                .with_columns(to=pl.col("home_zone_id"))
                .select(anchor_output_cols)
                .collect(engine="streaming")
            )

            non_home_anchor_steps = anchor_steps_at_position.filter(pl.col("activity") != "home")
            if non_home_anchor_steps.height == 0:
                sampled_anchor_steps = home_anchor_steps.head(0)
            else:
                non_home_anchor_steps_lf = non_home_anchor_steps.lazy()
                rigidity = self._sampling_rigidity() if utility_sampling else {}
                score_previous = (
                    utility_sampling and self.parameters.plan_update.update_plan_timings_from_modeled_travel_times
                )
                # The first trip changes home-night time; there is no earlier
                # trip whose arrival or travel cost needs to be estimated.
                model_previous = score_previous and not non_home_anchor_steps["_previous_is_first"].all()

                # Keep anchor candidates that can be reached from the previous
                # anchor and can then return home: previous anchor -> candidate
                # anchor -> home.
                _, _, _, sampled_anchor_steps_lf = self._sample_sequence_aware_destinations(
                    steps_lf=non_home_anchor_steps_lf,
                    destination_probability_lf=destination_probability_lf,
                    origin_cost_lf=cost_views["to_candidate"],
                    onward_cost_lf=cost_views["to_home"],
                    onward_left_on=["to", "home_zone_id"],
                    onward_right_on=["home_return_from", "home_return_to"],
                    onward_cost_col="cost_to_home",
                    alpha=alpha,
                    sequence_context_columns=self.spatialization_context_columns,
                    seed=seed,
                    output_cols=anchor_output_cols,
                    utility_values=({name: p.value_of_time for name, p in self.resolved_activity_parameters.items()}
                                    if self.parameters.destination_sequences.sampling_method == "utility" else None),
                    min_activity_time_constant=self.parameters.plan_update.min_activity_time_constant,
                    logit_scale=self.parameters.plan_update.transition_logit_scale,
                    mean_durations=mean_durations,
                    activity_rigidity=rigidity if utility_sampling else None,
                    # If no next stop is directly modeled, its timing cannot
                    # affect the choice. The cheapest onward cost is enough.
                    use_next_timing=utility_sampling and non_home_anchor_steps["_next_is_direct"].any(),
                    update_timings=self.parameters.plan_update.update_plan_timings_from_modeled_travel_times,
                    use_previous_timing=score_previous,
                    anchor_sampling=True,
                    mean_home_duration=mean_home_duration,
                    home_value=getattr(self.resolved_activity_parameters.get("home"), "value_of_time_stay_home", 0.0),
                    model_previous_arrival=model_previous,
                )
                sampled_anchor_steps = sampled_anchor_steps_lf.collect(engine="streaming")

            log_missing_anchor_destination_samples(
                sequence_key_cols=sequence_key_cols,
                expected_anchor_steps=non_home_anchor_steps,
                sampled_anchor_steps=sampled_anchor_steps,
            )

            sampled_anchor_steps_by_position.extend([home_anchor_steps, sampled_anchor_steps])

            next_anchor_locations = (
                pl.concat([home_anchor_steps, sampled_anchor_steps], how="vertical_relaxed")
                .select(sequence_key_cols + ["to"] + (["from"] if utility_sampling else []))
                .rename({"to": "next_from", **({"from": "_next_previous_origin"} if utility_sampling else {})})
            )
            if next_anchor_locations.height > 0:
                current_locations = (
                    current_locations
                    .join(next_anchor_locations, on=sequence_key_cols, how="left")
                    .with_columns(
                        pl.coalesce(pl.col("next_from"), pl.col("from")).cast(pl.UInt16).alias("from")
                    )
                    .drop("next_from")
                )
                if utility_sampling:
                    current_locations = current_locations.with_columns(
                        pl.coalesce("_next_previous_origin", "_previous_origin").alias("_previous_origin")
                    ).drop("_next_previous_origin")
        sampled_anchor_steps = (
            pl.concat(sampled_anchor_steps_by_position, how="vertical_relaxed")
            if sampled_anchor_steps_by_position
            else expanded_non_anchor_steps.head(0)
        )
        sampled_anchor_locations = (
            sampled_anchor_steps
            .select(
                sequence_key_cols
                + [
                    pl.col("seq_step_index").alias("next_anchor_seq_step_index"),
                    pl.col("to").alias("anchor_to"),
                ]
            )
        )
        spatialized_anchor_steps = (
            sampled_anchor_steps
            .with_columns(anchor_to=pl.col("to"))
            .drop(["from", "to"])
        )
        spatialized_non_anchor_steps = (
            expanded_non_anchor_steps
            .join(
                sampled_anchor_locations,
                on=sequence_key_cols + ["next_anchor_seq_step_index"],
                how="left",
            )
            .drop(["from", "to", "next_anchor_seq_step_index"])
        )
        result = pl.concat(
            [spatialized_anchor_steps, spatialized_non_anchor_steps],
            how="vertical_relaxed",
        )
        if utility_sampling:
            result = result.join(
                sequences.select(schedule_keys + ["seq_step_index", "_next_activity", "_next_departure",
                    "_next_arrival", "_next_next_departure", "_next_is_direct", "_previous_activity", "_previous_previous_activity",
                    "_previous_departure", "_previous_arrival", "_previous_is_anchor", "_previous_is_first",
                    "_home_night_reference"]),
                on=schedule_keys + ["seq_step_index"],
            )
        return result


    @trace_simulation_step("other destination sampling")
    def _spatialize_other_activities(
        self,
        sequences: pl.DataFrame,
        destination_probability: pl.DataFrame,
        costs: pl.DataFrame,
        alpha: float,
        seed: int,
        cost_views: dict[str, pl.LazyFrame],
        *, mean_durations: dict[str, float] | None = None,
        mean_home_duration: float | None = None,
    ) -> pl.DataFrame:
        """Sample destinations for non-anchor activities step by step."""
        logging.debug("Spatializing other activities...")
        sequence_step = (
            sequences
            .filter(pl.col("seq_step_index") == 1)
            .with_columns(pl.col("home_zone_id").alias("from"))
        )
        utility_sampling = self.parameters.destination_sequences.sampling_method == "utility"
        if utility_sampling:
            sequence_step = sequence_step.with_columns(
                pl.col("home_zone_id").alias("_previous_origin"),
                _previous_origin_known=pl.lit(False),
            )
        seq_step_index = 1
        spatialized_sequences: list[pl.DataFrame] = []
        while sequence_step.height > 0:
            step_counts = sequence_step.select(
                non_anchor=pl.col("is_anchor").not_().sum(),
                anchor=pl.col("is_anchor").sum(),
            ).row(0, named=True)
            non_anchor_count = int(step_counts["non_anchor"])
            anchor_count = int(step_counts["anchor"])
            log_destination_spatialization_step(
                seq_step_index=seq_step_index,
                sequence_step=sequence_step,
                non_anchor_count=non_anchor_count,
                anchor_count=anchor_count,
            )
            logging.debug("Spatializing step %s...", str(seq_step_index))
            spatialized_step = (
                self._spatialize_sequence_step(
                    seq_step_index,
                    sequence_step,
                    destination_probability,
                    costs,
                    alpha,
                    seed,
                    cost_views,
                    non_anchor_count,
                    anchor_count,
                    mean_durations=mean_durations,
                    mean_home_duration=mean_home_duration,
                )
                .with_columns(seq_step_index=pl.lit(seq_step_index).cast(pl.UInt8))
            )
            spatialized_sequences.append(spatialized_step)
            seq_step_index += 1
            sequence_step = (
                sequences.lazy()
                .filter(pl.col("seq_step_index") == seq_step_index)
                .join(
                    spatialized_step.lazy()
                    .select(self.spatialization_context_columns
                            + ["activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "to"]
                            + ([pl.col("from").alias("_previous_origin")] if utility_sampling else []))
                    .rename({"to": "from"}),
                    on=self.spatialization_context_columns
                    + ["activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id"],
                )
                .collect(engine="streaming")
            )
            if utility_sampling:
                sequence_step = sequence_step.with_columns(_previous_origin_known=pl.lit(True))
        return pl.concat(spatialized_sequences)


    def _spatialize_sequence_step(
        self,
        seq_step_index: int,
        sequence_step: pl.DataFrame,
        destination_probability: pl.DataFrame,
        costs: pl.DataFrame,
        alpha: float,
        seed: int,
        cost_views: dict[str, pl.LazyFrame],
        non_anchor_count: int,
        anchor_count: int,
        *, mean_durations: dict[str, float] | None = None,
        mean_home_duration: float | None = None,
    ) -> pl.DataFrame:
        """Sample destinations for one step between anchors."""
        if "activity_draw_id" not in sequence_step.columns:
            sequence_step = sequence_step.with_columns(
                pl.lit(0, dtype=pl.UInt32).alias("activity_draw_id")
            )
        sequence_step_lf = sequence_step.lazy()
        destination_probability_lf = destination_probability.lazy()
        sequence_key_cols = self.spatialization_context_columns + [
            "activity_draw_id",
            "activity_seq_id",
            "time_seq_id",
            "dest_draw_id",
        ]
        output_cols = self.spatialization_context_columns + [
            "activity_draw_id",
            "activity_seq_id",
            "time_seq_id",
            "dest_draw_id",
            "activity",
            "anchor_to",
            "from",
            "to",
            "departure_time",
            "arrival_time",
            "next_departure_time",
            "step_count",
        ]
        step_frames: list[pl.LazyFrame] = []

        if non_anchor_count > 0:
            non_anchor_steps = sequence_step_lf.filter(pl.col("is_anchor").not_())
            utility_sampling = self.parameters.destination_sequences.sampling_method == "utility"
            rigidity = self._sampling_rigidity() if utility_sampling else {}
            score_previous = (
                utility_sampling and self.parameters.plan_update.update_plan_timings_from_modeled_travel_times
            )
            model_previous = score_previous and not sequence_step.filter(
                pl.col("is_anchor").not_()
            )["_previous_is_first"].all()

            # Keep non-anchor candidates that fit between the current location
            # and the next anchor: current location -> candidate -> next anchor.
            (
                non_anchor_candidates,
                candidates_with_origin_costs,
                candidates_with_costs,
                sampled_non_anchor_steps,
            ) = self._sample_sequence_aware_destinations(
                steps_lf=non_anchor_steps,
                destination_probability_lf=destination_probability_lf,
                origin_cost_lf=cost_views["to_candidate"],
                onward_cost_lf=cost_views["to_anchor"],
                onward_left_on=["to", "anchor_to"],
                onward_right_on=["anchor_from", "anchor_to_cost"],
                onward_cost_col="cost_to_anchor",
                alpha=alpha,
                sequence_context_columns=self.spatialization_context_columns,
                seed=seed,
                output_cols=output_cols,
                utility_values=({name: p.value_of_time for name, p in self.resolved_activity_parameters.items()}
                                if self.parameters.destination_sequences.sampling_method == "utility" else None),
                min_activity_time_constant=self.parameters.plan_update.min_activity_time_constant,
                logit_scale=self.parameters.plan_update.transition_logit_scale,
                mean_durations=mean_durations,
                activity_rigidity=rigidity if utility_sampling else None,
                use_next_timing=utility_sampling,
                update_timings=self.parameters.plan_update.update_plan_timings_from_modeled_travel_times,
                filter_infeasible=utility_sampling,
                use_previous_timing=score_previous,
                mean_home_duration=mean_home_duration,
                home_value=getattr(self.resolved_activity_parameters.get("home"), "value_of_time_stay_home", 0.0),
                model_previous_arrival=model_previous,
            )
            step_frames.append(sampled_non_anchor_steps)
            log_step_dropout_diagnostics(
                seq_step_index=seq_step_index,
                sequence_step=sequence_step,
                costs=costs,
                non_anchor_candidates=non_anchor_candidates,
                candidates_with_origin_costs=candidates_with_origin_costs,
                candidates_with_costs=candidates_with_costs,
                sampled_non_anchor_steps=sampled_non_anchor_steps,
                sequence_key_cols=sequence_key_cols,
                transport_zones=self.transport_zones,
            )

        if anchor_count > 0:
            steps_anchor = (
                sequence_step_lf
                .filter(pl.col("is_anchor"))
                .with_columns(to=pl.col("anchor_to"))
                # Anchor destinations were sampled earlier, but we still check
                # the actual leg used at this step. If a cost disappears after a
                # cost update, the incomplete draw is removed before mode search.
                .join(
                    cost_views["anchor_leg_pairs"],
                    left_on=["from", "to"],
                    right_on=["anchor_leg_from", "anchor_leg_to"],
                )
                .select(output_cols)
            )
            step_frames.append(steps_anchor)

        return pl.concat(step_frames).collect(engine="streaming")

    @staticmethod
    def _drop_incomplete_destination_draws(
        *,
        activity_sequences: pl.DataFrame,
        iteration: int,
        sequence_context_columns: list[str],
    ) -> pl.DataFrame:
        """Drop destination draws that lost one or more steps during spatialization.

        A non-anchor step can disappear when there is no valid travel-cost path from
        the sampled destination to the next anchor destination. If we keep the
        remaining earlier steps, we create a truncated destination sequence that later
        becomes invalid input for mode-sequence search.
        """
        if "activity_draw_id" not in activity_sequences.columns:
            activity_sequences = activity_sequences.with_columns(
                pl.lit(0, dtype=pl.UInt32).alias("activity_draw_id")
            )
        sequence_key_cols = sequence_context_columns + [
            "activity_draw_id",
            "activity_seq_id",
            "time_seq_id",
            "dest_draw_id",
        ]
        activity_sequences_with_counts = (
            activity_sequences
            .with_columns(
                step_count_after_spatialization=pl.len().over(sequence_key_cols).cast(pl.UInt8),
            )
        )
        log_incomplete_destination_draws(
            iteration=iteration,
            activity_sequences_with_counts=activity_sequences_with_counts,
            sequence_key_cols=sequence_key_cols,
        )
        return (
            activity_sequences_with_counts
            .filter(pl.col("step_count_after_spatialization") == pl.col("step_count"))
            .drop(["step_count", "step_count_after_spatialization"])
            .sort(sequence_key_cols + ["seq_step_index"])
        )
