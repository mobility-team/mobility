import logging
import pathlib
from typing import Any

import polars as pl
from scipy.stats import norm

from mobility.runtime.assets.cache_schema import read_cached_parquet
from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from .debug_logs import (
    log_destination_sequence_diagnostics,
    log_destination_spatialization_step,
    log_incomplete_destination_draws,
    log_missing_anchor_destination_samples,
    log_step_dropout_diagnostics,
)
from .stable_key_index import StableKeyIndex
from mobility.runtime.assets.file_asset import FileAsset
from mobility.activities.activity import resolve_activity_parameters
from mobility.runtime.parameter_values import SensitivityCase
from mobility.trips.group_day_trips.core.progress import get_group_day_trips_progress
from .demand_subgroups import DEMAND_UNIT_COLS, DEMAND_UNIT_SCHEMA, demand_unit_hash


class DestinationSequences(FileAsset):
    """Persist destination sequences produced for one PopulationGroupDayTrips iteration."""

    OUTPUT_SCHEMA = {
        "demand_group_id": pl.UInt32,
        "demand_subgroup_id": pl.UInt32,
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
    REQUIRED_SCHEMA = {
        **DEMAND_UNIT_SCHEMA,
        "activity_seq_id": pl.UInt32,
        "time_seq_id": pl.UInt32,
        "dest_seq_id": pl.UInt32,
        "seq_step_index": pl.UInt8,
    }

    OUTPUT_COLUMNS = list(OUTPUT_SCHEMA)

    def __init__(
        self,
        *,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        previous_state: FileAsset | None = None,
        previous_destination_sequences: FileAsset | None = None,
        seed_asset: FileAsset | None = None,
        activity_sequences: FileAsset | None = None,
        activities: list[Any] | None = None,
        resolved_activity_parameters: dict[str, Any] | None = None,
        scenario: str | None = None,
        sensitivity_case: SensitivityCase | None = None,
        transport_zones: Any = None,
        transport_costs: Any = None,
        current_plans: pl.DataFrame | None = None,
        current_plan_steps: pl.DataFrame | None = None,
        destination_saturation: pl.DataFrame | None = None,
        demand_groups: pl.DataFrame | None = None,
        costs: pl.DataFrame | None = None,
        parameters: Any = None,
        seed: int | None = None,
    ) -> None:
        self.previous_state = previous_state
        self.previous_destination_sequences = previous_destination_sequences
        self.seed_asset = seed_asset
        self.activity_sequences = activity_sequences
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
        self.current_plans = current_plans
        self.current_plan_steps = current_plan_steps
        self.destination_saturation = destination_saturation
        self.demand_groups = demand_groups
        self.costs = costs
        self.parameters = parameters
        self.seed = seed
        inputs = {
            "version": 10,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "sensitivity_case": sensitivity_case,
            "activity_sequences_asset": activity_sequences if isinstance(activity_sequences, FileAsset) else None,
            "previous_state": previous_state,
            "previous_destination_sequences": previous_destination_sequences,
            "seed_asset": seed_asset,
            # Destination choice only needs the resolved activity/scenario values
            # and the current cost table. Run does not need to know these details.
            "resolved_activity_parameters": self.resolved_activity_parameters,
            "transport_zones": transport_zones,
            "transport_costs": transport_costs,
            "destination_sequence_parameters": (
                parameters.destination_sequences if parameters is not None else None
            ),
            "plan_update_shadow_price_flag": (
                parameters.plan_update.use_destination_shadow_prices
                if parameters is not None
                else None
            ),
            "behavior_change_scope": (
                parameters.behavior_change.scope_at(iteration)
                if parameters is not None
                else None
            ),
            "seed": seed,
        }
        cache_path = {
            "sequences": pathlib.Path(base_folder) / f"destination_sequences_{iteration}.parquet",
            "index": pathlib.Path(base_folder) / f"destination_sequence_index_{iteration}.parquet",
        }
        super().__init__(inputs, cache_path)


    def get_cached_asset(self) -> pl.DataFrame:
        """Return cached destination sequences for one iteration."""
        return read_cached_parquet(
            self.cache_path["sequences"],
            table_name="destination_sequences",
            required_schema=self.REQUIRED_SCHEMA,
        )


    def get_index(self) -> pl.DataFrame:
        """Return the destination-sequence index cached with this iteration."""
        return pl.read_parquet(self.cache_path["index"])


    def create_and_get_asset(self) -> pl.DataFrame:
        """Compute and persist destination sequences for one iteration."""
        get_group_day_trips_progress().iteration_step(self.iteration, "destination sequences")
        self._load_missing_runtime_inputs_from_previous_state()
        if self.seed is None and self.seed_asset is not None:
            self.seed = self.seed_asset.get()["destination_sequences"]

        scope = self.parameters.behavior_change.scope_at(self.iteration)
        destination_sequences = self._build_destination_sequences_for_scope(scope)

        self.cache_path["sequences"].parent.mkdir(parents=True, exist_ok=True)
        destination_sequences.write_parquet(self.cache_path["sequences"])
        return self.get_cached_asset()

    def _load_missing_runtime_inputs_from_previous_state(self) -> None:
        """Fill runtime dataframes from the cached state produced by the previous iteration."""
        if self.previous_state is None:
            return

        state = self.previous_state.get()
        if self.current_plans is None:
            self.current_plans = state.current_plans
        if self.current_plan_steps is None:
            self.current_plan_steps = state.current_plan_steps
        if self.destination_saturation is None:
            self.destination_saturation = state.destination_saturation
        if self.demand_groups is None:
            self.demand_groups = state.demand_groups
        if self.costs is None and self.transport_costs is not None:
            self.costs = self.transport_costs.get_costs_by_od(["cost", "distance"])
        elif self.costs is None:
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
            self.activity_sequences.get_cached_asset(),
            self.demand_groups,
            self.costs,
            self.parameters,
            self.seed,
        )

    def _sample_active_destination_sequences(self) -> pl.DataFrame:
        """Sample destination sequences for currently active activity sequences only."""
        filtered_sequences = self.activity_sequences.get_cached_asset()
        if filtered_sequences.height == 0:
            self._cache_empty_destination_sequence_index()
            return self._empty_destination_sequences()

        return self.run(
            self.activities,
            self.transport_zones,
            self.destination_saturation,
            filtered_sequences,
            self.demand_groups,
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

        reused = (
            self.current_plan_steps.lazy()
            .join(
                active_dest_sequences.lazy(),
                on=DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id"],
                how="inner",
            )
            .with_columns(iteration=pl.lit(self.iteration).cast(pl.UInt16()))
            .select(
                [
                    "demand_group_id",
                    "demand_subgroup_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "dest_seq_id",
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
                subset=DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id", "seq_step_index"],
                keep="first",
            )
            .collect(engine="streaming")
        )

        if reused.height == 0:
            raise ValueError(
                "Active non-stay-home plans could not be matched to reusable "
                f"destination sequences at iteration={self.iteration}."
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
                subset=DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id", "seq_step_index"],
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
        """Write a carried-forward destination index when no new sequence is sampled."""
        empty_keys = pl.DataFrame(schema={"destination_sequence_key": pl.String})
        StableKeyIndex(
            key_cols=["destination_sequence_key"],
            index_col="dest_seq_id",
            first_new_id=1,
        ).extend_and_cache(
            empty_keys,
            previous_asset=self.previous_destination_sequences,
            index_path=self.cache_path["index"],
        )

    def run(
        self,
        activities: list[Any],
        transport_zones: Any,
        destination_saturation: pl.DataFrame,
        activity_sequences: pl.DataFrame,
        demand_groups: pl.DataFrame,
        costs: pl.DataFrame,
        parameters: Any,
        seed: int,
    ) -> pl.DataFrame:
        """Compute destination sequences for one iteration."""
        utility_inputs = self._get_destination_probability_inputs(
            destination_saturation,
            costs,
            parameters.destination_sequences.cost_uncertainty_sd,
        )
        destination_probability = self._get_destination_probability(
            utility_inputs,
            activities,
            self.resolved_activity_parameters,
            parameters.destination_sequences.dest_prob_cutoff,
        )
        cost_views = self._spatialization_cost_views(costs)
        activity_sequences = (
            activity_sequences
            .filter(pl.col("activity_seq_id") != 0)
            .join(
                demand_groups.select(DEMAND_UNIT_COLS + ["home_zone_id"]),
                on=DEMAND_UNIT_COLS,
            )
            .select(
                [
                    "demand_group_id",
                    "demand_subgroup_id",
                    "home_zone_id",
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
        )
        source_activity_sequences = activity_sequences
        anchor_spatialized_sequences = self._spatialize_anchor_activities(
            source_activity_sequences,
            destination_probability,
            parameters.destination_sequences.alpha,
            seed,
            cost_views,
        )
        spatialized_activity_sequences = self._spatialize_other_activities(
            anchor_spatialized_sequences,
            destination_probability,
            costs,
            parameters.destination_sequences.alpha,
            seed,
            cost_views,
        )
        complete_activity_sequences = self._drop_incomplete_destination_draws(
            activity_sequences=spatialized_activity_sequences,
            iteration=self.iteration,
        )

        destination_sequences = (
            complete_activity_sequences
            .group_by(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_draw_id"])
            .agg(destination_sequence_key=pl.col("to").sort_by("seq_step_index").cast(pl.Utf8()))
            .with_columns(destination_sequence_key=pl.col("destination_sequence_key").list.join("-"))
            .sort(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_draw_id"])
        )
        destination_sequences = (
            destination_sequences
            .group_by(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "destination_sequence_key"])
            .agg(dest_draw_id=pl.col("dest_draw_id").min())
            .sort(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_draw_id"])
        )
        destination_sequences, _ = StableKeyIndex(
            key_cols=["destination_sequence_key"],
            index_col="dest_seq_id",
            first_new_id=1,
        ).extend_and_cache(
            destination_sequences,
            previous_asset=self.previous_destination_sequences,
            index_path=self.cache_path["index"],
        )
        destination_sequences = (
            complete_activity_sequences
            .join(
                destination_sequences.select(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_draw_id", "dest_seq_id"]),
                on=DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_draw_id"],
            )
            .drop(["home_zone_id", "activity", "dest_draw_id"])
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
        costs_lf = costs.lazy()
        return {
            "to_candidate": costs_lf.select(
                [
                    pl.col("from").alias("candidate_from"),
                    pl.col("to").alias("candidate_to"),
                    pl.col("cost").alias("cost_to_candidate"),
                ]
            ),
            "to_anchor": costs_lf.select(
                [
                    pl.col("from").alias("anchor_from"),
                    pl.col("to").alias("anchor_to_cost"),
                    pl.col("cost").alias("cost_to_anchor"),
                ]
            ),
            "to_home": costs_lf.select(
                [
                    pl.col("from").alias("home_return_from"),
                    pl.col("to").alias("home_return_to"),
                    pl.col("cost").alias("cost_to_home"),
                ]
            ),
            "anchor_leg_pairs": costs_lf.select(
                [
                    pl.col("from").alias("anchor_leg_from"),
                    pl.col("to").alias("anchor_leg_to"),
                ]
            ).unique(),
        }


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
        seed: int,
        output_cols: list[str],
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """Sample destinations while checking both legs around the candidate zone."""
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
        sampled = (
            candidates_with_costs
            .with_columns(
                sequence_cost_via_candidate=pl.col("cost_to_candidate") + pl.col(onward_cost_col),
            )
            .with_columns(
                p_ij=(
                    (
                        pl.col("p_ij").clip(1e-18).log()
                        - alpha * pl.col("sequence_cost_via_candidate")
                    ).exp()
                ),
            )
            .with_columns(
                noise=(
                    demand_unit_hash(
                        ["activity_seq_id", "time_seq_id", "dest_draw_id", "to"],
                        seed=seed,
                    )
                    .cast(pl.Float64)
                    .truediv(pl.lit(18446744073709551616.0))
                    .log()
                    .neg()
                ),
            )
            .with_columns(
                sample_score=pl.col("noise") / pl.col("p_ij").clip(1e-18)
                + pl.col("to").cast(pl.Float64) * 1e-18
            )
            .with_columns(
                min_score=pl.col("sample_score").min().over(
                    DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_draw_id"]
                )
            )
            .filter(pl.col("sample_score") == pl.col("min_score"))
            .select(output_cols)
        )
        return candidates, candidates_with_origin_costs, candidates_with_costs, sampled


    def _spatialize_anchor_activities(
        self,
        sequences: pl.DataFrame,
        destination_probability: pl.DataFrame,
        alpha: float,
        seed: int,
        cost_views: dict[str, pl.LazyFrame],
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
        sequence_key_cols = [
            "demand_group_id",
            "demand_subgroup_id",
            "home_zone_id",
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
            {"dest_draw_id": list(range(int(self.parameters.destination_sequences.k_destination_sequences)))},
            schema={"dest_draw_id": pl.UInt32},
        )
        sequences = (
            sequences
            .sort(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "seq_step_index"])
            .with_columns(
                next_anchor_seq_step_index=(
                    pl.when(pl.col("is_anchor"))
                    .then(pl.col("seq_step_index"))
                    .otherwise(pl.lit(None, dtype=pl.UInt8))
                    .backward_fill()
                    .over(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id"])
                )
            )
        )

        expanded_anchor_steps = (
            sequences
            .filter(pl.col("is_anchor"))
            .join(destination_draw_ids, how="cross")
            .sort(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "seq_step_index"])
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

        sampled_anchor_steps_by_position: list[pl.DataFrame] = []

        anchor_step_indexes = expanded_anchor_steps["seq_step_index"].unique().sort().to_list()
        for seq_step_index in anchor_step_indexes:
            anchor_steps_at_position = (
                expanded_anchor_steps
                .filter(pl.col("seq_step_index") == seq_step_index)
                .join(current_locations, on=sequence_key_cols)
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
                    seed=seed,
                    output_cols=anchor_output_cols,
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
                .select(sequence_key_cols + ["to"])
                .rename({"to": "next_from"})
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
        return pl.concat(
            [spatialized_anchor_steps, spatialized_non_anchor_steps],
            how="vertical_relaxed",
        )


    def _spatialize_other_activities(
        self,
        sequences: pl.DataFrame,
        destination_probability: pl.DataFrame,
        costs: pl.DataFrame,
        alpha: float,
        seed: int,
        cost_views: dict[str, pl.LazyFrame],
    ) -> pl.DataFrame:
        """Sample destinations for non-anchor activities step by step."""
        logging.debug("Spatializing other activities...")
        sequence_step = (
            sequences
            .filter(pl.col("seq_step_index") == 1)
            .with_columns(pl.col("home_zone_id").alias("from"))
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
                    .select(DEMAND_UNIT_COLS + ["home_zone_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "to"])
                    .rename({"to": "from"}),
                    on=DEMAND_UNIT_COLS + ["home_zone_id", "activity_seq_id", "time_seq_id", "dest_draw_id"],
                )
                .collect(engine="streaming")
            )
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
    ) -> pl.DataFrame:
        """Sample destinations for one step between anchors."""
        sequence_step_lf = sequence_step.lazy()
        destination_probability_lf = destination_probability.lazy()
        sequence_key_cols = [
            "demand_group_id",
            "demand_subgroup_id",
            "home_zone_id",
            "activity_seq_id",
            "time_seq_id",
            "dest_draw_id",
        ]
        output_cols = [
            "demand_group_id",
            "demand_subgroup_id",
            "home_zone_id",
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
                seed=seed,
                output_cols=output_cols,
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
    ) -> pl.DataFrame:
        """Drop destination draws that lost one or more steps during spatialization.

        A non-anchor step can disappear when there is no valid travel-cost path from
        the sampled destination to the next anchor destination. If we keep the
        remaining earlier steps, we create a truncated destination sequence that later
        becomes invalid input for mode-sequence search.
        """
        sequence_key_cols = DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_draw_id"]
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
