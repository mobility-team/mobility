import logging
import pathlib
from typing import Any

import polars as pl
from scipy.stats import norm

from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from .sequence_index import add_index
from mobility.runtime.assets.file_asset import FileAsset


class DestinationSequences(FileAsset):
    """Persist destination sequences produced for one PopulationGroupDayTrips iteration."""

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        activity_sequences: FileAsset | None = None,
        activities: list[Any] | None = None,
        resolved_activity_parameters: dict[str, Any] | None = None,
        transport_zones: Any = None,
        current_plans: pl.DataFrame | None = None,
        current_plan_steps: pl.DataFrame | None = None,
        destination_saturation: pl.DataFrame | None = None,
        demand_groups: pl.DataFrame | None = None,
        costs: pl.DataFrame | None = None,
        sequence_index_folder: pathlib.Path | None = None,
        parameters: Any = None,
        seed: int | None = None,
    ) -> None:
        self.activity_sequences = activity_sequences
        self.activities = activities
        self.resolved_activity_parameters = resolved_activity_parameters
        self.transport_zones = transport_zones
        self.current_plans = current_plans
        self.current_plan_steps = current_plan_steps
        self.destination_saturation = destination_saturation
        self.demand_groups = demand_groups
        self.costs = costs
        self.sequence_index_folder = sequence_index_folder
        self.parameters = parameters
        self.seed = seed
        inputs = {
            "version": 2,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"destination_sequences_{iteration}.parquet"
        super().__init__(inputs, cache_path)


    def get_cached_asset(self) -> pl.DataFrame:
        """Return cached destination sequences for one iteration."""
        return pl.read_parquet(self.cache_path)


    def create_and_get_asset(self) -> pl.DataFrame:
        """Compute and persist destination sequences for one iteration."""
        if self.activities is None:
            raise ValueError("Cannot build destination sequences without activities.")
        if self.transport_zones is None:
            raise ValueError("Cannot build destination sequences without transport zones.")
        if self.destination_saturation is None:
            raise ValueError("Cannot build destination sequences without destination saturation.")
        if self.demand_groups is None:
            raise ValueError("Cannot build destination sequences without demand groups.")
        if self.costs is None:
            raise ValueError("Cannot build destination sequences without costs.")
        if self.sequence_index_folder is None:
            raise ValueError("Cannot build destination sequences without a sequence index folder.")
        if self.parameters is None:
            raise ValueError("Cannot build destination sequences without parameters.")
        if self.seed is None:
            raise ValueError("Cannot build destination sequences without a seed.")
        if self.resolved_activity_parameters is None:
            raise ValueError("Cannot build destination sequences without resolved activity parameters.")
        if self.current_plans is None:
            raise ValueError("Cannot build destination sequences without current plans.")

        destination_sequences = self._build_destination_sequences_for_scope()

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        destination_sequences.write_parquet(self.cache_path)
        return self.get_cached_asset()

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
            parameters.cost_uncertainty_sd,
        )
        destination_probability = self._get_destination_probability(
            utility_inputs,
            activities,
            self.resolved_activity_parameters,
            parameters.dest_prob_cutoff,
        )
        activity_sequences = (
            activity_sequences
            .filter(pl.col("activity_seq_id") != 0)
            .join(demand_groups.select(["demand_group_id", "home_zone_id"]), on="demand_group_id")
            .select(
                [
                    "demand_group_id",
                    "home_zone_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "activity",
                    "is_anchor",
                    "seq_step_index",
                    "departure_time",
                    "arrival_time",
                    "next_departure_time",
                ]
            )
        )
        activity_sequences = self._spatialize_anchor_activities(activity_sequences, destination_probability, seed)
        activity_sequences = self._spatialize_other_activities(
            activity_sequences,
            destination_probability,
            costs,
            parameters.alpha,
            seed,
        )

        destination_sequences = (
            activity_sequences
            .group_by(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"])
            .agg(to=pl.col("to").sort_by("seq_step_index").cast(pl.Utf8()))
            .with_columns(to=pl.col("to").list.join("-"))
            .sort(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"])
        )
        destination_sequences = (
            destination_sequences
            .group_by(["demand_group_id", "activity_seq_id", "time_seq_id", "to"])
            .agg(dest_draw_id=pl.col("dest_draw_id").min())
            .sort(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"])
        )
        destination_sequences = add_index(
            destination_sequences,
            col="to",
            index_col_name="dest_seq_id",
            index_folder=self.sequence_index_folder,
        )
        destination_sequences = (
            activity_sequences
            .join(
                destination_sequences.select(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "dest_seq_id"]),
                on=["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"],
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
        return destination_sequences

    def _build_destination_sequences_for_scope(self) -> pl.DataFrame:
        """Return the destination sequences to use for this iteration."""
        scope = self.parameters.get_behavior_change_scope(self.iteration)

        if scope == BehaviorChangeScope.FULL_REPLANNING:
            return self._sample_all_destination_sequences()

        if scope == BehaviorChangeScope.DESTINATION_REPLANNING:
            return self._sample_active_destination_sequences()

        if scope == BehaviorChangeScope.MODE_REPLANNING:
            return self._reuse_current_destination_sequences()

        raise ValueError(f"Unsupported behavior change scope: {scope}")

    def _sample_all_destination_sequences(self) -> pl.DataFrame:
        """Sample destination sequences from all non-stay-home activity chains."""
        if self.activity_sequences is None:
            raise ValueError("Cannot build destination sequences without activity sequences.")
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
        if self.activity_sequences is None:
            raise ValueError("Cannot build destination sequences without activity sequences.")
        filtered_chains = self.activity_sequences.get_cached_asset()
        if filtered_chains.height == 0:
            return self._empty_destination_sequences()

        return self.run(
            self.activities,
            self.transport_zones,
            self.destination_saturation,
            filtered_chains,
            self.demand_groups,
            self.costs,
            self.parameters,
            self.seed,
        )

    def _reuse_current_destination_sequences(self) -> pl.DataFrame:
        """Reuse the current iteration's destination sequences without resampling."""
        active_dest_sequences = self._get_active_non_stay_home_plans().select(
            ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id"]
        ).unique()

        if active_dest_sequences.height == 0:
            return self._empty_destination_sequences()

        if self.current_plan_steps is None:
            raise ValueError(
                "No current plan steps available for active non-stay-home "
                f"plans at iteration={self.iteration}."
            )

        reused = (
            self.current_plan_steps.lazy()
            .join(
                active_dest_sequences.lazy(),
                on=["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id"],
                how="inner",
            )
            .with_columns(iteration=pl.lit(self.iteration).cast(pl.UInt16()))
            .select(
                [
                    "demand_group_id",
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
                subset=["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "seq_step_index"],
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

    def _get_active_non_stay_home_plans(self) -> pl.DataFrame:
        """Return the distinct currently active non-stay-home plans."""
        return (
            self.current_plans
            .filter(pl.col("time_seq_id") != 0)
            .select(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"])
            .unique()
        )

    def _empty_destination_sequences(self) -> pl.DataFrame:
        """Return an empty destination-sequences dataframe with the expected schema."""
        return pl.DataFrame(
            schema={
                "demand_group_id": pl.UInt32,
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
                    * pl.col("k_saturation_utility").fill_null(1.0)
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
        logging.info(
            "Computing the probability of choosing a destination based on current location, potential destinations, and activity (with radiation models)..."
        )
        costs_by_bin = destination_probability_inputs[0]
        cost_bin_to_destination = destination_probability_inputs[1]
        if resolved_activity_parameters is None:
            raise ValueError("Cannot compute destination probabilities without resolved activity parameters.")
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


    def _spatialize_anchor_activities(
        self,
        chains: pl.DataFrame,
        destination_probability: pl.DataFrame,
        seed: int,
    ) -> pl.DataFrame:
        """Sample destinations for anchor activities and fill anchor destinations."""
        logging.info("Spatializing anchor activities...")
        chains_lf = chains.lazy()
        destination_probability_lf = destination_probability.lazy()

        anchor_activities = (
            chains_lf
            .filter((pl.col("is_anchor")) & (pl.col("activity") != "home"))
            .select(["demand_group_id", "home_zone_id", "activity_seq_id", "time_seq_id", "activity"])
            .unique()
        )

        required_anchor_counts = (
            anchor_activities
            .group_by(["demand_group_id", "activity_seq_id", "time_seq_id"])
            .agg(required_anchor_activities=pl.col("activity").n_unique())
        )

        sampled_anchors = (
            anchor_activities
            .join(
                destination_probability_lf,
                left_on=["home_zone_id", "activity"],
                right_on=["from", "activity"],
            )
            .with_columns(
                noise=(
                    pl.struct(["demand_group_id", "activity_seq_id", "time_seq_id", "activity", "to"])
                    .hash(seed=seed)
                    .cast(pl.Float64)
                    .truediv(pl.lit(18446744073709551616.0))
                    .log()
                    .neg()
                )
            )
            .with_columns(
                sample_score=(
                    pl.col("noise") / pl.col("p_ij").clip(1e-18)
                    + pl.col("to").cast(pl.Float64) * 1e-18
                )
            )
            .sort(["demand_group_id", "activity_seq_id", "time_seq_id", "activity", "sample_score", "to"])
            .with_columns(
                dest_draw_id=pl.col("to").cum_count().over(
                    ["demand_group_id", "activity_seq_id", "time_seq_id", "activity"]
                )
                .cast(pl.UInt32)
            )
            .filter(pl.col("dest_draw_id") <= self.parameters.k_destination_sequences)
            .select(["demand_group_id", "activity_seq_id", "time_seq_id", "activity", "dest_draw_id", "to"])
        )

        valid_anchor_draws = (
            sampled_anchors
            .group_by(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"])
            .agg(sampled_anchor_activities=pl.col("activity").n_unique())
            .join(required_anchor_counts, on=["demand_group_id", "activity_seq_id", "time_seq_id"], how="left")
            .filter(pl.col("sampled_anchor_activities") == pl.col("required_anchor_activities"))
            .select(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"])
        )

        all_sequences = (
            chains_lf
            .select(["demand_group_id", "home_zone_id", "activity_seq_id", "time_seq_id"])
            .unique()
        )
        destination_draw_ids = pl.DataFrame(
            {"dest_draw_id": list(range(1, int(self.parameters.k_destination_sequences) + 1))},
            schema={"dest_draw_id": pl.UInt32},
        ).lazy()
        sequences_without_non_home_anchor = (
            all_sequences
            .join(
                required_anchor_counts.select(["demand_group_id", "activity_seq_id", "time_seq_id"]),
                on=["demand_group_id", "activity_seq_id", "time_seq_id"],
                how="anti",
            )
            .join(destination_draw_ids, how="cross")
        )
        sequences_with_valid_draws = all_sequences.join(
            valid_anchor_draws,
            on=["demand_group_id", "activity_seq_id", "time_seq_id"],
            how="inner",
        )
        sequence_draws = pl.concat(
            [sequences_with_valid_draws, sequences_without_non_home_anchor],
            how="vertical_relaxed",
        )
        return (
            chains_lf
            .join(
                sequence_draws.select(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"]),
                on=["demand_group_id", "activity_seq_id", "time_seq_id"],
                how="inner",
            )
            .join(
                sampled_anchors.rename({"to": "anchor_to"}),
                on=["demand_group_id", "activity_seq_id", "time_seq_id", "activity", "dest_draw_id"],
                how="left",
            )
            .with_columns(
                anchor_to=pl.when(pl.col("activity") == "home")
                .then(pl.col("home_zone_id"))
                .otherwise(pl.col("anchor_to"))
            )
            .sort(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "seq_step_index"])
            .with_columns(
                anchor_to=pl.col("anchor_to").backward_fill().over(
                    ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"]
                )
            )
            .collect(engine="streaming")
        )


    def _spatialize_other_activities(
        self,
        chains: pl.DataFrame,
        destination_probability: pl.DataFrame,
        costs: pl.DataFrame,
        alpha: float,
        seed: int,
    ) -> pl.DataFrame:
        """Sample destinations for non-anchor activities step by step."""
        logging.info("Spatializing other activities...")
        chains_step = (
            chains
            .filter(pl.col("seq_step_index") == 1)
            .with_columns(pl.col("home_zone_id").alias("from"))
        )
        seq_step_index = 1
        spatialized_chains: list[pl.DataFrame] = []
        while chains_step.height > 0:
            logging.info("Spatializing step %s...", str(seq_step_index))
            spatialized_step = (
                self._spatialize_trip_chain_step(
                    seq_step_index,
                    chains_step,
                    destination_probability,
                    costs,
                    alpha,
                    seed,
                )
                .with_columns(seq_step_index=pl.lit(seq_step_index).cast(pl.UInt8))
            )
            spatialized_chains.append(spatialized_step)
            seq_step_index += 1
            chains_step = (
                chains.lazy()
                .filter(pl.col("seq_step_index") == seq_step_index)
                .join(
                    spatialized_step.lazy()
                    .select(["demand_group_id", "home_zone_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "to"])
                    .rename({"to": "from"}),
                    on=["demand_group_id", "home_zone_id", "activity_seq_id", "time_seq_id", "dest_draw_id"],
                )
                .collect(engine="streaming")
            )
        return pl.concat(spatialized_chains)


    def _spatialize_trip_chain_step(
        self,
        seq_step_index: int,
        chains_step: pl.DataFrame,
        destination_probability: pl.DataFrame,
        costs: pl.DataFrame,
        alpha: float,
        seed: int,
    ) -> pl.DataFrame:
        """Sample destinations for one step between anchors."""
        chains_step_lf = chains_step.lazy()
        destination_probability_lf = destination_probability.lazy()
        costs_lf = costs.lazy()

        # Start from the base destination probabilities for non-anchor activities.
        # These probabilities come from the radiation model and only depend on the
        # current origin, the activity, and destination opportunities.
        non_anchor_candidates = (
            chains_step_lf
            .filter(pl.col("is_anchor").not_())
            .join(destination_probability_lf, on=["from", "activity"])
        )

        # Attach the two legs that define the chain cost of visiting a candidate
        # destination before continuing to the next anchor destination.
        candidates_with_costs = (
            non_anchor_candidates
            .join(
                costs_lf
                .select(
                    [
                        pl.col("from").alias("candidate_from"),
                        pl.col("to").alias("candidate_to"),
                        pl.col("cost").alias("cost_to_candidate"),
                    ]
                ),
                left_on=["from", "to"],
                right_on=["candidate_from", "candidate_to"],
            )
            .join(
                costs_lf
                .select(
                    [
                        pl.col("from").alias("anchor_from"),
                        pl.col("to").alias("anchor_to_cost"),
                        pl.col("cost").alias("cost_to_anchor"),
                    ]
                ),
                left_on=["to", "anchor_to"],
                right_on=["anchor_from", "anchor_to_cost"],
            )
        )

        # Apply an additional chain-aware correction on top of the base radiation
        # probability. This is an empirical adjustment intended to reduce
        # unrealistic intermediate stops that would create costly onward travel to
        # the next anchor.
        weighted_candidates = (
            candidates_with_costs
            .with_columns(
                chain_cost_via_candidate=pl.col("cost_to_candidate") + pl.col("cost_to_anchor"),
            )
            .with_columns(
                p_ij=(
                    (
                        pl.col("p_ij").clip(1e-18).log()
                        - alpha * pl.col("chain_cost_via_candidate")
                    ).exp()
                ),
            )
        )

        # Draw one destination per sequence and destination draw using the seeded
        # exponential race sampling already used elsewhere in this asset.
        sampled_non_anchor_steps = (
            weighted_candidates
            .with_columns(
                noise=(
                    pl.struct(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "to"])
                    .hash(seed=seed)
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
                    ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"]
                )
            )
            .filter(pl.col("sample_score") == pl.col("min_score"))
            .select(
                [
                    "demand_group_id",
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
                ]
            )
        )
        steps = sampled_non_anchor_steps
        steps_anchor = (
            chains_step_lf
            .filter(pl.col("is_anchor"))
            .with_columns(to=pl.col("anchor_to"))
            .select(
                [
                    "demand_group_id",
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
                ]
            )
        )
        return pl.concat([steps, steps_anchor]).collect(engine="streaming")
