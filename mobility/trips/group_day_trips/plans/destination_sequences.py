import logging
import pathlib
from typing import Any

import polars as pl
from scipy.stats import norm

from .sequence_index import add_index
from mobility.runtime.assets.file_asset import FileAsset


class DestinationSequences(FileAsset):
    """Persist destination sequences produced for one GroupDayTrips iteration."""

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        activities: list[Any] | None = None,
        transport_zones: Any = None,
        remaining_opportunities: pl.DataFrame | None = None,
        chains: pl.DataFrame | None = None,
        demand_groups: pl.DataFrame | None = None,
        costs: pl.DataFrame | None = None,
        sequence_index_folder: pathlib.Path | None = None,
        parameters: Any = None,
        seed: int | None = None,
    ) -> None:
        self.activities = activities
        self.transport_zones = transport_zones
        self.remaining_opportunities = remaining_opportunities
        self.chains = chains
        self.demand_groups = demand_groups
        self.costs = costs
        self.sequence_index_folder = sequence_index_folder
        self.parameters = parameters
        self.seed = seed
        inputs = {
            "version": 1,
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
        if self.remaining_opportunities is None:
            raise ValueError("Cannot build destination sequences without remaining opportunities.")
        if self.chains is None:
            raise ValueError("Cannot build destination sequences without chains.")
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

        utilities = self._get_utilities(
            self.activities,
            self.transport_zones,
            self.remaining_opportunities,
            self.costs,
            self.parameters.cost_uncertainty_sd,
        )
        destination_probability = self._get_destination_probability(
            utilities,
            self.activities,
            self.parameters.dest_prob_cutoff,
        )
        chains = (
            self.chains
            .filter(pl.col("activity_seq_id") != 0)
            .join(self.demand_groups.select(["demand_group_id", "home_zone_id"]), on="demand_group_id")
            .select(["demand_group_id", "home_zone_id", "activity_seq_id", "activity", "is_anchor", "seq_step_index"])
        )
        chains = self._spatialize_anchor_activities(chains, destination_probability, self.seed)
        chains = self._spatialize_other_activities(
            chains,
            destination_probability,
            self.costs,
            self.parameters.alpha,
            self.seed,
        )

        destination_sequences = (
            chains
            .group_by(["demand_group_id", "activity_seq_id"])
            .agg(to=pl.col("to").sort_by("seq_step_index").cast(pl.Utf8()))
            .with_columns(to=pl.col("to").list.join("-"))
            .sort(["demand_group_id", "activity_seq_id"])
        )
        destination_sequences = add_index(
            destination_sequences,
            col="to",
            index_col_name="dest_seq_id",
            index_folder=self.sequence_index_folder,
        )
        destination_sequences = (
            chains
            .join(
                destination_sequences.select(["demand_group_id", "activity_seq_id", "dest_seq_id"]),
                on=["demand_group_id", "activity_seq_id"],
            )
            .drop(["home_zone_id", "activity"])
            .with_columns(iteration=pl.lit(self.iteration).cast(pl.UInt32))
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        destination_sequences.write_parquet(self.cache_path)
        return self.get_cached_asset()


    def _get_utilities(
        self,
        activities: list[Any],
        transport_zones: Any,
        opportunities: pl.DataFrame,
        costs: pl.DataFrame,
        cost_uncertainty_sd: float,
    ) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """Assemble destination utilities with cost uncertainty."""
        utilities = [(activity.name, activity.get_utilities(transport_zones)) for activity in activities]
        utilities = [utility for utility in utilities if utility[1] is not None]
        utilities = [utility[1].with_columns(activity=pl.lit(utility[0])) for utility in utilities]

        activity_values = opportunities.schema["activity"].categories
        utilities = (
            pl.concat(utilities)
            .with_columns(activity=pl.col("activity").cast(pl.Enum(activity_values)))
        )

        def offset_costs(costs_df: pl.DataFrame, delta: float, probability: float) -> pl.DataFrame:
            return costs_df.with_columns(
                [
                    (pl.col("cost") + delta).alias("cost"),
                    pl.lit(probability).alias("prob"),
                ]
            )

        x = [-2.0, -1.0, 0.0, 1.0, 2.0]
        probabilities = norm.pdf(x, loc=0.0, scale=cost_uncertainty_sd)
        probabilities /= probabilities.sum()

        costs = pl.concat(
            [offset_costs(costs, x[i], probabilities[i]) for i in range(len(probabilities))]
        )
        costs = (
            costs.lazy()
            .join(opportunities.lazy(), on="to")
            .join(utilities.lazy(), on=["activity", "to"], how="left")
            .with_columns(
                utility=pl.col("utility").fill_null(0.0),
                effective_sink=(
                    pl.col("opportunity_capacity")
                    * pl.col("k_saturation_utility").fill_null(1.0)
                    * pl.col("prob")
                ).clip(0.0),
            )
            .drop("prob")
            .filter(pl.col("effective_sink") > 0.0)
            .with_columns(cost_bin=(pl.col("cost") - pl.col("utility")).floor())
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
        utilities: tuple[pl.LazyFrame, pl.LazyFrame],
        activities: list[Any],
        destination_probability_cutoff: float,
    ) -> pl.DataFrame:
        """Compute destination probabilities from utilities."""
        logging.info(
            "Computing the probability of choosing a destination based on current location, potential destinations, and activity (with radiation models)..."
        )
        costs_by_bin = utilities[0]
        cost_bin_to_destination = utilities[1]
        activities_lambda = {
            activity.name: activity.inputs["parameters"].radiation_lambda for activity in activities
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
        spatialized_anchors = (
            chains
            .filter((pl.col("is_anchor")) & (pl.col("activity") != "home"))
            .select(["demand_group_id", "home_zone_id", "activity_seq_id", "activity"])
            .unique()
            .join(
                destination_probability,
                left_on=["home_zone_id", "activity"],
                right_on=["from", "activity"],
            )
            .with_columns(
                noise=(
                    pl.struct(["demand_group_id", "activity_seq_id", "activity", "to"])
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
            .with_columns(
                min_score=pl.col("sample_score").min().over(["demand_group_id", "activity_seq_id", "activity"])
            )
            .filter(pl.col("sample_score") == pl.col("min_score"))
            .select(["demand_group_id", "activity_seq_id", "activity", "to"])
        )
        return (
            chains
            .join(
                spatialized_anchors.rename({"to": "anchor_to"}),
                on=["demand_group_id", "activity_seq_id", "activity"],
                how="left",
            )
            .with_columns(
                anchor_to=pl.when(pl.col("activity") == "home")
                .then(pl.col("home_zone_id"))
                .otherwise(pl.col("anchor_to"))
            )
            .sort(["demand_group_id", "activity_seq_id", "seq_step_index"])
            .with_columns(anchor_to=pl.col("anchor_to").backward_fill().over(["demand_group_id", "activity_seq_id"]))
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
                .with_columns(seq_step_index=pl.lit(seq_step_index).cast(pl.UInt32))
            )
            spatialized_chains.append(spatialized_step)
            seq_step_index += 1
            chains_step = (
                chains
                .filter(pl.col("seq_step_index") == seq_step_index)
                .join(
                    spatialized_step
                    .select(["demand_group_id", "home_zone_id", "activity_seq_id", "to"])
                    .rename({"to": "from"}),
                    on=["demand_group_id", "home_zone_id", "activity_seq_id"],
                )
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
        steps = (
            chains_step
            .filter(pl.col("is_anchor").not_())
            .join(destination_probability, on=["from", "activity"])
            .join(costs, left_on=["to", "anchor_to"], right_on=["from", "to"])
            .with_columns(
                p_ij=((pl.col("p_ij").clip(1e-18).log() - alpha * pl.col("cost")).exp()),
                noise=(
                    pl.struct(["demand_group_id", "activity_seq_id", "to"])
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
            .with_columns(min_score=pl.col("sample_score").min().over(["demand_group_id", "activity_seq_id"]))
            .filter(pl.col("sample_score") == pl.col("min_score"))
            .select(["demand_group_id", "home_zone_id", "activity_seq_id", "activity", "anchor_to", "from", "to"])
        )
        steps_anchor = (
            chains_step
            .filter(pl.col("is_anchor"))
            .with_columns(to=pl.col("anchor_to"))
            .select(["demand_group_id", "home_zone_id", "activity_seq_id", "activity", "anchor_to", "from", "to"])
        )
        return pl.concat([steps, steps_anchor])
