from __future__ import annotations

import logging
import os
import pathlib

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import SensitivityCase
from mobility.runtime.population_segments import (
    PopulationSegment,
    population_segment_defaults,
    resolve_population_segment_values,
)
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.costs.road_flow_manager import RoadFlowManager
from mobility.transport.costs.travel_costs_asset import TravelCostsBase


class TransportCosts(FileAsset):
    """Canonical multimodal transport-cost asset for one run state."""

    def __init__(
        self,
        modes,
        *,
        congestion: bool = False,
        road_flow_asset: VehicleODFlowsAsset | None = None,
    ):
        """Initialize the transport-cost asset.

        Args:
            modes: Transport modes contributing generalized costs.
            congestion: Whether this asset should build congested costs.
            road_flow_asset: Road vehicle flows applied to congested modes.
        """
        self.modes = modes
        self.road_flows = RoadFlowManager(self)
        inputs = {
            mode.inputs["parameters"].name: mode.inputs["generalized_cost"] for mode in modes
        }
        inputs["version"] = 1
        inputs["congestion"] = bool(congestion)
        inputs["road_flow_asset"] = road_flow_asset

        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
            / "transport_costs"
            / "transport_costs.parquet"
        )
        super().__init__(inputs, cache_path)

    def for_iteration(
        self,
        iteration: int,
        scenario: str | None = None,
        sensitivity_case: SensitivityCase | None = None,
    ) -> "TransportCosts":
        """Return the static transport-cost variant for one iteration.

        Args:
            iteration: One-based simulation iteration.
            scenario: Optional scenario name used to resolve scenario-varying
                parameter values.

        Returns:
            A transport-cost asset whose modes have been resolved for the given
            iteration, before any run-specific congestion state is applied.
        """

        resolved_modes = [
            mode.for_iteration(
                iteration,
                scenario=scenario,
                sensitivity_case=sensitivity_case,
            )
            for mode in self.modes
        ]
        return TransportCosts(
            resolved_modes,
            congestion=self.inputs["congestion"],
            road_flow_asset=self.inputs["road_flow_asset"],
        )

    def asset_for_congestion(self, congestion: bool) -> "TransportCosts":
        """Return the transport-cost variant for a congestion flag.

        Args:
            congestion: Whether to return the congested or free-flow variant.

        Returns:
            A transport-cost asset configured for the requested congestion mode.
        """
        return TransportCosts(
            self.modes,
            congestion=bool(congestion),
            road_flow_asset=None,
        )

    def asset_for_road_flows(
        self,
        road_flow_asset: VehicleODFlowsAsset | None,
    ) -> "TransportCosts":
        """Return the transport-cost variant for explicit road vehicle flows."""
        return TransportCosts(
            self.modes,
            congestion=(road_flow_asset is not None),
            road_flow_asset=road_flow_asset,
        )

    def get_cached_asset(self) -> pl.DataFrame:
        """Return the persisted canonical full-detail cost table.

        Returns:
            The cached multimodal cost table for this asset variant.
        """
        logging.debug("Transport costs already prepared. Reusing the file : %s", str(self.cache_path))
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build and persist the canonical full-detail cost table.

        Returns:
            The newly built multimodal cost table for this asset variant.
        """
        costs = self._build_full_detail_costs()
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        costs.write_parquet(self.cache_path)
        return costs

    def _build_full_detail_costs(self) -> pl.DataFrame:
        """Build the canonical OD-by-mode cost table."""
        costs = []

        # Put the car first so that road congestion is computed first.
        modes = sorted(self.modes, key=lambda mode: mode.inputs["parameters"].name != "car")

        for mode in modes:
            generalized_cost = mode.inputs["generalized_cost"]
            generalized_cost = self._resolved_generalized_cost(
                generalized_cost,
                memberships=set(),
                population_segments=[],
                use_defaults=True,
            )
            gc = pl.DataFrame(
                generalized_cost.get(
                    ["cost", "distance", "time"],
                    congestion=self.inputs["congestion"],
                    detail_distances=True,
                    road_flow_asset=self.inputs["road_flow_asset"],
                )
            )
            costs.append(gc)

        costs = pl.concat(costs, how="diagonal")

        dist_cols = [col for col in costs.columns if "_distance" in col]
        if dist_cols:
            dist_cols = {col: pl.col(col).fill_null(0.0) for col in dist_cols}
            costs = costs.with_columns(**dist_cols)

        costs = costs.with_columns(
            ghg_emissions_per_trip=self._ghg_emissions_per_trip_expr(costs.columns, modes)
        )

        return costs.with_columns(
            pl.col("from").cast(pl.Int32),
            pl.col("to").cast(pl.Int32),
        )

    def get_utility_profiles(
        self,
        demand_groups: pl.DataFrame,
        population_segments: list[PopulationSegment],
    ) -> tuple[pl.DataFrame, dict[int, list[InMemoryAsset]]]:
        """Assign compact generalized-cost profiles to demand subgroups.

        Demand subgroups with identical resolved parameters share one profile.
        The returned table preserves the demand-unit columns and adds
        ``utility_profile_id``.
        """
        required = {
            "demand_group_id",
            "demand_subgroup_id",
            "population_segments",
        }
        missing = sorted(required - set(demand_groups.columns))
        if missing:
            raise ValueError(
                "Utility profiles need demand-group columns: " + ", ".join(missing)
            )

        profile_ids: dict[tuple[str, ...], int] = {}
        profile_id_by_memberships: dict[tuple[str, ...], int] = {}
        profiles: dict[int, list[InMemoryAsset]] = {}
        assignments = []
        rows = demand_groups.select(sorted(required)).iter_rows(named=True)
        for row in rows:
            memberships_key = tuple(sorted(set(row["population_segments"] or [])))
            profile_id = profile_id_by_memberships.get(memberships_key)
            if profile_id is None:
                resolved_assets = [
                    self._resolved_generalized_cost(
                        mode.inputs["generalized_cost"],
                        memberships=set(memberships_key),
                        population_segments=population_segments,
                    )
                    for mode in self.modes
                ]
                profile_key = tuple(asset.inputs_hash for asset in resolved_assets)
                profile_id = profile_ids.get(profile_key)
                if profile_id is None:
                    profile_id = len(profile_ids)
                    profile_ids[profile_key] = profile_id
                    profiles[profile_id] = resolved_assets
                profile_id_by_memberships[memberships_key] = profile_id
            assignments.append(
                {
                    "demand_group_id": row["demand_group_id"],
                    "demand_subgroup_id": row["demand_subgroup_id"],
                    "utility_profile_id": profile_id,
                }
            )

        assignment_schema = {
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "utility_profile_id": pl.UInt32,
        }
        return pl.DataFrame(assignments, schema=assignment_schema), profiles

    def get_profile_costs_by_od_and_mode(
        self,
        profiles: dict[int, list[InMemoryAsset]],
        metrics: list[str],
    ) -> pl.DataFrame:
        """Compute OD-by-mode costs once for every distinct utility profile."""
        profile_costs = []
        default_generalized_costs = [
            self._resolved_generalized_cost(
                mode.inputs["generalized_cost"],
                memberships=set(),
                population_segments=[],
                use_defaults=True,
            )
            for mode in self.modes
        ]
        default_costs = self.get_costs_by_od_and_mode(
            metrics,
            detail_distances=False,
        )

        # Cache by generalized-cost asset, not by population profile. Profiles
        # that inherit one mode's default coefficients reuse its existing OD
        # rows, and profiles with the same override compute that mode only once.
        costs_by_generalized_cost = {
            generalized_cost.inputs_hash: default_costs
            .filter(pl.col("mode") == mode.inputs["parameters"].name)
            .drop("mode")
            for mode, generalized_cost in zip(
                self.modes,
                default_generalized_costs,
            )
        }
        for profile_id, generalized_costs in profiles.items():
            for mode, generalized_cost in zip(self.modes, generalized_costs):
                costs = costs_by_generalized_cost.get(generalized_cost.inputs_hash)
                if costs is None:
                    costs = pl.DataFrame(
                        generalized_cost.get(
                            list(metrics),
                            congestion=self.inputs["congestion"],
                            detail_distances=False,
                            road_flow_asset=self.inputs["road_flow_asset"],
                        )
                    )
                    costs_by_generalized_cost[generalized_cost.inputs_hash] = costs
                profile_costs.append(
                    costs.with_columns(
                        utility_profile_id=pl.lit(profile_id, dtype=pl.UInt32),
                        mode=pl.lit(mode.inputs["parameters"].name),
                    )
                )
        return pl.concat(profile_costs, how="diagonal")

    @staticmethod
    def _resolved_generalized_cost(
        generalized_cost: InMemoryAsset,
        *,
        memberships: set[str],
        population_segments: list[PopulationSegment],
        use_defaults: bool = False,
    ) -> InMemoryAsset:
        inputs = (
            population_segment_defaults(generalized_cost.inputs)
            if use_defaults
            else resolve_population_segment_values(
                generalized_cost.inputs,
                memberships=memberships,
                segments=population_segments,
            )
        )
        if inputs == generalized_cost.inputs:
            return generalized_cost
        clone = generalized_cost.__class__.__new__(generalized_cost.__class__)
        clone.__dict__ = dict(generalized_cost.__dict__)
        clone.inputs = inputs
        clone.inputs_hash = clone.compute_inputs_hash()
        for name, value in inputs.items():
            setattr(clone, name, value)
        return clone

    def _ghg_emissions_per_trip_expr(self, columns: list[str], modes) -> pl.Expr:
        """Compute per-trip GHG emissions from detailed distance columns."""
        expressions = []

        for mode in modes:
            params = mode.inputs["parameters"]
            if params.multimodal:
                distance_column = "public_transport_distance"
            elif params.name == "carpool":
                distance_column = "carpooling_distance"
            else:
                distance_column = f"{params.name}_distance"

            if distance_column not in columns:
                continue

            expressions.append(pl.col(distance_column) * float(params.ghg_intensity))

        if not expressions:
            return pl.lit(0.0)

        total = expressions[0]
        for expression in expressions[1:]:
            total = total + expression
        return total

    def get_costs_by_od_and_mode(
        self,
        metrics: list,
        detail_distances: bool = False,
    ) -> pl.DataFrame:
        """Project the canonical table to one OD-by-mode cost view.

        Args:
            metrics: Metrics to include in the projected table.
            detail_distances: Whether to include detailed distance columns.

        Returns:
            An OD-by-mode table with the requested metrics.
        """
        metrics = list(metrics)
        costs = FileAsset.get(self)

        dist_cols = [col for col in costs.columns if col.endswith("_distance")]
        selected_metrics = [metric for metric in metrics if metric != "ghg_emissions"]
        if (
            ("ghg_emissions" in metrics or "ghg_emissions_per_trip" in metrics)
            and "ghg_emissions_per_trip" not in selected_metrics
        ):
            selected_metrics.append("ghg_emissions_per_trip")
        if detail_distances:
            selected_metrics.extend(dist_cols)

        selected_metrics = list(dict.fromkeys(selected_metrics))
        columns = ["from", "to", "mode"] + selected_metrics

        available_columns = [column for column in columns if column in costs.columns]
        return costs.select(available_columns)

    def get_costs_by_od(
        self,
        metrics: list,
    ) -> pl.DataFrame:
        """Aggregate the canonical table to one OD-only expected-cost view.

        Args:
            metrics: Metrics required to compute the OD aggregation.

        Returns:
            An OD-only expected generalized-cost table.
        """
        costs = self.get_costs_by_od_and_mode(metrics, detail_distances=False)
        costs = costs.with_columns((pl.col("cost").neg().exp()).alias("prob"))
        costs = costs.with_columns(
            (pl.col("prob") / pl.col("prob").sum().over(["from", "to"])).alias("prob")
        )
        costs = costs.with_columns((pl.col("prob") * pl.col("cost")).alias("cost"))
        return costs.group_by(["from", "to"]).agg(pl.col("cost").sum())

    def get_prob_by_od_and_mode(
        self,
        metrics: list,
    ):
        """Return mode probabilities for each OD pair.

        Args:
            metrics: Metrics required to compute the mode probabilities.

        Returns:
            An OD-by-mode probability table.
        """
        costs = FileAsset.get(self)

        prob = (
            costs
            .with_columns(exp_u=pl.col("cost").neg().exp())
            .with_columns(prob=pl.col("exp_u") / pl.col("exp_u").sum().over(["from", "to"]))
            .sort(["prob"], descending=True)
            .with_columns(
                prob_cum=pl.col("prob").cum_sum().over(["from", "to"]),
                p_count=pl.col("prob").cum_count().over(["from", "to"]),
            )
            .with_columns(
                prob_cum=pl.col("prob_cum").shift(1, fill_value=0.0).over(["from", "to"])
            )
            .filter((pl.col("prob_cum") < 0.999))
            .with_columns(prob=pl.col("prob") / pl.col("prob").sum().over(["from", "to"]))
            .select(["from", "to", "mode", "prob"])
        )

        return prob

    def build_road_flow_asset(self, person_od_flows_by_mode) -> VehicleODFlowsAsset | None:
        """Build and persist road vehicle flows from current person OD flows."""
        return self.road_flows.build(person_od_flows_by_mode)

    def has_enabled_congestion(self) -> bool:
        """Return whether any mode uses congestion-sensitive costs.

        Returns:
            `True` when at least one mode is congestion-enabled.
        """
        return any(mode.inputs["parameters"].congestion for mode in self.modes)

    def should_recompute_congested_costs(self, iteration: int, update_interval: int) -> bool:
        """Return whether congestion should be recomputed at one iteration.

        Args:
            iteration: One-based simulation iteration.
            update_interval: Number of iterations between congestion updates.

        Returns:
            `True` when the iteration triggers a congestion refresh.
        """
        return update_interval > 0 and (iteration - 1) % update_interval == 0

    def remove_congestion_artifacts(
        self,
        road_flow_asset: VehicleODFlowsAsset,
    ) -> None:
        """Remove congestion-derived artifacts owned by this transport-cost state."""
        variant = self.asset_for_road_flows(road_flow_asset)
        if variant is not self:
            variant.remove()

        for mode in self.modes:
            travel_costs = mode.inputs.get("travel_costs")
            if travel_costs is None or isinstance(travel_costs, TravelCostsBase) is False:
                continue
            travel_costs.remove_congestion_artifacts(road_flow_asset)
