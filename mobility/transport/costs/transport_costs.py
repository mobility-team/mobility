from __future__ import annotations

import logging
import os
import pathlib
from collections.abc import Iterator

import polars as pl

from mobility.transport.costs.congestion_state_manager import CongestionStateManager
from mobility.runtime.assets.file_asset import FileAsset
from mobility.transport.costs.congestion_state import CongestionState
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.costs.travel_costs_asset import TravelCostsAsset


class TransportCosts(FileAsset):
    """Canonical multimodal transport-cost asset for one run state."""

    def __init__(
        self,
        modes,
        *,
        congestion: bool = False,
        congestion_state: CongestionState | None = None,
    ):
        """Initialize the transport-cost asset.

        Args:
            modes: Transport modes contributing generalized costs.
            congestion: Whether this asset should build congested costs.
            congestion_state: Persisted congestion state applied to congested
                modes when building this asset variant.
        """
        self.modes = modes
        self.congestion_states = CongestionStateManager(self)
        inputs = {
            mode.inputs["parameters"].name: mode.inputs["generalized_cost"] for mode in modes
        }
        inputs["version"] = 1
        inputs["congestion"] = bool(congestion)
        inputs["congestion_state"] = congestion_state

        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
            / "transport_costs"
            / "transport_costs.parquet"
        )
        super().__init__(inputs, cache_path)

    def for_iteration(self, iteration: int) -> "TransportCosts":
        """Return the static transport-cost variant for one iteration.

        Args:
            iteration: One-based simulation iteration.

        Returns:
            A transport-cost asset whose modes have been resolved for the given
            iteration, before any run-specific congestion state is applied.
        """

        resolved_modes = [
            mode.for_iteration(iteration) if hasattr(mode, "for_iteration") else mode
            for mode in self.modes
        ]
        return TransportCosts(
            resolved_modes,
            congestion=self.inputs["congestion"],
            congestion_state=self.inputs["congestion_state"],
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
            congestion_state=None,
        )

    def asset_for_congestion_state(
        self,
        congestion_state: CongestionState | None,
    ) -> "TransportCosts":
        """Return the transport-cost variant for an explicit congestion state.

        Args:
            congestion_state: Persisted congestion state to apply.

        Returns:
            A transport-cost asset bound to the provided congestion state.
        """
        return TransportCosts(
            self.modes,
            congestion=(congestion_state is not None),
            congestion_state=congestion_state,
        )

    def asset_for_iteration(self, run, iteration: int) -> "TransportCosts":
        """Return the transport-cost asset used at one run iteration.

        Args:
            run: PopulationGroupDayTrips run providing run identity and parameters.
            iteration: One-based simulation iteration.

        Returns:
            The transport-cost asset used as input for the requested iteration.

        Raises:
            ValueError: If the iteration is outside the run bounds.
        """
        if iteration < 1:
            raise ValueError("Iteration should be >= 1.")
        if iteration > int(run.parameters.n_iterations):
            raise ValueError(
                f"Iteration should be <= {int(run.parameters.n_iterations)} for this run."
            )

        asset = self.for_iteration(int(iteration))

        congestion_state = asset.load_congestion_state(
            run_key=run.inputs_hash,
            is_weekday=run.is_weekday,
            last_completed_iteration=iteration - 1,
            cost_update_interval=run.parameters.n_iter_per_cost_update,
        )
        logging.info(
            "TransportCosts asset_for_iteration: run_key=%s is_weekday=%s iteration=%s "
            "last_completed_iteration=%s congestion_state_iteration=%s congestion_enabled=%s",
            run.inputs_hash,
            str(run.is_weekday),
            str(iteration),
            str(iteration - 1),
            (
                str(congestion_state.iteration)
                if congestion_state is not None
                else "none"
            ),
            str(congestion_state is not None),
        )
        return asset.asset_for_congestion_state(congestion_state)

    def get_for_iteration(self, run, iteration: int):
        """Materialize the transport costs used at one run iteration.

        Args:
            run: PopulationGroupDayTrips run providing run identity and parameters.
            iteration: One-based simulation iteration.

        Returns:
            The canonical full-detail cost table used at the requested
            simulation iteration.
        """
        return self.asset_for_iteration(run, iteration).get()

    def get_cached_asset(self) -> pl.DataFrame:
        """Return the persisted canonical full-detail cost table.

        Returns:
            The cached multimodal cost table for this asset variant.
        """
        logging.info("Transport costs already prepared. Reusing the file : %s", str(self.cache_path))
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
            gc = pl.DataFrame(
                generalized_cost.get(
                    ["cost", "distance", "time"],
                    congestion=self.inputs["congestion"],
                    detail_distances=True,
                    congestion_state=self.inputs["congestion_state"],
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

    def build_congestion_state(self, od_flows_by_mode, *, run_key=None, is_weekday=None, iteration=None):
        """Build and persist a congestion state from current OD flows.

        Args:
            od_flows_by_mode: Per-mode OD flows aggregated from current plans.
            run_key: Unique identifier of the run owning the state.
            is_weekday: Whether the run is the weekday variant.
            iteration: Simulation iteration that produced the OD flows.

        Returns:
            The persisted congestion state, or `None` when no congestion state
            should be produced from the provided flows.
        """
        return self.congestion_states.build(
            od_flows_by_mode,
            run_key=run_key,
            is_weekday=is_weekday,
            iteration=iteration,
        )

    def load_congestion_state(
        self,
        *,
        run_key,
        is_weekday,
        last_completed_iteration: int,
        cost_update_interval: int,
    ) -> CongestionState | None:
        """Load the congestion state active after the completed run history.

        Args:
            run_key: Unique identifier of the run owning the state.
            is_weekday: Whether the run is the weekday variant.
            last_completed_iteration: Latest completed simulation iteration.
            cost_update_interval: Number of iterations between congestion updates.

        Returns:
            The latest persisted congestion state compatible with the provided
            completed run history, or `None` when no congestion state exists yet.
        """
        return self.congestion_states.load(
            run_key=run_key,
            is_weekday=is_weekday,
            last_completed_iteration=last_completed_iteration,
            cost_update_interval=cost_update_interval,
        )

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
        congestion_state: CongestionState,
    ) -> None:
        """Remove congestion-derived artifacts owned by this transport-cost state."""
        variant = self.asset_for_congestion_state(congestion_state)
        if variant is not self:
            variant.remove()

        for mode in self.modes:
            travel_costs = mode.inputs.get("travel_costs")
            if travel_costs is None or isinstance(travel_costs, TravelCostsAsset) is False:
                continue
            travel_costs.remove_congestion_artifacts(congestion_state)

    def iter_run_congestion_artifacts(
        self,
        run,
    ) -> Iterator[tuple["TransportCosts", CongestionState, dict[str, VehicleODFlowsAsset]]]:
        """Yield persisted congestion states and flow assets for one run."""
        if self.has_enabled_congestion() is False:
            return

        update_interval = run.parameters.n_iter_per_cost_update
        if update_interval == 0:
            return

        for completed_iteration in range(1, int(run.parameters.n_iterations) + 1):
            if self.should_recompute_congested_costs(completed_iteration, update_interval) is False:
                continue

            next_transport_costs = self.for_iteration(completed_iteration + 1)
            flow_assets_by_mode = {}
            for mode in next_transport_costs.congestion_states._iter_congestion_enabled_modes():
                mode_name = mode.inputs["parameters"].name
                flow_asset = VehicleODFlowsAsset.from_inputs(
                    run_key=run.inputs_hash,
                    is_weekday=run.is_weekday,
                    iteration=completed_iteration,
                    mode_name=mode_name,
                )
                if flow_asset.cache_path.exists():
                    flow_assets_by_mode[mode_name] = flow_asset

            if not flow_assets_by_mode:
                continue

            yield (
                next_transport_costs,
                CongestionState(
                    run_key=str(run.inputs_hash),
                    is_weekday=bool(run.is_weekday),
                    iteration=int(completed_iteration),
                    flow_assets_by_mode=flow_assets_by_mode,
                ),
                flow_assets_by_mode,
            )

    def get_costs_for_next_iteration(
        self,
        *,
        run,
        iteration: int,
        od_flows_by_mode,
    ):
        """Build next-iteration OD costs from current OD flows.

        Args:
            run: PopulationGroupDayTrips run providing run identity.
            iteration: One-based simulation iteration that just completed.
            od_flows_by_mode: Per-mode OD flows aggregated from current plans.

        Returns:
            The OD-only cost table to use as input for the next iteration.
        """
        congestion_state = self.build_congestion_state(
            od_flows_by_mode,
            run_key=run.inputs_hash,
            is_weekday=run.is_weekday,
            iteration=iteration,
        )
        next_asset = self.asset_for_congestion_state(congestion_state)
        return next_asset.get_costs_by_od(["cost", "distance"])
