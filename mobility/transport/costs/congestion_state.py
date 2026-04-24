from dataclasses import dataclass

from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset


@dataclass(frozen=True)
class CongestionState:
    """Explicit run-owned congestion state for one iteration.

    This stores the cached OD-flow assets that define the congestion view
    currently active for each congestion-enabled mode after one completed
    iteration. Travel-cost and graph assets do not need to store hidden mutable
    pointers to derived congested snapshots: they can rebuild the appropriate
    iteration-specific artifact directly from these cached flow assets.

    Attributes:
        run_key: Identifier of the owning PopulationGroupDayTrips run.
        is_weekday: Whether the state belongs to the weekday or weekend run.
        iteration: Iteration that produced the flow assets stored in this state.
        flow_assets_by_mode: Mapping from mode name to the cached OD-flow asset
            used to rebuild congestion-dependent artifacts for that mode.
    """

    run_key: str
    is_weekday: bool
    iteration: int
    flow_assets_by_mode: dict[str, VehicleODFlowsAsset]

    def for_mode(self, mode_name: str) -> VehicleODFlowsAsset | None:
        """Return the cached flow asset backing the given mode."""
        return self.flow_assets_by_mode.get(str(mode_name))
