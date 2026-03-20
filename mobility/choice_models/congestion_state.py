from dataclasses import dataclass

from mobility.transport_costs.od_flows_asset import VehicleODFlowsAsset


@dataclass(frozen=True)
class CongestionState:
    """Explicit run-owned congestion state for one iteration.

    This stores the persisted OD-flow assets that define the current congestion
    view for each congestion-enabled mode. Travel-cost readers can derive the
    appropriate snapshot artifacts directly from these assets without keeping
    hidden mutable pointers.
    """

    run_key: str
    is_weekday: bool
    iteration: int
    flow_assets_by_mode: dict[str, VehicleODFlowsAsset]

    def for_mode(self, mode_name: str) -> VehicleODFlowsAsset | None:
        """Return the persisted flow asset backing the given mode."""
        return self.flow_assets_by_mode.get(str(mode_name))
