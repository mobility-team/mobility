from __future__ import annotations

from mobility.runtime.assets.file_asset import FileAsset


class TravelCostsAsset(FileAsset):
    """Base class for per-mode travel-cost assets with congestion helpers."""

    def asset_for_road_flows(self, road_flow_asset):
        """Return the effective asset for one road-flow asset."""
        return self

    def remove_congestion_artifacts(self, road_flow_asset) -> None:
        """Remove congestion-derived artifacts owned by this asset."""
        variant = self.asset_for_road_flows(road_flow_asset)
        if variant is not None and variant is not self:
            variant.remove()
