from __future__ import annotations

from mobility.runtime.assets.file_asset import FileAsset


class TravelCostsAsset(FileAsset):
    """Base class for per-mode travel-cost assets with congestion helpers."""

    def asset_for_congestion_state(self, congestion_state):
        """Return the effective asset for one congestion state."""
        return self

    def remove_congestion_artifacts(self, congestion_state) -> None:
        """Remove congestion-derived artifacts owned by this asset."""
        variant = self.asset_for_congestion_state(congestion_state)
        if variant is not None and variant is not self:
            variant.remove()
