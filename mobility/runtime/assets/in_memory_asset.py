from typing import Any

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.resolver import (
    asset_resolution_context,
    get_current_asset_resolver,
)

class InMemoryAsset(Asset):
    """Asset whose value exists only for the active resolution context."""

    def __init__(self, inputs):
        super().__init__(inputs)

    def get_cached_hash(self):
        return self.inputs_hash

    def get(self, *args: Any, **kwargs: Any):
        """Return this computation's value from the active execution."""
        resolver = get_current_asset_resolver()
        if resolver is not None:
            return self._resolve_value(resolver, *args, **kwargs)

        with asset_resolution_context() as resolver:
            return self._resolve_value(resolver, *args, **kwargs)

    def _resolve_value(self, resolver, *args: Any, **kwargs: Any):
        return resolver.get_in_memory_asset(
            self,
            lambda: self.create_and_get_asset(*args, **kwargs),
            spill_to_disk=getattr(self, "spill_execution_value", None),
            load_from_disk=getattr(self, "load_execution_value", None),
        )

    def create_and_get_asset(self):
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement create_and_get_asset()."
        )
