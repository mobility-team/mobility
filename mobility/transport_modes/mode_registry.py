from typing import List

from mobility.transport_modes.transport_mode import TransportMode


class ModeRegistry:
    """Register transport modes by id."""

    def __init__(
        self,
        modes: List[TransportMode],
    ):
        """Build a mode registry from a list of transport mode instances.

        Args:
            modes: Available transport modes. Mode ids are derived from
                ``mode.inputs['parameters'].name`` and must be unique.

        Raises:
            ValueError: If the list is empty or contains duplicate mode names.
            TypeError: If a list item is not a ``TransportMode``.
        """
        if not modes:
            raise ValueError("ModeRegistry requires at least one mode.")

        self._modes = {}
        for mode in modes:
            if not isinstance(mode, TransportMode):
                raise TypeError(
                    "ModeRegistry expects TransportMode instances, "
                    f"got {type(mode)}."
                )
            mode_id = self._mode_id(mode)
            if mode_id in self._modes:
                raise ValueError(
                    f"Duplicate mode id '{mode_id}' in ModeRegistry input."
                )
            self._modes[mode_id] = mode

    def get(self, mode_id: str) -> TransportMode:
        """Return a registered mode by id."""
        if mode_id not in self._modes:
            raise ValueError(
                f"Unknown mode id '{mode_id}'. Available mode ids: {list(self._modes.keys())}."
            )
        return self._modes[mode_id]

    @staticmethod
    def _mode_id(mode: TransportMode) -> str:
        """Resolve mode id from TransportMode parameters."""
        try:
            return mode.inputs["parameters"].name
        except Exception as exc:
            raise ValueError(
                f"Could not resolve mode id for {type(mode)}. Expected "
                "`mode.inputs['parameters'].name`."
            ) from exc
