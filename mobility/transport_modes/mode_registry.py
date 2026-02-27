from typing import List

from mobility.transport_modes.transport_mode import TransportMode


class ModeRegistry:
    """Register transport modes by id (``mode.name``)."""

    def __init__(
        self,
        modes: List[TransportMode],
    ):
        """Build a mode registry from a list of transport mode instances.

        Args:
            modes: Available transport modes. Mode ids are derived from
                ``mode.name`` and must be unique.

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
            if mode.name in self._modes:
                raise ValueError(
                    f"Duplicate mode id '{mode.name}' in ModeRegistry input."
                )
            self._modes[mode.name] = mode

    def get(self, mode_id: str) -> TransportMode:
        """Return a registered mode by id."""
        if mode_id not in self._modes:
            raise ValueError(
                f"Unknown mode id '{mode_id}'. Available mode ids: {list(self._modes.keys())}."
            )
        return self._modes[mode_id]
