from typing import List

from mobility.transport_modes.transport_mode import TransportMode


class ModeRegistry:
    """Register transport modes and default leg modes for public transport.

    Modes are indexed by ``mode.name``. This registry is used by
    ``PublicTransportMode`` to resolve access/egress leg modes when they are
    not passed explicitly.
    """

    def __init__(
        self,
        modes: List[TransportMode],
        pt_access_mode_id: str = "walk",
        pt_egress_mode_id: str = "walk",
    ):
        """Build a registry from a list of transport mode instances.

        Args:
            modes: Available transport modes. Mode ids are derived from
                ``mode.name`` and must be unique.
            pt_access_mode_id: Default mode id for first PT leg resolution.
            pt_egress_mode_id: Default mode id for last PT leg resolution.

        Raises:
            ValueError: If the list is empty, contains duplicate mode names, or
                PT default ids are not found in registered modes.
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

        self.pt_access_mode_id = pt_access_mode_id
        self.pt_egress_mode_id = pt_egress_mode_id

        if self.pt_access_mode_id not in self._modes:
            raise ValueError(
                "ModeRegistry pt_access_mode_id must reference a registered mode, "
                f"got '{self.pt_access_mode_id}'."
            )
        if self.pt_egress_mode_id not in self._modes:
            raise ValueError(
                "ModeRegistry pt_egress_mode_id must reference a registered mode, "
                f"got '{self.pt_egress_mode_id}'."
            )

    def get(self, mode_id: str) -> TransportMode:
        """Return a registered mode by id."""
        if mode_id not in self._modes:
            raise ValueError(
                f"Unknown mode id '{mode_id}'. Available mode ids: {list(self._modes.keys())}."
            )
        return self._modes[mode_id]
