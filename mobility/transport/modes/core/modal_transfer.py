from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Coordinates(BaseModel):
    """WGS84 coordinate used for transfer shortcuts."""

    model_config = ConfigDict(extra="forbid")

    lon: Annotated[float, Field(ge=-180.0, le=180.0)]
    lat: Annotated[float, Field(ge=-90.0, le=90.0)]


class IntermodalTransfer(BaseModel):
    """Parameters governing transfer between two transport modes."""

    model_config = ConfigDict(extra="forbid")

    # Max travel time from/to the connection nodes, in hours.
    max_travel_time: Annotated[float, Field(gt=0.0)]
    average_speed: Annotated[float, Field(gt=0.0)]  # km/h

    # Average transfer time between the two connected modes, in minutes.
    transfer_time: Annotated[float, Field(ge=0.0)]

    # Optional shortcuts to make some connections faster than average.
    shortcuts_transfer_time: Annotated[float | None, Field(default=None, ge=0.0)]
    shortcuts_locations: Annotated[list[Coordinates] | None, Field(default=None)]

    @field_validator("shortcuts_locations", mode="before")
    @classmethod
    def coerce_shortcuts_locations(
        cls, value: Any
    ) -> list[dict[str, float]] | list[Coordinates] | None:
        """Accept both {'lon','lat'} items and [lon, lat] / (lon, lat) pairs."""
        if value is None:
            return None

        if not isinstance(value, list):
            return value

        coerced: list[dict[str, float] | Coordinates] = []
        for item in value:
            if isinstance(item, (dict, Coordinates)):
                coerced.append(item)
                continue

            if (
                isinstance(item, (list, tuple))
                and len(item) == 2
                and all(isinstance(v, (int, float)) for v in item)
            ):
                coerced.append({"lon": float(item[0]), "lat": float(item[1])})
                continue

            coerced.append(item)

        return coerced


class WalkIntermodalTransfer(IntermodalTransfer):
    """Default walk <-> public transport transfer settings."""

    max_travel_time: Annotated[float, Field(default=20.0 / 60.0, gt=0.0)]
    average_speed: Annotated[float, Field(default=5.0, gt=0.0)]
    transfer_time: Annotated[float, Field(default=1.0, ge=0.0)]


class BicycleIntermodalTransfer(IntermodalTransfer):
    """Default bicycle <-> public transport transfer settings."""

    max_travel_time: Annotated[float, Field(default=20.0 / 60.0, gt=0.0)]
    average_speed: Annotated[float, Field(default=15.0, gt=0.0)]
    transfer_time: Annotated[float, Field(default=2.0, ge=0.0)]


class CarIntermodalTransfer(IntermodalTransfer):
    """Default car <-> public transport transfer settings."""

    max_travel_time: Annotated[float, Field(default=20.0 / 60.0, gt=0.0)]
    average_speed: Annotated[float, Field(default=50.0, gt=0.0)]
    transfer_time: Annotated[float, Field(default=15.0, ge=0.0)]


def default_intermodal_transfer_for_mode(
    mode_name: str, vehicle: str | None = None
) -> IntermodalTransfer:
    """Build default intermodal transfer parameters for a leg mode.

    Args:
        mode_name: Leg mode name.
        vehicle: Optional vehicle type from mode parameters.

    Returns:
        Default transfer parameters matching the mode.

    Raises:
        ValueError: If no default mapping exists for the provided mode.
    """
    if vehicle == "car" or mode_name == "car":
        return CarIntermodalTransfer()

    if vehicle == "bicycle" or mode_name == "bicycle":
        return BicycleIntermodalTransfer()

    if mode_name == "walk":
        return WalkIntermodalTransfer()

    raise ValueError(
        f"No default IntermodalTransfer is defined for mode '{mode_name}'"
        + (f" (vehicle='{vehicle}')." if vehicle is not None else ".")
    )
