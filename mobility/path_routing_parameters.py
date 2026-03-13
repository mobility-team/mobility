import warnings
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PathRoutingParameters(BaseModel):
    """Coarse OD prefilter parameters for path-based modes.

    `max_beeline_distance` is the distance, in km, used to
    keep candidate OD pairs before detailed routing.

    `filter_max_speed` and `filter_max_time` are deprecated legacy inputs for
    custom configurations. When both are provided, they are converted to
    `max_beeline_distance = filter_max_speed * filter_max_time`.
    """

    model_config = ConfigDict(extra="forbid")

    max_beeline_distance: Annotated[float | None, Field(default=None, gt=0.0)]  # km
    filter_max_speed: Annotated[float | None, Field(default=None, gt=0.0)]  # km/h
    filter_max_time: Annotated[float | None, Field(default=None, gt=0.0)]  # h

    @model_validator(mode="before")
    @classmethod
    def normalize_filter_definition(cls, data: Any) -> Any:

        normalized = dict(data)

        if normalized.get("max_beeline_distance") is not None:
            normalized["filter_max_speed"] = None
            normalized["filter_max_time"] = None
            return normalized

        filter_max_speed = normalized.get("filter_max_speed")
        filter_max_time = normalized.get("filter_max_time")

        if filter_max_speed is not None and filter_max_time is not None:
            warnings.warn(
                (
                    "`filter_max_speed` and `filter_max_time` are deprecated for "
                    "PathRoutingParameters; use `max_beeline_distance` instead."
                ),
                DeprecationWarning,
                stacklevel=2,
            )
            normalized["max_beeline_distance"] = filter_max_speed * filter_max_time
            normalized["filter_max_speed"] = None
            normalized["filter_max_time"] = None
            return normalized

        raise ValueError(
            "PathRoutingParameters requires `max_beeline_distance` or both "
            "`filter_max_speed` and `filter_max_time`."
        )
