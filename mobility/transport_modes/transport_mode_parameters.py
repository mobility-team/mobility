"""Pydantic parameter model for transport modes."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class TransportModeParameters(BaseModel):
    """Common parameters for transport mode definitions."""

    model_config = ConfigDict(extra="forbid")

    name: Annotated[
        str,
        Field(
            title="Mode name",
            description="Unique name used to identify the transport mode.",
        ),
    ]

    ghg_intensity: Annotated[
        float,
        Field(
            ge=0.0,
            title="GHG intensity",
            description="Greenhouse gas intensity in kgCO2e per passenger.km.",
            json_schema_extra={"unit": "kgCO2e/pass.km"},
        ),
    ]

    congestion: Annotated[
        bool,
        Field(
            default=False,
            title="Congestion enabled",
            description=(
                "Whether congestion feedback should be enabled for this mode "
                "when supported by travel cost computations."
            ),
        ),
    ]

    vehicle: Annotated[
        str | None,
        Field(
            default=None,
            title="Vehicle type",
            description="Vehicle family used by this mode, when relevant.",
        ),
    ]

    multimodal: Annotated[
        bool,
        Field(
            default=False,
            title="Multimodal",
            description="Whether this mode combines multiple sub-modes.",
        ),
    ]

    return_mode: Annotated[
        str | None,
        Field(
            default=None,
            title="Return mode",
            description="Optional return mode name for asymmetric multimodal chains.",
        ),
    ]

    survey_ids: Annotated[
        list[str] | None,
        Field(
            default=None,
            title="Survey IDs",
            description="Survey mode identifiers mapped to this transport mode.",
        ),
    ]


class CarModeParameters(TransportModeParameters):
    """Parameters for car mode."""

    name: Literal["car"] = "car"
    ghg_intensity: float = 0.218
    congestion: bool = False
    vehicle: Literal["car"] = "car"
    multimodal: bool = False
    return_mode: None = None
    survey_ids: list[str] = Field(
        default_factory=lambda: ["3.30", "3.31", "3.32", "3.33", "3.39"]
    )


class WalkModeParameters(TransportModeParameters):
    """Parameters for walk mode."""

    name: Literal["walk"] = "walk"
    ghg_intensity: float = 0.0
    congestion: bool = False
    vehicle: None = None
    multimodal: bool = False
    return_mode: None = None
    survey_ids: list[str] = Field(default_factory=lambda: ["1.10", "1.11", "1.13"])


class BicycleModeParameters(TransportModeParameters):
    """Parameters for bicycle mode."""

    name: Literal["bicycle"] = "bicycle"
    ghg_intensity: float = 0.00017
    congestion: bool = False
    vehicle: Literal["bicycle"] = "bicycle"
    multimodal: bool = False
    return_mode: None = None
    survey_ids: list[str] = Field(default_factory=lambda: ["2.20"])


class PublicTransportModeParameters(TransportModeParameters):
    """Parameters for public transport mode."""

    ghg_intensity: float = 0.05
    multimodal: bool = True
    survey_ids: list[str] = Field(
        default_factory=lambda: [
            "4.42",
            "4.43",
            "5.50",
            "5.51",
            "5.52",
            "5.53",
            "5.54",
            "5.55",
            "5.56",
            "5.57",
            "5.58",
            "5.59",
            "6.60",
            "6.61",
            "6.62",
            "6.63",
            "6.69",
        ]
    )


class CarpoolModeParameters(TransportModeParameters):
    """Parameters for carpool mode."""

    name: Literal["carpool"] = "carpool"
    ghg_intensity: float = 0.109
    multimodal: bool = True
    return_mode: Literal["carpool_return"] = "carpool_return"
    survey_ids: list[str] = Field(default_factory=list)
