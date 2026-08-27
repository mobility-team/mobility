import geopandas as gpd
import pathlib
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import (
    ParameterValue,
    SensitivityCase,
    SensitivityValue,
    resolve_parameter_values,
)
from mobility.spatial.osm import GeofabrikRegions, GeofabrikExtract, OSMCountryBorder
from mobility.spatial.transport_zones import TransportZones


VariableFloat = float | ParameterValue | SensitivityValue
VariableInt = int | ParameterValue | SensitivityValue
VariablePath = pathlib.Path | str | ParameterValue | SensitivityValue
VariableString = str | ParameterValue | SensitivityValue


class BorderCrossingSpeedModifierParameters(BaseModel):
    """Parameters used to change speeds at national borders."""

    model_config = ConfigDict(extra="forbid")

    max_speed: Annotated[VariableFloat, Field(default=30.0)]
    time_penalty: Annotated[VariableFloat, Field(default=0.0)]
    geofabrik_extract_date: Annotated[VariableString, Field(default="240101")]


class LimitedSpeedZonesModifierParameters(BaseModel):
    """Parameters used to apply a speed limit inside selected zones."""

    model_config = ConfigDict(extra="forbid")

    zones_geometry_file_path: VariablePath
    max_speed: Annotated[VariableFloat, Field(default=30.0)]


class RoadLaneNumberModifierParameters(BaseModel):
    """Parameters used to change the lane count inside selected zones."""

    model_config = ConfigDict(extra="forbid")

    zones_geometry_file_path: VariablePath
    lane_delta: Annotated[VariableInt, Field(default=0)]


class NewRoadModifierParameters(BaseModel):
    """Parameters used to add a road from a line geometry."""

    model_config = ConfigDict(extra="forbid")

    zones_geometry_file_path: VariablePath
    max_speed: Annotated[VariableFloat, Field(default=30.0)]
    capacity: Annotated[VariableFloat, Field(default=1800.0)]
    alpha: Annotated[VariableFloat, Field(default=0.15)]
    beta: Annotated[VariableFloat, Field(default=4.0)]


class SpeedModifier(InMemoryAsset):
    """Base class for road graph changes that can vary during a run."""

    def __init__(self, inputs: dict):
        super().__init__(inputs)
        parameters = inputs["parameters"]
        for field_name in parameters.__class__.model_fields:
            setattr(self, field_name, getattr(parameters, field_name))

    def for_iteration(
        self,
        iteration: int,
        scenario: str | None = None,
        sensitivity_case: SensitivityCase | None = None,
    ) -> "SpeedModifier":
        """Return this modifier with plain values for one run iteration."""
        parameters = self.inputs["parameters"]
        resolved_parameters = resolve_parameter_values(
            parameters,
            scenario=scenario,
            iteration=iteration,
            sensitivity_case=sensitivity_case,
        )
        if resolved_parameters == parameters:
            return self

        constructor_inputs = {
            name: value
            for name, value in self.inputs.items()
            if name != "parameters"
        }
        return self.__class__(
            **constructor_inputs,
            parameters=resolved_parameters,
        )

    def _check_parameters_are_resolved(self) -> None:
        """Fail clearly when a modifier is used before selecting run values."""
        unresolved_types = (ParameterValue, SensitivityValue)
        unresolved_fields = [
            field_name
            for field_name in self.parameters.__class__.model_fields
            if isinstance(getattr(self.parameters, field_name), unresolved_types)
        ]
        if unresolved_fields:
            raise ValueError(
                f"{self.__class__.__name__} has unresolved values for "
                f"{unresolved_fields}. Resolve it with for_iteration(...) "
                "before preparing the path graph."
            )


class BorderCrossingSpeedModifier(SpeedModifier):

    def __init__(
        self,
        transport_zones: TransportZones,
        max_speed: VariableFloat | None = None,
        time_penalty: VariableFloat | None = None,
        geofabrik_extract_date: VariableString | None = None,
        parameters: BorderCrossingSpeedModifierParameters | None = None,
    ):
        """Create a national-border speed modifier.

        Args:
            transport_zones: Transport zones whose study boundary is inspected.
            max_speed: Maximum border-crossing speed, in km/h.
            time_penalty: Fixed border-crossing time penalty, in minutes.
            geofabrik_extract_date: Geofabrik extract date in ``YYMMDD`` format.
            parameters: Optional pre-built parameter model.
        """
        self.modifier_type = "border_crossing"
        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=BorderCrossingSpeedModifierParameters,
            explicit_args={
                "max_speed": max_speed,
                "time_penalty": time_penalty,
                "geofabrik_extract_date": geofabrik_extract_date,
            },
            owner_name="BorderCrossingSpeedModifier",
        )
        inputs = {
            "transport_zones": transport_zones,
            "parameters": parameters,
        }
        super().__init__(inputs)

    def get(self):
        """Return the plain values consumed by the graph modification script."""
        self._check_parameters_are_resolved()
        transport_zones = self.inputs["transport_zones"]
        transport_zones.get()
        boundary = gpd.read_file(transport_zones.inputs["study_area"].cache_path["boundary"]).geometry[0]
        
        if boundary.is_valid is False:
            boundary = boundary.buffer(0.0)
            
        if boundary.is_valid is False:
            raise ValueError(
                """
                The boundary of the study area created by StudyArea is not 
                valid (and it could not be fixed with a 0.0 buffer).
                """
            )

        regions = GeofabrikRegions(
            extract_date=self.parameters.geofabrik_extract_date
        ).get()
        regions = regions[regions.intersects(boundary)]

        borders = []
        has_borders = False

        for _index, region in regions.iterrows():
            extract = GeofabrikExtract(region.url)
            border = OSMCountryBorder(extract)
            border_path = border.get()
            border_geometry = gpd.read_file(border_path)
            border_intersects_boundary = bool(
                not border_geometry.empty
                and border_geometry.intersects(boundary).any()
            )
            if border_intersects_boundary:
                borders.append(str(border_path))
                has_borders = True
            

        return {
            "modifier_type": self.modifier_type,
            "max_speed": self.parameters.max_speed,
            "time_penalty": self.parameters.time_penalty,
            "borders": borders,
            "has_borders": has_borders
        }


class LimitedSpeedZonesModifier(SpeedModifier):

    def __init__(
        self,
        zones_geometry_file_path: VariablePath | None = None,
        max_speed: VariableFloat | None = None,
        parameters: LimitedSpeedZonesModifierParameters | None = None,
    ):
        """Create a speed limit inside one or more geometries.

        Args:
            zones_geometry_file_path: GIS file containing the affected zones.
            max_speed: Maximum speed inside the zones, in km/h.
            parameters: Optional pre-built parameter model.
        """
        self.modifier_type = "limited_speed_zones"
        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=LimitedSpeedZonesModifierParameters,
            explicit_args={
                "zones_geometry_file_path": zones_geometry_file_path,
                "max_speed": max_speed,
            },
            required_fields=["zones_geometry_file_path"],
            owner_name="LimitedSpeedZonesModifier",
        )
        super().__init__({"parameters": parameters})

    def get(self):
        """Return the plain values consumed by the graph modification script."""
        self._check_parameters_are_resolved()
        return {
            "modifier_type": self.modifier_type,
            "max_speed": self.parameters.max_speed,
            "zones_geometry_file_path": str(
                self.parameters.zones_geometry_file_path
            ),
        }


class RoadLaneNumberModifier(SpeedModifier):

    def __init__(
        self,
        zones_geometry_file_path: VariablePath | None = None,
        lane_delta: VariableInt | None = None,
        parameters: RoadLaneNumberModifierParameters | None = None,
    ):
        """Create a lane count change inside one or more geometries.

        Args:
            zones_geometry_file_path: GIS file containing the affected zones.
            lane_delta: Number of lanes added or removed from affected roads.
            parameters: Optional pre-built parameter model.
        """
        self.modifier_type = "lane_number_modification"
        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=RoadLaneNumberModifierParameters,
            explicit_args={
                "zones_geometry_file_path": zones_geometry_file_path,
                "lane_delta": lane_delta,
            },
            required_fields=["zones_geometry_file_path"],
            owner_name="RoadLaneNumberModifier",
        )
        super().__init__({"parameters": parameters})

    def get(self):
        """Return the plain values consumed by the graph modification script."""
        self._check_parameters_are_resolved()
        return {
            "modifier_type": self.modifier_type,
            "lane_delta": self.parameters.lane_delta,
            "zones_geometry_file_path": str(
                self.parameters.zones_geometry_file_path
            ),
        }


class NewRoadModifier(SpeedModifier):

    def __init__(
        self,
        zones_geometry_file_path: VariablePath | None = None,
        max_speed: VariableFloat | None = None,
        capacity: VariableFloat | None = None,
        alpha: VariableFloat | None = None,
        beta: VariableFloat | None = None,
        parameters: NewRoadModifierParameters | None = None,
    ):
        """Create a road from a line geometry.

        Args:
            zones_geometry_file_path: GIS file containing the new road line.
            max_speed: Maximum speed of the new road, in km/h.
            capacity: Capacity of the new road, in vehicles per hour.
            alpha: Alpha parameter of the volume-delay function.
            beta: Beta parameter of the volume-delay function.
            parameters: Optional pre-built parameter model.
        """
        self.modifier_type = "new_road"
        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=NewRoadModifierParameters,
            explicit_args={
                "zones_geometry_file_path": zones_geometry_file_path,
                "max_speed": max_speed,
                "capacity": capacity,
                "alpha": alpha,
                "beta": beta,
            },
            required_fields=["zones_geometry_file_path"],
            owner_name="NewRoadModifier",
        )
        super().__init__({"parameters": parameters})

    def get(self):
        """Return the plain values consumed by the graph modification script."""
        self._check_parameters_are_resolved()
        return {
            "modifier_type": self.modifier_type,
            "capacity": self.parameters.capacity,
            "alpha": self.parameters.alpha,
            "beta": self.parameters.beta,
            "max_speed": self.parameters.max_speed,
            "zones_geometry_file_path": str(
                self.parameters.zones_geometry_file_path
            ),
        }
