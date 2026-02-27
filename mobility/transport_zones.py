"""Module providing transport zones for the desired study area in France and Switzerland."""
from __future__ import annotations

import os
import logging
import geopandas as gpd
import pathlib

from importlib import resources
from typing import Annotated, Literal, List, Union
from shapely.geometry import Point
from pydantic import BaseModel, ConfigDict, Field, model_validator

from mobility.file_asset import FileAsset
from mobility.study_area import StudyArea, StudyAreaParameters
from mobility.parsers.osm import OSMData
from mobility.r_utils.r_script import RScript

class TransportZones(FileAsset):
    """
    A FileAsset class for the management of transport zones.

    This class is responsible for creating, caching, and retrieving transport
    zones based on specified criteria such as city ID, method, and radius.

    The transport zone is based on either a list of local admin units ids
    or one local admin unit id and a radius within which all local admin unit ids should be included.

    It uses the R script 'prepare_transport_zones' to do so.

    Parameters
    ----------
    local_admin_unit_id : Union[str, List[str]]
        The geographical code of the centre local admin unit, or of all the local admin units to include. Examples:
            "fr-09122" (Foix)
            "ch-6621" (Genève)
            ["fr-09122", "fr-09121"", "fr-09130"", "fr-09273",  "fr-09329"] (set of adjacent communes)
    level_of_detail : Literal[0, 1], default=0
        If 0, uses the communal level.
        If 1, creates intra-communal transport zones to enable more precision in calculations. If there are more than 20 000 m² of building
        within the commune, one sub-zone is created for every 20 000 m². These buildings are then grouped using k-medoids to ensure consistent clusters.
        We use Voronoi constellations around the clusters centers to finally create these sub-communal transport zones.
            
    radius : float, default=40.0
        Local admin units within this radius (in km) of the center admin unit will be included.

    Methods
    -------
    get_cached_asset()
        Retrieve cached transport zones with the given inputs.
    create_and_get_asset()
        Create and retrieve transport zones with the given inputs.

    """

    def __init__(
            self,
            local_admin_unit_id: Union[str, List[str]] | None = None,
            level_of_detail: Literal[0, 1] | None = None,
            radius: float | None = None,
            inner_radius: float | None = None,
            inner_local_admin_unit_id: List[str] | None = None,
            cutout_geometries: gpd.GeoDataFrame = None,
            parameters: "TransportZonesParameters" | None = None,
        ):

        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=TransportZonesParameters,
            explicit_args={
                "local_admin_unit_id": local_admin_unit_id,
                "level_of_detail": level_of_detail,
                "radius": radius,
                "inner_radius": inner_radius,
                "inner_local_admin_unit_id": inner_local_admin_unit_id,
            },
            required_fields=["local_admin_unit_id"],
            owner_name="TransportZones",
        )

        study_area = StudyArea(
            parameters=StudyAreaParameters(
                local_admin_unit_id=parameters.local_admin_unit_id,
                radius=parameters.radius,
            ),
            cutout_geometries=cutout_geometries
        )
        
        osm_buildings = OSMData(
            study_area,
            object_type="a",
            key="building",
            geofabrik_extract_date="240101",
            split_local_admin_units=True
        )

        inputs = {
            "version": "2",
            "study_area": study_area,
            "osm_buildings": osm_buildings,
            "parameters": parameters,
            "cutout_geometries": cutout_geometries
        }

        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "transport_zones.gpkg"

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> gpd.GeoDataFrame:
        """
        Retrieve cached transport zones with the given inputs.

        Returns
        -------
        transport_zones : geopandas.geodataframe.GeoDataFrame
            Transport zones for the given local admin unit(s), radius, and level of detail.

        """        
        if self.value is None:
            
            logging.info("Transport zones already created. Reusing the file " + str(self.cache_path))
            transport_zones = gpd.read_file(self.cache_path)
            self.value = transport_zones
            return transport_zones
        
        else:
            
            return self.value


    def create_and_get_asset(self) -> gpd.GeoDataFrame:
        """
        Create and retrieve transport zones with the given inputs.
        
        It uses the R script 'prepare_transport_zones' to do so.

        Returns
        -------
        transport_zones : geopandas.geodataframe.GeoDataFrame
            Transport zones for the given local admin unit(s), radius, and level of detail.

        """
        logging.info("Creating transport zones...")
        
        study_area_fp = self.study_area.cache_path["polygons"]
        osm_buildings_fp = self.osm_buildings.get()
        
        script = RScript(resources.files('mobility.r_utils').joinpath('prepare_transport_zones.R'))
        script.run(
            args=[
                str(study_area_fp),
                str(osm_buildings_fp),
                str(self.inputs["parameters"].level_of_detail),
                str(self.cache_path)
            ]
        )
        
        transport_zones = gpd.read_file(self.cache_path)
        
        # Remove transport zones that are not adjacent to at least another one
        # (= filter "islands" that were selected but are not connected to the 
        # study area)
        transport_zones = self.remove_isolated_zones(transport_zones)
        
        # Set inner / outer flag
        local_admin_unit_id = self.inputs["parameters"].local_admin_unit_id
        inner_radius = self.inputs["parameters"].inner_radius
        inner_local_admin_unit_id = self.inputs["parameters"].inner_local_admin_unit_id
        
        transport_zones = self.flag_inner_zones(
            transport_zones,
            local_admin_unit_id,
            inner_radius,
            inner_local_admin_unit_id
        )
        
        # Cut the transport zones 
        transport_zones = self.apply_cutout(
            transport_zones,
            self.inputs["cutout_geometries"]
        )
        
        transport_zones.to_file(self.cache_path)
        
        return transport_zones
    
    
    def remove_isolated_zones(self, transport_zones):
        
        pairs = gpd.sjoin(
            transport_zones.reset_index(names="_i"),
            transport_zones.reset_index(names="_j"),
            how="inner",
            predicate="touches"
        )
        keep_ids = pairs.groupby("_i")["_j"].nunique().index
        transport_zones = transport_zones.loc[transport_zones.index.isin(keep_ids)].copy()
        
        return transport_zones


    def flag_inner_zones(
            self,
            transport_zones,
            local_admin_unit_id,
            inner_radius,
            inner_local_admin_unit_id
        ):
        
        if isinstance(local_admin_unit_id, str) and inner_radius is not None:
            
            lau_xy = transport_zones.loc[
                transport_zones["local_admin_unit_id"] == local_admin_unit_id,
                ["x", "y"]
            ]
            
            lau_xy = Point(lau_xy.iloc[0]["x"], lau_xy.iloc[0]["y"])
            inner_buffer = lau_xy.buffer(inner_radius*1000.0)
            
            transport_zones["is_inner_zone"] = transport_zones.intersects(inner_buffer)

        elif isinstance(local_admin_unit_id, list) and inner_local_admin_unit_id is not None:
            
            if isinstance(inner_local_admin_unit_id, str):
                inner_local_admin_unit_id = [inner_local_admin_unit_id]
            
            transport_zones["is_inner_zone"] = transport_zones["local_admin_unit_id"].isin(inner_local_admin_unit_id)
            
        else:    
        
            raise ValueError("Could not set the transport zones inner/outer flag from the provided inputs.")
            
        
            
        return transport_zones
    
    
    def apply_cutout(self, transport_zones, cutout_geometries):
        
        if cutout_geometries is not None:
            transport_zones = gpd.overlay(transport_zones, cutout_geometries, how="difference")
            
        return transport_zones


class TransportZonesParameters(BaseModel):

    model_config = ConfigDict(extra="forbid")

    local_admin_unit_id: Annotated[
        Union[str, list[str]],
        Field(
            title="Study area local admin unit ID(s)",
            description=(
                "Center local admin unit ID, or a list of local admin unit IDs "
                "to define the study area."
            ),
        ),
    ]

    radius: Annotated[
        float,
        Field(
            default=40.0,
            ge=5.0,
            le=100.0,
            title="Study area radius",
            description="Radius in km around the selected local admin unit.",
            json_schema_extra={"unit": "km"},
        ),
    ]

    level_of_detail: Annotated[
        Literal[0, 1],
        Field(
            default=0,
            title="Transport zones level of detail",
            description=(
                "Whether local admin units will be split into subzones "
                "(level of detail = 1), according to their building footprint density."
            ),
        ),
    ]

    inner_radius: Annotated[
        float | None,
        Field(
            default=None,
            title="Study area inner radius",
            description=(
                "Radius in km around the selected local admin unit,used to flag "
                "as local is_inner_zone. This can be used to filter out results "
                "from the border of the simulated study area, where the simulation "
                "will be less reliable."
            ),
            json_schema_extra={"unit": "km"},
        ),
    ]

    inner_local_admin_unit_id: Annotated[
        list[str] | None,
        Field(
            default=None,
            title="Inner local admin unit IDs",
            description=(
                "List of local admin unit IDs marked as inner zones. This can be "
                "used to filter out results from the border of the simulated "
                "study area, where the simulation will be less reliable."
            ),
        ),
    ]

    @model_validator(mode="after")
    def set_derived_defaults(self) -> "TransportZonesParameters":
        if self.inner_radius is None:
            self.inner_radius = self.radius

        if isinstance(self.local_admin_unit_id, list) and self.inner_local_admin_unit_id is None:
            self.inner_local_admin_unit_id = self.local_admin_unit_id

        return self
