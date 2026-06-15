from __future__ import annotations
import os
import logging
import geopandas as gpd
import pathlib
import geojson
from typing import Union, List

from pydantic import BaseModel, ConfigDict, Field
from typing import Annotated

from mobility.countries import normalize_country_codes
from mobility.runtime.assets.file_asset import FileAsset
from mobility.spatial.local_admin_units import LocalAdminUnits


class StudyArea(FileAsset):
    """
    A FileAsset class that manages study areas.
    
    Works for either a set of local admin units, or for a center commune + a radius (by default 40 km).
    
    The inputs are LocalAdminUnits, the local admin unit id and the radius:
    if the LocalAdminUnits asset, the list of local admin units or the centre commune+radius did not change, the class will reuse the existing asset.

    Attributes:
        local_admin_unit_id (str or list of str): The geographical code of the centre commune, or of all the local admin units to include.
        radius (float): Local admin units within this radius (in km) of the center commune will be included. Only used when local_admin_unit_id contains a single geographical code. Default is 40.

    Methods:
        get_cached_asset: Retrieve a cached transport zones GeoDataFrame.
        create_and_get_asset: Create and retrieve transport zones based on the current inputs.
        filter_within_radius: Filter local admin units within a specified radius.
        create_study_area_boundary:
    """

    def __init__(
            self,
            local_admin_unit_id: Union[str, List[str]] | None = None,
            radius: float | None = None,
            cutout_geometries: gpd.GeoDataFrame = None,
            parameters: "StudyAreaParameters" | None = None,
        ):

        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=StudyAreaParameters,
            explicit_args={
                "local_admin_unit_id": local_admin_unit_id,
                "radius": radius,
            },
            required_fields=["local_admin_unit_id"],
            owner_name="StudyArea",
        )

        if isinstance(parameters.local_admin_unit_id, list):
            local_admin_units = LocalAdminUnits(
                local_admin_unit_ids=parameters.local_admin_unit_id,
            )
        else:
            center_local_admin_unit = LocalAdminUnits(
                local_admin_unit_ids=[parameters.local_admin_unit_id],
            )
            local_admin_units = LocalAdminUnits(
                center_local_admin_unit=center_local_admin_unit,
                radius=parameters.radius,
            )

        inputs = {
            "version": "2",
            "local_admin_units": local_admin_units,
            "parameters": parameters,
            "cutout_geometries": cutout_geometries
        }

        cache_path = {
            "polygons": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "study_area.gpkg",
            "boundary": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "study_area_boundary.geojson"
        }

        super().__init__(inputs, cache_path)
        self._countries: list[str] | None = None

    def get_cached_asset(self) -> gpd.GeoDataFrame:
        """
        Retrieves the study area from the cache.

        Returns:
            gpd.GeoDataFrame: The cached study area.
        """
        
        if self.value is None:
            
            logging.info("Study area already created. Reusing the file " + str(self.cache_path))
            local_admin_units = gpd.read_file(self.cache_path["polygons"])
            self._countries = self._extract_countries(local_admin_units)
            self.value = local_admin_units
            return local_admin_units
        
        else:
            
            return self.value


    def create_and_get_asset(self) -> gpd.GeoDataFrame:
        """
        Creates study area based on the current inputs and retrieves it.
 
        Returns:
            gpd.GeoDataFrame: The newly created transport zones.
        """

        logging.info("Creating study area...")

        local_admin_unit_id = self.inputs["parameters"].local_admin_unit_id

        if isinstance(local_admin_unit_id, str):
            local_admin_units = self.inputs["local_admin_units"].get()
            local_admin_units = self.filter_within_radius(
                local_admin_units,
                local_admin_unit_id,
                self.inputs["parameters"].radius
            )
            
        else:
            local_admin_units = self.inputs["local_admin_units"].get()
            missing_ids = sorted(set(local_admin_unit_id) - set(local_admin_units["local_admin_unit_id"]))
            if missing_ids:
                raise ValueError(f"No local admin unit found for: {missing_ids}.")


        local_admin_units = local_admin_units[
            ["local_admin_unit_id", "local_admin_unit_name", "country",
             "urban_unit_category", "geometry"]
        ].copy()
        

        self.create_study_area_boundary(local_admin_units)
        
        local_admin_units = self.apply_cutout(
            local_admin_units,
            self.inputs["cutout_geometries"]
        )

        self._countries = self._extract_countries(local_admin_units)
        
        local_admin_units.to_file(self.cache_path["polygons"], driver="GPKG", index=False)
        
        return local_admin_units


    def filter_within_radius(self, local_admin_units: gpd.GeoDataFrame, local_admin_unit_id: str, radius: int) -> gpd.GeoDataFrame:
        """
        Filters local admin units within a specified radius from a given city. It selects local admin units within the
        specified radius from the centroid of the target local admin unit.

        Args:
            local_admin_units (gpd.GeoDataFrame): The GeoDataFrame containing city data.
            local_admin_unit_id (str): The geographic code of the target city.
            radius (int): The radius in kilometers around the target city.

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame of cities filtered within the specified radius.
        """

        local_admin_unit = local_admin_units[local_admin_units["local_admin_unit_id"] == local_admin_unit_id]
        if local_admin_unit.empty:
            raise ValueError(f"No local admin unit with code '{local_admin_unit_id}' found.")
        buffer = local_admin_unit.centroid.buffer(radius * 1000).iloc[0]
        local_admin_units = local_admin_units[local_admin_units.within(buffer)]

        return local_admin_units

    @property
    def countries(self) -> list[str]:
        """Return the country codes present in the study area."""
        if self._countries is None:
            self._countries = self._extract_countries(self.get())
        return self._countries

    def create_study_area_boundary(
            self,
            study_area: gpd.GeoDataFrame
        ):
        """
        Creates a combined boundary polygon for all transport zones and saves it as a GeoJSON file.

        This method merges the geometries of all provided transport zones into a single polygon, 
        which represents the combined boundary of these zones. It then saves this boundary as a 
        GeoJSON file to be used in subsequent operations.

        Args:
            study_area (gpd.GeoDataFrame): A GeoDataFrame containing the geometries of the study area.

        Returns:
            Returns nothing but saves the GeoJSON in the project folder as "study_area_boundary.geojson"
        """
        
        # Merge all transport zones into one polygon
        boundary = study_area.to_crs(4326).geometry.union_all()
        
        # Store the boundary as a geojson file
        boundary_geojson = geojson.Feature(geometry=boundary, properties={})
        
        with open(self.cache_path["boundary"], "w") as f:
            geojson.dump(boundary_geojson, f)
        
        return None
    
    
        
    def apply_cutout(self, study_area, cutout_geometries):
        
        if cutout_geometries is not None:
            study_area = gpd.overlay(study_area, cutout_geometries, how="difference")
            
        return study_area

    @staticmethod
    def _extract_countries(study_area: gpd.GeoDataFrame) -> list[str]:
        if "country" not in study_area.columns:
            raise ValueError("Study area should contain a `country` column.")
        countries = normalize_country_codes(study_area["country"].tolist())
        if not countries:
            raise ValueError("Study area should contain at least one country.")
        return countries
    
        
class StudyAreaParameters(BaseModel):

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
            ge=0.0,
            le=100.0,
            title="Study area radius",
            description="Radius in km around the selected local admin unit.",
            json_schema_extra={
                "unit": "km"
            },
        ),
    ]
