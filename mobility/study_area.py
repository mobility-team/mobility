import os
import logging
import pandas as pd
import geopandas as gpd
import pathlib
import geojson
from typing import Union, List

from mobility.file_asset import FileAsset
from mobility.parsers.local_admin_units import LocalAdminUnits


class StudyArea(FileAsset):
    """
    A FileAsset class that manages study areas.
    
    Works for either a set of local admin units, or for a center commune + a radius (by default 40 km).
    
    The inputs are LocalAdminUnits, the local admin unit id and the radius:
    if the LocalAdminUnits asset, the list of local admin units or the centre commune+radius did not change, the class will reuse the existing asset.

    Attributes:
        local_admin_unit_id (str or list of str): The geographical code of the centre commune, or of all the local admin units to include.
        radius (int): Local admin units within this radius (in km) of the center commune will be included. Only used when local_admin_unit_id contains a single geographical code. Default is 40.

    Methods:
        get_cached_asset: Retrieve a cached transport zones GeoDataFrame.
        create_and_get_asset: Create and retrieve transport zones based on the current inputs.
        filter_within_radius: Filter local admin units within a specified radius.
        ids_to_countries: 
        create_study_area_boundary:
    """

    def __init__(self, local_admin_unit_id: Union[str, List[str]], radius: int = 40):
        inputs = {
            "local_admin_units": LocalAdminUnits(),
            "local_admin_unit_id": local_admin_unit_id,
            "radius": radius
        }

        cache_path = {
            "polygons": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "study_area.gpkg",
            "boundary": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "study_area_boundary.geojson"
        }

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> gpd.GeoDataFrame:
        """
        Retrieves the study area from the cache.

        Returns:
            gpd.GeoDataFrame: The cached study area.
        """
        
        if self.value is None:
            
            logging.info("Study area already created. Reusing the file " + str(self.cache_path))
            local_admin_units = gpd.read_file(self.cache_path["polygons"])
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

        local_admin_unit_id = self.inputs["local_admin_unit_id"]
        local_admin_units = self.inputs["local_admin_units"].get()

        if isinstance(local_admin_unit_id, str):
            
            local_admin_units = self.filter_within_radius(
                local_admin_units,
                local_admin_unit_id,
                self.inputs["radius"]
            )
            
        else:
            
            local_admin_units = local_admin_units[local_admin_units["local_admin_unit_id"].isin(local_admin_unit_id)]


        local_admin_units = local_admin_units[
            ["local_admin_unit_id", "local_admin_unit_name", "country",
             "urban_unit_category", "geometry"]
        ].copy()
        
        local_admin_units.to_file(self.cache_path["polygons"], driver="GPKG", index=False)

        self.create_study_area_boundary(local_admin_units)

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


    
    def ids_to_countries(self, local_admin_unit_ids):
        #Where is this used?
        local_admin_units = self.get()
        id_country_map = local_admin_units[["local_admin_unit_id", "country"]].set_index("local_admin_unit_id").to_dict()
        return pd.Series(local_admin_unit_ids).map(id_country_map)
    

        
    def create_study_area_boundary(
            self,
            study_area
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
        boundary = study_area.to_crs(4326).unary_union
        
        # Store the boundary as a temporary geojson file
        boundary_geojson = geojson.Feature(geometry=boundary, properties={})
        
        with open(self.cache_path["boundary"], "w") as f:
            geojson.dump(boundary_geojson, f)
            
        return None
    
        
        
        