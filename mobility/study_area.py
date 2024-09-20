import os
import logging
import pandas as pd
import geopandas as gpd
import pathlib
from typing import Union, List

from mobility.asset import Asset
from mobility.parsers.local_admin_units import LocalAdminUnits


class StudyArea(Asset):
    """
    A class for managing transport zones, inheriting from the Asset class.

    This class is responsible for creating, caching, and retrieving transport
    zones based on specified criteria such as city ID, method, and radius.

    Attributes:
        insee_city_id (str): The INSEE code of the city.
        radius (int): The radius around the city to define transport zones, applicable if method is 'radius'.

    Methods:
        get_cached_asset: Retrieve a cached transport zones GeoDataFrame.
        create_and_get_asset: Create and retrieve transport zones based on the current inputs.
        filter_cities_within_radius: Filter cities within a specified radius.
        prepare_transport_zones_df: Prepare the transport zones GeoDataFrame.
    """

    def __init__(self, local_admin_unit_id: Union[str, List[str]], radius: int = 40):
        """
        Initializes a TransportZones object based on a list of local admin unit 
        ids or one local admin unit id and a radius within which all local admin 
        unit ids should be included.

        Args:
            local_admin_unit_id (str or list): id or ids of the local admin unit(s).
            radius (int, optional): Radius in kilometers (defaults to 40 km).
        """

        inputs = {
            "local_admin_units": LocalAdminUnits(),
            "local_admin_unit_id": local_admin_unit_id,
            "radius": radius
        }

        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "study_area.gpkg"

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> gpd.GeoDataFrame:
        """
        Retrieves the transport zones from the cache.

        Returns:
            gpd.GeoDataFrame: The cached transport zones.
        """
        
        if self.value is None:
            
            logging.info("Transport zones already created. Reusing the file " + str(self.cache_path))
            local_admin_units = gpd.read_file(self.cache_path)
            self.value = local_admin_units
            return local_admin_units
        
        else:
            
            return self.value


    def create_and_get_asset(self) -> gpd.GeoDataFrame:
        """
        Creates transport zones based on the current inputs and retrieves them.

        Returns:
            gpd.GeoDataFrame: The newly created transport zones.
        """

        logging.info("Creating transport zones...")

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
        
        local_admin_units.to_file(self.cache_path, driver="GPKG", index=False)

        return local_admin_units


    def filter_within_radius(self, local_admin_units: gpd.GeoDataFrame, local_admin_unit_id: str, radius: int) -> gpd.GeoDataFrame:
        """
        Filters cities within a specified radius from a given city. It selects cities within the
        specified radius from the centroid of the target city.

        Args:
            cities (gpd.GeoDataFrame): The GeoDataFrame containing city data.
            insee_city_id (str): The INSEE code of the target city.
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
        local_admin_units = self.get()
        id_country_map = local_admin_units[["local_admin_unit_id", "country"]].set_index("local_admin_unit_id").to_dict()
        return pd.Series(local_admin_unit_ids).map(id_country_map)
        
        
        