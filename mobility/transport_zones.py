import os
import logging
import geopandas as gpd
import pathlib

from importlib import resources
from typing import Literal, List, Union

from mobility.file_asset import FileAsset
from mobility.study_area import StudyArea
from mobility.parsers.osm import OSMData
from mobility.r_utils.r_script import RScript

class TransportZones(FileAsset):
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

    def __init__(
            self,
            local_admin_unit_id: Union[str, List[str]],
            level_of_detail: Literal[0, 1] = 0,
            radius: int = 40
        ):
        """
        Initializes a TransportZones object based on a list of local admin unit 
        ids or one local admin unit id and a radius within which all local admin 
        unit ids should be included.

        Args:
            local_admin_unit_id (str or list): id or ids of the local admin unit(s).
            radius (int, optional): Radius in kilometers (defaults to 40 km).
        """
        
        study_area = StudyArea(local_admin_unit_id, radius)
        
        osm_buildings = OSMData(
            study_area,
            object_type="a",
            key="building",
            geofabrik_extract_date="240101",
            split_local_admin_units=True
        )

        inputs = {
            "study_area": study_area,
            "level_of_detail": level_of_detail,
            "osm_buildings": osm_buildings
        }

        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "transport_zones.gpkg"

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> gpd.GeoDataFrame:
        
        if self.value is None:
            
            logging.info("Transport zones already created. Reusing the file " + str(self.cache_path))
            transport_zones = gpd.read_file(self.cache_path)
            self.value = transport_zones
            return transport_zones
        
        else:
            
            return self.value


    def create_and_get_asset(self) -> gpd.GeoDataFrame:

        logging.info("Creating transport zones...")
        
        study_area_fp = self.study_area.cache_path["polygons"]
        osm_buildings_fp = self.osm_buildings.get()
        
        script = RScript(resources.files('mobility.r_utils').joinpath('prepare_transport_zones.R'))
        script.run(
            args=[
                study_area_fp,
                osm_buildings_fp,
                str(self.level_of_detail),
                self.cache_path
            ]
        )
        
        transport_zones = gpd.read_file(self.cache_path)
        
        return transport_zones


