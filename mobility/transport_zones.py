"""Module providing transport zones for the desired study area in France and Switzerland."""

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
            
    radius : int, default=40
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
            local_admin_unit_id: Union[str, List[str]],
            level_of_detail: Literal[0, 1] = 0,
            radius: int = 40
        ):
        
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
                study_area_fp,
                osm_buildings_fp,
                str(self.level_of_detail),
                self.cache_path
            ]
        )
        
        transport_zones = gpd.read_file(self.cache_path)
        
        return transport_zones


