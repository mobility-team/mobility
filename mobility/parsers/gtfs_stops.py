import os
import pathlib
import logging
import pandas as pd
import geopandas as gpd
import zipfile

from mobility import TransportZones
from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file

class GTFSStops(FileAsset):
    """
    Class to get GTFS stops for the study territory.
    Currently covers France and Switzerland using two national datasets.
    """
    
    def __init__(self, admin_prefixes: list):
        
        inputs = {"admin_prefixes": admin_prefixes}
        
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs/gtfs_stops.gpkg"
        
        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self, bbox=None) -> pd.DataFrame:

        logging.info("GTFS stops already prepared. Reusing the file : " + str(self.cache_path))
        
        gtfs_stops = gpd.read_file(self.cache_path, bbox=bbox)

        return gtfs_stops
    
    
    def create_and_get_asset(self, bbox=None) -> pd.DataFrame:
        
        logging.info("Preparing GTFS stops.")
    
        gtfs_stops = []
        
        for prefix in self.inputs["admin_prefixes"]:
            if prefix == "fr":
                gtfs_stops.append(self.get_french_gtfs_stops())
            elif prefix == "ch":
                gtfs_stops.append(self.get_swiss_gtfs_stops())
        
        gtfs_stops = pd.concat(gtfs_stops)
        gtfs_stops.to_file(self.cache_path)
        
        if bbox is not None:
            gtfs_stops = gtfs_stops.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]

        return gtfs_stops
    
    
    def get_french_gtfs_stops(self):
        
        logging.info("Preparing french GTFS stops...")
        
        url = "https://www.data.gouv.fr/fr/datasets/r/6200ccb0-4cdb-4b34-8d79-4ca70d0d729f"
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs"
        zip_path = data_folder / "gtfs-stops-france-export-2024-02-01.zip"
        path = data_folder / "gtfs-stops-france-export-2024-02-01.csv"
        
        download_file(url, zip_path)
        
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_folder)
            
        stops = pd.read_csv(path, usecols=["dataset_datagouv_id", "stop_lon", "stop_lat"])
        points = gpd.points_from_xy(stops['stop_lon'], stops['stop_lat'])
        stops = gpd.GeoDataFrame(stops, geometry=points)
        stops.crs = "EPSG:4326"
        stops.to_crs(3035, inplace=True)
        
        stops["country"] = "fr"
        stops["dataset_url"] = "https://transport.data.gouv.fr/api/datasets/" + stops["dataset_datagouv_id"]
        stops["resource_url"] = None
        
        stops = stops[["country", "dataset_url", "resource_url", "geometry"]]
        
        return stops
    
    
    def get_swiss_gtfs_stops(self):
        
        logging.info("Preparing swiss GTFS stops...")
        
        url = "https://gtfs.geops.ch/dl/complete/stops.txt"
        stops = pd.read_csv(url, usecols=["stop_id", "stop_lon", "stop_lat"], dtype={"stop_id": "str", "stop_lon": "float", "stop_lon": "float"})
        points = gpd.points_from_xy(stops['stop_lon'], stops['stop_lat'])
        stops = gpd.GeoDataFrame(stops, geometry=points)
        stops.crs = "EPSG:4326"
        stops.to_crs(3035, inplace=True)
        
        stops["country"] = "ch"
        stops["dataset_url"] = None
        stops["resource_url"] = "https://gtfs.geops.ch/dl/gtfs_complete.zip"
        
        stops = stops[["country", "dataset_url", "resource_url", "geometry"]]
        
        return stops
    
    
