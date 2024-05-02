import os
import pathlib
import json
import requests
import logging
import zipfile
import pandas as pd
import geopandas as gpd

from importlib import resources
from mobility.asset import Asset
from mobility.transport_zones import TransportZones
from mobility.r_script import RScript

from mobility.parsers.download_file import download_file

class GTFS(Asset):
    
    def __init__(self, transport_zones: TransportZones):
        
        inputs = {"transport_zones": transport_zones}
        
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "gtfs_router.rds"

        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self):
        return self.cache_path
    
    def create_and_get_asset(self):
        
        logging.info("Downloading GTFS files for stops within the transport zones...")
        
        transport_zones = self.inputs["transport_zones"]
        
        stops = self.get_stops(transport_zones)
        gtfs_files = self.download_gtfs_files(stops)
        gtfs_router = self.prepare_gtfs_router(transport_zones, gtfs_files)

        return gtfs_router
    
        
    def get_stops(self, transport_zones):
        
        transport_zones = transport_zones.get()
        
        path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs/all_gtfs_stops.gpkg"
        
        if path.exists() is False:
        
            url = "https://www.data.gouv.fr/fr/datasets/r/69cf54c6-6591-4920-b1d6-2a5292964606"
            stops_data_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs/raw_stops.csv"
            download_file(url, stops_data_path)
                
            stops = pd.read_csv(stops_data_path)
            points = gpd.points_from_xy(stops['stop_lon'], stops['stop_lat'])
            stops = gpd.GeoDataFrame(stops, geometry=points)
            stops.crs = "EPSG:4326"
            stops.to_crs(2154, inplace=True)
            
            logging.info("Linking GTFS stops to their resource on transport.data.gouv.fr, this is going to take a while (but is needed only once)...")
            
            def get_redirect_url(dataset_id):
                logging.info("Fetching transport.data.gouv.fr url for dataset : " + str(dataset_id))
                response = requests.get("https://transport.data.gouv.fr/datasets/" + str(dataset_id), allow_redirects=True)
                return response.url
                
            dataset_ids = list(stops["dataset_id"].unique())
            dataset_urls = [{"dataset_id": dataset_id, "page_url": get_redirect_url(dataset_id)} for dataset_id in dataset_ids]
            dataset_urls = pd.DataFrame.from_dict(dataset_urls)
            
            stops = pd.merge(stops, dataset_urls, on="dataset_id")
            
            stops = stops[["page_url", "geometry"]]
            
            stops.to_file(path)
            
        else:
            
            bbox = tuple(transport_zones.total_bounds)
            stops = gpd.read_file(path, bbox=bbox)
            
            
        stops = gpd.sjoin(stops, transport_zones, how="inner", op='within')
        
        return stops
    
    
    def prepare_gtfs_router(self, transport_zones, gtfs_files):
        
        gtfs_files = ",".join(gtfs_files)
        
        script = RScript(resources.files('mobility.R').joinpath('prepare_gtfs_router.R'))
        script.run(args=[str(transport_zones.cache_path), gtfs_files, str(self.cache_path)])
            
        return self.cache_path
    
    
    def download_gtfs_files(self, stops):
        
        gtfs_urls = self.get_gtfs_urls()
        
        stops = pd.merge(stops, gtfs_urls, on="page_url")
        
        resources = stops[["gtfs_url", "gtfs_title", "gtfs_datagouv_id"]].drop_duplicates().to_dict(orient="records")
        
        # Download the GTFS files and save the list of paths to the downloaded files
        # in the json file that will be reused once this function has run
        gtfs_files = []
        
        for resource in resources:
            filename = resource["gtfs_datagouv_id"] + "_" + resource["gtfs_title"]
            path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs" / filename
            if path.suffix != ".zip":
                path = path.with_suffix('.zip')
            download_file(
                resource["gtfs_url"],
                path
            )
            
            if os.path.getsize(path) < 1024:
                
                logging.info("Downloaded file size is inferior to 1 ko, it will not be used by mobility.")
                
            else:
            
                # Check if the downloaded file is a regular GTFS zip file
                try:
                    with zipfile.ZipFile(path, 'r') as zip_ref:
                        zip_contents = zip_ref.namelist()              
                    if "agency.txt" in zip_contents:
                        gtfs_files.append(str(path))
                        
                except:
                    logging.info("Downloaded file is not a regular GTFS zip file, it will not be used by mobility.")
            
        return gtfs_files
            
            
            
    def get_gtfs_urls(self):
        
        url = "https://transport.data.gouv.fr/api/datasets"
        path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs/gtfs_metadata.json"
        download_file(url, path)
            
        with open(path, "r", encoding="UTF-8") as f:
            metadata = json.load(f)
            
        gtfs_urls = []
            
        for dataset_metadata in metadata:
            print("Metadata")
            print(dataset_metadata["resources"])
            gtfs_resources = [r for r in dataset_metadata["resources"] if r["format"] == "GTFS"]
            for r in gtfs_resources:
                gtfs_urls.append({
                    "title": dataset_metadata["title"],
                    "page_url": dataset_metadata["page_url"],
                    "gtfs_datagouv_id": r["datagouv_id"],
                    "gtfs_url": r["original_url"],
                    "gtfs_title": r["title"]
                })
                
                
        gtfs_urls = pd.DataFrame.from_dict(gtfs_urls)
        
        return gtfs_urls
            
    
