import os
import pathlib
import json
import logging

from importlib import resources
from mobility.file_asset import FileAsset
from mobility.transport_zones import TransportZones
from mobility.r_utils.r_script import RScript

from mobility.parsers.download_file import download_file
from mobility.parsers.gtfs_stops import GTFSStops

from mobility.transport_modes.public_transport.gtfs_data import GTFSData

class GTFSRouter(FileAsset):
    
    def __init__(self, transport_zones: TransportZones, additional_gtfs_files: list = None):
        
        inputs = {
            "transport_zones": transport_zones,
            "additional_gtfs_files": additional_gtfs_files,
            "download_date": os.environ["MOBILITY_GTFS_DOWNLOAD_DATE"]
        }
        
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "gtfs_router.rds"

        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self):
        return self.cache_path
    
    def create_and_get_asset(self):
        
        logging.info("Downloading GTFS files for stops within the transport zones...")
        
        transport_zones = self.inputs["transport_zones"]
        
        stops = self.get_stops(transport_zones)

        gtfs_files = self.get_gtfs_files(stops)
        
        if self.inputs["additional_gtfs_files"] is not None:
            gtfs_files.extend(self.inputs["additional_gtfs_files"])
        
        self.prepare_gtfs_router(transport_zones, gtfs_files)

        return self.cache_path
    
        
    def get_stops(self, transport_zones):
        
        transport_zones = transport_zones.get()
        
        admin_prefixes = ["fr", "ch"]
        admin_prefixes = [prefix for prefix in admin_prefixes if transport_zones["local_admin_unit_id"].str.contains(prefix).any()]
        
        stops = GTFSStops(admin_prefixes)
        stops = stops.get(bbox=tuple(transport_zones.total_bounds))
        
        return stops
    
    
    def prepare_gtfs_router(self, transport_zones, gtfs_files):
        
        gtfs_files = ",".join(gtfs_files)
        
        script = RScript(resources.files('mobility.r_utils').joinpath('prepare_gtfs_router.R'))
        script.run(args=[str(transport_zones.cache_path), gtfs_files, str(self.cache_path)])
            
        return None
    
    
    def get_gtfs_files(self, stops):
        
        gtfs_urls = self.get_gtfs_urls(stops)
        gtfs_files = [GTFSData(gtfs_url).get() for gtfs_url in gtfs_urls]
        gtfs_files = [str(f[0]) for f in gtfs_files if f[1] == True]
            
        return gtfs_files
            
            
            
    def get_gtfs_urls(self, stops):
        
        gtfs_urls = []
        
        # Add resource urls that are already known (for Switzerland for example)
        gtfs_urls.extend(stops["resource_url"].dropna().unique().tolist())
        
        # Add transport.data.gouv.fr resource urls by matching their datagouv_id in the global metadata file
        datagouv_dataset_urls = stops["dataset_url"].dropna().unique()
        datagouv_dataset_ids = [pathlib.Path(url).name for url in datagouv_dataset_urls]
        
        url = "https://transport.data.gouv.fr/api/datasets"
        path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs/gtfs_metadata.json"
        download_file(url, path)
            
        with open(path, "r", encoding="UTF-8") as f:
            metadata = json.load(f)
            
        for dataset_metadata in metadata:
            if dataset_metadata["datagouv_id"] in datagouv_dataset_ids:
                gtfs_resources = [r for r in dataset_metadata["resources"] if "format" in r.keys()]
                gtfs_resources = [r for r in gtfs_resources if r["format"] == "GTFS"]
                for r in gtfs_resources:
                    gtfs_urls.append(r["original_url"])
                
        return gtfs_urls