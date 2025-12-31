import os
import pathlib
import logging
import zipfile
import hashlib

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file, clean_path

class GTFSData(FileAsset):
    """
    Simple FileAsset to store GTFS files.
    
    Checks that the GTFS zip is >1ko and contains an agency.txt file.
    """
    
    def __init__(self, url: str):
        
        path = clean_path(url)
        name = hashlib.md5(url.encode('utf-8')).hexdigest() + "_" + path.name
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs" / name
        
        if cache_path.suffix != ".zip":
            cache_path = cache_path.with_suffix('.zip')
            
        self.name = name 
            
        inputs = {
            "url": url,
            "download_date": os.environ["MOBILITY_GTFS_DOWNLOAD_DATE"]
        }

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self):
        
        file_ok = self.is_gtfs_file_ok(self.cache_path)
        
        return [self.cache_path, file_ok]
    
    def create_and_get_asset(self):
        
        download_file(
            self.url,
            self.cache_path
        )
        
        file_ok = self.is_gtfs_file_ok(self.cache_path)
        
        return [self.cache_path, file_ok]

    
    def is_gtfs_file_ok(self, path):
        
        if os.path.exists(path) is False:
            return False
        
        if os.path.getsize(path) < 1024:
            logging.info("Downloaded file size is inferior to 1 ko, it will not be used by mobility.")
            return False
        
        if "e8f2aceaaaa2493f6041dc7f0251f325-5d7ae44c16ad373ca1afbc4590f53256_gtfs-2015-chamonix-mobilit" in path.name:
            logging.info("Manual exception, GTFS not used from path", path)
            return False
        if "datasud" in path.name:
            logging.info("Manual exception, GTFS not used from path", path)
            return False
        if "4a590cb87669fe2bc39328652ef1d2e9_gtfs_generic_eu" in path.name:
            logging.info(f"Manual exception, Flixbus GTFS not used from path {path.name}")
            return False

        
        try:
            with zipfile.ZipFile(path, 'r') as zip_ref:
                zip_contents = zip_ref.namelist()
            has_an_agency = "agency.txt" in zip_contents
            if has_an_agency:
                logging.debug("Downloaded file is a proper GTFS zip which contains an agency file.")
            else:
                logging.info("Downloaded file is a proper GTFS zip but does not contain an agency file, it will not be used by Mobility.")
            return True
        except:
            logging.info("Downloaded file is not a regular GTFS zip file, it will not be used by Mobility.")
            return False
    
    
    def get_agencies_names(self, path):
                
        with zipfile.ZipFile(path, 'r') as gtfs_folder:
            with gtfs_folder.open("agency.txt") as agency:
                agencies = agency.read().decode('utf-8')
                logging.info(agencies)
                return agencies


        
    
    
        
        
        
